//using Confluent.Kafka;
//using Polly;
//using Polly.Retry;
//using System.Collections.Concurrent;
//using System.Threading.Channels;

//namespace WebApplication1
//{
    
//    public class ProductionKafkaWorker2 : BackgroundService
//    {
//        private static readonly TimeSpan ConsumeTimeout = TimeSpan.FromMilliseconds(100);
//        private static readonly TimeSpan CommitInterval = TimeSpan.FromMilliseconds(100);
//        private const int CommitBatchSize = 1000;

//        private readonly IConsumer<string, string> _consumer;
//        private readonly IProducer<string, string> _dlqProducer;
//        private readonly ILogger<ProductionKafkaWorker> _logger;

//        private readonly ConcurrentDictionary<Partition, PartitionContext> _activePartitions = new();

//        private readonly Channel<TopicPartitionOffset> _commitChannel =
//            Channel.CreateBounded<TopicPartitionOffset>(10000);

//        private readonly string _sourceTopic = "test-topic";
//        private readonly string _dlqTopic = "test-topic-dlq";

//        private readonly AsyncRetryPolicy _retryPolicy;
//        private readonly Dictionary<TopicPartition, Offset> _pendingOffsets = new();
//        private int _pendingCommitCount;

//        public ProductionKafkaWorker2(ILogger<ProductionKafkaWorker> logger, IConfiguration config)
//        {
//            _logger = logger;

//            var consumerConfig = new ConsumerConfig
//            {
//                BootstrapServers = config["Kafka:BootstrapServers"],
//                GroupId = "business-group-1",
//                EnableAutoCommit = false,
//                AutoOffsetReset = AutoOffsetReset.Earliest
//            };

//            var producerConfig = new ProducerConfig
//            {
//                BootstrapServers = config["Kafka:BootstrapServers"]
//            };

//            _consumer = new ConsumerBuilder<string, string>(consumerConfig)
//                .SetPartitionsAssignedHandler((c, partitions) =>
//                {
//                    foreach (var p in partitions)
//                        StartPartitionWorker(p.Partition);
//                })
//                .SetPartitionsRevokedHandler((c, partitions) =>
//                {
//                    try
//                    {
//                        StopPartitionWorkers(partitions.Select(p => p.Partition));
//                        DrainCommitChannel();
//                        CommitPendingOffsets(partitions.Select(p => p.TopicPartition));
//                    }
//                    catch (Exception ex)
//                    {
//                        _logger.LogError(ex,"撤销分区");
//                    }
//                })
//                .Build();

//            _dlqProducer = new ProducerBuilder<string, string>(producerConfig).Build();

//            _retryPolicy = Policy
//                .Handle<Exception>(ex => ex is not OperationCanceledException)
//                .WaitAndRetryAsync(3,
//                    retry => TimeSpan.FromMilliseconds(200 * Math.Pow(2, retry)));
//        }

//        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
//        {
//            _consumer.Subscribe(_sourceTopic);
//            var lastCommitTime = DateTime.UtcNow;

//            try
//            {
//                while (!stoppingToken.IsCancellationRequested)
//                {
//                    var result = _consumer.Consume(ConsumeTimeout);
//                    if (result != null && _activePartitions.TryGetValue(result.Partition, out var ctx))
//                    {
//                        await ctx.Channel.Writer.WriteAsync(result, stoppingToken);
//                    }

//                    DrainCommitChannel();

//                    if (ShouldCommit(lastCommitTime))
//                    {
//                        CommitPendingOffsets();
//                        lastCommitTime = DateTime.UtcNow;
//                    }
//                }
//            }
//            catch (OperationCanceledException) { }
//            finally
//            {
//                StopPartitionWorkers(_activePartitions.Keys.ToArray());
//                DrainCommitChannel();
//                CommitPendingOffsets();
//                _dlqProducer.Flush(TimeSpan.FromSeconds(10));
//                _consumer.Close();
//            }
//        }

//        private void StartPartitionWorker(Partition partition)
//        {
//            var ctx = new PartitionContext
//            {
//                Partition = partition,
//                Channel = Channel.CreateBounded<ConsumeResult<string, string>>(
//                    new BoundedChannelOptions(1000)
//                    {
//                        FullMode = BoundedChannelFullMode.Wait,
//                        SingleReader = true
//                    }),
//                CommitChannel = _commitChannel,
//                Cts = new CancellationTokenSource()
//            };

//            ctx.ProcessingTask = Task.Run(() => ProcessPartitionLoop(ctx));
//            _activePartitions.TryAdd(partition, ctx);
//        }

//        private async Task ProcessPartitionLoop(PartitionContext ctx)
//        {
//            var reader = ctx.Channel.Reader;
//            var batch = new List<ConsumeResult<string, string>>(500);
//            long lastProcessTime = Environment.TickCount64;

//            try
//            {
//                while (true)
//                {
//                    // 1. 尝试非阻塞读取，一次性排空当前已到位的消息
//                    while (batch.Count < 500 && reader.TryRead(out var msg))
//                    {
//                        batch.Add(msg);
//                    }

//                    // 2. 检查是否需要处理（满500或超时）
//                    if (batch.Count >= 500 || (batch.Any() && (Environment.TickCount64 - lastProcessTime) > 100))
//                    {
//                        await ProcessBatch(ctx, batch);
//                        lastProcessTime = Environment.TickCount64;
//                    }

//                    // 3. 如果 batch 还没满且没超时，说明现在 Channel 里空了，进行异步等待
//                    if (batch.Count < 500)
//                    {
//                        try
//                        {
//                            // 只有在真的没数据时，才创建一次带超时的等待
//                            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
//                            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ctx.Cts.Token, cts.Token);

//                            if (await reader.WaitToReadAsync(linkedCts.Token))
//                            {
//                                // 有新消息到了，进入下一轮 while(TryRead)
//                                continue;
//                            }
//                        }
//                        catch (OperationCanceledException) { /* 仅仅是超时或撤销 */ }
//                    }

//                    if (reader.Completion.IsCompleted)
//                    {
//                        if (batch.Any())
//                        {
//                            await ProcessBatch(ctx, batch);
//                        }

//                        break;
//                    }
//                }
//            }
//            catch (Exception ex) { _logger.LogCritical(ex, $"分区 {ctx.Partition} 崩溃"); }
//        }

//        private async Task ProcessBatch(PartitionContext ctx, List<ConsumeResult<string, string>> batch)
//        {
//            ConsumeResult<string, string> lastSuccessfulMsg = null;
//            foreach (var msg in batch)
//            {
//                try
//                {
//                    await _retryPolicy.ExecuteAsync(async () =>
//                    {
//                        await ProcessMessage(msg);
//                    });
//                    lastSuccessfulMsg = msg; // 记录最后一条成功
//                }
//                catch (Exception ex)
//                {
//                    if (!await TrySendToDlq(msg, ex.Message))
//                    {
//                        throw new InvalidOperationException($"消息进入DLQ失败，停止处理分区 {ctx.Partition}，offset={msg.Offset}", ex);
//                    }

//                    lastSuccessfulMsg = msg; // 进入 DLQ 也算该位置处理完成
//                }
//            }
//            if (lastSuccessfulMsg != null)
//            {
//                // 只上报这一个位置即可
//                await ctx.CommitChannel.Writer.WriteAsync(
//                    new TopicPartitionOffset(lastSuccessfulMsg.TopicPartition, lastSuccessfulMsg.Offset + 1));
//            }
//            batch.Clear();
//        }

//        private bool ShouldCommit(DateTime lastCommitTime)
//        {
//            return _pendingOffsets.Any()
//                && (_pendingCommitCount >= CommitBatchSize || DateTime.UtcNow - lastCommitTime >= CommitInterval);
//        }

//        private void DrainCommitChannel()
//        {
//            while (_commitChannel.Reader.TryRead(out var tpo))
//            {
//                if (!_pendingOffsets.TryGetValue(tpo.TopicPartition, out var existing) || tpo.Offset > existing)
//                {
//                    _pendingOffsets[tpo.TopicPartition] = tpo.Offset;
//                }

//                _pendingCommitCount++;
//            }
//        }

//        private void CommitPendingOffsets(IEnumerable<TopicPartition>? topicPartitions = null)
//        {
//            List<TopicPartitionOffset> offsetsToCommit;

//            if (topicPartitions == null)
//            {
//                offsetsToCommit = _pendingOffsets
//                    .Select(kv => new TopicPartitionOffset(kv.Key, kv.Value))
//                    .ToList();
//            }
//            else
//            {
//                var topicPartitionSet = topicPartitions.ToHashSet();
//                offsetsToCommit = _pendingOffsets
//                    .Where(kv => topicPartitionSet.Contains(kv.Key))
//                    .Select(kv => new TopicPartitionOffset(kv.Key, kv.Value))
//                    .ToList();
//            }

//            if (offsetsToCommit.Count == 0)
//            {
//                return;
//            }

//            try
//            {
//                _consumer.Commit(offsetsToCommit);

//                foreach (var offset in offsetsToCommit)
//                {
//                    _pendingOffsets.Remove(offset.TopicPartition);
//                }

//                if (!_pendingOffsets.Any())
//                {
//                    _pendingCommitCount = 0;
//                }
//            }
//            catch (Exception ex)
//            {
//                _logger.LogError(ex, "Commit失败");
//            }
//        }

//        private async Task<bool> TrySendToDlq(ConsumeResult<string, string> msg, string error)
//        {
//            try
//            {
//                var dlqMessage = new Message<string, string>
//                {
//                    Key = msg.Message.Key,
//                    Value = msg.Message.Value,
//                    Headers = new Headers
//                {
//                    { "error", System.Text.Encoding.UTF8.GetBytes(error) }
//                }
//                };

//                await _dlqProducer.ProduceAsync(_dlqTopic, dlqMessage);
//                return true;
//            }
//            catch (Exception ex)
//            {
//                _logger.LogCritical(ex, "DLQ失败，数据可能丢失");
//                return false;
//            }
//        }

//        private async Task ProcessMessage(ConsumeResult<string, string> msg)
//        {
//            // ✅ 建议：这里做幂等控制（数据库唯一键 / Redis去重）

//            _logger.LogInformation($"处理消息: {msg.Message.Value}");

//            await Task.CompletedTask;
//        }

//        private void StopPartitionWorkers(IEnumerable<Partition> partitions)
//        {
//            foreach (var p in partitions)
//            {
//                if (_activePartitions.TryRemove(p, out var ctx))
//                {
//                    ctx.Channel.Writer.Complete();

//                    try
//                    {
//                        if (!ctx.ProcessingTask.Wait(TimeSpan.FromSeconds(10)))
//                        {
//                            ctx.Cts.Cancel();
//                            ctx.ProcessingTask.Wait(TimeSpan.FromSeconds(5));
//                        }
//                    }
//                    catch { }
//                }
//            }
//        }
//    }
//}

