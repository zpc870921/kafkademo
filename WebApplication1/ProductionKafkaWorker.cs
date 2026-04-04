using Confluent.Kafka;
using Polly;
using Polly.Retry;
using System.Collections.Concurrent;
using System.Threading.Channels;

namespace WebApplication1
{
    public class PartitionContext
    {
        public Partition Partition { get; set; }
        public Channel<ConsumeResult<string, string>> Channel { get; set; }
        public Task ProcessingTask { get; set; }
        public CancellationTokenSource Cts { get; set; }
        public Channel<TopicPartitionOffset> CommitChannel { get; set; }
    }
    public class ProductionKafkaWorker : BackgroundService
    {
        private readonly IConsumer<string, string> _consumer;
        private readonly IProducer<string, string> _dlqProducer;
        private readonly ILogger<ProductionKafkaWorker> _logger;

        private readonly ConcurrentDictionary<Partition, PartitionContext> _activePartitions = new();

        private readonly Channel<TopicPartitionOffset> _commitChannel =
            Channel.CreateBounded<TopicPartitionOffset>(10000);

        private readonly string _sourceTopic = "test-topic";
        private readonly string _dlqTopic = "test-topic-dlq";

        private readonly AsyncRetryPolicy _retryPolicy;

        public ProductionKafkaWorker(ILogger<ProductionKafkaWorker> logger, IConfiguration config)
        {
            _logger = logger;

            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = config["Kafka:BootstrapServers"],
                GroupId = "business-group-1",
                EnableAutoCommit = false,
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            var producerConfig = new ProducerConfig
            {
                BootstrapServers = config["Kafka:BootstrapServers"]
            };

            _consumer = new ConsumerBuilder<string, string>(consumerConfig)
                .SetPartitionsAssignedHandler((c, partitions) =>
                {
                    foreach (var p in partitions)
                        StartPartitionWorker(p.Partition);
                })
                .SetPartitionsRevokedHandler((c, partitions) =>
                {
                    try
                    {
                        StopPartitionWorkers(partitions.Select(p => p.Partition));
                        c.Commit();
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex,"撤销分区");
                    }
                })
                .Build();

            _dlqProducer = new ProducerBuilder<string, string>(producerConfig).Build();

            _retryPolicy = Policy
                .Handle<Exception>(ex => ex is not OperationCanceledException)
                .WaitAndRetryAsync(3,
                    retry => TimeSpan.FromMilliseconds(200 * Math.Pow(2, retry)));
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _consumer.Subscribe(_sourceTopic);

            var commitTask = Task.Run(() => CommitLoop(stoppingToken), stoppingToken);

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    var result = _consumer.Consume(stoppingToken);
                    if (result == null) continue;

                    if (_activePartitions.TryGetValue(result.Partition, out var ctx))
                    {
                        await ctx.Channel.Writer.WriteAsync(result, stoppingToken);
                    }
                }
            }
            catch (OperationCanceledException) { }
            finally
            {
                _consumer.Close();
                _commitChannel.Writer.Complete();
                await commitTask;
            }
        }

        private void StartPartitionWorker(Partition partition)
        {
            var ctx = new PartitionContext
            {
                Partition = partition,
                Channel = Channel.CreateBounded<ConsumeResult<string, string>>(
                    new BoundedChannelOptions(1000)
                    {
                        FullMode = BoundedChannelFullMode.Wait,
                        SingleReader = true
                    }),
                CommitChannel = _commitChannel,
                Cts = new CancellationTokenSource()
            };

            ctx.ProcessingTask = Task.Run(() => ProcessPartitionLoop(ctx));
            _activePartitions.TryAdd(partition, ctx);
        }

        private async Task ProcessPartitionLoop(PartitionContext ctx)
        {
            var reader = ctx.Channel.Reader;
            var batch = new List<ConsumeResult<string, string>>(500);
            long lastProcessTime = Environment.TickCount64;

            try
            {
                while (!ctx.Cts.Token.IsCancellationRequested)
                {
                    // 1. 尝试非阻塞读取，一次性排空当前已到位的消息
                    while (batch.Count < 500 && reader.TryRead(out var msg))
                    {
                        batch.Add(msg);
                    }

                    // 2. 检查是否需要处理（满500或超时）
                    if (batch.Count >= 500 || (batch.Any() && (Environment.TickCount64 - lastProcessTime) > 100))
                    {
                        await ProcessBatch(ctx, batch);
                        lastProcessTime = Environment.TickCount64;
                    }

                    // 3. 如果 batch 还没满且没超时，说明现在 Channel 里空了，进行异步等待
                    if (batch.Count < 500)
                    {
                        try
                        {
                            // 只有在真的没数据时，才创建一次带超时的等待
                            using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
                            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ctx.Cts.Token, cts.Token);

                            if (await reader.WaitToReadAsync(linkedCts.Token))
                            {
                                // 有新消息到了，进入下一轮 while(TryRead)
                                continue;
                            }
                        }
                        catch (OperationCanceledException) { /* 仅仅是超时或撤销 */ }
                    }

                    if (reader.Completion.IsCompleted && !batch.Any()) break;
                }
            }
            catch (Exception ex) { _logger.LogCritical(ex, $"分区 {ctx.Partition} 崩溃"); }
        }

        private async Task ProcessBatch(PartitionContext ctx, List<ConsumeResult<string, string>> batch)
        {
            ConsumeResult<string, string> lastSuccessfulMsg = null;
            foreach (var msg in batch)
            {
                try
                {
                    await _retryPolicy.ExecuteAsync(async () =>
                    {
                        await ProcessMessage(msg);
                    });
                    lastSuccessfulMsg = msg; // 记录最后一条成功
                }
                catch (Exception ex)
                {
                    await SendToDlq(msg, ex.Message);
                    lastSuccessfulMsg = msg; // 进入 DLQ 也算该位置处理完成
                }
            }
            if (lastSuccessfulMsg != null)
            {
                // 只上报这一个位置即可
                await ctx.CommitChannel.Writer.WriteAsync(
                    new TopicPartitionOffset(lastSuccessfulMsg.TopicPartition, lastSuccessfulMsg.Offset + 1));
            }
            batch.Clear();
        }

        private async Task CommitLoop(CancellationToken ct)
        {
            var reader = _commitChannel.Reader;

            var offsetMap = new Dictionary<TopicPartition, Offset>();
            //var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(100));

            try
            {
                //while (!ct.IsCancellationRequested)
                while (await reader.WaitToReadAsync(ct))
                {
                    // 1️⃣ 尝试读取
                    while (reader.TryRead(out var tpo))
                    {
                        offsetMap[tpo.TopicPartition] = tpo.Offset;
                        // 2️⃣ 数量触发
                        if (offsetMap.Count >= 1000)
                        {
                            CommitOffsets(offsetMap);
                        }
                    }

                    // 3️⃣ 时间触发
                    //await timer.WaitForNextTickAsync(ct);

                    if (offsetMap.Any())
                    {
                        CommitOffsets(offsetMap);
                    }
                    // 适当微休，防止死循环空转消耗 CPU
                    await Task.Delay(100, ct);
                }
            }
            catch (OperationCanceledException)
            {

            }
        }

        private void CommitOffsets(Dictionary<TopicPartition, Offset> offsetMap)
        {
            try
            {
                _consumer.Commit(offsetMap.Select(kv =>
                    new TopicPartitionOffset(kv.Key, kv.Value)));

                offsetMap.Clear();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Commit失败");
            }
        }

        private async Task SendToDlq(ConsumeResult<string, string> msg, string error)
        {
            try
            {
                var dlqMessage = new Message<string, string>
                {
                    Key = msg.Message.Key,
                    Value = msg.Message.Value,
                    Headers = new Headers
                {
                    { "error", System.Text.Encoding.UTF8.GetBytes(error) }
                }
                };

                await _dlqProducer.ProduceAsync(_dlqTopic, dlqMessage);
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "DLQ失败，数据可能丢失");
            }
        }

        private async Task ProcessMessage(ConsumeResult<string, string> msg)
        {
            // ✅ 建议：这里做幂等控制（数据库唯一键 / Redis去重）

            _logger.LogInformation($"处理消息: {msg.Message.Value}");

            await Task.CompletedTask;
        }

        private void StopPartitionWorkers(IEnumerable<Partition> partitions)
        {
            foreach (var p in partitions)
            {
                if (_activePartitions.TryRemove(p, out var ctx))
                {
                    ctx.Channel.Writer.Complete();
                    ctx.Cts.CancelAfter(TimeSpan.FromSeconds(10));

                    try
                    {
                        ctx.ProcessingTask.Wait(TimeSpan.FromSeconds(10));
                    }
                    catch { }
                }
            }
        }
    }
}

