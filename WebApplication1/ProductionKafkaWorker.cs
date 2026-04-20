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
    }
    public class ProductionKafkaWorker : BackgroundService
    {
        private readonly IConsumer<string, string> _consumer;
        private readonly IProducer<string, string> _dlqProducer;
        private readonly ILogger<ProductionKafkaWorker> _logger;

        private readonly ConcurrentDictionary<Partition, PartitionContext> _activePartitions = new();

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
                EnableAutoCommit = true,
                EnableAutoOffsetStore=false,
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

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            // 使用 LongRunning 避免每次 Consume 都分配 ThreadPool 工作项
            return Task.Factory.StartNew(() => ConsumeLoop(stoppingToken),
                stoppingToken, TaskCreationOptions.LongRunning, TaskScheduler.Default).Unwrap();
        }

        private async Task ConsumeLoop(CancellationToken stoppingToken)
        {
            _consumer.Subscribe(_sourceTopic);
            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    var result = _consumer.Consume(stoppingToken);
                    if (result == null) continue;

                    if (_activePartitions.TryGetValue(result.Partition, out var ctx))
                    {
                        try
                        {
                            await ctx.Channel.Writer.WriteAsync(result, stoppingToken);
                        }
                        catch (ChannelClosedException)
                        {
                            // 竞态窗口：消息写入时分区刚好被撤销并关闭了 Channel。
                            // 该消息 offset 未被 StoreOffset，Kafka 重均衡后会重新投递，符合 at-least-once 语义。
                            _logger.LogWarning("分区 {Partition} 的 Channel 已关闭（发生在写入时），消息将被重新投递。", result.Partition);
                        }
                    }
                }
            }
            catch (OperationCanceledException) { }
            finally
            {
                // 1. 先触发各分区消费任务暂停写入，防止生成新的未提交游标
                StopPartitionWorkers(_activePartitions.Keys.ToList());

                // 2. 将存在于本地但还没发送到 Broker 的位移最后进行一次同步强制提交！
                try { _consumer.Commit(); } catch { /* 忽略没改变的报错 */ }

                // 3. 刷新 DLQ 生产者缓冲区，确保死信消息不丢
                try { _dlqProducer.Flush(TimeSpan.FromSeconds(10)); } catch { }
                _dlqProducer.Dispose();

                // 4. 最后才能释放消费者客户端
                _consumer.Close();
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
                Cts = new CancellationTokenSource()
            };

            ctx.ProcessingTask = Task.Run(() => ProcessPartitionLoop(ctx));
            _activePartitions.TryAdd(partition, ctx);
        }

        private async Task ProcessPartitionLoop(PartitionContext ctx)
        {
            var reader = ctx.Channel.Reader;
            var batch = new List<ConsumeResult<string, string>>(500);

            try
            {
                while (!ctx.Cts.Token.IsCancellationRequested)
                {
                    batch.Clear();

                    bool hasData;
                    try
                    {
                        // 等待第一条消息到达
                        hasData = await reader.WaitToReadAsync(ctx.Cts.Token);
                    }
                    catch (OperationCanceledException) 
                    { 
                        break; // 被整体取消中断，优雅退出
                    }

                    if (!hasData)
                    {
                        break; // 取消了且 Channel 已经被停止推送（数据已全部排空），不再接受新轮次，安全退出任务
                    }

                    // 采用带超时和缓冲的批量读取逻辑 (高效且不空转 CPU)
                    using var batchCts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
                    using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ctx.Cts.Token, batchCts.Token);

                    try
                    {
                        while (batch.Count < 500 && reader.TryRead(out var msg))
                        {
                            batch.Add(msg);
                        }

                        // 如果还没满 500 条并且没有取消，可以在一定时间限制内继续收取
                        while (batch.Count < 500 && await reader.WaitToReadAsync(linkedCts.Token))
                        {
                            if (reader.TryRead(out var msg)) batch.Add(msg);
                        }
                    }
                    catch (OperationCanceledException) { /* catch 住 linkedCts 的 200ms 时间到了 */ }

                    if (batch.Count > 0)
                    {
                        await SafeProcessBatch(ctx, batch);
                    }
                }
            }
            catch (OperationCanceledException) { /* 正常退出 */ }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "分区 {Partition} 发生不可恢复的致命异常停止", ctx.Partition);
                // 实际生产中这里如果崩了，应当引入报警，并考虑重新抛出以触发应用重启
            }
        }

        private async Task SafeProcessBatch(PartitionContext ctx, List<ConsumeResult<string, string>> batch)
        {
            ConsumeResult<string, string> lastSuccessfulMsg = null;

            foreach (var msg in batch)
            {
                bool messageDealt = false;
                while (!messageDealt && !ctx.Cts.IsCancellationRequested) // 卡死重试循环：保证消息绝不丢失
                {
                    try
                    {
                        // 你的业务多波次重试
                        await _retryPolicy.ExecuteAsync(async () => await ProcessMessage(msg));
                        messageDealt = true;
                    }
                    catch (Exception)
                    {
                        // 系统性异常进了这里，必须保证能放进 DLQ
                        try
                        {
                            await SendToDlq(msg, "达到重试上限");
                            messageDealt = true;
                        }
                        catch (Exception dlqEx)
                        {
                            _logger.LogCritical(dlqEx, "死信队列也连不上了！系统将暂停 5 秒后重试写入DLQ，确保数据不丢失。");
                            await Task.Delay(5000, ctx.Cts.Token); // 阻断循环，直到网络恢复
                        }
                    }
                }

                if (messageDealt) lastSuccessfulMsg = msg; // 只有 100% 成功或 100% 存入DLQ，才算成功
            }

            if (lastSuccessfulMsg != null)
            {
                try
                {
                    // 依赖 Librdkafka 高效提交机制。StoreOffset 是线程安全的。
                    // 提交的 offset 是下一次消费的起点，所以 + 1
                    _consumer.StoreOffset(new TopicPartitionOffset(lastSuccessfulMsg.TopicPartition, lastSuccessfulMsg.Offset + 1));
                }
                catch (KafkaException ex)
                {
                    _logger.LogError(ex, "StoreOffset 本地写入失败");
                }
            }
        }

        

        private async Task SendToDlq(ConsumeResult<string, string> msg, string error)
        {
            // 注意：此方法不捕获异常，异常由 SafeProcessBatch 的卡死重试循环负责处理。
            // 这样可以保证 DLQ 连接失败时不会错误地将消息标记为"已处理"导致丢失。
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

        private async Task ProcessMessage(ConsumeResult<string, string> msg)
        {
            // ✅ 建议：这里做幂等控制（数据库唯一键 / Redis去重）

            _logger.LogInformation("处理消息: {Value}", msg.Message.Value);

            await Task.CompletedTask;
        }

        private void StopPartitionWorkers(IEnumerable<Partition> partitions)
        {
            var contexts = new List<PartitionContext>();

            // 第一步：先对所有待撤销分区完成 Channel 写入端关闭 + 立即发出取消信号
            // 这样所有分区的处理任务会几乎同时感知到取消，而不是串行等待
            foreach (var p in partitions)
            {
                if (_activePartitions.TryRemove(p, out var ctx))
                {
                    ctx.Channel.Writer.Complete();   // 不再接受新消息，让处理任务尽快 drain
                    ctx.Cts.CancelAfter(TimeSpan.FromSeconds(10)); // 给 10s 机会处理完剩余消息
                    contexts.Add(ctx);
                }
            }

            // 第二步：并行等待所有分区的处理任务完成（最多 10 秒）
            // 过滤掉可能为 null 的 ProcessingTask（防止 TryAdd 之前的竞态窗口）
            var waitTasks = contexts
                .Select(ctx => ctx.ProcessingTask)
                .Where(t => t != null)
                .ToArray();
            if (waitTasks.Length > 0)
            {
                Task.WaitAll(waitTasks, TimeSpan.FromSeconds(12)); // 略大于 CancelAfter 时间
            }

            foreach (var ctx in contexts)
            {
                try { ctx.Cts.Dispose(); } catch { }
            }
        }
    }
}

