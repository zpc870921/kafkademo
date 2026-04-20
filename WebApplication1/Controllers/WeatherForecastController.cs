using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using System.Diagnostics;

namespace WebApplication1.Controllers
{
    [ApiController]
    [Route("api/[controller]/[action]")]
    public class WeatherController : ControllerBase
    {
        private static readonly string[] Summaries =
        [
            "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
        ];

        [HttpGet(Name = "GetWeatherForecast")]
        public IEnumerable<WeatherForecast> Get()
        {
            return Enumerable.Range(1, 5).Select(index => new WeatherForecast
            {
                Date = DateOnly.FromDateTime(DateTime.Now.AddDays(index)),
                TemperatureC = Random.Shared.Next(-20, 55),
                Summary = Summaries[Random.Shared.Next(Summaries.Length)]
            })
            .ToArray();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        [HttpGet]
        public async Task<IActionResult> demo()
        {
            // Kafka 配置
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092", // 你的 Kafka 地址
                Acks = Acks.All // 消息确认模式
            };

            // 创建生产者
            using var producer = new ProducerBuilder<Null, string>(config).Build();

            try
            {
                // 发送100条消息
                var produceTasks = new List<Task<DeliveryResult<Null, string>>>();

                for (int i = 1; i <= 100; i++)
                {
                    var message = new Message<Null, string>
                    {
                        Value = $"Hello Kafka from .NET! #{i}"
                    };

                    // 异步发送并将任务加入列表
                    produceTasks.Add(producer.ProduceAsync("test-topic", message));
                }

                // 等待所有发送完成
                var results = await Task.WhenAll(produceTasks);

                // 输出发送结果
                foreach (var result in results)
                {
                    Console.WriteLine($"发送成功：Topic={result.Topic}, Partition={result.Partition}, Offset={result.Offset}");
                }

                // 确保生产者刷新所有消息
                producer.Flush(TimeSpan.FromSeconds(10));
            }
            catch (Exception ex)
            {
                Console.WriteLine($"发送失败：{ex.Message}");
            }
            return Ok();
        }

        /// <summary>
        /// 使用 Kafka 事务发布消息，保证批量消息原子性（全部成功或全部回滚）
        /// </summary>
        [HttpPost]
        public async Task<IActionResult> TransactionalPublish([FromBody] string[] messages)
        {
            if (messages == null || messages.Length == 0)
                return BadRequest("消息列表不能为空");

            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9092",
                Acks = Acks.All,
                // 事务必需配置
                TransactionalId = $"txn-weather-{Environment.MachineName}",
                // 开启幂等以保证 Exactly-Once 语义
                EnableIdempotence = true,
                MessageSendMaxRetries = 3
            };

            using var producer = new ProducerBuilder<string, string>(config).Build();

            // 初始化事务（每个 TransactionalId 只需调用一次）
            producer.InitTransactions(TimeSpan.FromSeconds(10));

            try
            {
                // 开启事务
                producer.BeginTransaction();

                foreach (var msg in messages)
                {
                    var kafkaMessage = new Message<string, string>
                    {
                        Key = Guid.NewGuid().ToString(),
                        Value = msg
                    };

                    await producer.ProduceAsync("test-topic", kafkaMessage);
                }

                // 提交事务 —— 所有消息原子性可见
                producer.CommitTransaction();

                return Ok(new { success = true, count = messages.Length });
            }
            catch (Exception ex)
            {
                // 回滚事务 —— 所有消息都不会被消费者看到
                try { producer.AbortTransaction(); } catch { }
                return StatusCode(500, new { success = false, error = ex.Message });
            }
        }
    }
}
