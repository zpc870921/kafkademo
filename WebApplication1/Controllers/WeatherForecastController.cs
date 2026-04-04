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
    }
}
