using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;

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
                // 发送消息
                var result = await producer.ProduceAsync(
                    "test-topic",  // 主题名称
                    new Message<Null, string>
                    {
                        Value = "Hello Kafka from .NET!" // 消息内容
                    });

                Console.WriteLine($"发送成功：Topic={result.Topic}, Partition={result.Partition}, Offset={result.Offset}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"发送失败：{ex.Message}");
            }
            return Ok();
        }
    }
}
