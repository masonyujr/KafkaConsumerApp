using KafkaConsumerApp.Services;

namespace KafkaConsumerApp
{
    class Program
    {
        static void Main(string[] args)
        {
            string bootstrapServers = "your-aws-msk-endpoint:9092";
            string topic = "your-kafka-topic";
            Console.WriteLine("open the Kafka Service");
            var consumerService = new KafkaConsumerService(bootstrapServers, topic);
            consumerService.StartConsuming();
        }
    }
}
