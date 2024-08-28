using Confluent.Kafka;
using Avro.IO;
using Avro.Generic;
using KafkaConsumerApp.Models;
using System;
using System.IO;

namespace KafkaConsumerApp.Services
{
    public class KafkaConsumerService
    {
        private readonly string _bootstrapServers;
        private readonly string _topic;

        public KafkaConsumerService(string bootstrapServers, string topic)
        {
            _bootstrapServers = bootstrapServers;
            _topic = topic;
        }

        public void StartConsuming()
        {
            var config = new ConsumerConfig
            {
                GroupId = "kafka-consumer-group",
                BootstrapServers = _bootstrapServers,
                AutoOffsetReset = AutoOffsetReset.Earliest,
            };
            Console.WriteLine("set up Kafka broker parameters");
            var schemaManager = new AvroSchemaManager();
            var schema = schemaManager.GetSchema();

            using var consumer = new ConsumerBuilder<string, byte[]>(config)
                .SetKeyDeserializer(Deserializers.Utf8)
                .SetValueDeserializer(Deserializers.ByteArray)
                .Build();
            Console.WriteLine("subscribe to Kafka topic");
            consumer.Subscribe(_topic);

            Console.WriteLine($"Consuming messages from topic: {_topic}");

            try
            {
                while (true)
                {
                    var consumeResult = consumer.Consume();

                    var key = consumeResult.Message.Key;
                    var valueBytes = consumeResult.Message.Value;

                    using var stream = new MemoryStream(valueBytes);
                    var decoder = new BinaryDecoder(stream);
                    var reader = new GenericDatumReader<GenericRecord>(schema, schema);
                    var record = reader.Read(null, decoder);

                    var poco = new MyPocoModel
                    {
                        JMSCorrelationID = record["JMSCorrelationID"].ToString(),
                        Timestamp = record["Timestamp"].ToString(),
                        Payload = record["Payload"].ToString()
                    };

                    Console.WriteLine($"Key: {key}, Value: JMSCorrelationID = {poco.JMSCorrelationID}, Timestamp = {poco.Timestamp}, Payload = {poco.Payload}");
                }
            }
            catch (ConsumeException e)
            {
                Console.WriteLine($"Error occurred: {e.Error.Reason}");
            }
            finally
            {
                Console.WriteLine("Close the Kafka Consumer Service");
                consumer.Close();
            }
        }
    }
}
