using System;
using System.Collections.Generic;
using System.Linq;
using Avro;
using Avro.Generic;

namespace KafkaConsumerApp.Services
{
    public class AvroSchemaManager
    {
        private readonly string _schemaJson = @"
        {
            ""type"": ""record"",
            ""name"": ""MyPocoModel"",
            ""fields"": [
                {""name"": ""JMSCorrelationID"", ""type"": ""string""},
                {""name"": ""Timestamp"", ""type"": ""string""},
                {""name"": ""Payload"", ""type"": ""string""}
            ]
        }";

        public RecordSchema GetSchema()
        {
            Console.WriteLine("get the AVro schema");
            return (RecordSchema)Schema.Parse(_schemaJson);
        }
    }
}
