using Confluent.Kafka;
using System.Collections.Generic;

namespace CsKafka
{
    internal static class Message
    {
        public static Message<string, byte[]> Create(string key, byte[] value)
            =>
            new Message<string, byte[]>()
            {
                Key = key,
                Value = value
            };

        public static Message<string, byte[]> CreateWithHeaders(
            string key, 
            byte[] value, 
            IEnumerable<(string Key, byte[] Value)> headers)
        {
            var message = new Message<string, byte[]>()
            {
                Key = key,
                Value = value,
                Headers = new Headers()
            };

            foreach (var header in headers)
                message.Headers.Add(header.Key, header.Value);

            return message;
        }
    }
}
