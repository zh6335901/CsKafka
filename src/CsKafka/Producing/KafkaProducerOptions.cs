using Confluent.Kafka;
using System;
using System.Collections.Generic;

namespace CsKafka.Producing
{
    /// <summary>
    /// Kafka producer options
    /// </summary>
    public class KafkaProducerOptions
    {
        private KafkaProducerOptions(
            string clientId, 
            string bootstrapServers, 
            ProducerConfig inner)
        {
            ClientId = clientId;
            BootstrapServers = bootstrapServers;
            Inner = inner;
        }

        public string ClientId { get; init; }

        public string BootstrapServers { get; init; }

        public ProducerConfig Inner { get; init; }

        /// <summary>
        /// Creates KafkaProducerOptions instance that wraps a Confluent.Kafka ProducerConfig with some specified settings
        /// </summary>
        /// <param name="clientId">Kafka producer client id</param>
        /// <param name="bootstrapServers">Kafka bootstrap servers</param>
        /// <param name="acks">Acks enum value</param>
        /// <param name="batching">Batching options</param>
        /// <param name="compressionType">Compression type, Default is CompressionType.None</param>
        /// <param name="retries">Retries count, Default is 2</param>
        /// <param name="retryBackoffMs">Retry backoff interval, Default is 100ms</param>
        /// <param name="requestTimeoutMs">Ack timeout (assuming Acks != Acks.Zero). Default is 5s</param>
        /// <param name="messageMaxBytes">The max bytes of message, Default is 1MB</param>
        /// <param name="custom">Custom producer config props</param>
        /// <returns></returns>
        public static KafkaProducerOptions Create(
            string clientId,
            string bootstrapServers, 
            Acks acks,
            Batching batching,
            CompressionType compressionType = CompressionType.None,
            int retries = 2,
            int retryBackoffMs = 100,
            int requestTimeoutMs = 5000, 
            int messageMaxBytes = 1024 * 1024, 
            IDictionary<string, string>? custom = null)
        {
            custom ??= new Dictionary<string, string>();

            var (lingerMs, maxInFlight) = batching switch
            {
                Batching.Linger l                         => (l.LingerMs, (int?)null),
                Batching.EnsureOrder e                    => (e.LingerMs, 1),
                Batching.EnsureOrderByEnableIdempotence e => (e.LingerMs, 5),
                Batching.Custom c                         => (c.LingerMs, c.MaxInFlight),
                _                                         => throw new NotSupportedException()
            };

            var innerProducerConfig = new ProducerConfig(custom)
            {
                ClientId = clientId,
                BootstrapServers = bootstrapServers,
                Acks = acks,
                MaxInFlight = maxInFlight ?? 1_000_000, // default 1_000_000
                LingerMs = lingerMs,
                CompressionType = compressionType,
                MessageSendMaxRetries = retries,
                RetryBackoffMs = retryBackoffMs,
                RequestTimeoutMs = requestTimeoutMs,
                MessageMaxBytes = messageMaxBytes,
                EnableIdempotence = batching is Batching.EnsureOrderByEnableIdempotence ? true : false,
            };

            return new KafkaProducerOptions(clientId, bootstrapServers, innerProducerConfig);
        }
    }
}
