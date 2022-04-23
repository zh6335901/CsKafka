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
            string bootstrapServers, 
            ProducerConfig inner)
        {
            BootstrapServers = bootstrapServers;
            Inner = inner;
        }

        public string BootstrapServers { get; private init; }

        public ProducerConfig Inner { get; private init; }

        /// <summary>
        /// Creates KafkaProducerOptions instance that wraps a Confluent.Kafka ProducerConfig with some specified settings
        /// </summary>
        /// <param name="clientId">Kafka producer client id</param>
        /// <param name="bootstrapServers">Kafka bootstrap servers</param>
        /// <param name="acks">Acks enum value</param>
        /// <param name="batching">Batching options</param>
        /// <param name="compressionType">Compression type, Default is CompressionType.None</param>
        /// <param name="retries">Retries count, Default is 2</param>
        /// <param name="retryBackoff">Retry backoff interval, Default is 100ms</param>
        /// <param name="requestTimeout">Ack timeout (assuming Acks != Acks.Zero), Default is 5s</param>
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
            TimeSpan? retryBackoff = null,
            TimeSpan? requestTimeout = null, 
            int messageMaxBytes = 1024 * 1024, 
            IDictionary<string, string>? custom = null)
        {
            custom ??= new Dictionary<string, string>();
            retryBackoff ??= TimeSpan.FromMilliseconds(100);
            requestTimeout ??= TimeSpan.FromSeconds(5);

            var (lingerSpan, maxInFlight) = batching switch
            {
                Batching.Linger l                         => (l.LingerSpan, (int?)null),
                Batching.EnsureOrder e                    => (e.LingerSpan, 1),
                Batching.EnsureOrderByEnableIdempotence e => (e.LingerSpan, 5),
                Batching.Custom c                         => (c.LingerSpan, c.MaxInFlight),
                _                                         => throw new NotSupportedException()
            };

            var innerProducerConfig = new ProducerConfig(custom)
            {
                ClientId = clientId,
                BootstrapServers = bootstrapServers,
                Acks = acks,
                MaxInFlight = maxInFlight ?? 1_000_000, // default 1_000_000
                LingerMs = lingerSpan.TotalMilliseconds,
                CompressionType = compressionType,
                MessageSendMaxRetries = retries,
                RetryBackoffMs = (int)retryBackoff.Value.TotalMilliseconds,
                RequestTimeoutMs = (int)requestTimeout.Value.TotalMilliseconds,
                MessageMaxBytes = messageMaxBytes,
                EnableIdempotence = batching is Batching.EnsureOrderByEnableIdempotence ? true : false,
            };

            return new KafkaProducerOptions(bootstrapServers, innerProducerConfig);
        }
    }
}
