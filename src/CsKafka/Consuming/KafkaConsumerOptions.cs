using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Linq;

namespace CsKafka.Consuming
{
    /// <summary>
    /// Kafka consumer options
    /// </summary>
    public class KafkaConsumerOptions
    {
        private KafkaConsumerOptions(
            string groupId, 
            string bootstrapServers, 
            IEnumerable<string> topics, 
            Buffering buffering,
            ConsumerConfig inner) 
        {
            GroupId = groupId;
            BootstrapServers = bootstrapServers;
            Topics = topics.ToList();
            Buffering = buffering;
            Inner = inner;
        }

        public string GroupId { get; private init; }

        public string BootstrapServers { get; private init; }

        public IReadOnlyList<string> Topics { get; private init; }

        public Buffering Buffering { get; private init; }

        public ConsumerConfig Inner { get; private init; }

        /// <summary>
        /// Creates KafkaConsumerOptions instance that wraps a Confluent.Kafka ConsumerConfig with some specified settings
        /// </summary>
        /// <param name="clientId">Kafka consumer client id</param>
        /// <param name="groupId">Kafka consumer group id</param>
        /// <param name="bootstrapServers">Kafka bootstrap servers</param>
        /// <param name="topics">Topics that would be consumed</param>
        /// <param name="autoOffsetReset">Specifies handling when consumer group does not yet have an offset recorded</param>
        /// <param name="buffering">Consumer buffering options</param>
        /// <param name="fetchMaxBytes">The max bytes return for a fetch request, Default 100KiB, Confluent.Kafka default: 500MiB</param>
        /// <param name="messageMaxBytes">The max bytes of message, Default: use `fetchMaxBytes` value, Confluent.Kafka default: 1MiB</param>
        /// <param name="statisticsInterval">Stats reporting interval for the consumer, Default: no reporting</param>
        /// <param name="autoCommitInterval">Consumed offsets commit interval. Default 5s.</param>
        /// <param name="allowAutoCreateTopics">Allow auto topic creation</param>
        /// <param name="custom">Custom consumer config props</param>
        /// <returns></returns>
        public static KafkaConsumerOptions Create(
            string clientId,
            string groupId,
            string bootstrapServers,
            IEnumerable<string> topics,
            AutoOffsetReset autoOffsetReset,
            Buffering? buffering = null,
            int? fetchMaxBytes = null,
            int? messageMaxBytes = null,
            TimeSpan? statisticsInterval = null,
            TimeSpan? autoCommitInterval = null,
            bool? allowAutoCreateTopics = null,
            IDictionary<string, string>? custom = null)
        {
            buffering ??= Buffering.Default;
            custom ??= new Dictionary<string, string>();
            fetchMaxBytes ??= 100 * 1024;
            messageMaxBytes ??= fetchMaxBytes;
            autoCommitInterval ??= TimeSpan.FromSeconds(5);

            var config = new ConsumerConfig(custom)
            {
                ClientId = clientId,
                GroupId = groupId,
                BootstrapServers = bootstrapServers,
                AutoOffsetReset = autoOffsetReset,
                FetchMaxBytes = fetchMaxBytes,
                MessageMaxBytes = messageMaxBytes,
                StatisticsIntervalMs = (int?)statisticsInterval?.TotalMilliseconds,
                AutoCommitIntervalMs = (int)autoCommitInterval.Value.TotalMilliseconds,
                AllowAutoCreateTopics = allowAutoCreateTopics,
                EnableAutoCommit = true,
                EnableAutoOffsetStore = false,
            };

            return new KafkaConsumerOptions(groupId, bootstrapServers, topics, buffering, config);
        }
    }
}
