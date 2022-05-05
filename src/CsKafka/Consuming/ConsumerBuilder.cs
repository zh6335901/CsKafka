using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;

namespace CsKafka.Consuming
{
    internal static class ConsumerBuilder
    {
        public static IConsumer<string, byte[]> BuildWithLogging(
            ConsumerConfig config,
            ILogger logger,
            Action<List<TopicPartitionOffset>> onRevoke)
        {
            return new ConsumerBuilder<string, byte[]>(config)
                .SetLogHandler((_, m) => OnConsumerLog(m))
                .SetErrorHandler((_, e) => OnConsumerError(e))
                .SetPartitionsAssignedHandler((_, ps) => OnPartitionAssigned(ps))
                .SetPartitionsRevokedHandler((_, xs) => OnPartitionRevoked(xs))
                .SetOffsetsCommittedHandler((_, cos) => OnOffsetsCommited(cos))
                .Build();

            void OnConsumerLog(LogMessage m)
                =>
                logger.LogInformationIfEnabled(
                    "[Consuming] {message} level={level} name={name} facility={facility}",
                    m.Message,
                    m.Level,
                    m.Name,
                    m.Facility
                );

            void OnConsumerError(Error e)
                =>
                logger.LogError(
                    "[Consuming] Error {reason} code={code} isBrokerError={isBrokerError} isFatal={isFatal}",
                    e.Reason,
                    e.Code,
                    e.IsBrokerError,
                    e.IsFatal
                );

            void OnPartitionAssigned(List<TopicPartition> ps)
            {
                var ts = ps.GroupBy(p => p.Topic);

                foreach (var t in ts)
                {
                    logger.LogInformationIfEnabled(
                        "[Consuming] Assigned {topic} {partitions}",
                        t.Key,
                        string.Join(",", t.Select(x => x.Partition.ToString()))
                    );
                }
            }

            void OnPartitionRevoked(List<TopicPartitionOffset> xs)
            {
                var ts = xs.GroupBy(p => p.Topic);

                foreach (var t in ts)
                {
                    logger.LogInformationIfEnabled(
                        "[Consuming] Revoked {topic} {partition}",
                        t.Key,
                        string.Join(",", t.Select(x => x.Partition.ToString()))
                    );
                }

                onRevoke(xs);
            }

            void OnOffsetsCommited(CommittedOffsets cos)
            {
                var ts = cos.Offsets.GroupBy(o => o.Topic);

                foreach (var t in ts)
                {
                    var topic = t.Key;
                    var offsets = string.Join(",", t);

                    if (cos.Error.IsError)
                        logger.LogError(
                            "[Consuming] Committed {topic} {offsets} reason={error} code={code} isBrokerError={isBrokerError}",
                            topic,
                            offsets,
                            cos.Error.Reason,
                            cos.Error.Code,
                            cos.Error.IsBrokerError
                        );
                    else
                        logger.LogInformationIfEnabled(
                            "[Consuming] Committed {topic} {offsets}",
                            topic,
                            offsets
                        );
                }
            }
        }
    }
}
