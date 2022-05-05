using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace CsKafka.Producing
{
    /// <summary>
    /// Kafka producer that wraps a Confluent.Kafka.Producer
    /// </summary>
    public class KafkaProducer : IDisposable
    {
        public KafkaProducer(
            string topic,
            KafkaProducerOptions options,
            ILogger<KafkaProducer> logger)
        {
            if (string.IsNullOrEmpty(topic))
                throw new ArgumentNullException(nameof(topic));

            ArgumentNullException.ThrowIfNull(options, nameof(options));
            ArgumentNullException.ThrowIfNull(logger, nameof(logger));

            Topic = topic;
            Inner = new ProducerBuilder<string, byte[]>(options.Inner)
                .SetLogHandler((_, m) =>
                    logger.LogInformationIfEnabled(
                        "[Producing] {message} level={level} name={name} facility={facility}",
                        m.Message,
                        m.Level,
                        m.Name,
                        m.Facility
                    )
                )
                .SetErrorHandler((_, e) =>
                    logger.LogError(
                        "[Producing] Error {reason} code={code} isBrokerError={isBrokerError} isFatal={isFatal}",
                        e.Reason,
                        e.Code,
                        e.IsBrokerError,
                        e.IsFatal
                    )
                )
                .Build();
        }

        public string Topic { get; }

        public IProducer<string, byte[]> Inner { get; }

        /// <summary>
        /// Produces a single message
        /// </summary>
        /// <param name="message">Message that to be produced</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns></returns>
        public Task<DeliveryResult<string, byte[]>> ProduceAsync(
            Message<string, byte[]> message,
            CancellationToken cancellationToken = default)
            =>
            Inner.ProduceAsync(Topic, message, cancellationToken);

        /// <summary>
        /// Produces a single message
        /// </summary>
        /// <param name="key">The key of message</param>
        /// <param name="value">The value of message</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns></returns>
        public Task<DeliveryResult<string, byte[]>> ProduceAsync(
            string key,
            byte[] value,
            CancellationToken cancellationToken = default)
            =>
            ProduceAsync(Message.Create(key, value), cancellationToken);

        /// <summary>
        /// Produces a single message
        /// </summary>
        /// <param name="key">The key of message</param>
        /// <param name="value">The value of message</param>
        /// <param name="headers">The header list of message</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns></returns>
        public Task<DeliveryResult<string, byte[]>> ProduceAsync(
            string key,
            byte[] value,
            IEnumerable<(string Key, byte[] Value)> headers,
            CancellationToken cancellationToken = default)
            =>
            ProduceAsync(Message.CreateWithHeaders(key, value, headers), cancellationToken);

        /// <summary>
        /// Produces a batch of messages
        /// </summary>
        /// <param name="messages">A batch of messages that to be produced</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns></returns>
        public Task<DeliveryReport<string, byte[]>[]> ProduceBatchAsync(
            IEnumerable<Message<string, byte[]>> messages,
            CancellationToken cancellationToken = default)
        {
            var messageCount = messages.Count();

            if (messageCount == 0)
                return Task.FromResult(new DeliveryReport<string, byte[]>[0]);

            var tcs = new TaskCompletionSource<DeliveryReport<string, byte[]>[]>();
            var reports = new DeliveryReport<string, byte[]>[messageCount];
            var completedCount = 0;

            cancellationToken.Register(() => tcs.TrySetCanceled());

            var handler = (DeliveryReport<string, byte[]> report) =>
            {
                if (report.Error.IsError)
                {
                    var exception = new KafkaException(report.Error);
                    tcs.TrySetException(exception);
                }
                else
                {
                    Interlocked.Increment(ref completedCount);
                    reports[completedCount - 1] = report;

                    if (completedCount == messageCount)
                        tcs.TrySetResult(reports);
                }
            };

            foreach (var message in messages)
                Inner.Produce(Topic, message, handler);

            Inner.Flush(cancellationToken);

            return tcs.Task;
        }

        /// <summary>
        /// Produces a batch of messages
        /// </summary>
        /// <param name="messages">A batch of message tuples with supplied key/value that to be produced</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns></returns>
        public Task<DeliveryReport<string, byte[]>[]> ProduceBatchAsync(
            IEnumerable<(string Key, byte[] Value)> messages,
            CancellationToken cancellationToken = default)
            =>
            ProduceBatchAsync(
                messages.Select(m => Message.Create(m.Key, m.Value)),
                cancellationToken
            );

        /// <summary>
        /// Produces a batch of messages
        /// </summary>
        /// <param name="messages">A batch of message tuples with supplied key/value/headers that to be produced</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns></returns>
        public Task<DeliveryReport<string, byte[]>[]> ProduceBatchAsync(
            IEnumerable<(
                string Key,
                byte[] Value,
                IEnumerable<(string Key, byte[] Value)> Headers
            )> messages,
            CancellationToken cancellationToken = default)
            =>
            ProduceBatchAsync(
                messages.Select(m => Message.CreateWithHeaders(m.Key, m.Value, m.Headers)),
                cancellationToken
            );

        public void Dispose() => Inner.Dispose();
    }
}
