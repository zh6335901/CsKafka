using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;

namespace CsKafka.Consuming
{
    internal class InFlightMessageCounter
    {
        private readonly ILogger<InFlightMessageCounter> _logger;
        private readonly long _minInFlightBytes;
        private readonly long _maxInFlightBytes;

        private long _inflightBytes;

        public InFlightMessageCounter(
            ILogger<InFlightMessageCounter> logger,
            long minInFlightBytes,
            long maxInFlightBytes)
        {
            ArgumentNullException.ThrowIfNull(logger, nameof(logger));
            if (minInFlightBytes < 1)
                throw new ArgumentOutOfRangeException(
                    nameof(minInFlightBytes), "minInFlightBytes must be positive value");
            if (maxInFlightBytes < 1)
                throw new ArgumentOutOfRangeException(
                    nameof(maxInFlightBytes), "maxInFlightBytes must be positive value");
            if (minInFlightBytes > maxInFlightBytes)
                throw new ArgumentOutOfRangeException(
                    nameof(maxInFlightBytes), "maxInFlightBytes must be greater than minInFlightBytes");

            _logger = logger;
            _minInFlightBytes = minInFlightBytes;
            _maxInFlightBytes = maxInFlightBytes;
        }

        public double InFlightMb => _inflightBytes / 1024.0 / 1024.0;

        public void Delta(long bytesCount) => Interlocked.Add(ref _inflightBytes, bytesCount);

        public bool IsOverLimit() => Volatile.Read(ref _inflightBytes) > _maxInFlightBytes;

        public void AwaitThreshold(IConsumer<string, byte[]> consumer, CancellationToken cancellationToken)
        {
            // If in flight bytes is over limit
            // Waiting until it less than minInFlightBytes or cancellation requested
            if (IsOverLimit())
            {
                _logger.LogInformation("");

                while (Volatile.Read(ref _inflightBytes) > _minInFlightBytes 
                    && !cancellationToken.IsCancellationRequested)                
                    ShowConsumerWeAreStillAlive();                
            }

            // Avoid our assignments be revoked due to MAXPOLL
            void ShowConsumerWeAreStillAlive()
            {
                var partitions = consumer.Assignment;
                consumer.Pause(partitions);
                consumer.Consume(1);
                consumer.Resume(partitions);
            }
        }
    }
}
