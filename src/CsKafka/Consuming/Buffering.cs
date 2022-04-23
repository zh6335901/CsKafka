using System;

namespace CsKafka.Consuming
{
    /// <summary>
    /// The buffering options of consuming
    /// </summary>
    /// <param name="MinInFlightBytes">Minimum total size of consumed messages in-memory for the consumer to attempt to fill</param>
    /// <param name="MaxInFlightBytes">Maximum total size of consumed messages in-memory</param>
    /// <param name="MaxBatchDelay">Message batch linger time</param>
    /// <param name="MaxBatchSize">Maximum number of messages to group per batch</param>
    public sealed record Buffering(
        long MaxInFlightBytes,
        long MinInFlightBytes,
        TimeSpan MaxBatchDelay,
        int MaxBatchSize)
    {
        /// <summary>
        /// Default buffering options
        /// - MaxInFlightBytes: 16MiB
        /// - MinInFilghtBytes: 2/3 MaxInFlightBytes
        /// - MaxBatchDelay: 500ms
        /// - MaxBatchSize: 500
        /// </summary>
        public static readonly Buffering Default 
            = new Buffering(
                16 * 1024 * 1024,
                16 * 1024 * 1024 * 2 / 3, // 2/3 MaxInFlightBytes
                TimeSpan.FromMilliseconds(500),
                1000);
    };
}
