using System;

namespace CsKafka.Consuming
{
    /// <summary>
    /// The buffering options of consuming
    /// </summary>
    /// <param name="MinInFlightBytes"></param>
    /// <param name="MaxInFlightBytes">Maximum total size of consumed messages in-memory</param>
    /// <param name="MaxBatchDelay">Message batch linger time</param>
    /// <param name="MaxBatchSize">Maximum number of messages to group per batch</param>
    public sealed record Buffering(
        long MinInFlightBytes,
        long MaxInFlightBytes,
        TimeSpan MaxBatchDelay,
        int MaxBatchSize);
}
