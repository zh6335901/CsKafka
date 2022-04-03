namespace CsKafka.Producing
{
    /// <summary>
    /// Defines semantics for batch of messages into message sets in order to balance
    /// - Latency per produce call
    /// - The order of messages
    /// </summary>
    public abstract record Batching
    {
        /// <summary>
        /// Produce individually, lingering for throughput + compression
        /// </summary>
        /// <param name="LingerMs"></param>
        public sealed record Linger(int LingerMs) : Batching;

        /// <summary>
        /// Use MaxInFlight = 1 to ensure messages in order
        /// </summary>
        /// <param name="LingerMs"></param>
        public sealed record EnsureOrder(int LingerMs) : Batching;

        /// <summary>
        /// Use EnableIdempotence = true and MaxInFlight = 5 to ensure messages in order
        /// </summary>
        /// <param name="LingerMs"></param>
        public sealed record EnsureOrderByEnableIdempotence(int LingerMs) : Batching;

        /// <summary>
        /// Apply custom-defined settings
        /// </summary>
        /// <param name="LingerMs"></param>
        /// <param name="MaxInFlight"></param>
        public sealed record Custom(int LingerMs, int MaxInFlight) : Batching;
    }
}
