namespace CsKafka.Producing
{
    /// <summary>
    /// Defines semantics for batch of messages into message sets in order to balance
    /// - Latency per produce call
    /// - The order of messages
    /// </summary>
    public abstract record Batching
    {
        public sealed record Linger(int LingerMs) : Batching;

        public sealed record EnsureOrder(int LingerMs) : Batching;

        public sealed record EnsureOrderByEnableIdempotence(int LingerMs) : Batching;

        public sealed record Custom(int LingerMs, int MaxInFlight) : Batching;
    }
}
