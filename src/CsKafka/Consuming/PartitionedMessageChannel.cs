using Confluent.Kafka;
using System;
using System.Collections.Concurrent;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace CsKafka.Consuming
{
    internal record PartitionAddedEventArgs<T>(
        ChannelReader<T> MessageChannelReader);

    internal class PartitionedMessageChannel<T>
    {
        private readonly UnboundedChannelOptions _channelOptions 
            = new UnboundedChannelOptions() 
            {
                SingleReader = true,
                SingleWriter = true,
            };

        private readonly ConcurrentDictionary<TopicPartition, Lazy<Channel<T>>> _channels = new();

        public event EventHandler<PartitionAddedEventArgs<T>>? OnPartitionAdded;

        public ValueTask WriteAsync(TopicPartition key, T message)
        {
            var channel = _channels.GetOrAdd(key, Factory);

            return channel.Value.Writer.WriteAsync(message);

            Lazy<Channel<T>> Factory(TopicPartition key)
                =>
                new Lazy<Channel<T>>(() =>
                {
                    var channel = CreateChannel();
                    OnPartitionAdded?.Invoke(this, new PartitionAddedEventArgs<T>(channel.Reader));

                    return channel;
                });
        }

        public void Revoke(TopicPartition key)
        {
            if (_channels.TryRemove(key, out Lazy<Channel<T>>? channel))
                _ = channel!.Value.Writer.TryComplete();
        }

        public void Close()
        {
            foreach (var channel in _channels.Values)
                _ = channel!.Value.Writer.TryComplete();
        }

        private Channel<T> CreateChannel()
            => Channel.CreateUnbounded<T>(_channelOptions);
    }
}
