using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace CsKafka.Shared
{
    internal static class ChannelReaderExtensions
    {
        public static async ValueTask<(int Count, bool IsCompleted)> FillBufferAsync<T>(
            this ChannelReader<T> channelReader, 
            T[] buffer, 
            TimeSpan maxDelay)
        {
            using var cts = new CancellationTokenSource();
            cts.CancelAfter(maxDelay);

            var i = 0;
            var n = buffer.Length;
            var ct = cts.Token;

            try
            {
                while (i < n && await channelReader.WaitToReadAsync(ct).NoContext())
                {
                    while (i < n && !ct.IsCancellationRequested
                        && channelReader.TryRead(out T? item))
                    {
                        buffer[i] = item;
                        i++;
                    }
                }
            }
            catch (OperationCanceledException) { /* ignore */ }

            var isCompleted = channelReader.Completion.IsCompleted;

            return (i, isCompleted);
        }
    }
}
