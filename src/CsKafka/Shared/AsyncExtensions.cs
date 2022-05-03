using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace CsKafka.Shared
{
    internal static class AsyncExtensions
    {
        public static ConfiguredTaskAwaitable NoContext(this Task task) 
            => task.ConfigureAwait(false);

        public static ConfiguredTaskAwaitable<T> NoContext<T>(this Task<T> task) 
            => task.ConfigureAwait(false);

        public static ConfiguredValueTaskAwaitable NoContext(this ValueTask valueTask)
            => valueTask.ConfigureAwait(false);

        public static ConfiguredValueTaskAwaitable<T> NoContext<T>(this ValueTask<T> valueTask)
            => valueTask.ConfigureAwait(false);
    }
}
