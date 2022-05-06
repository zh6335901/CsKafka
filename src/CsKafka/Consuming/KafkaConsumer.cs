using Confluent.Kafka;
using CsKafka.Shared;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace CsKafka.Consuming
{
    /// <summary>
    /// Kafka consumer that wraps a Confluent.Kafka.Consumer.
    /// It provides a batched consumption mode, batches that belongs to same topic partition will be consumed sequentially.
    /// But batches that belongs to different topic partitions can run concurrently.
    /// It consumes constantly until Stop() method invoked or handler is faulted.
    /// </summary>
    public class KafkaConsumer
    {
        private readonly Task _consumingTask;
        private readonly CancellationTokenSource _cancellationTokenSource;
        private readonly ILogger<KafkaConsumer> _logger;

        private KafkaConsumer(
            Task consumingTask, 
            CancellationTokenSource cts, 
            ILogger<KafkaConsumer> logger) 
        {
            _consumingTask = consumingTask;
            _cancellationTokenSource = cts;
            _logger = logger;
        }

        public bool IsRunning => !_consumingTask.IsCompleted;

        /// <summary>
        /// Request cancellation of consuming
        /// </summary>
        public void Stop() 
        {
            if (!_cancellationTokenSource.IsCancellationRequested)
                _cancellationTokenSource.Cancel();
        }

        /// <summary>
        /// Use 'await _consumer.AwaitShutdown()' to await asynchronously until consume loop stop or is faulted. 
        /// </summary>
        /// <returns>Consuming task</returns>
        public Task AwaitShutdown() => _consumingTask;

        /// <summary>
        /// Start a kafka consumer and return it's instance.
        /// </summary>
        /// <param name="options">Kafka consumer options</param>
        /// <param name="handler">The handler of messages</param>
        /// <param name="logger">Kafka consumer logger</param>
        /// <returns>The instnace of KafkaConsumer</returns>
        public static KafkaConsumer Start(
            KafkaConsumerOptions options,
            Func<ConsumeResult<string, byte[]>[], Task> handler,
            ILogger<KafkaConsumer> logger)
        {
            var cts = new CancellationTokenSource();
            var ct = cts.Token;
            var impl = new ConsumerImpl(options, handler, ct, logger);
            var consumingTask = impl.Start();

            return new KafkaConsumer(consumingTask, cts, logger);
        }

        /// <summary>
        /// Start a kafka consumer that schedules handlers grouped by message key, And return it's instance.
        /// </summary>
        /// <param name="options">Kafka consumer options</param>
        /// <param name="keyHandler">The handler of messages that has same message key</param>
        /// <param name="degreeOfParallelism">The number of keyHandlers running concurrently</param>
        /// <param name="logger">Kafka consumer logger</param>
        /// <returns>The instnace of KafkaConsumer</returns>
        public static KafkaConsumer StartByKey(
            KafkaConsumerOptions options,
            Func<ConsumeResult<string, byte[]>[], Task> keyHandler,
            int degreeOfParallelism,
            ILogger<KafkaConsumer> logger)
        {
            var cts = new CancellationTokenSource();
            var ct = cts.Token;

            var semaphore = new SemaphoreSlim(degreeOfParallelism);
            var handler = async (ConsumeResult<string, byte[]>[] results) =>
            {
                var tasks = results
                    .GroupBy(r => r.Message.Key)
                    .Select(async g => 
                    {
                        await semaphore.WaitAsync(ct);
                        try { await keyHandler(g.ToArray()); }
                        finally { semaphore.Release(); }
                    })
                    .ToArray();

                await Task.WhenAll(tasks);
            };

            var impl = new ConsumerImpl(options, handler, ct, logger);
            var consumingTask = impl.Start();

            return new KafkaConsumer(consumingTask, cts, logger);
        }

        private class ConsumerImpl
        {
            private readonly KafkaConsumerOptions _options;
            private readonly PartitionedMessageChannel<ConsumeResult<string, byte[]>> _partitionedMessageChannel;
            private readonly InFlightMessageCounter _inflightMessageCounter;
            private readonly IConsumer<string, byte[]> _consumer;

            private readonly TaskCompletionSource _consumingTaskCompletionSource;
            private readonly CancellationTokenSource _cancellationTokenSource;

            private readonly ILogger<KafkaConsumer> _logger;

            public ConsumerImpl(
                KafkaConsumerOptions options,
                Func<ConsumeResult<string, byte[]>[], Task> handler,
                CancellationToken ct,
                ILogger<KafkaConsumer> logger)
            {
                var buffering = options.Buffering;
                _inflightMessageCounter = new InFlightMessageCounter(
                    logger,
                    buffering.MinInFlightBytes,
                    buffering.MaxInFlightBytes);

                _consumingTaskCompletionSource = new TaskCompletionSource();
                _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(ct);
                _logger = logger;

                _partitionedMessageChannel = new PartitionedMessageChannel<ConsumeResult<string, byte[]>>();
                _partitionedMessageChannel.OnPartitionAdded += 
                    async (_, e) => await ConsumePartition(e.MessageChannelReader, buffering, handler);

                var onRevoke = (List<TopicPartitionOffset> xs)
                    => xs.ForEach(x => _partitionedMessageChannel.Revoke(x.TopicPartition));

                _options = options;
                _consumer = ConsumerBuilder.BuildWithLogging(options.Inner, logger, onRevoke);
            }

            public async Task Start()
            {
                _consumer.Subscribe(_options.Topics);
               
                var ct = _cancellationTokenSource.Token;
                using var _ = ct.Register(() => _consumingTaskCompletionSource.TrySetResult());

                var loopTask = Task.Run(
                    async () => 
                    {
                        while (!ct.IsCancellationRequested)
                        {
                            _inflightMessageCounter.AwaitThreshold(_consumer, ct);

                            try
                            {
                                var result = _consumer.Consume(ct);
                                if (result != null)
                                {
                                    var messageBytesCount = ApproximateMessageBytes(result);
                                    _inflightMessageCounter.Delta(+messageBytesCount);

                                    await _partitionedMessageChannel.WriteAsync(
                                        result.TopicPartition,
                                        result);
                                }
                            }
                            catch (ConsumeException ex)
                            {
                                _logger.LogError(ex, "[Consuming] Exception {name}", _consumer.Name);
                            }
                            catch (OperationCanceledException ex)
                            {
                                _logger.LogError(ex, "[Consuming] Cancelled {name}", _consumer.Name);
                            }
                        }             
                    }, 
                    ct
                );

                try 
                {
                    await _consumingTaskCompletionSource.Task;
                }
                finally
                {
                    _consumer.Dispose();
                    _cancellationTokenSource.Dispose();
                    _partitionedMessageChannel.Close();
                }                
            }

            private async Task ConsumePartition(
                ChannelReader<ConsumeResult<string, byte[]>> reader, 
                Buffering buffering,
                Func<ConsumeResult<string, byte[]>[], Task> handler)
            {
                var buffer = new ConsumeResult<string, byte[]>[buffering.MaxBatchSize];
                var isCompleted = false;

                while (!isCompleted)
                {
                    (var batch, isCompleted) = await NextBatch();

                    if (batch.Length > 0)
                    {
                        try
                        {
                            await handler.Invoke(batch);

                            var lastItem = batch.MaxBy(c => c.Offset.Value);
                            _consumer.StoreOffset(lastItem);

                            var batchSize = batch.Sum(c => ApproximateMessageBytes(c));
                            _inflightMessageCounter.Delta(-batchSize);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "[Consuming] Exiting batch processing loop due to handler exception");

                            _consumingTaskCompletionSource.TrySetException(ex);

                            if (!_cancellationTokenSource.IsCancellationRequested)
                                _cancellationTokenSource.Cancel();

                            break;
                        }
                    }
                }
                
                async Task<(ConsumeResult<string, byte[]>[], bool)> NextBatch()
                {
                    var (len, isCompleted) = await reader.FillBufferAsync(
                        buffer, 
                        buffering.MaxBatchDelay);

                    var batch = new ConsumeResult<string, byte[]>[len];
                    Array.Copy(buffer, batch, len);
                    Array.Clear(buffer);

                    return (batch, isCompleted);
                }
            }

            private static long ApproximateMessageBytes(ConsumeResult<string, byte[]> result)
            {
                var message = result.Message;
                var keyLen = message.Key.Length * sizeof(char);
                var valueLen = message.Value.Length * sizeof(byte);

                return keyLen + valueLen + 16;
            }
        }
    }
}
