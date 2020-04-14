﻿
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Simulate a Polly Policy. You're defining a policy, then you're executing a code that could potentially be retried a number of times, based on your policy
    /// </summary>
    public class SyncPolicy
    {
        static SyncPolicy() { }

        private SyncPolicy() { }

        public int RetryCount { get; private set; }

        // function that will say if the exception is transient like, and we can retry
        private Func<Exception, bool> isRetriable;

        // function that will return a new TimeSpan, based on the retry current index
        private Func<int, TimeSpan> sleepDurationProvider;


        /// <summary>
        /// Execute an operation based on a retry policy, synchronously
        /// </summary>
        public TResult Execute<TResult>(Func<TResult> operation)
        => InternalExecuteAsync(new Func<Task<TResult>>(() => Task.FromResult(operation())), CancellationToken.None).GetAwaiter().GetResult();

        /// <summary>
        /// Execute an operation based on a retry policy, asynchronously
        /// </summary>
        public Task ExecuteAsync(Func<Task> operation, CancellationToken cancellationToken)
        => InternalExecuteAsync(new Func<Task<bool>>(async () => { await operation(); return true; }), cancellationToken);

        /// <summary>
        /// Execute an operation based on a retry policy, asynchronously
        /// </summary>
        public Task ExecuteAsync(Func<CancellationToken, Task> operation, CancellationToken cancellationToken)
        => InternalExecuteAsync(new Func<Task<bool>>(async () => { await operation(cancellationToken); return true; }), cancellationToken);

        /// <summary>
        /// Execute an operation based on a retry policy, asynchronously, and return the result
        /// </summary>
        public Task<TResult> ExecuteAsync<TResult>(Func<Task<TResult>> operation, CancellationToken cancellationToken)
        => InternalExecuteAsync(operation, cancellationToken);

        /// <summary>
        /// Execute an operation based on a retry policy, asynchronously, and return the result
        /// </summary>
        public Task<TResult> ExecuteAsync<TResult>(Func<CancellationToken, Task<TResult>> operation, CancellationToken cancellationToken)
        => InternalExecuteAsync(new Func<Task<TResult>>(() => operation(cancellationToken)), cancellationToken);

        /// <summary>
        /// Execute an operation based on a retry policy, asynchronously, and return the result
        /// </summary>
        private async Task<TResult> InternalExecuteAsync<TResult>(Func<Task<TResult>> operation, CancellationToken cancellationToken = default)
        {
            if (operation == null)
                return default;

            // try count
            int tryCount = 0;
            try
            {
                while (true)
                {
                    Console.WriteLine($"while (true) iteration index {tryCount}");

                    if (cancellationToken != null && cancellationToken != CancellationToken.None)
                        cancellationToken.ThrowIfCancellationRequested();

                    try
                    {
                        // try to make the action requested
                        var result = await operation().ConfigureAwait(false);

                        return result;
                    }
                    catch (Exception ex)
                    {
                        // Did we excesseed the retry count ?
                        var canRetry = tryCount < this.RetryCount;

                        Console.WriteLine($"can retry ? : Max retry count: {this.RetryCount}. trycount index: {tryCount}");

                        if (!canRetry)
                            throw ex;

                        // Do we have a Func that explicitely say if we can retry or not
                        if (this.isRetriable != null)
                        {
                            canRetry = isRetriable(ex);
                            Console.WriteLine($"is retriable ? : {canRetry}");
                        }

                        if (!canRetry)
                            throw ex;
                    }

                    if (tryCount < int.MaxValue)
                        tryCount++;

                    TimeSpan waitDuration = this.sleepDurationProvider?.Invoke(tryCount) ?? TimeSpan.Zero;

                    Console.WriteLine($"Delay : {waitDuration.Hours}:{waitDuration.Minutes}:{waitDuration.Seconds}.{waitDuration.Milliseconds}");
                    if (waitDuration > TimeSpan.Zero)
                        await Task.Delay(waitDuration, cancellationToken).ConfigureAwait(false);
                }
            }
            finally
            {
                Console.WriteLine("Finally reached");
            }
        }

        /// <summary>
        /// Gets a policy retrying forever with no delay
        /// </summary>
        public static SyncPolicy RetryForever() => WaitAndRetry(-1, TimeSpan.Zero);

        /// <summary>
        /// Gets a policy retrying forever with no delay and a predicate used to define whether a policy handles a given exception
        /// </summary>
        public static SyncPolicy RetryForever(Func<Exception, bool> isRetriable)
            => WaitAndRetry(-1, new Func<int, TimeSpan>(_ => TimeSpan.Zero), isRetriable);

        /// <summary>
        /// Gets a policy retrying for a defined number of iterations, with no delay
        /// </summary>
        public static SyncPolicy Retry(int retryCount = 1) => WaitAndRetry(retryCount, TimeSpan.Zero);

        /// <summary>
        /// Gets a policy retrying for a defined number of iterations, with no delay, and a predicate used to define whether a policy handles a given exception
        /// </summary>
        public static SyncPolicy Retry(int retryCount, Func<Exception, bool> isRetriable)
            => WaitAndRetry(retryCount, new Func<int, TimeSpan>(_ => TimeSpan.Zero), isRetriable);

        /// <summary>
        /// Gets a policy retrying forever with a specified constant delay between each iteration
        /// </summary>
        public static SyncPolicy WaitAndRetryForever(TimeSpan sleepDuration = default)
            => WaitAndRetry(-1, new Func<int, TimeSpan>(_ => sleepDuration), new Func<Exception, bool>(_ => true));

        /// <summary>
        /// Gets a policy retrying forever with a predicate defining a delay between each iteration, based on the iteration index, and a predicate used to define whether a policy handles a given exception
        /// </summary>
        public static SyncPolicy WaitAndRetryForever(Func<int, TimeSpan> sleepDurationProvider, Func<Exception, bool> isRetriable)
            => WaitAndRetry(-1, sleepDurationProvider, isRetriable);

        /// <summary>
        /// Gets a policy retrying for a defined number of iterations, a specified constant delay between each iteration
        /// </summary>
        public static SyncPolicy WaitAndRetry(int retryCount, TimeSpan sleepDuration = default)
            => WaitAndRetry(retryCount, new Func<int, TimeSpan>(_ => sleepDuration), new Func<Exception, bool>(_ => true));

        /// <summary>
        /// Gets a policy retrying for a defined number of iterations, a specified constant delay between each iteration, and a predicate used to define whether a policy handles a given exception
        /// </summary>
        public static SyncPolicy WaitAndRetry(int retryCount, TimeSpan sleepDuration, Func<Exception, bool> isRetriable)
            => WaitAndRetry(retryCount, new Func<int, TimeSpan>(_ => sleepDuration), isRetriable);

        /// <summary>
        /// Gets a policy retrying for a defined number of iterations, with a predicate defining a delay between each iteration, based on the iteration index, and a predicate used to define whether a policy handles a given exception
        /// </summary>
        public static SyncPolicy WaitAndRetry(int retryCount, Func<int, TimeSpan> sleepDurationProvider, Func<Exception, bool> isRetriable)
        {
            var policy = new SyncPolicy { RetryCount = retryCount };

            policy.isRetriable = isRetriable ?? new Func<Exception, bool>(_ => true);
            policy.sleepDurationProvider = sleepDurationProvider ?? new Func<int, TimeSpan>(_ => TimeSpan.Zero);

            return policy;
        }
    }
}
