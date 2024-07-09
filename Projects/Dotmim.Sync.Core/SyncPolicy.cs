using System;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Simulate a Polly Policy. You're defining a policy, then you're executing a code that could potentially be retried a number of times, based on your policy.
    /// </summary>
    public class SyncPolicy
    {
        // function that will be called when we are going to retry
        private Func<Exception, int, TimeSpan, object, Task> onRetryAsync;

        // function that will say if the exception is transient like, and we can retry
        private Func<Exception, object, bool> isRetriable;

        static SyncPolicy() { }

        private SyncPolicy() { }

        /// <summary>
        /// Gets or Sets the max retry iteration count.
        /// </summary>
        public int RetryCount { get; set; }

        /// <summary>
        /// Gets or Sets the Function that will define the duration to wait for the retry iteration N.
        /// </summary>
        public Func<int, TimeSpan> SleepDuration { get; set; }

        /// <summary>
        /// Gets a policy retrying forever with no delay.
        /// </summary>
        public static SyncPolicy RetryForever() => WaitAndRetry(-1, TimeSpan.Zero, null, null);

        /// <summary>
        /// Gets a policy retrying forever with no delay and a predicate used to define whether a policy handles a given exception.
        /// </summary>
        public static SyncPolicy RetryForever(
            Func<Exception, object, bool> isRetriable,
            Func<Exception, int, TimeSpan, object, Task> onRetry = null)
            => WaitAndRetry(-1, null, isRetriable, onRetry);

        /// <summary>
        /// Gets a policy retrying for a defined number of iterations, with no delay.
        /// </summary>
        public static SyncPolicy Retry(int retryCount = 1) => WaitAndRetry(retryCount, TimeSpan.Zero);

        /// <summary>
        /// Gets a policy retrying for a defined number of iterations, with no delay, and a predicate used to define whether a policy handles a given exception.
        /// </summary>
        public static SyncPolicy Retry(int retryCount, Func<Exception, object, bool> isRetriable,
                                                       Func<Exception, int, TimeSpan, object, Task> onRetry = null)
            => WaitAndRetry(retryCount, new Func<int, TimeSpan>(_ => TimeSpan.Zero), isRetriable, onRetry);

        /// <summary>
        /// Gets a policy retrying forever with a specified constant delay between each iteration.
        /// </summary>
        public static SyncPolicy WaitAndRetryForever(TimeSpan sleepDuration = default)
            => WaitAndRetry(-1, new Func<int, TimeSpan>(_ => sleepDuration), null, null);

        /// <summary>
        /// Gets a policy retrying forever with a predicate defining a delay between each iteration, based on the iteration index, and a predicate used to define whether a policy handles a given exception.
        /// </summary>
        public static SyncPolicy WaitAndRetryForever(
            Func<int, TimeSpan> sleepDurationProvider,
            Func<Exception, object, bool> isRetriable,
            Func<Exception, int, TimeSpan, object, Task> onRetry = null)
            => WaitAndRetry(-1, sleepDurationProvider, isRetriable, onRetry);

        /// <summary>
        /// Gets a policy retrying for a defined number of iterations, a specified constant delay between each iteration.
        /// </summary>
        public static SyncPolicy WaitAndRetry(int retryCount, TimeSpan sleepDuration = default)
            => WaitAndRetry(retryCount, new Func<int, TimeSpan>(_ => sleepDuration), new Func<Exception, object, bool>((ex, arg) => true), null);

        /// <summary>
        /// Gets a policy retrying for a defined number of iterations, a specified constant delay between each iteration, and a predicate used to define whether a policy handles a given exception.
        /// </summary>
        public static SyncPolicy WaitAndRetry(int retryCount, TimeSpan sleepDuration,
                                Func<Exception, object, bool> isRetriable,
                                Func<Exception, int, TimeSpan, object, Task> onRetry = null)
            => WaitAndRetry(retryCount, new Func<int, TimeSpan>(_ => sleepDuration), isRetriable, onRetry);

        /// <summary>
        /// Gets a policy retrying for a defined number of iterations, with a predicate defining a delay between each iteration, based on the iteration index, and a predicate used to define whether a policy handles a given exception.
        /// </summary>
        public static SyncPolicy WaitAndRetry(int retryCount, Func<int, TimeSpan> sleepDurationProvider,
                                                              Func<Exception, object, bool> isRetriable,
                                                              Func<Exception, int, TimeSpan, object, Task> onRetry = null)
        {
            var policy = new SyncPolicy
            {
                RetryCount = retryCount,
                isRetriable = isRetriable ?? new Func<Exception, object, bool>((ex, arg) => true),
                SleepDuration = sleepDurationProvider ?? new Func<int, TimeSpan>(_ => TimeSpan.Zero),
                onRetryAsync = onRetry ?? new Func<Exception, int, TimeSpan, object, Task>((ex, rc, waitDuration, arg) => Task.CompletedTask),
            };
            return policy;
        }

        /// <summary>
        /// Execute an operation based on a retry policy, synchronously.
        /// </summary>
        public TResult Execute<TResult>(Func<TResult> operation)
        => this.InternalExecuteAsync(new Func<Task<TResult>>(() => Task.FromResult(operation())), null, CancellationToken.None).GetAwaiter().GetResult();

        /// <summary>
        /// Execute an operation based on a retry policy, asynchronously.
        /// </summary>
        public Task ExecuteAsync(Func<Task> operation, CancellationToken cancellationToken = default)
        => this.InternalExecuteAsync(
            new Func<Task<bool>>(async () =>
            {
                await operation().ConfigureAwait(false);
                return true;
            }), null, cancellationToken);

        /// <summary>
        /// Execute an operation based on a retry policy, asynchronously.
        /// </summary>
        public Task ExecuteAsync(Func<CancellationToken, Task> operation, CancellationToken cancellationToken = default)
        => this.InternalExecuteAsync(
            new Func<Task<bool>>(async () =>
            {
                await operation(cancellationToken).ConfigureAwait(false);
                return true;
            }), null, cancellationToken);

        /// <summary>
        /// Execute an operation based on a retry policy, asynchronously, and return the result.
        /// </summary>
        public Task<TResult> ExecuteAsync<TResult>(Func<Task<TResult>> operation, CancellationToken cancellationToken = default)
        => this.InternalExecuteAsync(operation, null, cancellationToken);

        /// <summary>
        /// Execute an operation based on a retry policy, asynchronously, and return the result.
        /// </summary>
        public Task<TResult> ExecuteAsync<TResult>(
            Func<CancellationToken, Task<TResult>> operation,
            object arg = default,
            CancellationToken cancellationToken = default)
        => this.InternalExecuteAsync(new Func<Task<TResult>>(() => operation(cancellationToken)), arg, cancellationToken);

        /// <summary>
        /// Execute an operation based on a retry policy, asynchronously, and return the result.
        /// </summary>
        private async Task<TResult> InternalExecuteAsync<TResult>(
            Func<Task<TResult>> operation,
            object arg = default,
            CancellationToken cancellationToken = default)
        {
            if (operation == null)
                return default;

            // try count
            var tryCount = 0;
            try
            {
                while (true)
                {
                    if (cancellationToken != default && cancellationToken != CancellationToken.None)
                        cancellationToken.ThrowIfCancellationRequested();

                    Exception handledException = null;

                    try
                    {
                        // try to make the action requested
                        var result = await operation().ConfigureAwait(false);

                        return result;
                    }
                    catch (Exception ex)
                    {
                        // Did we excesseed the retry count ?
                        var canRetry = tryCount < this.RetryCount || this.RetryCount == -1;

                        if (!canRetry)
                            throw;

                        // Do we have a Func that explicitely say if we can retry or not
                        if (this.isRetriable != null)
                            canRetry = this.isRetriable(ex, arg);

                        if (!canRetry)
                            throw;

                        if (this.onRetryAsync != null)
                            handledException = ex;
                    }

                    if (tryCount < int.MaxValue)
                        tryCount++;

                    var waitDuration = this.SleepDuration?.Invoke(tryCount) ?? TimeSpan.Zero;

                    if (this.onRetryAsync != null)
                        await this.onRetryAsync(handledException, tryCount, waitDuration, arg).ConfigureAwait(false);

                    if (waitDuration > TimeSpan.Zero)
                        await Task.Delay(waitDuration, cancellationToken).ConfigureAwait(false);
                }
            }
            finally
            {
            }
        }
    }
}