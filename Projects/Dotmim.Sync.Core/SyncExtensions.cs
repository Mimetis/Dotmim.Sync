
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
    /// These extensions methods mimic the Polly stuff
    /// </summary>
    public static class _
    {

        //public static Task<T> WaitAndRetryAsync<T>(int retryCount = 1, TimeSpan sleepDuration = default,
        //                    Func<Exception, bool> isRetriable = default, Func<CancellationToken, Task<T>> action = default, CancellationToken cancellationToken = default)
        //    => WaitAndRetryAsync<T>(retryCount, new Func<int, TimeSpan>(_ => sleepDuration), isRetriable, action, cancellationToken);

        //public static Task<T> WaitAndRetryAsync<T>(int retryCount = 1, TimeSpan sleepDuration = default, Func<CancellationToken, Task<T>> action = default, CancellationToken cancellationToken = default)
        //    => WaitAndRetryAsync<T>(retryCount, new Func<int, TimeSpan>(_ => sleepDuration), new Func<Exception, bool>(_ => false), action, cancellationToken);

        //public static Task<T> WaitAndRetryAsync<T>(int retryCount = 1, Func<CancellationToken, Task<T>> action = default, CancellationToken cancellationToken = default)
        //    => WaitAndRetryAsync<T>(retryCount, new Func<int, TimeSpan>(_ => TimeSpan.Zero), new Func<Exception, bool>(_ => false), action, cancellationToken);


        [DebuggerHidden()]
        public static Task WaitAndRetryAsync(int retryCount = 1, Func<int, TimeSpan> sleepDurationProvider = default,
                            Func<Exception, bool> isRetriable = default, Func<CancellationToken, Task> action = default, CancellationToken cancellationToken = default)
            => WaitAndRetryAsync(retryCount, sleepDurationProvider, isRetriable,
                new Func<CancellationToken, Task<bool>>(async ct => { await action(ct); return true; }), cancellationToken);


        [DebuggerHidden()]
        public async static Task<T> WaitAndRetryAsync<T>(int retryCount = 1, Func<int, TimeSpan> sleepDurationProvider = default,
                            Func<Exception, bool> isRetriable = default, Func<CancellationToken, Task<T>> action = default, CancellationToken cancellationToken = default)
        {
            if (action == null)
                return default;

            // try count
            int tryCount = 0;
            try
            {
                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    try
                    {
                        // try to make the action requested
                        var result = await action(cancellationToken).ConfigureAwait(false);

                        return result;
                    }
                    catch (Exception ex)
                    {
                        // Did we excesseed the retry count ?
                        var canRetry = tryCount < retryCount;

                        if (!canRetry)
                            throw ex;

                        // Do we have a Func that explicitely say if we can retry or not
                        if (isRetriable != null)
                            canRetry = isRetriable(ex);

                        if (!canRetry)
                            throw ex;
                    }

                    if (tryCount < int.MaxValue)
                        tryCount++;

                    TimeSpan waitDuration = sleepDurationProvider?.Invoke(tryCount) ?? TimeSpan.Zero;

                    if (waitDuration > TimeSpan.Zero)
                        await Task.Delay(waitDuration, cancellationToken).ConfigureAwait(false);
                }
            }
            finally
            {

            }

        }

    }













    /// <summary>
    /// Contains only extensions methods
    /// </summary>
    public static class SyncExtensions
    {
        /// <summary>
        /// Validates, that all column filters do refer to a an existing column of the target table
        /// </summary>
        /// <param name="filters"></param>
        /// <param name="tableDescription"></param>
        public static void ValidateColumnFilters(this SyncFilter filter, SyncTable tableDescription)
        {
            if (filter == null)
                return;

            // TODO : Validate column filters
            //foreach (var c in filters)
            //{
            //    if (c.IsVirtual)
            //        continue;

            //    var columnFilter = tableDescription.Columns[c.ColumnName];

            //    if (columnFilter == null)
            //        throw new InvalidExpressionException($"Column {c.ColumnName} does not exist in Table {tableDescription.TableName}");
            //}
        }
    }
}
