using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Dotmim.Sync
{
    /// <summary>
    /// Enumerable extensions.
    /// </summary>
    internal static class EnumerableExtensions
    {
        /// <summary>
        /// Sorts an enumeration based on dependency.
        /// </summary>
        /// <param name="source">source enumeration.</param>
        /// <param name="dependencies">dependencies.</param>
        /// <param name="throwOnCycle">if <see langword="true"/> throw exception if Cyclic dependency found.</param>
        /// <param name="defaultCapacity">default capacity of sorterd buffer.</param>
        public static IEnumerable<T> SortByDependencies<T>(
            this IEnumerable<T> source,
            Func<T, IEnumerable<T>> dependencies,
            bool throwOnCycle = false,
            int defaultCapacity = 10)
        {

            if (source is ICollection<T> collections)
            {
                defaultCapacity = collections.Count + 1;
            }

            var sorted = new List<T>(defaultCapacity);
            var visited = new HashSet<T>();

            foreach (var item in source)
                Visit(item, visited, sorted, dependencies, throwOnCycle);

            return sorted;
        }

        /// <summary>
        /// Compare two IEnumerable of T. If both are null, return true. If one is null and not the other, return false.
        /// </summary>
        public static bool CompareWith<T>(this IEnumerable<T> source, IEnumerable<T> other, Func<T, T, bool> compare)
        {
            // checking null ref
            if ((source == null && other != null) || (source != null && other == null))
                return false;

            // If both are null, return true
            if (source == null && other == null)
                return true;
            var lstSource = source.ToList();

            if (lstSource.Count != other.Count())
                return false;

            // Check all items are identical
            return lstSource.All(sourceItem => other.Any(otherItem => compare(sourceItem, otherItem)));
        }

        /// <summary>
        /// Compare two IEnumerable of T. If both are null, return true. If one is null and not the other, return false.
        /// </summary>
        public static bool CompareWith<T>(this IEnumerable<T> source, IEnumerable<T> other)
            where T : class
        {
            // checking null ref
            if ((source == null && other != null) || (source != null && other == null))
                return false;

            // If both are null, return true
            if (source == null && other == null)
                return true;

            var lstSource = source.ToList();

            if (lstSource.Count != other.Count())
                return false;

            // Check all items are identical
            return lstSource.All(sourceItem => other.Any(otherItem =>
            {
                var cSourceItem = sourceItem as SyncNamedItem<T>;
                var cOtherItem = otherItem as SyncNamedItem<T>;

                if (cSourceItem != null && cOtherItem != null)
                    return cSourceItem.EqualsByProperties(otherItem);
                else
                    return sourceItem.Equals(otherItem);
            }));
        }

        /// <summary>
        /// For each async method.
        /// </summary>
        public static Task ForEachAsync<T>(this IEnumerable<T> source, Func<T, Task> body, int maxDegreeOfParallelism = DataflowBlockOptions.Unbounded, TaskScheduler scheduler = null)
        {
            var options = new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = maxDegreeOfParallelism,
            };
            if (scheduler != null)
                options.TaskScheduler = scheduler;

            var block = new ActionBlock<T>(body, options);

            foreach (var item in source)
                block.Post(item);

            block.Complete();
            return block.Completion;
        }

        /// <summary>
        /// Observes the task to avoid the UnobservedTaskException event to be raised.
        /// </summary>
        public static void Forget(this Task task)
        {
            // note: this code is inspired by a tweet from Ben Adams: https://twitter.com/ben_a_adams/status/1045060828700037125
            // Only care about tasks that may fault (not completed) or are faulted,
            // so fast-path for SuccessfullyCompleted and Canceled tasks.
            if (!task.IsCompleted || task.IsFaulted)
            {
                // use "_" (Discard operation) to remove the warning IDE0058: Because this call is not awaited, execution of the current method continues before the call is completed
                // https://learn.microsoft.com/en-us/dotnet/csharp/fundamentals/functional/discards?WT.mc_id=DT-MVP-5003978#a-standalone-discard
                _ = ForgetAwaited(task);
            }

            // Allocate the async/await state machine only when needed for performance reasons.
            // More info about the state machine: https://blogs.msdn.microsoft.com/seteplia/2017/11/30/dissecting-the-async-methods-in-c/?WT.mc_id=DT-MVP-5003978
            async static Task ForgetAwaited(Task task)
            {
                try
                {
                    // No need to resume on the original SynchronizationContext, so use ConfigureAwait(false)
                    await task.ConfigureAwait(false);
                }
                catch
                {
                    // Nothing to do here
                }
            }
        }

        private static void Visit<T>(
             T item,
             HashSet<T> visited,
             List<T> sorted, Func<T, IEnumerable<T>> dependencies,
             bool throwOnCycle)
        {
            var added = visited.Add(item);

            if (!added && throwOnCycle && !sorted.Contains(item))
                throw new Exception("Cyclic dependency found");

            foreach (var dep in dependencies(item))
                Visit(dep, visited, sorted, dependencies, throwOnCycle);

            sorted.Add(item);
        }

        // public static async Task<TResult[]> ForEachAsync<T, TResult>(this IEnumerable<T> items, Func<T, TResult> body, int maxThreads = 4)
        // {
        //    var q = new ConcurrentQueue<T>(items);
        //    var qResults = new ConcurrentQueue<TResult>();
        //    var tasks = new List<Task>();

        // for (int n = 0; n < maxThreads; n++)
        //    {
        //        tasks.Add(Task.Run(() =>
        //        {
        //            while (q.TryDequeue(out T item))
        //                qResults.Enqueue(body(item));
        //        }));
        //    }

        // await Task.WhenAll(tasks);

        // return qResults.ToArray();
        // }

        // public static async Task ForEachAsync<T>(this IEnumerable<T> source, Func<T, Task> body, int maxDegreeOfParallelism = 10)
        // {
        //    using var semaphore = new SemaphoreSlim(initialCount: maxDegreeOfParallelism, maxCount: maxDegreeOfParallelism);
        //    var tasks = source.Select(async item =>
        //   {
        //       await semaphore.WaitAsync();
        //       try
        //       {
        //           return Task.Run(() => body(item));
        //       }
        //       finally
        //       {
        //           semaphore.Release();
        //       }
        //   });
        //    await Task.WhenAll(tasks);
        // }

        // public static async Task<IEnumerable<U>> ForEachAsync<T, U>(this IEnumerable<T> source, Func<T, Task<U>> body, int maxDegreeOfParallelism = 10)
        // {
        //    using var semaphore = new SemaphoreSlim(initialCount: maxDegreeOfParallelism, maxCount: maxDegreeOfParallelism);

        // var tasks = source.Select(async item =>
        //    {
        //        await semaphore.WaitAsync();
        //        try
        //        {
        //            return await Task.Run(() => body(item));
        //        }
        //        finally
        //        {
        //            semaphore.Release();
        //        }
        //    });
        //    return await Task.WhenAll(tasks);
        // }

        // public static Task ForEachAsync<T>(this IEnumerable<T> source, int dop, Func<T, Task> body)
        // {
        //    async Task AwaitPartition(IEnumerator<T> partition)
        //    {
        //        using (partition)
        //        {
        //            while (partition.MoveNext())
        //            { await body(partition.Current); }
        //        }
        //    }
        //    return Task.WhenAll(
        //        Partitioner
        //            .Create(source)
        //            .GetPartitions(dop)
        //            .AsParallel()
        //            .Select(p => AwaitPartition(p)));
        // }

        // public static Task ForEachAsync<TSource, TResult>(this IEnumerable<TSource> source, Func<TSource, Task<TResult>> taskSelector, Action<TSource, TResult> resultProcessor, int dop = 4)
        // {
        //    var oneAtATime = new SemaphoreSlim(initialCount: 1, maxCount: 1);
        //    return Task.WhenAll(
        //         from partition in Partitioner.Create(source).GetPartitions(dop)
        //             from item in source
        //             select ProcessAsync(item, taskSelector, resultProcessor, oneAtATime));
        // }

        // private static async Task ProcessAsync<TSource, TResult>(TSource item, Func<TSource, Task<TResult>> taskSelector, Action<TSource, TResult> resultProcessor, SemaphoreSlim oneAtATime)
        // {
        //    TResult result = await taskSelector(item);
        //    await oneAtATime.WaitAsync();
        //    try { resultProcessor(item, result); }
        //    finally { oneAtATime.Release(); }
        // }
    }
}