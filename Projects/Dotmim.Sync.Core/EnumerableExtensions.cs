using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Dotmim.Sync
{
    internal static class EnumerableExtensions
    {
        /// <summary>
        /// Sorts an enumeration based on dependency
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source">source enumeration.</param>
        /// <param name="dependencies">dependencies.</param>
        /// <param name="throwOnCycle">if <see langword="true"/> throw exception if Cyclic dependency found.</param>
        /// <param name="defaultCapacity">default capacity of sorterd buffer.</param>
        /// <returns></returns>
        public static IEnumerable<T> SortByDependencies<T>(this IEnumerable<T> source
            , Func<T, IEnumerable<T>> dependencies
            , bool throwOnCycle = false
            , int defaultCapacity = 10)
        {

            if (source is ICollection<T>)
            {
                defaultCapacity = ((ICollection<T>)source).Count + 1;
            }
            var sorted = new List<T>(defaultCapacity);
            var visited = new HashSet<T>();

            foreach (var item in source)
                Visit(item, visited, sorted, dependencies, throwOnCycle);

            return sorted;
        }

        private static void Visit<T>(T item
            , HashSet<T> visited
            , List<T> sorted, Func<T, IEnumerable<T>> dependencies
            , bool throwOnCycle)
        {
            if (!visited.Contains(item))
            {
                visited.Add(item);

                foreach (var dep in dependencies(item))
                    Visit(dep, visited, sorted, dependencies, throwOnCycle);

                sorted.Add(item);
            }
            else
            {
                if (throwOnCycle && !sorted.Contains(item))
                    throw new Exception("Cyclic dependency found");
            }
        }

        public static bool CompareWith<T>(this IEnumerable<T> source, IEnumerable<T> other, Func<T, T, bool> compare)
        {
            // checking null ref
            if ((source == null && other != null) || (source != null && other == null))
                return false;

            // If both are null, return true
            if (source == null && other == null)
                return true;

            if (source.Count() != other.Count())
                return false;

            // Check all items are identical
            return source.All(sourceItem => other.Any(otherItem => compare(sourceItem, otherItem)));

        }
        public static bool CompareWith<T>(this IEnumerable<T> source, IEnumerable<T> other) where T : class
        {
            // checking null ref
            if ((source == null && other != null) || (source != null && other == null))
                return false;

            // If both are null, return true
            if (source == null && other == null)
                return true;

            if (source.Count() != other.Count())
                return false;

            // Check all items are identical
            return source.All(sourceItem => other.Any(otherItem =>
            {
                var cSourceItem = sourceItem as SyncNamedItem<T>;
                var cOtherItem = otherItem as SyncNamedItem<T>;

                if (cSourceItem != null && cOtherItem != null)
                    return cSourceItem.EqualsByProperties(otherItem);
                else
                    return sourceItem.Equals(otherItem);

            }));

        }


        public static Task ForEachAsync<T>(this IEnumerable<T> source, Func<T, Task> body, int maxDegreeOfParallelism = DataflowBlockOptions.Unbounded, TaskScheduler scheduler = null)
        {
            var options = new ExecutionDataflowBlockOptions
            {
                MaxDegreeOfParallelism = maxDegreeOfParallelism
            };
            if (scheduler != null)
                options.TaskScheduler = scheduler;

            var block = new ActionBlock<T>(body, options);

            foreach (var item in source)
                block.Post(item);

            block.Complete();
            return block.Completion;
        }

        //public static Task ForEachAsync<T>(this IEnumerable<T> source, int dop, Func<T, Task> body)
        //{
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
        //}


        //public static Task ForEachAsync<TSource, TResult>(this IEnumerable<TSource> source, Func<TSource, Task<TResult>> taskSelector, Action<TSource, TResult> resultProcessor, int dop = 4)
        //{
        //    var oneAtATime = new SemaphoreSlim(initialCount: 1, maxCount: 1);
        //    return Task.WhenAll(
        //         from partition in Partitioner.Create(source).GetPartitions(dop)
        //             from item in source
        //             select ProcessAsync(item, taskSelector, resultProcessor, oneAtATime));
        //}

        //private static async Task ProcessAsync<TSource, TResult>(TSource item, Func<TSource, Task<TResult>> taskSelector, Action<TSource, TResult> resultProcessor, SemaphoreSlim oneAtATime)
        //{
        //    TResult result = await taskSelector(item);
        //    await oneAtATime.WaitAsync();
        //    try { resultProcessor(item, result); }
        //    finally { oneAtATime.Release(); }
        //}

    }
}
