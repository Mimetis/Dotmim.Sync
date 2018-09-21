using System;
using System.Collections.Generic;

namespace Dotmim.Sync
{
    static class EnumerableExtensions
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
            , int defaultCapacity  = 10)
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

    }
}
