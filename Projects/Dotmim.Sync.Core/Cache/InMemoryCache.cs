using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Cache
{
    public class InMemoryCache : ICache
    {
        /// <summary>
        /// The cache store. A dictionary that stores different memory caches by the type being cached.
        /// </summary>
        private ConcurrentDictionary<String, Object> cacheStore;

        /// <summary>
        /// Initializes in memory cache class.
        /// </summary>
        public InMemoryCache()
        {
            cacheStore = new ConcurrentDictionary<String, Object>();
        }

        /// <summary>
        /// Sets the specified cache object
        /// </summary>
        public virtual void Set<T>(string cacheKey, T value)
        {
            cacheStore.AddOrUpdate(cacheKey, value, (s, v) => value);
        }


        /// <summary>
        /// Attempts to retrieve data from cache.
        /// </summary>
        public virtual T GetValue<T>(string key)
        {
            if (!cacheStore.ContainsKey(key))
                return default(T);

            return (T)cacheStore[key];

        }

        /// <summary>
        /// Removes the specified item from cache.
        /// </summary>
        public virtual void Remove(string key)
        {
            cacheStore.TryRemove(key, out object removedValue);
        }


        /// <summary>
        /// Clear all items from cache
        /// </summary>
        public virtual void Clear()
        {
            cacheStore.Clear();
        }
    }
}
