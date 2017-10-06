
using Dotmim.Sync.Cache;
using Dotmim.Sync.Serialization;
using Microsoft.AspNetCore.Http;
using Newtonsoft.Json;
using System;
using System.Linq;

namespace Dotmim.Sync.Web
{
    public class SessionCache : ICache
    {
        /// <summary>
        /// The cache store. A dictionary that stores different memory caches by the type being cached.
        /// </summary>
        private DmSerializer serializer;
        private HttpContext context;

        /// <summary>
        /// Initializes in memory cache class.
        /// </summary>
        public SessionCache(HttpContext context)
        {
            this.serializer = new DmSerializer();
            this.context = context;
            
        }

        /// <summary>
        /// Sets the specified cache object
        /// </summary>
        public virtual void Set<T>(string cacheKey, T value)
        {
            var strValue = JsonConvert.SerializeObject(value);
            this.context.Session.SetString(cacheKey, strValue);
        }


        /// <summary>
        /// Attempts to retrieve data from cache.
        /// </summary>
        public virtual T GetValue<T>(string key)
        {

            var serializedObject = this.context.Session.GetString(key);

            if (serializedObject != null)
                return JsonConvert.DeserializeObject<T>(serializedObject);

            return default(T);
        }

        /// <summary>
        /// Removes the specified item from cache.
        /// </summary>
        public virtual void Remove(string key)
        {
            if (this.context.Session.Keys.Any(k => string.Equals(k, key, StringComparison.InvariantCultureIgnoreCase)))
                this.context.Session.Remove(key);
        }


        /// <summary>
        /// Clear all items from cache
        /// </summary>
        public virtual void Clear()
        {
            this.context.Session.Clear();
        }
    }
}
