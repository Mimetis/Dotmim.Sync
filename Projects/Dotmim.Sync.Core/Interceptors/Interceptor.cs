using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    public class Interceptors
    {
        // Internal table builder cache
        private readonly ConcurrentDictionary<Type, Lazy<ISyncInterceptor>> dictionary
            = new ConcurrentDictionary<Type, Lazy<ISyncInterceptor>>();

        //private readonly Dictionary<Type, ISyncInterceptor> dictionary = new Dictionary<Type, ISyncInterceptor>();

        [DebuggerStepThrough]
        public InterceptorWrapper<T> GetInterceptor<T>() where T : ProgressArgs
        {
            var typeofT = typeof(T);

            // Get a lazy command instance
            var lazyInterceptor = dictionary.GetOrAdd(typeofT,
                k => new Lazy<ISyncInterceptor>(() => new InterceptorWrapper<T>()));

            return lazyInterceptor.Value as InterceptorWrapper<T>;
        }

        /// <summary>
        /// Gets a boolean returning true if an interceptor of type T, exists
        /// </summary>
        public bool Contains<T>() where T : ProgressArgs => this.dictionary.ContainsKey(typeof(T));

    }
  
}
