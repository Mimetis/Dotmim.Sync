using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Manage all On[Method]s
    /// </summary>
    public class Interceptors
    {
        // Internal table builder cache
        internal readonly ConcurrentDictionary<Type, IList> internalInterceptors = new();

        /// <summary>
        /// Get all interceptors of T
        /// </summary>
        public List<InterceptorWrapper<T>> GetInterceptors<T>() where T : ProgressArgs
        {
            var syncInterceptors = internalInterceptors.GetOrAdd(typeof(T), new List<InterceptorWrapper<T>>());
            return (List<InterceptorWrapper<T>>)syncInterceptors;
        }

        /// <summary>
        /// Returns a boolean value indicating if we have any interceptors for the current type T
        /// </summary>
        public bool HasInterceptors<T>() where T : ProgressArgs => this.GetInterceptors<T>().Count > 0;

        /// <summary>
        /// Remove all interceptors based on type of ProgressArgs
        /// </summary>
        public void Clear<T>() where T : ProgressArgs => this.GetInterceptors<T>().Clear();

        /// <summary>
        /// Remove all interceptors 
        /// </summary>
        public void Clear() => this.internalInterceptors.Clear();

        /// <summary>
        /// Remove interceptor based on Id
        /// </summary>
        public void Clear(Guid id)
        {
            foreach (var interceptors in this.internalInterceptors)
            {
                var list = interceptors.Value;

                var i = list.Cast<ISyncInterceptor>().FirstOrDefault(i => i.Id == id);

                if (i != null)
                    list.Remove(i);
            }

        }

        /// <summary>
        /// Add an interceptor of T
        /// </summary>
        public Guid Add<T>(Action<T> action) where T : ProgressArgs
        {
            var interceptors = this.GetInterceptors<T>();
            var interceptor = new InterceptorWrapper<T>();
            interceptor.Set(action);
            interceptors.Add(interceptor);
            return interceptor.Id;
        }

        /// <summary>
        /// Add an async interceptor of T
        /// </summary>
        public Guid Add<T>(Func<T, Task> func) where T : ProgressArgs
        {
            var interceptors = this.GetInterceptors<T>();
            var interceptor = new InterceptorWrapper<T>();
            interceptor.Set(func);
            interceptors.Add(interceptor);
            return interceptor.Id;
        }

    }

}
