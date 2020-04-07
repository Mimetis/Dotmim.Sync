using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    public class Interceptors
    {
        private readonly Dictionary<Type, ISyncInterceptor> dictionary = new Dictionary<Type, ISyncInterceptor>();

        public InterceptorWrapper<T> GetInterceptor<T>() where T : ProgressArgs
        {
            InterceptorWrapper<T> interceptor = null;
            var typeofT = typeof(T);

            // try get the interceptor from the dictionary and cast it
            if (this.dictionary.TryGetValue(typeofT, out var i))
                interceptor = (InterceptorWrapper<T>)i;

            // if null, create a new one
            if (interceptor == null)
            {
                interceptor = new InterceptorWrapper<T>();
                this.dictionary.Add(typeofT, interceptor);
            }

            return interceptor;
        }


    }
  
}
