using Dotmim.Sync.Enumerations;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Encapsulate 1 func to intercept one event
    /// </summary>
    public class InterceptorWrapper<T> : ISyncInterceptor<T> where T : class
    {
        private Func<T, Task> wrapperAsync;
        private static Func<T, Task> empty = new Func<T, Task>(t => Task.CompletedTask);


        /// <summary>
        /// Create a new empty interceptor
        /// </summary>
        public InterceptorWrapper() => this.wrapperAsync = empty;

        /// <summary>
        /// Set a Func<T, Task> as interceptor
        /// </summary>
        public void Set(Func<T, Task> run) => this.wrapperAsync = run != null ? run : empty;

        /// <summary>
        /// Set an Action<T> as interceptor
        /// </summary>
        [DebuggerStepThrough]
        public void Set(Action<T> run)
        {
            this.wrapperAsync = run != null ? (t =>
            {
                Debug.WriteLine("Begin Run");
                
                run(t);
                
                Debug.WriteLine("End Run");
                
                return Task.CompletedTask;
            }) : empty;

        }

        /// <summary>
        /// Run the Action or Func as the Interceptor
        /// </summary>
        [DebuggerStepThrough]
        public async Task RunAsync(T args, CancellationToken cancellationToken)
        {
            if (this.wrapperAsync != null)
                await this.wrapperAsync(args);

            if (cancellationToken.IsCancellationRequested)
                cancellationToken.ThrowIfCancellationRequested();
        }

        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool cleanup) => this.wrapperAsync = null;
    }
}

