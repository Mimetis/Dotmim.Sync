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
        private Func<T, Task> wrapper;
        private static Func<T, Task> empty = new Func<T, Task>(t => Task.CompletedTask);

        /// <summary>
        /// Create a new interceptor wrapper accepting a Func<T, Task>
        /// </summary>
        public InterceptorWrapper(Func<T, Task> run) => this.Set(run);

        /// <summary>
        /// Create a new interceptor wrapper accepting an Action<T>
        /// </summary>
        public InterceptorWrapper(Action<T> run) => this.Set(run);

        /// <summary>
        /// Create a new empty interceptor
        /// </summary>
        public InterceptorWrapper() => this.wrapper = empty;

        /// <summary>
        /// Set a Func<T, Task> as interceptor
        /// </summary>
        public void Set(Func<T, Task> run) => this.wrapper = run != null ? run : empty;

        /// <summary>
        /// Set an Action<T> as interceptor
        /// </summary>
        [DebuggerStepThrough]
        public void Set(Action<T> run)
        {
            this.wrapper = run != null ? (t =>
            {
                run(t);
                return Task.CompletedTask;
            }) : empty;

        }

        /// <summary>
        /// Run the Action or Func as the Interceptor
        /// </summary>
        [DebuggerStepThrough]
        public async Task RunAsync(T args, CancellationToken cancellationToken)
        {
            await (this.wrapper == null ? Task.CompletedTask : this.wrapper(args));

            if (cancellationToken.IsCancellationRequested)
                cancellationToken.ThrowIfCancellationRequested();
        }

        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool cleanup) => this.wrapper = null;
    }
}

