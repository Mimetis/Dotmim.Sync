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
    public class InterceptorWrapper<T> : ISyncInterceptor<T>
        where T : class
    {
        private Func<T, Task> wrapperAsync;
        internal static Func<T, Task> Empty = new Func<T, Task>(t => Task.CompletedTask);

        /// <summary>
        /// Initializes a new instance of the <see cref="InterceptorWrapper{T}"/> class.
        /// </summary>
        public InterceptorWrapper()
        {
            this.Id = Guid.NewGuid();
            this.wrapperAsync = Empty;
        }

        /// <summary>
        /// Gets a value indicating whether the interceptor is empty
        /// </summary>
        public bool IsEmpty => this.wrapperAsync == Empty;

        /// <summary>
        /// Gets the Interceptor Id.
        /// </summary>
        public Guid Id { get; }

        /// <summary>
        /// Set a Func as interceptor.
        /// </summary>
        public void Set(Func<T, Task> run) => this.wrapperAsync = run != null ? run : Empty;

        /// <summary>
        /// Set an Action as interceptor.
        /// </summary>
        [DebuggerStepThrough]
        public void Set(Action<T> run)
        {
            this.wrapperAsync = run != null ? (t =>
            {
                run(t);
                return Task.CompletedTask;
            }) : Empty;

        }

        /// <summary>
        /// Run the Action or Func as the Interceptor.
        /// </summary>
        [DebuggerStepThrough]
        public async Task RunAsync(T args, CancellationToken cancellationToken)
        {
            if (this.wrapperAsync != null)
                await this.wrapperAsync(args).ConfigureAwait(false);

            if (cancellationToken.IsCancellationRequested)
                cancellationToken.ThrowIfCancellationRequested();
        }

        public void Dispose()
        {
            this.Dispose(true);
            //GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool cleanup) => this.wrapperAsync = null;
    }
}

