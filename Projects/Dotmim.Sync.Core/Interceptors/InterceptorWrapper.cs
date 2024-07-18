using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Encapsulate 1 func to intercept one event.
    /// </summary>
    public class InterceptorWrapper<T> : ISyncInterceptor<T>
        where T : class
    {
        private static Func<T, Task> empty = new(t => Task.CompletedTask);

        private Func<T, Task> wrapperAsync;

        /// <summary>
        /// Initializes a new instance of the <see cref="InterceptorWrapper{T}"/> class.
        /// </summary>
        public InterceptorWrapper()
        {
            this.Id = Guid.NewGuid();
            this.wrapperAsync = empty;
        }

        /// <summary>
        /// Gets a value indicating whether the interceptor is empty.
        /// </summary>
        public bool IsEmpty => this.wrapperAsync == empty;

        /// <summary>
        /// Gets the Interceptor Id.
        /// </summary>
        public Guid Id { get; }

        /// <summary>
        /// Set a Func as interceptor.
        /// </summary>
        public void Set(Func<T, Task> run) => this.wrapperAsync = run != null ? run : empty;

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
            }) : empty;
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

        /// <summary>
        /// Dispose the interceptor.
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Dispose the interceptor.
        /// </summary>
        protected virtual void Dispose(bool cleanup) => this.wrapperAsync = null;
    }
}