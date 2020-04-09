using Dotmim.Sync.Enumerations;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

//namespace Dotmim.Sync
//{

//    /// <summary>
//    /// Encapsulate 1 func to intercept one event
//    /// </summary>
//    public class InterceptorWrapper<T> : ISyncInterceptor<T> where T : ProgressArgs
//    {
//        /// <summary>The synchronization context captured upon construction.  This will never be null.</summary>
//        private readonly SynchronizationContext m_synchronizationContext;
//        private Func<T, Task> wrapper;
//        private static Func<T, Task> empty = new Func<T, Task>(t => Task.CompletedTask);
//        private readonly SendOrPostCallback m_invokeHandlers;

//        public InterceptorWrapper()
//        {
//            this.m_synchronizationContext = SynchronizationContext.Current ?? ProgressStatics.DefaultContext;
//            this.wrapper = empty;
//            this.m_invokeHandlers = new SendOrPostCallback(InvokeHandlers);
//        }

//        /// <summary>
//        /// Create a new interceptor wrapper accepting a Func<T, Task>
//        /// </summary>
//        public InterceptorWrapper(Func<T, Task> run) : this() => this.Set(run);

//        /// <summary>
//        /// Create a new interceptor wrapper accepting an Action<T>
//        /// </summary>
//        public InterceptorWrapper(Action<T> run) : this() => this.Set(run);


//        /// <summary>
//        /// Set a Func<T, Task> as interceptor
//        /// </summary>
//        public void Set(Func<T, Task> run) => this.wrapper = run != null ? run : empty;

//        /// <summary>
//        /// Set an Action<T> as interceptor
//        /// </summary>
//        public void Set(Action<T> run)
//        {
//            this.wrapper = run != null ? new Func<T, Task>(t =>
//            {
//                run(t);
//                return Task.CompletedTask;
//            }) : empty;

//        }

//        /// <summary>
//        /// Run the Action or Func as the Interceptor
//        /// </summary>
//        public Task RunAsync(T args, CancellationToken cancellationToken)
//        {
//            //await (this.wrapper == null ? Task.CompletedTask : this.wrapper(args));

//            var handler = this.wrapper;

//            if (handler != null)
//                m_synchronizationContext.Send(m_invokeHandlers, args);

//            if (cancellationToken.IsCancellationRequested)
//                cancellationToken.ThrowIfCancellationRequested();

//            return Task.CompletedTask;
//        }

//        private void InvokeHandlers(object state)
//        {
//            var value = (T)state;

//            var handler = this.wrapper;

//            handler?.Invoke(value);
//        }

//        public void Dispose()
//        {
//            this.Dispose(true);
//            GC.SuppressFinalize(this);
//        }

//        protected virtual void Dispose(bool cleanup) => this.wrapper = null;
//    }
//}

namespace Dotmim.Sync
{

    /// <summary>
    /// Encapsulate 1 func to intercept one event
    /// </summary>
    public class InterceptorWrapper<T> : ISyncInterceptor<T> where T : ProgressArgs
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
        public void Set(Action<T> run)
        {
            this.wrapper = run != null ? new Func<T, Task>(t =>
            {
                run(t);
                return Task.CompletedTask;
            }) : empty;

        }

        /// <summary>
        /// Run the Action or Func as the Interceptor
        /// </summary>
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

