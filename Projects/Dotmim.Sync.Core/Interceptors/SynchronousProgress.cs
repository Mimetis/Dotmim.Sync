using System;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Text;
using System.Threading;

namespace Dotmim.Sync
{
    /// <summary>
    /// Provides an IProgress{T} that invokes callbacks for each reported progress value.
    /// </summary>
    /// <typeparam name="T">Specifies the type of the progress report value.</typeparam>
    /// <remarks>
    /// Any handler provided to the constructor or event handlers registered with
    /// the <see cref="ProgressChanged"/> event are invoked through a
    /// <see cref="System.Threading.SynchronizationContext"/> instance captured
    /// when the instance is constructed.  If there is no current SynchronizationContext
    /// at the time of construction, the callbacks will be invoked on the ThreadPool.
    /// </remarks>
    public class SynchronousProgress<T> : IProgress<T>
    {
        /// <summary>The synchronization context captured upon construction.  This will never be null.</summary>
        private readonly SynchronizationContext synchronizationContext;

        /// <summary>The handler specified to the constructor.  This may be null.</summary>
        private readonly Action<T> handler;

        /// <summary>A cached delegate used to post invocation to the synchronization context.</summary>
        private readonly SendOrPostCallback invokeHandlers;


        /// <summary>
        /// Initializes a new instance of the <see cref="SynchronousProgress{T}"/> class.
        /// </summary>
        public SynchronousProgress()
        {
            // Capture the current synchronization context.  "current" is determined by Current.
            // If there is no current context, we use a default instance targeting the ThreadPool.
            synchronizationContext = SynchronizationContext.Current ?? ProgressStatics.DefaultContext;
            Contract.Assert(synchronizationContext != null);
            invokeHandlers = new SendOrPostCallback(InvokeHandlers);
        }

        /// <summary>Initializes the <see cref="Progress{T}"/> with the specified callback.</summary>
        /// <param name="handler">
        /// A handler to invoke for each reported progress value.  This handler will be invoked
        /// in addition to any delegates registered with the <see cref="ProgressChanged"/> event.
        /// Depending on the <see cref="System.Threading.SynchronizationContext"/> instance captured by 
        /// could be invoked concurrently with itself.
        /// </param>
        /// <exception cref="System.ArgumentNullException">The <paramref name="handler"/> is null (Nothing in Visual Basic).</exception>
        public SynchronousProgress(Action<T> handler) : this()
        {
            if (handler == null) throw new ArgumentNullException("handler");
            this.handler = handler;
        }

        /// <summary>Raised for each reported progress value.</summary>
        /// <remarks>
        /// Handlers registered with this event will be invoked on the 
        /// <see cref="System.Threading.SynchronizationContext"/> captured when the instance was constructed.
        /// </remarks>
        public event EventHandler<T> ProgressChanged;

        /// <summary>Reports a progress change.</summary>
        /// <param name="value">The value of the updated progress.</param>
        protected virtual void OnReport(T value)
        {
            // If there's no handler, don't bother going through the [....] context.
            // Inside the callback, we'll need to check again, in case 
            // an event handler is removed between now and then.
            var handler = this.handler;
            var changedEvent = ProgressChanged;
            if (handler != null || changedEvent != null)
            {
                // Post the processing to the [....] context.
                // (If T is a value type, it will get boxed here.)
                synchronizationContext.Post(invokeHandlers, value);
            }
        }

        /// <summary>Reports a progress change.</summary>
        /// <param name="value">The value of the updated progress.</param>
        void IProgress<T>.Report(T value) => OnReport(value);

        /// <summary>Invokes the action and event callbacks.</summary>
        /// <param name="state">The progress value.</param>
        private void InvokeHandlers(object state)
        {
            var value = (T)state;

            var handler = this.handler;
            var changedEvent = ProgressChanged;

            handler?.Invoke(value);
            changedEvent?.Invoke(this, value);
        }
    }

    /// <summary>Holds static values for <see cref="Progress{T}"/>.</summary>
    /// <remarks>This avoids one static instance per type T.</remarks>
    internal static class ProgressStatics
    {
        /// <summary>A default synchronization context that targets the ThreadPool.</summary>
        internal static readonly SynchronizationContext DefaultContext = new SynchronizationContext();
    }
}
