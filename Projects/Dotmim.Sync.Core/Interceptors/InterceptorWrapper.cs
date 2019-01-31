using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Encapsulate 1 func to intercept one event
    /// </summary>
    public class InterceptorWrapper<T> : ISyncInterceptor<T> where T : ProgressArgs
    {
        private Func<T, Task> run;
        private static Func<T, Task> emptyRun = new Func<T, Task>(t => Task.CompletedTask);
        private Type typeOfT;

        /// <summary>
        /// Create a new interceptor wrapper accepting to Func, first used before and second used after after
        /// </summary>
        public InterceptorWrapper(Func<T, Task> run) => this.Set(run);

        /// <summary>
        /// Create a new empty interceptor
        /// </summary>
        public InterceptorWrapper() => this.Set(emptyRun);


        public void Set(Func<T, Task> run)
        {
            var newTypeOfT = typeof(T);

            if (this.typeOfT == null || this.typeOfT != newTypeOfT || this.run != run)
            {
                this.run = run;
                this.typeOfT = typeof(T);
            }
        }

        public async Task RunAsync(T args)
        {
            if (run == null)
            {
                await Task.CompletedTask;
            }
            else
            {
                await run(args);

                if (args.Action == ChangeApplicationAction.Rollback)
                    CoreProvider.RaiseRollbackException(args.Context, "Rollback by user during a progress event");
            }
        }

        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool cleanup)
        {
            this.run = null;
            this.typeOfT = null;
        }
    }
}
