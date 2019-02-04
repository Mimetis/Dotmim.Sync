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
        private Func<T, Task> runFunc;
        private Action<T> runAction;
        private static Func<T, Task> emptyRunFunc = new Func<T, Task>(t => Task.CompletedTask);
        private static Action<T> emptyRunAction = new Action<T>(t => { });
        private Type typeOfT;
        private bool isAction = false;

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
        public InterceptorWrapper()
        {
            this.Set(emptyRunFunc);
            this.Set(emptyRunAction);

        }


        public void Set(Func<T, Task> run)
        {
            var newTypeOfT = typeof(T);

            if (this.typeOfT == null || this.typeOfT != newTypeOfT || this.runFunc != run)
            {
                this.isAction = false;
                this.runFunc = run;
                this.typeOfT = typeof(T);
            }
        }

        public void Set(Action<T> run)
        {
            var newTypeOfT = typeof(T);

            if (this.typeOfT == null || this.typeOfT != newTypeOfT || this.runAction != run)
            {
                this.isAction = true;
                this.runAction = run;
                this.typeOfT = typeof(T);
            }
        }

        public async Task RunAsync(T args)
        {
            if (runFunc == null && runAction == null)
            {
                await Task.CompletedTask;
            }
            else
            {
                if (this.isAction)
                {
                    runAction(args);
                }
                else
                {
                    await runFunc(args);

                }

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
            this.runFunc = null;
            this.runAction = null;
            this.typeOfT = null;
        }
    }
}
