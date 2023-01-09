using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public class DropAllArgs : ProgressArgs
    {
        public DropAllArgs(SyncContext context, DbConnection connection, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.ConfirmYouWantToDeleteTables = new Func<bool>(() =>
            {
                throw new Exception("You did not confirm you want to delete tables. Please use localOrchestrator.OnDropAll interceptor to confirm you agree to drop tables");
            });
        }

        internal bool Confirm() => this.ConfirmYouWantToDeleteTables();

        public Func<bool> ConfirmYouWantToDeleteTables { get; set; }

        public override int EventId => SyncEventsId.DropAll.Id;
    }

    /// <summary>
    /// Partial interceptors extensions 
    /// </summary>
    public static partial class InterceptorsExtensions
    {

        /// <summary>
        /// Intercept the provider when a DropAll is called where you specified you want to drop the tables
        /// </summary>
        public static Guid OnDropAll(this BaseOrchestrator orchestrator, Action<DropAllArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a DropAll is called where you specified you want to drop the tables
        /// </summary>
        public static Guid OnDropAll(this BaseOrchestrator orchestrator, Func<DropAllArgs, Task> action)
            => orchestrator.AddInterceptor(action);


    }
    public static partial class SyncEventsId
    {
        public static EventId DropAll => CreateEventId(9876, nameof(DropAll));
    }
}
