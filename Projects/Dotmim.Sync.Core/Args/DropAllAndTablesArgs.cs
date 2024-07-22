using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Event args raised before a DropAll is performed.
    /// When you want to drop ALSO the tables, you need to confirm it.
    /// </summary>
    public class DropAllArgs : ProgressArgs
    {
        /// <inheritdoc cref="DropAllArgs" />
        public DropAllArgs(SyncContext context, DbConnection connection, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.ConfirmYouWantToDeleteTables = new Func<bool>(() =>
            {
                throw new Exception("You did not confirm you want to delete tables. Please use localOrchestrator.OnDropAll interceptor to confirm you agree to drop tables");
            });
        }

        /// <summary>
        /// Gets or Sets the confirmation function to confirm you want to delete all tables.
        /// </summary>
        public Func<bool> ConfirmYouWantToDeleteTables { get; set; }

        /// <summary>
        /// Return true if you want to delete all tables.
        /// </summary>
        internal bool Confirm() => this.ConfirmYouWantToDeleteTables();

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => 9876;
    }

    /// <summary>
    /// Partial Interceptors extensions.
    /// </summary>
    public partial class InterceptorsExtensions
    {

        /// <summary>
        /// Intercept the provider when a DropAll is called where you specified you want to drop the tables.
        /// </summary>
        public static Guid OnDropAll(this BaseOrchestrator orchestrator, Action<DropAllArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a DropAll is called where you specified you want to drop the tables.
        /// </summary>
        public static Guid OnDropAll(this BaseOrchestrator orchestrator, Func<DropAllArgs, Task> action)
            => orchestrator.AddInterceptor(action);
    }
}