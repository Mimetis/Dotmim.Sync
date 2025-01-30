using Dotmim.Sync.Enumerations;
using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Event args raised when a database is outdated. Always happens on the client side.
    /// </summary>
    public class OutdatedArgs : ProgressArgs
    {

        /// <inheritdoc cref="OutdatedArgs" />
        public OutdatedArgs(SyncContext context, ScopeInfoClient cScopeInfoClient, ScopeInfo sScopeInfo, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.ScopeInfoClientFromClient = cScopeInfoClient;
            this.ScopeInfoFromServer = sScopeInfo;
        }

        /// <summary>
        /// Gets or sets an action enumeration value for the action to handle the outdated peer.
        /// </summary>
        public OutdatedAction Action { get; set; } = OutdatedAction.Rollback;

        /// <summary>
        /// Gets the client scope info used to check if the client is outdated.
        /// </summary>
        public ScopeInfoClient ScopeInfoClientFromClient { get; }

        /// <summary>
        /// Gets the server scope info to check if client is outdated.
        /// </summary>
        public ScopeInfo ScopeInfoFromServer { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel" />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Information;

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override string Message => $"Database Out Dated. Last Client Sync Endpoint ({this.ScopeInfoClientFromClient.LastServerSyncTimestamp}) < Last Server Cleanup Metadatas ({this.ScopeInfoFromServer.LastCleanupTimestamp}).";

        /// <inheritdoc cref="ProgressArgs.EventId" />
        public override int EventId => 5000;
    }

    /// <summary>
    /// Outdated action enumeration.
    /// </summary>
    public enum OutdatedAction
    {
        /// <summary>
        /// Reinitialize the whole sync database, applying all rows from the server to the client.
        /// </summary>
        Reinitialize,

        /// <summary>
        /// Reinitialize the whole sync database, applying all rows from the server to the client, after trying a client upload.
        /// </summary>
        ReinitializeWithUpload,

        /// <summary>
        /// Rollback the synchronization request.
        /// </summary>
        Rollback,
    }

    /// <summary>
    /// Interceptor extension methods.
    /// </summary>
    public partial class InterceptorsExtensions
    {

        /// <summary>
        /// Intercept the provider action when a database is out dated. Always happens on the client side.
        /// <example>
        /// <code>
        /// agent.LocalOrchestrator.OnOutdated(oa =>
        /// {
        ///     Console.ForegroundColor = ConsoleColor.Red;
        ///     Console.WriteLine("local database is too old to synchronize with the server.");
        ///     Console.ResetColor();
        ///     Console.WriteLine("Do you want to synchronize anyway, and potentially lost data ? ");
        ///     Console.Write("Enter a value ('r' for reinitialize or 'ru' for reinitialize with upload): ");
        ///     var answer = Console.ReadLine();
        ///
        ///     if (answer.ToLowerInvariant() == "r")
        ///         oa.Action = OutdatedAction.Reinitialize;
        ///     else if (answer.ToLowerInvariant() == "ru")
        ///         oa.Action = OutdatedAction.ReinitializeWithUpload;
        ///
        /// });
        /// </code>
        /// </example>
        /// </summary>
        public static Guid OnOutdated(this BaseOrchestrator orchestrator, Action<OutdatedArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <inheritdoc cref="OnOutdated(BaseOrchestrator, Action{OutdatedArgs})"/>
        public static Guid OnOutdated(this BaseOrchestrator orchestrator, Func<OutdatedArgs, Task> action)
            => orchestrator.AddInterceptor(action);
    }
}