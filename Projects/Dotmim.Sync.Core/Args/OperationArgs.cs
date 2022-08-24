using System;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Microsoft.Extensions.Logging;

namespace Dotmim.Sync
{

    public class OperationArgs : ProgressArgs
    {
        public OperationArgs(SyncContext context, ScopeInfo serverScopeInfo, ScopeInfo clientScopeInfo, ScopeInfoClient scopeInfoClient, DbConnection connection = null, DbTransaction transaction = null)
        : base(context, connection, transaction)

        {
            this.ScopeInfoFromServer = serverScopeInfo;
            this.ScopeInfoFromClient = clientScopeInfo;
            this.ScopeInfoClient = scopeInfoClient;
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Information;

        public override string Source => Connection.Database;
        public override string Message => $"[{Connection.Database}] Overriding operation from server. Operation: {Operation}.";

        public SyncOperation Operation { get; set; }

        public override int EventId => SyncEventsId.Provisioned.Id;

        public ScopeInfo ScopeInfoFromServer { get; }
        public ScopeInfo ScopeInfoFromClient { get; }
        public ScopeInfoClient ScopeInfoClient { get; }
    }

    public static partial class InterceptorsExtensions
    {

        /// <summary>
        /// Intercept the provider before it begins a database provisioning
        /// </summary>
        public static Guid OnGettingOperation(this BaseOrchestrator orchestrator, Action<OperationArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider before it begins a database provisioning
        /// </summary>
        public static Guid OnGettingOperation(this BaseOrchestrator orchestrator, Func<OperationArgs, Task> action)
            => orchestrator.AddInterceptor(action);

      

    }

    public static partial class SyncEventsId
    {
        public static EventId Operation => CreateEventId(5200, nameof(Operation));
    }



}