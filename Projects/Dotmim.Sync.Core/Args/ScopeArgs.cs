using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync
{
    /// <summary>
    /// Args generated when a scope is about to be loaded
    /// </summary>
    public class ScopeLoadingArgs : ProgressArgs
    {
        public ScopeLoadingArgs(SyncContext context, string scopeName, string scopeTableInfoName, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.ScopeName = scopeName;
            this.ScopeTableInfoName = scopeTableInfoName;
        }

        /// <summary>
        /// Gets the scope name to load from the client database
        /// </summary>
        public string ScopeName { get; }

        /// <summary>
        /// Gets the table where the scope will be loaded from.
        /// </summary>
        public string ScopeTableInfoName { get; }

        public override string Message => $"Loading scope {this.ScopeName} from table {this.ScopeTableInfoName}";

    }

    /// <summary>
    /// Args generated when a scope has been loaded from client database
    /// </summary>
    public class ScopeLoadedArgs : ProgressArgs
    {
        public ScopeLoadedArgs(SyncContext context, ScopeInfo scope, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.ScopeInfo = scope;
        }

        /// <summary>
        /// Gets the current scope from the local database
        /// </summary>
        public ScopeInfo ScopeInfo { get; }

        public override string Message => $"[{Connection.Database}] [{ScopeInfo?.Name}] [Version {ScopeInfo.Version}] Last sync:{ScopeInfo?.LastSync} Last sync duration:{ScopeInfo?.GetLastSyncDurationString()} ";
    }

    /// <summary>
    /// Args generated before and after a scope has been applied
    /// </summary>
    public class ServerScopeLoadedArgs : ProgressArgs
    {
        public ServerScopeLoadedArgs(SyncContext context, ServerScopeInfo scope, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.ScopeInfo = scope;
        }

        /// <summary>
        /// Gets the current scope from the local database
        /// </summary>
        public ServerScopeInfo ScopeInfo { get; }

        public override string Message => $"[{Connection.Database}] [{ScopeInfo?.Name}] [Version {ScopeInfo.Version}]";

    }


    /// <summary>
    /// Args generated before and after a scope has been applied
    /// </summary>
    public class ServerHistoryScopeArgs : ProgressArgs
    {
        public ServerHistoryScopeArgs(SyncContext context, ServerHistoryScopeInfo scope, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.ScopeInfo = scope;
        }

        /// <summary>
        /// Gets the current scope from the local database
        /// </summary>
        public ServerHistoryScopeInfo ScopeInfo { get; }

        public override string Message => $"[{Connection.Database}] [{ScopeInfo?.Name}]";

    }
}
