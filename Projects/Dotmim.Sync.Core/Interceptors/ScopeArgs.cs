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
        public ScopeLoadingArgs(SyncContext context, string scopeName, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.ScopeName = scopeName;
        }

        /// <summary>
        /// Gets the scope name to load from the client database
        /// </summary>
        public string ScopeName { get; }

        public override string Message => this.ScopeName;

    }
    /// <summary>
    /// Args generated when a scope has been loaded
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

        public override string Message => $"Id:{ScopeInfo?.Id} LastSync:{ScopeInfo?.LastSync} " +
            $"LastSyncDuration:{ScopeInfo?.LastSyncDuration} ";

    }

    /// <summary>
    /// Args generated before and after a scope has been applied
    /// </summary>
    public class ServerScopeArgs : ProgressArgs
    {
        public ServerScopeArgs(SyncContext context, ServerScopeInfo scope, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.ScopeInfo = scope;
        }

        /// <summary>
        /// Gets the current scope from the local database
        /// </summary>
        public ServerScopeInfo ScopeInfo { get; }

        public override string Message => $"Server scope name:{ScopeInfo?.Name}";

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

        public override string Message => $"Scope name:{ScopeInfo?.Name}";

    }
}
