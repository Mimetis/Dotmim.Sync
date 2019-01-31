using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync
{
    /// <summary>
    /// Args generated before and after a scope has been applied
    /// </summary>
    public class ScopeArgs : ProgressArgs
    {
        public ScopeArgs(SyncContext context, ScopeInfo scope, DbConnection connection, DbTransaction transaction) 
            : base(context, connection, transaction)
        {
            this.ScopeInfo = scope;
        }

        /// <summary>
        /// Gets the current scope from the local database
        /// </summary>
        public ScopeInfo ScopeInfo { get; }

        public override string Message => $"Id:{ScopeInfo.Id} LastSync:{ScopeInfo.LastSync} " +
            $"LastSyncDuration:{ScopeInfo.LastSyncDuration} " +
            $"SyncState:{ScopeInfo.SyncState}";

    }
}
