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
    public class ScopeArgs : BaseArgs
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
    }
}
