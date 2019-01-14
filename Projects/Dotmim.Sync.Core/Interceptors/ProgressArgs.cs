using Dotmim.Sync.Enumerations;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;

namespace Dotmim.Sync
{
    /// <summary>
    /// General sync progress. Only return a full string line if specified
    /// </summary>
    public class ProgressArgs : BaseArgs
    {
        public ProgressArgs(SyncContext context, string message, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction) => this.Message = message;

        public ProgressArgs(SyncContext context, DbConnection connection, DbTransaction transaction)
        : base(context, connection, transaction) { }

        public string Message { get; set; }
    }
}
