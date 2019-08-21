using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync
{

    /// <summary>
    /// Event args generated when a connection is opened
    /// </summary>
    public class ConnectionOpenArgs : ProgressArgs
    {
        public ConnectionOpenArgs(SyncContext context, DbConnection connection)
            : base(context, connection)
        {
        }

        public override string Message => $"";

    }

    /// <summary>
    /// Event args generated when a connection is closed 
    /// </summary>
    public class ConnectionCloseArgs : ProgressArgs
    {
        public ConnectionCloseArgs(SyncContext context, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
        }

        public override string Message => $"";

    }

    /// <summary>
    /// Event args generated when a transaction is opened
    /// </summary>
    public class TransactionOpenArgs : ProgressArgs
    {
        public TransactionOpenArgs(SyncContext context, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
        }

        public override string Message => $"";

    }

    /// <summary>
    /// Event args generated when a transaction is commit
    /// </summary>
    public class TransactionCommitArgs : ProgressArgs
    {
        public TransactionCommitArgs(SyncContext context, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
        }

        public override string Message => $"";

    }


    /// <summary>
    /// Event args generated during BeginSession stage
    /// </summary>
    public class SessionBeginArgs : ProgressArgs
    {
        public SessionBeginArgs(SyncContext context, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
        }

        public override string Message => $"";

    }
    /// <summary>
    /// Event args generated during EndSession stage
    /// </summary>
    public class SessionEndArgs : ProgressArgs
    {
        public SessionEndArgs(SyncContext context, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
        }

        public override string Message => $"";

    }
}
