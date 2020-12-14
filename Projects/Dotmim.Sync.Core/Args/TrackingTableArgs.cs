using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync
{
    public class TrackingTableCreatedArgs : ProgressArgs
    {
        public SyncTable Table { get; }

        public TrackingTableCreatedArgs(SyncContext context, SyncTable table, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Table = table;
        }

        public override string Message => $"[{Connection.Database}] [{this.Table.GetFullName()}] tracking table created.";

        public override int EventId => 43;
    }

    public class TrackingTableCreatingArgs : TableCreatedArgs
    {
        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; }

        public TrackingTableCreatingArgs(SyncContext context, SyncTable table, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, table, connection, transaction) => this.Command = command;

    }

    public class TrackingTableDroppedArgs : ProgressArgs
    {
        public SyncTable Table { get; }

        public TrackingTableDroppedArgs(SyncContext context, SyncTable table, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            Table = table;
        }

        public override string Message => $"[{Connection.Database}] [{Table.GetFullName()}] tracking table dropped.";

        public override int EventId => 45;
    }

    public class TrackingTableDroppingArgs : TableDroppedArgs
    {
        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; }

        public TrackingTableDroppingArgs(SyncContext context, SyncTable table, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, table, connection, transaction)
        {
            this.Command = command;
        }

    }
}
