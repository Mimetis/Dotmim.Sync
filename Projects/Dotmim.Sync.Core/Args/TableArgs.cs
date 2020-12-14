using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync
{
    public class TableCreatedArgs : ProgressArgs
    {
        public SyncTable Table { get; }

        public TableCreatedArgs(SyncContext context, SyncTable table, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Table = table;
        }

        public override string Message => $"[{Connection.Database}] [{this.Table.GetFullName()}] table created.";

        public override int EventId => 43;
    }

    public class TableCreatingArgs : TableCreatedArgs
    {
        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; }

        public TableCreatingArgs(SyncContext context, SyncTable table, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, table, connection, transaction) => this.Command = command;

    }

    public class TableDroppedArgs : ProgressArgs
    {
        public SyncTable Table { get; }

        public TableDroppedArgs(SyncContext context, SyncTable table, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            Table = table;
        }

        public override string Message => $"[{Connection.Database}] [{Table.GetFullName()}] table dropped.";

        public override int EventId => 45;
    }

    public class TableDroppingArgs : TableDroppedArgs
    {
        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; }

        public TableDroppingArgs(SyncContext context, SyncTable table, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, table, connection, transaction)
        {
            this.Command = command;
        }

    }
}
