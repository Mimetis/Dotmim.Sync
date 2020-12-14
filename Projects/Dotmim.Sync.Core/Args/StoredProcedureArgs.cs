using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync
{
    public class StoredProcedureCreatedArgs : ProgressArgs
    {
        public SyncTable Table { get; }
        public DbStoredProcedureType StoredProcedureType { get; }

        public StoredProcedureCreatedArgs(SyncContext context, SyncTable table, DbStoredProcedureType StoredProcedureType, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Table = table;
            this.StoredProcedureType = StoredProcedureType;
        }

        public override string Message => $"[{Connection.Database}] [{this.Table.GetFullName()}] StoredProcedure [{this.StoredProcedureType}] created.";

        public override int EventId => 43;
    }

    public class StoredProcedureCreatingArgs : StoredProcedureCreatedArgs
    {
        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; }

        public StoredProcedureCreatingArgs(SyncContext context, SyncTable table, DbStoredProcedureType StoredProcedureType, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, table, StoredProcedureType, connection, transaction) => this.Command = command;

    }

    public class StoredProcedureDroppedArgs : ProgressArgs
    {
        public SyncTable Table { get; }
        public DbStoredProcedureType StoredProcedureType { get; }

        public StoredProcedureDroppedArgs(SyncContext context, SyncTable table, DbStoredProcedureType StoredProcedureType, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            Table = table;
            this.StoredProcedureType = StoredProcedureType;
        }

        public override string Message => $"[{Connection.Database}] [{Table.GetFullName()}] StoredProcedure [{this.StoredProcedureType}] dropped.";

        public override int EventId => 45;
    }

    public class StoredProcedureDroppingArgs : StoredProcedureDroppedArgs
    {
        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; }

        public StoredProcedureDroppingArgs(SyncContext context, SyncTable table, DbStoredProcedureType StoredProcedureType, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, table, StoredProcedureType, connection, transaction)
        {
            this.Command = command;
        }

    }
}
