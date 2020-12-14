using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync
{
    public class SchemaCreatedArgs : ProgressArgs
    {
        public SyncTable Table { get; }

        public SchemaCreatedArgs(SyncContext context, SyncTable table, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Table = table;
        }

        public override string Message => $"[{Connection.Database}] [{this.Table.SchemaName}] schema created.";

        public override int EventId => 43;
    }

    public class SchemaCreatingArgs : TableCreatedArgs
    {
        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; }

        public SchemaCreatingArgs(SyncContext context, SyncTable table, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, table, connection, transaction) => this.Command = command;

    }

    public class SchemaDroppedArgs : ProgressArgs
    {
        public SyncTable Table { get; }

        public SchemaDroppedArgs(SyncContext context, SyncTable table, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            Table = table;
        }

        public override string Message => $"[{Connection.Database}] [{Table.SchemaName}] schema dropped.";

        public override int EventId => 45;
    }

    public class SchemaDroppingArgs : TableDroppedArgs
    {
        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; }

        public SchemaDroppingArgs(SyncContext context, SyncTable table, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, table, connection, transaction)
        {
            this.Command = command;
        }

    }
}
