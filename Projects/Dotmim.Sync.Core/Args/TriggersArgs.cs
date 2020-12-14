using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync
{
    public class TriggerCreatedArgs : ProgressArgs
    {
        public SyncTable Table { get; }
        public DbTriggerType TriggerType { get; }

        public TriggerCreatedArgs(SyncContext context, SyncTable table, DbTriggerType triggerType, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Table = table;
            this.TriggerType = triggerType;
        }

        public override string Message => $"[{Connection.Database}] [{this.Table.GetFullName()}] trigger [{this.TriggerType}] created.";

        public override int EventId => 43;
    }

    public class TriggerCreatingArgs : TriggerCreatedArgs
    {
        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; }

        public TriggerCreatingArgs(SyncContext context, SyncTable table, DbTriggerType triggerType, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, table, triggerType, connection, transaction) => this.Command = command;

    }

    public class TriggerDroppedArgs : ProgressArgs
    {
        public SyncTable Table { get; }
        public DbTriggerType TriggerType { get; }

        public TriggerDroppedArgs(SyncContext context, SyncTable table, DbTriggerType triggerType, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            Table = table;
            this.TriggerType = triggerType;
        }

        public override string Message => $"[{Connection.Database}] [{Table.GetFullName()}] trigger [{this.TriggerType}] dropped.";

        public override int EventId => 45;
    }

    public class TriggerDroppingArgs : TriggerDroppedArgs
    {
        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; }

        public TriggerDroppingArgs(SyncContext context, SyncTable table, DbTriggerType triggerType, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, table, triggerType, connection, transaction)
        {
            this.Command = command;
        }

    }
}
