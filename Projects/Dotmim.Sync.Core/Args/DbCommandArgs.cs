using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync.Args
{
    internal class DbCommandArgs : ProgressArgs
    {
        public DbCommandArgs(SyncContext context, DbCommand command, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.Command = command;
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Sql;

        public override string Source => Connection.Database;
        public override string Message => $"[{Connection.Database}] Sql Statement:{Command.CommandText}.";

        public override int EventId => SyncEventsId.ConnectionOpen.Id;

        public DbCommand Command { get; }
    }

}
