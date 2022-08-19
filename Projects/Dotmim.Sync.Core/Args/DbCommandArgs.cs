using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Args
{
    public class DbCommandArgs : ProgressArgs
    {
        public DbCommandArgs(SyncContext context, DbCommand command, DbCommandType commandType, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.Command = command;
            this.CommandType = commandType;
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Sql;

        public override string Source => Connection.Database;
        public override string Message => $"[{Connection.Database}] Sql Statement:{Command.CommandText}.";

        public override int EventId => SyncEventsId.ConnectionOpen.Id;

        public DbCommand Command { get; }
        public DbCommandType CommandType { get; }
    }
    public static partial class InterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider action when changes are going to be applied on each table defined in the configuration schema
        /// </summary>
        public static Guid OnDbCommand(this BaseOrchestrator orchestrator, Action<DbCommandArgs> func)
            => orchestrator.AddInterceptor(func);
        /// <summary>
        /// Intercept the provider action when changes are going to be applied on each table defined in the configuration schema
        /// </summary>
        public static Guid OnDbCommand(this BaseOrchestrator orchestrator, Func<DbCommandArgs, Task> func)
            => orchestrator.AddInterceptor(func);

    }

}
