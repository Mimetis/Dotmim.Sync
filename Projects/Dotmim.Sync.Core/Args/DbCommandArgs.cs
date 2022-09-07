using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync
{


    public class GetCommandArgs : ProgressArgs
    {
        public GetCommandArgs(ScopeInfo scopeInfo, SyncContext context, DbCommand command, bool isBatch, SyncTable table, DbCommandType commandType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.ScopeInfo = scopeInfo;
            this.Command = command;
            this.IsBatch = isBatch;
            this.Table = table;
            this.CommandType = commandType;
            this.Filter = filter;
        }

        public ScopeInfo ScopeInfo { get; }
        public DbCommand Command { get; set; }
        public bool IsBatch { get; set; }
        public SyncTable Table { get; }
        public DbCommandType CommandType { get; }
        public SyncFilter Filter { get; }

        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Sql;
        public override string Source => Connection.Database;
        public override string Message => $"[{Connection.Database}] Sql Statement:{Command.CommandText}.";

    }

    public class ExecuteCommandArgs : ProgressArgs
    {
        public ExecuteCommandArgs(SyncContext context, DbCommand command, DbCommandType commandType, DbConnection connection, DbTransaction transaction)
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
        /// </summary>
        public static Guid OnExecuteCommand(this BaseOrchestrator orchestrator, Action<ExecuteCommandArgs> func)
            => orchestrator.AddInterceptor(func);
        /// <summary>
        /// </summary>
        public static Guid OnExecuteCommand(this BaseOrchestrator orchestrator, Func<ExecuteCommandArgs, Task> func)
            => orchestrator.AddInterceptor(func);

        /// <summary>
        /// </summary>
        public static Guid OnGetCommand(this BaseOrchestrator orchestrator, Action<GetCommandArgs> func)
            => orchestrator.AddInterceptor(func);

        /// <summary>
        /// </summary>
        public static Guid OnGetCommand(this BaseOrchestrator orchestrator, Func<GetCommandArgs, Task> func)
            => orchestrator.AddInterceptor(func);
    }
}
