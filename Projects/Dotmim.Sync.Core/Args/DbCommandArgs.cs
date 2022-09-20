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
        /// Occurs when a command is about to be executed on the underline provider
        /// <example>
        /// <code>
        /// agent.RemoteOrchestrator.OnExecuteCommand(args =>
        /// {
        ///     Console.WriteLine(args.Command.CommandText);
        /// });
        /// </code>
        /// </example>
        /// </summary>
        public static Guid OnExecuteCommand(this BaseOrchestrator orchestrator, Action<ExecuteCommandArgs> func)
            => orchestrator.AddInterceptor(func);

        /// <inheritdoc cref="OnExecuteCommand(BaseOrchestrator, Action{ExecuteCommandArgs})"/>
        public static Guid OnExecuteCommand(this BaseOrchestrator orchestrator, Func<ExecuteCommandArgs, Task> func)
            => orchestrator.AddInterceptor(func);

        /// <summary>
        /// Occurs every time we get a command from the underline provider
        /// <para>
        /// You can change the command text and even the parameters values if needed
        /// </para>
        /// <example>
        /// <code>
        /// agent.RemoteOrchestrator.OnGetCommand(args =>
        /// {
        ///     if (args.Command.CommandType == CommandType.StoredProcedure)
        ///     {
        ///         args.Command.CommandText = args.Command.CommandText.Replace("_filterproducts_", "_default_");
        ///     }
        /// });
        /// </code>
        /// </example>
        /// </summary>
        public static Guid OnGetCommand(this BaseOrchestrator orchestrator, Action<GetCommandArgs> func)
            => orchestrator.AddInterceptor(func);

        /// <inheritdoc cref="OnGetCommand(BaseOrchestrator, Action{GetCommandArgs})"/>
        public static Guid OnGetCommand(this BaseOrchestrator orchestrator, Func<GetCommandArgs, Task> func)
            => orchestrator.AddInterceptor(func);
    }
}
