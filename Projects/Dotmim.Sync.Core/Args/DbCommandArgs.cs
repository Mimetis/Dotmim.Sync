using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Event args generated after a command has been retrieved from a provider.
    /// </summary>
    public class GetCommandArgs : ProgressArgs
    {
        /// <inheritdoc cref="GetCommandArgs"/>
        public GetCommandArgs(ScopeInfo scopeInfo, SyncContext context, DbCommand command, bool isBatch, SyncTable table, DbCommandType commandType, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.ScopeInfo = scopeInfo;
            this.Command = command;
            this.IsBatch = isBatch;
            this.Table = table;
            this.CommandType = commandType;
        }

        /// <summary>
        /// Gets the scope info.
        /// </summary>
        public ScopeInfo ScopeInfo { get; }

        /// <summary>
        /// Gets or sets the command to be executed.
        /// </summary>
        public DbCommand Command { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the command is a batch command.
        /// </summary>
        public bool IsBatch { get; set; }

        /// <summary>
        /// Gets the table.
        /// </summary>
        public SyncTable Table { get; }

        /// <summary>
        /// Gets the command type to be executed.
        /// </summary>
        public DbCommandType CommandType { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel"/>
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Sql;

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message => $"Sql Statement:{this.Command.CommandText}.";
    }

    /// <summary>
    /// Event args generated before a command is executed.
    /// </summary>
    public class ExecuteCommandArgs : ProgressArgs
    {
        /// <inheritdoc cref="ExecuteCommandArgs"/>
        public ExecuteCommandArgs(SyncContext context, DbCommand command, DbCommandType commandType, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.Command = command;
            this.CommandType = commandType;
        }

        /// <summary>
        /// Gets the command to be executed.
        /// </summary>
        public DbCommand Command { get; }

        /// <summary>
        /// Gets the command type to be executed.
        /// </summary>
        public DbCommandType CommandType { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel"/>
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Sql;

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message => $"Sql Statement:{this.Command.CommandText}.";

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => SyncEventsId.ConnectionOpen.Id;
    }

    /// <summary>
    /// Interceptors extensions.
    /// </summary>
    public partial class InterceptorsExtensions
    {
        /// <summary>
        /// Occurs when a command is about to be executed on the underline provider.
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
        /// Occurs every time we get a command from the underline provider.
        /// <para>
        /// You can change the command text and even the parameters values if needed.
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