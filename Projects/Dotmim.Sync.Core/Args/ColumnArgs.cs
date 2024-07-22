using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Event args raised when a column has been created.
    /// </summary>
    public class ColumnCreatedArgs : ProgressArgs
    {

        /// <inheritdoc cref = "ColumnCreatedArgs" />
        public ColumnCreatedArgs(SyncContext context, string columnName, SyncTable table, ParserName tableName, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.TableName = tableName;
            this.ColumnName = columnName;
            this.Table = table;
        }

        /// <summary>
        /// Gets the column name.
        /// </summary>
        public string ColumnName { get; }

        /// <summary>
        /// Gets the table.
        /// </summary>
        public SyncTable Table { get; }

        /// <summary>
        /// Gets the table name, parsed.
        /// </summary>
        public ParserName TableName { get; }

        /// <summary>
        /// Gets the sync progress level.
        /// </summary>
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message => $"[{this.ColumnName}] Added.";

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => 12350;
    }

    /// <summary>
    /// Event args raised when a column is creating.
    /// </summary>
    public class ColumnCreatingArgs : ProgressArgs
    {
        /// <inheritdoc cref = "ColumnCreatingArgs" />
        public ColumnCreatingArgs(SyncContext context, string columnName, SyncTable table, ParserName tableName, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
       : base(context, connection, transaction)
        {
            this.ColumnName = columnName;
            this.Table = table;
            this.TableName = tableName;
            this.Command = command;
        }

        /// <summary>
        /// Gets or sets a value indicating whether gets or sets if the column creation should be canceled.
        /// </summary>
        public bool Cancel { get; set; }

        /// <summary>
        /// Gets or sets the command used to create the column.
        /// </summary>
        public DbCommand Command { get; set; }

        /// <summary>
        /// Gets the column name.
        /// </summary>
        public string ColumnName { get; }

        /// <summary>
        /// Gets the table.
        /// </summary>
        public SyncTable Table { get; }

        /// <summary>
        /// Gets the table name, parsed.
        /// </summary>
        public ParserName TableName { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel"/>
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message => $"[{this.ColumnName}] Adding.";

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => 12300;
    }

    /// <summary>
    /// Event args raised when a column is dropped.
    /// </summary>
    public class ColumnDroppedArgs : ProgressArgs
    {
        /// <inheritdoc cref = "ColumnDroppedArgs" />
        public ColumnDroppedArgs(SyncContext context, string columnName, SyncTable table, ParserName tableName, DbConnection connection = null, DbTransaction transaction = null)
         : base(context, connection, transaction)
        {
            this.TableName = tableName;
            this.ColumnName = columnName;
            this.Table = table;
        }

        /// <summary>
        /// Gets the column name.
        /// </summary>
        public string ColumnName { get; }

        /// <summary>
        /// Gets the table.
        /// </summary>
        public SyncTable Table { get; }

        /// <summary>
        /// Gets the table name, parsed.
        /// </summary>
        public ParserName TableName { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel"/>
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message => $"[{this.ColumnName}] Dropped.";

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => 12450;
    }

    /// <summary>
    /// Event args raised when a column is dropping.
    /// </summary>
    public class ColumnDroppingArgs : ProgressArgs
    {

        /// <inheritdoc cref = "ColumnDroppingArgs" />
        public ColumnDroppingArgs(SyncContext context, string columnName, SyncTable table, ParserName tableName, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Command = command;
            this.TableName = tableName;
            this.ColumnName = columnName;
            this.Table = table;
        }

        /// <summary>
        /// Gets or sets a value indicating whether gets or sets if the column dropping should be canceled.
        /// </summary>
        public bool Cancel { get; set; }

        /// <summary>
        /// Gets or sets the command used to drop the column.
        /// </summary>
        public DbCommand Command { get; set; }

        /// <summary>
        /// Gets the column name.
        /// </summary>
        public string ColumnName { get; }

        /// <summary>
        /// Gets the table.
        /// </summary>
        public SyncTable Table { get; }

        /// <summary>
        /// Gets the table name, parsed.
        /// </summary>
        public ParserName TableName { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel"/>
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message => $"[{this.ColumnName}] Dropping.";

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => 12400;
    }

    /// <summary>
    /// Partial Interceptors extensions.
    /// </summary>
    public partial class InterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider when a column is creating.
        /// </summary>
        public static Guid OnColumnCreating(this BaseOrchestrator orchestrator, Action<ColumnCreatingArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a column is creating.
        /// </summary>
        public static Guid OnColumnCreating(this BaseOrchestrator orchestrator, Func<TableCreatingArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a column is created.
        /// </summary>
        public static Guid OnColumnCreated(this BaseOrchestrator orchestrator, Action<ColumnCreatedArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a column is created.
        /// </summary>
        public static Guid OnColumnCreated(this BaseOrchestrator orchestrator, Func<ColumnCreatedArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a column is dropping.
        /// </summary>
        public static Guid OnColumnDropping(this BaseOrchestrator orchestrator, Action<ColumnDroppingArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a column is dropping.
        /// </summary>
        public static Guid OnColumnDropping(this BaseOrchestrator orchestrator, Func<ColumnDroppingArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a column is dropped.
        /// </summary>
        public static Guid OnColumnDropped(this BaseOrchestrator orchestrator, Action<ColumnDroppedArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a column is dropped.
        /// </summary>
        public static Guid OnColumnDropped(this BaseOrchestrator orchestrator, Func<ColumnDroppedArgs, Task> action)
            => orchestrator.AddInterceptor(action);
    }
}