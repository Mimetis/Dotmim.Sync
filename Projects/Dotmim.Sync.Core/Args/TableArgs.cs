using Dotmim.Sync.Enumerations;
using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Event args generated when a schema is created.
    /// </summary>
    public class SchemaNameCreatedArgs : ProgressArgs
    {
        /// <inheritdoc cref="SchemaNameCreatedArgs" />
        public SchemaNameCreatedArgs(SyncContext context, ScopeInfo scopeInfo, SyncTable table, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.ScopeInfo = scopeInfo;
            this.Table = table;
        }

        /// <summary>
        /// Gets the scope info on which the schema is created.
        /// </summary>
        public ScopeInfo ScopeInfo { get; }

        /// <summary>
        /// Gets the table on which the schema is created.
        /// </summary>
        public SyncTable Table { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel" />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override string Message => $"[{this.Table.SchemaName}] Schema Created.";

        /// <inheritdoc cref="ProgressArgs.EventId" />
        public override int EventId => 12050;
    }

    /// <summary>
    /// Event args generated when a schema is creating.
    /// </summary>
    public class SchemaNameCreatingArgs : ProgressArgs
    {

        /// <inheritdoc cref="SchemaNameCreatingArgs" />
        public SchemaNameCreatingArgs(SyncContext context, ScopeInfo scopeInfo, SyncTable table, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.ScopeInfo = scopeInfo;
            this.Table = table;
            this.Command = command;
        }

        /// <summary>
        /// Gets or sets a value indicating whether gets or sets if the operation should be canceled.
        /// </summary>
        public bool Cancel { get; set; }

        /// <summary>
        /// Gets or sets the command used to create the schema.
        /// </summary>
        public DbCommand Command { get; set; }

        /// <summary>
        /// Gets the scope info on which the schema is creating.
        /// </summary>
        public ScopeInfo ScopeInfo { get; }

        /// <summary>
        /// Gets the table on which the schema is creating.
        /// </summary>
        public SyncTable Table { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel" />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override string Message => $"[{this.Table.SchemaName}] Schema Creating.";

        /// <inheritdoc cref="ProgressArgs.EventId" />
        public override int EventId => 12000;
    }

    /// <summary>
    /// Event args generated when a table is created.
    /// </summary>
    public class TableCreatedArgs : ProgressArgs
    {
        /// <inheritdoc cref="TableCreatedArgs" />
        public TableCreatedArgs(SyncContext context, ScopeInfo scopeInfo, SyncTable table, string tableFullName, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.TableFullName = tableFullName;
            this.ScopeInfo = scopeInfo;
            this.Table = table;
        }

        /// <summary>
        /// Gets the scope info on which the table is created.
        /// </summary>
        public ScopeInfo ScopeInfo { get; }

        /// <summary>
        /// Gets the table created.
        /// </summary>
        public SyncTable Table { get; }

        /// <summary>
        /// Gets the table name created.
        /// </summary>
        public string TableFullName { get; }

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override string Message => $"[{this.Table.GetFullName()}] Table Created.";

        /// <inheritdoc cref="ProgressArgs.ProgressLevel" />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        /// <inheritdoc cref="ProgressArgs.EventId" />
        public override int EventId => 12150;
    }

    /// <summary>
    /// Event args generated when a table is creating.
    /// </summary>
    public class TableCreatingArgs : ProgressArgs
    {
        /// <inheritdoc cref="TableCreatingArgs" />
        public TableCreatingArgs(SyncContext context, ScopeInfo scopeInfo, SyncTable table, string tableFullName, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.ScopeInfo = scopeInfo;
            this.Table = table;
            this.TableFullName = tableFullName;
            this.Command = command;
        }

        /// <summary>
        /// Gets or sets a value indicating whether gets or sets if the operation should be canceled.
        /// </summary>
        public bool Cancel { get; set; }

        /// <summary>
        /// Gets or sets the command used to create the table.
        /// </summary>
        public DbCommand Command { get; set; }

        /// <summary>
        /// Gets the scope info on which the table is creating.
        /// </summary>
        public ScopeInfo ScopeInfo { get; }

        /// <summary>
        /// Gets the table creating.
        /// </summary>
        public SyncTable Table { get; }

        /// <summary>
        /// Gets the table name creating.
        /// </summary>
        public string TableFullName { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel" />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override string Message => $"[{this.Table.GetFullName()}] Table Creating.";

        /// <inheritdoc cref="ProgressArgs.EventId" />
        public override int EventId => 12100;
    }

    /// <summary>
    /// Event args generated when a table is dropped.
    /// </summary>
    public class TableDroppedArgs : ProgressArgs
    {

        /// <inheritdoc cref="TableDroppedArgs"/>
        public TableDroppedArgs(SyncContext context, ScopeInfo scopeInfo, SyncTable table, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.ScopeInfo = scopeInfo;
            this.Table = table;
        }

        /// <summary>
        /// Gets the scope info on which the table is dropped.
        /// </summary>
        public ScopeInfo ScopeInfo { get; }

        /// <summary>
        /// Gets the table dropped.
        /// </summary>
        public SyncTable Table { get; }

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message => $"[{this.Table.GetFullName()}] Table Dropped.";

        /// <inheritdoc cref="ProgressArgs.ProgressLevel"/>
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => 12250;
    }

    /// <summary>
    /// Event args generated when a table is dropping.
    /// </summary>
    public class TableDroppingArgs : ProgressArgs
    {

        /// <inheritdoc cref="TableDroppingArgs"/>
        public TableDroppingArgs(SyncContext context, ScopeInfo scopeInfo, SyncTable table, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Command = command;
            this.ScopeInfo = scopeInfo;
            this.Table = table;
        }

        /// <summary>
        /// Gets or sets a value indicating whether gets or sets if the operation should be canceled.
        /// </summary>
        public bool Cancel { get; set; }

        /// <summary>
        /// Gets or sets the command used to drop the table.
        /// </summary>
        public DbCommand Command { get; set; }

        /// <summary>
        /// Gets the scope info on which the table is dropping.
        /// </summary>
        public ScopeInfo ScopeInfo { get; }

        /// <summary>
        /// Gets the table dropping.
        /// </summary>
        public SyncTable Table { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel"/>
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message => $"[{this.Table.GetFullName()}] Table Dropping.";

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => 12200;
    }

    /// <summary>
    /// Partial Interceptors extensions.
    /// </summary>
    public partial class InterceptorsExtensions
    {

        /// <summary>
        /// Intercept the provider when database schema is created (works only on SQL Server).
        /// </summary>
        public static Guid OnSchemaNameCreated(this BaseOrchestrator orchestrator, Action<SchemaNameCreatedArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when database schema is created (works only on SQL Server).
        /// </summary>
        public static Guid OnSchemaNameCreated(this BaseOrchestrator orchestrator, Func<SchemaNameCreatedArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when database schema is creating (works only on SQL Server).
        /// </summary>
        public static Guid OnSchemaNameCreating(this BaseOrchestrator orchestrator, Action<SchemaNameCreatingArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when database schema is creating (works only on SQL Server).
        /// </summary>
        public static Guid OnSchemaNameCreating(this BaseOrchestrator orchestrator, Func<SchemaNameCreatingArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a table is creating.
        /// </summary>
        public static Guid OnTableCreating(this BaseOrchestrator orchestrator, Action<TableCreatingArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a table is creating.
        /// </summary>
        public static Guid OnTableCreating(this BaseOrchestrator orchestrator, Func<TableCreatingArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a table is created.
        /// </summary>
        public static Guid OnTableCreated(this BaseOrchestrator orchestrator, Action<TableCreatedArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a table is created.
        /// </summary>
        public static Guid OnTableCreated(this BaseOrchestrator orchestrator, Func<TableCreatedArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a table is dropping.
        /// </summary>
        public static Guid OnTableDropping(this BaseOrchestrator orchestrator, Action<TableDroppingArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a table is dropping.
        /// </summary>
        public static Guid OnTableDropping(this BaseOrchestrator orchestrator, Func<TableDroppingArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a table is dropped.
        /// </summary>
        public static Guid OnTableDropped(this BaseOrchestrator orchestrator, Action<TableDroppedArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a table is dropped.
        /// </summary>
        public static Guid OnTableDropped(this BaseOrchestrator orchestrator, Func<TableDroppedArgs, Task> action)
            => orchestrator.AddInterceptor(action);
    }
}