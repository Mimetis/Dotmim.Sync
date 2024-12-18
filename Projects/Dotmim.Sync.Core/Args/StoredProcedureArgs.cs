using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Event args generated after a stored procedure is created.
    /// </summary>
    public class StoredProcedureCreatedArgs : ProgressArgs
    {

        /// <inheritdoc cref="StoredProcedureCreatedArgs" />
        public StoredProcedureCreatedArgs(SyncContext context, ScopeInfo scopeInfo, SyncTable table, DbStoredProcedureType storedProcedureType, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.ScopeInfo = scopeInfo;
            this.Table = table;
            this.StoredProcedureType = storedProcedureType;
        }

        /// <summary>
        /// Gets the scope info on which the stored procedure is created.
        /// </summary>
        public ScopeInfo ScopeInfo { get; }

        /// <summary>
        /// Gets the table on which the stored procedure is created.
        /// </summary>
        public SyncTable Table { get; }

        /// <summary>
        /// Gets the type of the stored procedure created.
        /// </summary>
        public DbStoredProcedureType StoredProcedureType { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel"/>
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message => $"[{this.Table.GetFullName()}] Stored Procedure [{this.StoredProcedureType}] Created.";

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => 11050;
    }

    /// <summary>
    /// Event args generated before a stored procedure is creating.
    /// </summary>
    public class StoredProcedureCreatingArgs : ProgressArgs
    {
        /// <inheritdoc cref="StoredProcedureCreatingArgs" />
        public StoredProcedureCreatingArgs(SyncContext context, ScopeInfo scopeInfo, SyncTable table, DbStoredProcedureType storedProcedureType, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Command = command;
            this.ScopeInfo = scopeInfo;
            this.Table = table;
            this.StoredProcedureType = storedProcedureType;
        }

        /// <summary>
        /// Gets the scope info on which the stored procedure is creating.
        /// </summary>
        public ScopeInfo ScopeInfo { get; }

        /// <summary>
        /// Gets the table on which the stored procedure is creating.
        /// </summary>
        public SyncTable Table { get; }

        /// <summary>
        /// Gets the type of the stored procedure creating.
        /// </summary>
        public DbStoredProcedureType StoredProcedureType { get; }

        /// <summary>
        /// Gets or sets a value indicating whether gets or sets the cancel flag. If true, the stored procedure creation will be canceled.
        /// </summary>
        public bool Cancel { get; set; }

        /// <summary>
        /// Gets or sets the command used to create the stored procedure.
        /// </summary>
        public DbCommand Command { get; set; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel"/>
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message => $"[{this.Table.GetFullName()}] Stored Procedure [{this.StoredProcedureType}] Creating.";

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => 11000;
    }

    /// <summary>
    /// Event args generated after a stored procedure is dropped.
    /// </summary>
    public class StoredProcedureDroppedArgs : ProgressArgs
    {
        /// <inheritdoc cref="StoredProcedureDroppedArgs" />
        public StoredProcedureDroppedArgs(SyncContext context, ScopeInfo scopeInfo, SyncTable table, DbStoredProcedureType storedProcedureType, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.ScopeInfo = scopeInfo;
            this.Table = table;
            this.StoredProcedureType = storedProcedureType;
        }

        /// <summary>
        /// Gets the scope info on which the stored procedure is dropped.
        /// </summary>
        public ScopeInfo ScopeInfo { get; }

        /// <summary>
        /// Gets the table on which the stored procedure is dropped.
        /// </summary>
        public SyncTable Table { get; }

        /// <summary>
        /// Gets the type of the stored procedure dropped.
        /// </summary>
        public DbStoredProcedureType StoredProcedureType { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel"/>
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message => $"[{this.Table.GetFullName()}] Stored Procedure [{this.StoredProcedureType}] Dropped.";

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => 11150;
    }

    /// <summary>
    /// Event args generated before a stored procedure is dropping.
    /// </summary>
    public class StoredProcedureDroppingArgs : ProgressArgs
    {
        /// <inheritdoc cref="StoredProcedureDroppingArgs" />
        public StoredProcedureDroppingArgs(SyncContext context, ScopeInfo scopeInfo, SyncTable table, DbStoredProcedureType storedProcedureType, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Command = command;
            this.ScopeInfo = scopeInfo;
            this.Table = table;
            this.StoredProcedureType = storedProcedureType;
        }

        /// <summary>
        /// Gets the scope info on which the stored procedure is dropping.
        /// </summary>
        public ScopeInfo ScopeInfo { get; }

        /// <summary>
        /// Gets the table on which the stored procedure is dropping.
        /// </summary>
        public SyncTable Table { get; }

        /// <summary>
        /// Gets the type of the stored procedure dropping.
        /// </summary>
        public DbStoredProcedureType StoredProcedureType { get; }

        /// <summary>
        /// Gets or sets a value indicating whether gets or sets the cancel flag. If true, the stored procedure dropping will be canceled.
        /// </summary>
        public bool Cancel { get; set; }

        /// <summary>
        /// Gets or sets the command used to drop the stored procedure.
        /// </summary>
        public DbCommand Command { get; set; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel"/>
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message => $"[{this.Table.GetFullName()}] Stored Procedure [{this.StoredProcedureType}] Dropping.";

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => 11100;
    }

    /// <summary>
    /// Partial Interceptors extensions.
    /// </summary>
    public partial class InterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider when a Stored Procedure is creating.
        /// </summary>
        public static Guid OnStoredProcedureCreating(this BaseOrchestrator orchestrator, Action<StoredProcedureCreatingArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a Stored Procedure is creating.
        /// </summary>
        public static Guid OnStoredProcedureCreating(this BaseOrchestrator orchestrator, Func<StoredProcedureCreatingArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a Stored Procedure is created.
        /// </summary>
        public static Guid OnStoredProcedureCreated(this BaseOrchestrator orchestrator, Action<StoredProcedureCreatedArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a Stored Procedure is created.
        /// </summary>
        public static Guid OnStoredProcedureCreated(this BaseOrchestrator orchestrator, Func<StoredProcedureCreatedArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a Stored Procedure is dropping.
        /// </summary>
        public static Guid OnStoredProcedureDropping(this BaseOrchestrator orchestrator, Action<StoredProcedureDroppingArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a Stored Procedure is dropping.
        /// </summary>
        public static Guid OnStoredProcedureDropping(this BaseOrchestrator orchestrator, Func<StoredProcedureDroppingArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a Stored Procedure is dropped.
        /// </summary>
        public static Guid OnStoredProcedureDropped(this BaseOrchestrator orchestrator, Action<StoredProcedureDroppedArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a Stored Procedure is dropped.
        /// </summary>
        public static Guid OnStoredProcedureDropped(this BaseOrchestrator orchestrator, Func<StoredProcedureDroppedArgs, Task> action)
            => orchestrator.AddInterceptor(action);
    }
}