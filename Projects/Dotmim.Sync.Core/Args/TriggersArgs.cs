using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Event args generated when a trigger is created.
    /// </summary>
    public class TriggerCreatedArgs : ProgressArgs
    {
        /// <inheritdoc cref="TriggerCreatedArgs" />
        public TriggerCreatedArgs(SyncContext context, ScopeInfo scopeInfo, SyncTable table, DbTriggerType triggerType, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.ScopeInfo = scopeInfo;
            this.Table = table;
            this.TriggerType = triggerType;
        }

        /// <summary>
        /// Gets the scope information after the trigger has been created.
        /// </summary>
        public ScopeInfo ScopeInfo { get; }

        /// <summary>
        /// Gets the table on which the trigger is created.
        /// </summary>
        public SyncTable Table { get; }

        /// <summary>
        /// Gets the trigger type.
        /// </summary>
        public DbTriggerType TriggerType { get; }

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message => $"[{this.Table.GetFullName()}] Trigger [{this.TriggerType}] Created.";

        /// <inheritdoc cref="ProgressArgs.ProgressLevel"/>
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => 15050;
    }

    /// <summary>
    /// Event args generated when a trigger is creating.
    /// </summary>
    public class TriggerCreatingArgs : ProgressArgs
    {

        /// <inheritdoc cref="TriggerCreatingArgs" />
        public TriggerCreatingArgs(SyncContext context, ScopeInfo scopeInfo, SyncTable table, DbTriggerType triggerType, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.ScopeInfo = scopeInfo;
            this.Table = table;
            this.TriggerType = triggerType;
            this.Command = command;
        }

        /// <summary>
        /// Gets or sets a value indicating whether gets the table on which the trigger is creating.
        /// </summary>
        public bool Cancel { get; set; }

        /// <summary>
        /// Gets or sets the command used to create the trigger.
        /// </summary>
        public DbCommand Command { get; set; }

        /// <summary>
        /// Gets the scope information before the trigger is creating.
        /// </summary>
        public ScopeInfo ScopeInfo { get; }

        /// <summary>
        /// Gets the table on which the trigger is creating.
        /// </summary>
        public SyncTable Table { get; }

        /// <summary>
        /// Gets the trigger type.
        /// </summary>
        public DbTriggerType TriggerType { get; }

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message => $"[{this.Table.GetFullName()}] Trigger [{this.TriggerType}] Creating.";

        /// <inheritdoc cref="ProgressArgs.ProgressLevel"/>
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => 15000;
    }

    /// <summary>
    /// Event args generated when a trigger is dropped.
    /// </summary>
    public class TriggerDroppedArgs : ProgressArgs
    {
        /// <inheritdoc cref="TriggerDroppedArgs" />
        public TriggerDroppedArgs(SyncContext context, SyncTable table, DbTriggerType triggerType, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Table = table;
            this.TriggerType = triggerType;
        }

        /// <summary>
        /// Gets the table on which the trigger is dropped.
        /// </summary>
        public SyncTable Table { get; }

        /// <summary>
        /// Gets the trigger type.
        /// </summary>
        public DbTriggerType TriggerType { get; }

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message => $"[{this.Table.GetFullName()}] Trigger [{this.TriggerType}] Dropped.";

        /// <inheritdoc cref="ProgressArgs.ProgressLevel"/>
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => 15150;
    }

    /// <summary>
    /// Event args generated when a trigger is dropping.
    /// </summary>
    public class TriggerDroppingArgs : ProgressArgs
    {

        /// <inheritdoc cref="TriggerDroppingArgs" />
        public TriggerDroppingArgs(SyncContext context, SyncTable table, DbTriggerType triggerType, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Table = table;
            this.TriggerType = triggerType;
            this.Command = command;
        }

        /// <summary>
        /// Gets the table on which the trigger is dropping.
        /// </summary>
        public SyncTable Table { get; }

        /// <summary>
        /// Gets the trigger type.
        /// </summary>
        public DbTriggerType TriggerType { get; }

        /// <summary>
        /// Gets or sets a value indicating whether the trigger dropping should be canceled.
        /// </summary>
        public bool Cancel { get; set; }

        /// <summary>
        /// Gets or sets the command used to drop the trigger.
        /// </summary>
        public DbCommand Command { get; set; }

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message => $"[{this.Table.GetFullName()}] Trigger [{this.TriggerType}] Dropping.";

        /// <inheritdoc cref="ProgressArgs.ProgressLevel"/>
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => 15100;
    }

    /// <summary>
    /// Partial Interceptors extensions.
    /// </summary>
    public partial class InterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider when a trigger is creating.
        /// </summary>
        public static Guid OnTriggerCreating(this BaseOrchestrator orchestrator, Action<TriggerCreatingArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a trigger is creating.
        /// </summary>
        public static Guid OnTriggerCreating(this BaseOrchestrator orchestrator, Func<TriggerCreatingArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a trigger is created.
        /// </summary>
        public static Guid OnTriggerCreated(this BaseOrchestrator orchestrator, Action<TriggerCreatedArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a trigger is created.
        /// </summary>
        public static Guid OnTriggerCreated(this BaseOrchestrator orchestrator, Func<TriggerCreatedArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a trigger is dropping.
        /// </summary>
        public static Guid OnTriggerDropping(this BaseOrchestrator orchestrator, Action<TriggerDroppingArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a trigger is dropping.
        /// </summary>
        public static Guid OnTriggerDropping(this BaseOrchestrator orchestrator, Func<TriggerDroppingArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a trigger is dropped.
        /// </summary>
        public static Guid OnTriggerDropped(this BaseOrchestrator orchestrator, Action<TriggerDroppedArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a trigger is dropped.
        /// </summary>
        public static Guid OnTriggerDropped(this BaseOrchestrator orchestrator, Func<TriggerDroppedArgs, Task> action)
            => orchestrator.AddInterceptor(action);
    }
}