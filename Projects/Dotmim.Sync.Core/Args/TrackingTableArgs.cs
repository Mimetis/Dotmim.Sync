using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Event args generated after a tracking table is created.
    /// </summary>
    public class TrackingTableCreatedArgs : ProgressArgs
    {
        /// <inheritdoc cref="TrackingTableCreatedArgs" />
        public TrackingTableCreatedArgs(SyncContext context, SyncTable table, ParserName trackingTableName, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Table = table;
            this.TrackingTableName = trackingTableName;
        }

        /// <summary>
        /// Gets the tracking table created.
        /// </summary>
        public SyncTable Table { get; }

        /// <summary>
        /// Gets the tracking table name.
        /// </summary>
        public ParserName TrackingTableName { get; }

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override string Message => $"[{this.TrackingTableName}] tracking table created.";

        /// <inheritdoc cref="ProgressArgs.ProgressLevel" />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        /// <inheritdoc cref="ProgressArgs.EventId" />
        public override int EventId => 14050;
    }

    /// <summary>
    /// Event args generated before a tracking table is creating.
    /// </summary>
    public class TrackingTableCreatingArgs : ProgressArgs
    {
        /// <inheritdoc cref="TrackingTableCreatingArgs" />
        public TrackingTableCreatingArgs(SyncContext context, SyncTable table, ParserName trackingTableName, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Table = table;
            this.TrackingTableName = trackingTableName;
            this.Command = command;
        }

        /// <summary>
        /// Gets the tracking table to be created.
        /// </summary>
        public SyncTable Table { get; }

        /// <summary>
        /// Gets the tracking table name.
        /// </summary>
        public ParserName TrackingTableName { get; }

        /// <summary>
        /// Gets or sets a value indicating whether the tracking table creation should be canceled.
        /// </summary>
        public bool Cancel { get; set; }

        /// <summary>
        /// Gets or sets the command to be executed.
        /// </summary>
        public DbCommand Command { get; set; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel" />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override string Message => $"[{this.TrackingTableName}] tracking table creating.";

        /// <inheritdoc cref="ProgressArgs.EventId" />
        public override int EventId => 14000;
    }

    /// <summary>
    /// Gets the tracking table dropped.
    /// </summary>
    public class TrackingTableDroppedArgs : ProgressArgs
    {
        /// <inheritdoc cref="TrackingTableDroppedArgs" />
        public TrackingTableDroppedArgs(SyncContext context, SyncTable table, ParserName trackingTableName, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Table = table;
            this.TrackingTableName = trackingTableName;
        }

        /// <summary>
        /// Gets the tracking table dropped.
        /// </summary>
        public SyncTable Table { get; }

        /// <summary>
        /// Gets the tracking table name.
        /// </summary>
        public ParserName TrackingTableName { get; }

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override string Message => $"[{this.TrackingTableName}] Tracking Table Dropped.";

        /// <inheritdoc cref="ProgressArgs.ProgressLevel" />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        /// <inheritdoc cref="ProgressArgs.EventId" />
        public override int EventId => 14150;
    }

    /// <summary>
    /// Event args generated before a tracking table is dropping.
    /// </summary>
    public class TrackingTableDroppingArgs : ProgressArgs
    {

        /// <inheritdoc cref="TrackingTableDroppingArgs" />
        public TrackingTableDroppingArgs(SyncContext context, SyncTable table, ParserName trackingTableName, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Table = table;
            this.TrackingTableName = trackingTableName;
            this.Command = command;
        }

        /// <summary>
        /// Gets or sets a value indicating whether the tracking table dropping should be canceled.
        /// </summary>
        public bool Cancel { get; set; }

        /// <summary>
        /// Gets or sets the command to be executed.
        /// </summary>
        public DbCommand Command { get; set; }

        /// <summary>
        /// Gets the tracking table to be dropped.
        /// </summary>
        public SyncTable Table { get; }

        /// <summary>
        /// Gets the tracking table name.
        /// </summary>
        public ParserName TrackingTableName { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel" />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override string Message => $"[{this.TrackingTableName}] Tracking Table Dropping.";

        /// <inheritdoc cref="ProgressArgs.EventId" />
        public override int EventId => 14100;
    }

    /// <summary>
    /// Event args generated after a tracking table is renamed.
    /// </summary>
    [Obsolete("This method is obsolete. DMS is not in charge anymore to rename a tracking table.")]
    public class TrackingTableRenamedArgs : ProgressArgs
    {
        /// <inheritdoc cref="TrackingTableRenamedArgs" />
        public TrackingTableRenamedArgs(SyncContext context, SyncTable table, ParserName trackingTableName, ParserName oldTrackingTableName, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.TrackingTableName = trackingTableName;
            this.OldTrackingTableName = oldTrackingTableName;
        }

        /// <summary>
        /// Gets the tracking table renamed.
        /// </summary>
        public ParserName TrackingTableName { get; }

        /// <summary>
        /// Gets the old tracking table name.
        /// </summary>
        public ParserName OldTrackingTableName { get; }

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override string Message => $"[{this.TrackingTableName}] Tracking Table Renamed.";

        /// <inheritdoc cref="ProgressArgs.ProgressLevel" />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        /// <inheritdoc cref="ProgressArgs.EventId" />
        public override int EventId => 14250;
    }

    /// <summary>
    /// Event args generated before a tracking table is renaming.
    /// </summary>
    [Obsolete("This method is obsolete. DMS is not in charge anymore to rename a tracking table.")]
    public class TrackingTableRenamingArgs : ProgressArgs
    {
        /// <inheritdoc cref="TrackingTableRenamingArgs" />
        public TrackingTableRenamingArgs(SyncContext context, SyncTable table, ParserName trackingTableName, ParserName oldTrackingTableName, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Table = table;
            this.TrackingTableName = trackingTableName;
            this.Command = command;
            this.OldTrackingTableName = oldTrackingTableName;
        }

        /// <summary>
        /// Gets the tracking table to be renamed.
        /// </summary>
        public SyncTable Table { get; }

        /// <summary>
        /// Gets the tracking table name.
        /// </summary>
        public ParserName TrackingTableName { get; }

        /// <summary>
        /// Gets or sets a value indicating whether the tracking table renaming should be canceled.
        /// </summary>
        public bool Cancel { get; set; }

        /// <summary>
        /// Gets or sets the command to be executed.
        /// </summary>
        public DbCommand Command { get; set; }

        /// <summary>
        /// Gets the old tracking table name.
        /// </summary>
        public ParserName OldTrackingTableName { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel" />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override string Message => $"[{this.TrackingTableName}] Tracking Table Renaming.";

        /// <inheritdoc cref="ProgressArgs.EventId" />
        public override int EventId => 14200;
    }

    /// <summary>
    /// Partial Interceptors extensions.
    /// </summary>
    public partial class InterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider when a tracking table is creating.
        /// </summary>
        public static Guid OnTrackingTableCreating(this BaseOrchestrator orchestrator, Action<TrackingTableCreatingArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a tracking table is creating.
        /// </summary>
        public static Guid OnTrackingTableCreating(this BaseOrchestrator orchestrator, Func<TrackingTableCreatingArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a tracking table is creating.
        /// </summary>
        public static Guid OnTrackingTableCreated(this BaseOrchestrator orchestrator, Action<TrackingTableCreatedArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a tracking table is creating.
        /// </summary>
        public static Guid OnTrackingTableCreated(this BaseOrchestrator orchestrator, Func<TrackingTableCreatedArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a tracking table is dropping.
        /// </summary>
        public static Guid OnTrackingTableDropping(this BaseOrchestrator orchestrator, Action<TrackingTableDroppingArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a tracking table is dropping.
        /// </summary>
        public static Guid OnTrackingTableDropping(this BaseOrchestrator orchestrator, Func<TrackingTableDroppingArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a tracking table is dropped.
        /// </summary>
        public static Guid OnTrackingTableDropped(this BaseOrchestrator orchestrator, Action<TrackingTableDroppedArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a tracking table is dropped.
        /// </summary>
        public static Guid OnTrackingTableDropped(this BaseOrchestrator orchestrator, Func<TrackingTableDroppedArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a tracking table is creating.
        /// </summary>
        [Obsolete("This method is obsolete. DMS is not in charge anymore to rename a tracking table.")]

        public static Guid OnTrackingTableRenaming(this BaseOrchestrator orchestrator, Action<TrackingTableRenamingArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a tracking table is creating.
        /// </summary>
        [Obsolete("This method is obsolete. DMS is not in charge anymore to rename a tracking table.")]
        public static Guid OnTrackingTableRenaming(this BaseOrchestrator orchestrator, Func<TrackingTableRenamingArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a tracking table is creating.
        /// </summary>
        [Obsolete("This method is obsolete. DMS is not in charge anymore to rename a tracking table.")]
        public static Guid OnTrackingTableRenamed(this BaseOrchestrator orchestrator, Action<TrackingTableRenamedArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider when a tracking table is creating.
        /// </summary>
        [Obsolete("This method is obsolete. DMS is not in charge anymore to rename a tracking table.")]
        public static Guid OnTrackingTableRenamed(this BaseOrchestrator orchestrator, Func<TrackingTableRenamedArgs, Task> action)
            => orchestrator.AddInterceptor(action);
    }
}