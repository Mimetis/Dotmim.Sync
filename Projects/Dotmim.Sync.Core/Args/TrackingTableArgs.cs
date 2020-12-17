using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync
{
    public class TrackingTableCreatedArgs : ProgressArgs
    {
        public SyncTable Table { get; }
        public ParserName TrackingTableName { get; }

        public TrackingTableCreatedArgs(SyncContext context, SyncTable table, ParserName trackingTableName, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Table = table;
            this.TrackingTableName = trackingTableName;
        }

        public override string Message => $"[{Connection.Database}] [{this.TrackingTableName}] tracking table created.";

        public override int EventId => 43;
    }

    public class TrackingTableCreatingArgs : ProgressArgs
    {
        public SyncTable Table { get; }
        public ParserName TrackingTableName { get; }
        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; }

        public TrackingTableCreatingArgs(SyncContext context, SyncTable table, ParserName trackingTableName, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Table = table;
            this.TrackingTableName = trackingTableName;
            this.Command = command;
        }
        public override string Message => $"[{Connection.Database}] [{this.TrackingTableName}] tracking table creating.";

    }

    public class TrackingTableDroppedArgs : ProgressArgs
    {
        public SyncTable Table { get; }
        public ParserName TrackingTableName { get; }

        public TrackingTableDroppedArgs(SyncContext context, SyncTable table, ParserName trackingTableName, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Table = table;
            this.TrackingTableName = trackingTableName;
        }

        public override string Message => $"[{Connection.Database}] [{this.TrackingTableName}] tracking table dropped.";

        public override int EventId => 45;
    }

    public class TrackingTableDroppingArgs : ProgressArgs
    {
        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; }
        public SyncTable Table { get; }
        public ParserName TrackingTableName { get; }

        public TrackingTableDroppingArgs(SyncContext context, SyncTable table, ParserName trackingTableName, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Table = table;
            this.TrackingTableName = trackingTableName;
            this.Command = command;
        }

    }

    public class TrackingTableRenamedArgs : ProgressArgs
    {
        public ParserName TrackingTableName { get; }
        public ParserName OldTrackingTableName { get; set; }

        public TrackingTableRenamedArgs(SyncContext context, SyncTable table, ParserName trackingTableName, ParserName oldTrackingTableName, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.TrackingTableName = trackingTableName;
            this.OldTrackingTableName = oldTrackingTableName;
        }

        public override string Message => $"[{Connection.Database}] [{this.TrackingTableName}] tracking table renamed.";

        public override int EventId => 43;
    }

    public class TrackingTableRenamingArgs : ProgressArgs
    {
        public SyncTable Table { get; }
        public ParserName TrackingTableName { get; }
        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; }
        public ParserName OldTrackingTableName { get; set; }

        public TrackingTableRenamingArgs(SyncContext context, SyncTable table, ParserName trackingTableName, ParserName oldTrackingTableName, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Table = table;
            this.TrackingTableName = trackingTableName;
            this.Command = command;
            this.OldTrackingTableName = oldTrackingTableName;

        }
        public override string Message => $"[{Connection.Database}] [{this.TrackingTableName}] tracking table renaming.";

    }

    /// <summary>
    /// Partial interceptors extensions 
    /// </summary>
    public static partial class InterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider when a tracking table is creating
        /// </summary>
        public static void OnTrackingTableCreating(this BaseOrchestrator orchestrator, Action<TrackingTableCreatingArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider when a tracking table is creating
        /// </summary>
        public static void OnTrackingTableCreated(this BaseOrchestrator orchestrator, Action<TrackingTableCreatedArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider when a tracking table is dropping
        /// </summary>
        public static void OnTrackingTableDropping(this BaseOrchestrator orchestrator, Action<TrackingTableDroppingArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider when a tracking table is dropped
        /// </summary>
        public static void OnTrackingTableDropped(this BaseOrchestrator orchestrator, Action<TrackingTableDroppedArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider when a tracking table is creating
        /// </summary>
        public static void OnTrackingTableRenaming(this BaseOrchestrator orchestrator, Action<TrackingTableRenamingArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider when a tracking table is creating
        /// </summary>
        public static void OnTrackingTableRenamed(this BaseOrchestrator orchestrator, Action<TrackingTableRenamedArgs> action)
            => orchestrator.SetInterceptor(action);

    }

}
