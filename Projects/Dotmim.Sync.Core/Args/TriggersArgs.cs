using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync
{
    public class TriggerCreatedArgs : ProgressArgs
    {
        public SyncTable Table { get; }
        public DbTriggerType TriggerType { get; }

        public TriggerCreatedArgs(SyncContext context, SyncTable table, DbTriggerType triggerType, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Table = table;
            this.TriggerType = triggerType;
        }

        public override string Message => $"[{Connection.Database}] [{this.Table.GetFullName()}] trigger [{this.TriggerType}] created.";

        public override int EventId => 43;
    }

    public class TriggerCreatingArgs : ProgressArgs
    {
        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; }
        public SyncTable Table { get; }
        public DbTriggerType TriggerType { get; }

        public TriggerCreatingArgs(SyncContext context, SyncTable table, DbTriggerType triggerType, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Table = table;
            this.TriggerType = triggerType;
            this.Command = command;
        }
        public override string Message => $"[{Connection.Database}] [{Table.GetFullName()}] trigger [{this.TriggerType}] creating.";


    }

    public class TriggerDroppedArgs : ProgressArgs
    {
        public SyncTable Table { get; }
        public DbTriggerType TriggerType { get; }

        public TriggerDroppedArgs(SyncContext context, SyncTable table, DbTriggerType triggerType, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Table = table;
            this.TriggerType = triggerType;
        }

        public override string Message => $"[{Connection.Database}] [{Table.GetFullName()}] trigger [{this.TriggerType}] dropped.";

        public override int EventId => SyncEventsId.DropTrigger.Id;
    }

    public class TriggerDroppingArgs : ProgressArgs
    {
        public SyncTable Table { get; }
        public DbTriggerType TriggerType { get; }

        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; }

        public TriggerDroppingArgs(SyncContext context, SyncTable table, DbTriggerType triggerType, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Table = table;
            this.TriggerType = triggerType;
            this.Command = command;
        }
        public override string Message => $"[{Connection.Database}] [{Table.GetFullName()}] trigger [{this.TriggerType}] dropping.";


    }

    /// <summary>
    /// Partial interceptors extensions 
    /// </summary>
    public static partial class InterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider when a trigger is creating
        /// </summary>
        public static void OnTriggerCreating(this BaseOrchestrator orchestrator, Action<TriggerCreatingArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider when a trigger is created
        /// </summary>
        public static void OnTriggerCreated(this BaseOrchestrator orchestrator, Action<TriggerCreatedArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider when a trigger is dropping
        /// </summary>
        public static void OnTriggerDropping(this BaseOrchestrator orchestrator, Action<TriggerDroppingArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider when a trigger is dropped
        /// </summary>
        public static void OnTriggerDropped(this BaseOrchestrator orchestrator, Action<TriggerDroppedArgs> action)
            => orchestrator.SetInterceptor(action);

    }
}
