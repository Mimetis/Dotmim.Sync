using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public class StoredProcedureCreatedArgs : ProgressArgs
    {
        public SyncTable Table { get; }
        public DbStoredProcedureType StoredProcedureType { get; }

        public StoredProcedureCreatedArgs(SyncContext context, SyncTable table, DbStoredProcedureType StoredProcedureType, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Table = table;
            this.StoredProcedureType = StoredProcedureType;
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;
        public override string Source => Connection.Database;
        public override string Message => $"[{this.Table.GetFullName()}] Stored Procedure [{this.StoredProcedureType}] Created.";

        public override int EventId => SyncEventsId.StoredProcedureCreated.Id;
    }

    public class StoredProcedureCreatingArgs : ProgressArgs
    {
        public SyncTable Table { get; }
        public DbStoredProcedureType StoredProcedureType { get; }
        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; set; }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;
        public StoredProcedureCreatingArgs(SyncContext context, SyncTable table, DbStoredProcedureType StoredProcedureType, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Command = command;
            this.Table = table;
            this.StoredProcedureType = StoredProcedureType;
        }
        public override string Source => Connection.Database;
        public override string Message => $"[{this.Table.GetFullName()}] Stored Procedure [{this.StoredProcedureType}] Creating.";
        public override int EventId => SyncEventsId.StoredProcedureCreating.Id;
    }

    public class StoredProcedureDroppedArgs : ProgressArgs
    {
        public SyncTable Table { get; }
        public DbStoredProcedureType StoredProcedureType { get; }

        public StoredProcedureDroppedArgs(SyncContext context, SyncTable table, DbStoredProcedureType StoredProcedureType, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            Table = table;
            this.StoredProcedureType = StoredProcedureType;
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;
        public override string Source => Connection.Database;
        public override string Message => $"[{Table.GetFullName()}] Stored Procedure [{this.StoredProcedureType}] Dropped.";
        public override int EventId => SyncEventsId.StoredProcedureDropped.Id;
    }

    public class StoredProcedureDroppingArgs : ProgressArgs
    {
        public SyncTable Table { get; }
        public DbStoredProcedureType StoredProcedureType { get; }
        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; set; }

        public StoredProcedureDroppingArgs(SyncContext context, SyncTable table, DbStoredProcedureType StoredProcedureType, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Command = command;
            this.Table = table;
            this.StoredProcedureType = StoredProcedureType;
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;
        public override string Source => Connection.Database;
        public override string Message => $"[{Table.GetFullName()}] Stored Procedure [{this.StoredProcedureType}] Dropping.";
        public override int EventId => SyncEventsId.StoredProcedureDropping.Id;

    }


    /// <summary>
    /// Partial interceptors extensions 
    /// </summary>
    public static partial class InterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider when a Stored Procedure is creating
        /// </summary>
        public static void OnStoredProcedureCreating(this BaseOrchestrator orchestrator, Action<StoredProcedureCreatingArgs> action)
            => orchestrator.SetInterceptor(action);
        /// <summary>
        /// Intercept the provider when a Stored Procedure is creating
        /// </summary>
        public static void OnStoredProcedureCreating(this BaseOrchestrator orchestrator, Func<StoredProcedureCreatingArgs, Task> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider when a Stored Procedure is created
        /// </summary>
        public static void OnStoredProcedureCreated(this BaseOrchestrator orchestrator, Action<StoredProcedureCreatedArgs> action)
            => orchestrator.SetInterceptor(action);
        /// <summary>
        /// Intercept the provider when a Stored Procedure is created
        /// </summary>
        public static void OnStoredProcedureCreated(this BaseOrchestrator orchestrator, Func<StoredProcedureCreatedArgs, Task> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider when a Stored Procedure is dropping
        /// </summary>
        public static void OnStoredProcedureDropping(this BaseOrchestrator orchestrator, Action<StoredProcedureDroppingArgs> action)
            => orchestrator.SetInterceptor(action);
        /// <summary>
        /// Intercept the provider when a Stored Procedure is dropping
        /// </summary>
        public static void OnStoredProcedureDropping(this BaseOrchestrator orchestrator, Func<StoredProcedureDroppingArgs, Task> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider when a Stored Procedure is dropped
        /// </summary>
        public static void OnStoredProcedureDropped(this BaseOrchestrator orchestrator, Action<StoredProcedureDroppedArgs> action)
            => orchestrator.SetInterceptor(action);
        /// <summary>
        /// Intercept the provider when a Stored Procedure is dropped
        /// </summary>
        public static void OnStoredProcedureDropped(this BaseOrchestrator orchestrator, Func<StoredProcedureDroppedArgs, Task> action)
            => orchestrator.SetInterceptor(action);

    }
    public static partial class SyncEventsId
    {
        public static EventId StoredProcedureCreating => CreateEventId(11000, nameof(StoredProcedureCreating));
        public static EventId StoredProcedureCreated => CreateEventId(11050, nameof(StoredProcedureCreated));
        public static EventId StoredProcedureDropping => CreateEventId(11100, nameof(StoredProcedureDropping));
        public static EventId StoredProcedureDropped => CreateEventId(11150, nameof(StoredProcedureDropped));
    }
}
