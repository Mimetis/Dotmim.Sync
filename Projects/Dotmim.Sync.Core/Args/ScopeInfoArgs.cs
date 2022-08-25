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


    public class ScopeInfoTableDroppedArgs : ProgressArgs
    {
        public DbScopeType ScopeType { get; }
        public string ScopeName { get; }

        public ScopeInfoTableDroppedArgs(SyncContext context, string scopeName, DbScopeType scopeType, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.ScopeType = scopeType;
            this.ScopeName = scopeName;
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        public override string Source => Connection.Database;
        public override string Message => $"[{Connection.Database}] Scope Table [{ScopeType}] Dropped.";

        public override int EventId => SyncEventsId.ScopeInfoTableDropped.Id;
    }

    public class ScopeInfoTableCreatedArgs : ProgressArgs
    {
        public ScopeInfoTableCreatedArgs(SyncContext context, string scopeName, DbScopeType scopeType, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.ScopeType = scopeType;
            this.ScopeName = scopeName;
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;
        public DbScopeType ScopeType { get; }
        public string ScopeName { get; }
        public override int EventId => SyncEventsId.ScopeInfoTableCreated.Id;
        public override string Source => Connection.Database;
        public override string Message => $"[{Connection.Database}] Scope Table [{ScopeType}] Created.";
    }

    public class ScopeInfoTableDroppingArgs : ProgressArgs
    {
        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; set; }
        public DbScopeType ScopeType { get; }
        public string ScopeName { get; }

        public ScopeInfoTableDroppingArgs(SyncContext context, string scopeName, DbScopeType scopeType, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Command = command;
            this.ScopeType = scopeType;
            this.ScopeName = scopeName;
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;
        public override string Source => Connection.Database;
        public override string Message => $"[{Connection.Database}] Scope Table [{ScopeType}] Dropping.";
        public override int EventId => SyncEventsId.ScopeInfoTableDropping.Id;

    }

    public class ScopeInfoTableCreatingArgs : ProgressArgs
    {
        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; set; }
        public DbScopeType ScopeType { get; }
        public string ScopeName { get; }
        public ScopeInfoTableCreatingArgs(SyncContext context, string scopeName, DbScopeType scopeType, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Command = command;
            this.ScopeType = scopeType;
            this.ScopeName = scopeName;
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;
        public override string Source => Connection.Database;
        public override string Message => $"[{Connection.Database}] Scope Table [{ScopeType}] Creating.";
        public override int EventId => SyncEventsId.ScopeInfoTableCreating.Id;
    }

    public class ScopeInfoLoadedArgs : ProgressArgs
    {
        public ScopeInfoLoadedArgs(SyncContext context, ScopeInfo scopeInfo, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.ScopeInfo = scopeInfo;
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;
        public override string Source => Connection.Database;

        public override string Message => $"[{Connection.Database}] [{ScopeInfo?.Name}] [Version {ScopeInfo?.Version}].";
        
        public ScopeInfo ScopeInfo { get; set; }

        public override int EventId => SyncEventsId.ScopeInfoLoaded.Id;
    }

    public class ScopeInfoLoadingArgs : ProgressArgs
    {
        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; set; }
        public string ScopeName { get; }
        public ScopeInfoLoadingArgs(SyncContext context, string scopeName, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Command = command;
            this.ScopeName = scopeName;
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;
        public override int EventId => SyncEventsId.ScopeInfoLoading.Id;
        public override string Source => Connection.Database;
        public override string Message => $"[{Connection.Database}] Client Scope Loading.";
    }


    public class ScopeInfoSavingArgs : ProgressArgs
    {
        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; set; }

        public ScopeInfoSavingArgs(SyncContext context, ScopeInfo scopeInfo, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Command = command;
            this.ScopeInfo = scopeInfo;
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;
        public override string Source => Connection.Database;
        public override string Message => $"[{Connection.Database}] Scope Info Saving.";

        public ScopeInfo ScopeInfo { get; }
        public override int EventId => SyncEventsId.ScopeInfoSaving.Id;
    }

    public class ScopeInfoSavedArgs : ProgressArgs
    {
        public ScopeInfoSavedArgs(SyncContext context,  ScopeInfo scopeInfo, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.ScopeInfo = scopeInfo;
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;
        public override string Source => Connection.Database;
        public override string Message => $"[{Connection.Database}] Scope Info Saved.";

        public ScopeInfo ScopeInfo { get; }
        public override int EventId => SyncEventsId.ScopeInfoSaved.Id;
    }

    public static partial class InterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider action when a scope table is creating
        /// </summary>
        public static Guid OnScopeInfoTableCreating(this BaseOrchestrator orchestrator, Action<ScopeInfoTableCreatingArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider action when a scope table is creating
        /// </summary>
        public static Guid OnScopeInfoTableCreating(this BaseOrchestrator orchestrator, Func<ScopeInfoTableCreatingArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope table is created
        /// </summary>
        public static Guid OnScopeInfoTableCreated(this BaseOrchestrator orchestrator, Action<ScopeInfoTableCreatedArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider action when a scope table is created
        /// </summary>
        public static Guid OnScopeInfoTableCreated(this BaseOrchestrator orchestrator, Func<ScopeInfoTableCreatedArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope table is dropping
        /// </summary>
        public static Guid OnScopeInfoTableDropping(this BaseOrchestrator orchestrator, Action<ScopeInfoTableDroppingArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider action when a scope table is dropping
        /// </summary>
        public static Guid OnScopeInfoTableDropping(this BaseOrchestrator orchestrator, Func<ScopeInfoTableDroppingArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope table is dropped
        /// </summary>
        public static Guid OnScopeInfoTableDropped(this BaseOrchestrator orchestrator, Action<ScopeInfoTableDroppedArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider action when a scope table is dropped
        /// </summary>
        public static Guid OnScopeInfoTableDropped(this BaseOrchestrator orchestrator, Func<ScopeInfoTableDroppedArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a client scope is about to be loaded from client database
        /// </summary>
        public static Guid OnScopeInfoLoading(this BaseOrchestrator orchestrator, Action<ScopeInfoLoadingArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider action when a client scope is about to be loaded from client database
        /// </summary>
        public static Guid OnScopeInfoLoading(this BaseOrchestrator orchestrator, Func<ScopeInfoLoadingArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope is loaded from client or server database
        /// </summary>
        public static Guid OnScopeInfoLoaded(this BaseOrchestrator orchestrator, Action<ScopeInfoLoadedArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider action when a scope is loaded from client or server database
        /// </summary>
        public static Guid OnScopeInfoLoaded(this BaseOrchestrator orchestrator, Func<ScopeInfoLoadedArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope is saving
        /// </summary>
        public static Guid OnScopeSaving(this BaseOrchestrator orchestrator, Action<ScopeInfoSavingArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider action when a scope is saving
        /// </summary>
        public static Guid OnScopeSaving(this BaseOrchestrator orchestrator, Func<ScopeInfoSavingArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope is saved
        /// </summary>
        public static Guid OnScopeSaved(this BaseOrchestrator orchestrator, Action<ScopeInfoSavedArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider action when a scope is saved
        /// </summary>
        public static Guid OnScopeSaved(this BaseOrchestrator orchestrator, Func<ScopeInfoSavedArgs, Task> action)
            => orchestrator.AddInterceptor(action);

    }
    public static partial class SyncEventsId
    {
        public static EventId ScopeInfoTableCreating => CreateEventId(7000, nameof(ScopeInfoTableCreating));
        public static EventId ScopeInfoTableCreated => CreateEventId(7050, nameof(ScopeInfoTableCreated));
        public static EventId ScopeInfoTableDropping => CreateEventId(7100, nameof(ScopeInfoTableDropping));
        public static EventId ScopeInfoTableDropped => CreateEventId(7150, nameof(ScopeInfoTableDropped));
        public static EventId ScopeInfoLoading => CreateEventId(7200, nameof(ScopeInfoLoading));
        public static EventId ScopeInfoLoaded => CreateEventId(7250, nameof(ScopeInfoLoaded));
        public static EventId ScopeInfoSaving => CreateEventId(7400, nameof(ScopeInfoSaving));
        public static EventId ScopeInfoSaved => CreateEventId(7450, nameof(ScopeInfoSaved));
    }

}
