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


    public class ScopeTableDroppedArgs : ProgressArgs
    {
        public DbScopeType ScopeType { get; }
        public string ScopeName { get; }

        public ScopeTableDroppedArgs(SyncContext context, string scopeName, DbScopeType scopeType, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.ScopeType = scopeType;
            this.ScopeName = scopeName;
        }

        public override string Source => Connection.Database;
        public override string Message => $"Scope Table [{ScopeType}] Dropped.";

        public override int EventId => SyncEventsId.ScopeTableDropped.Id;
    }

    public class ScopeTableCreatedArgs : ProgressArgs
    {
        public ScopeTableCreatedArgs(SyncContext context, string scopeName, DbScopeType scopeType, DbConnection connection = null, DbTransaction transaction = null) 
            : base(context, connection, transaction)
        {
            this.ScopeType = scopeType;
            this.ScopeName = scopeName;
        }
        public DbScopeType ScopeType { get; }
        public string ScopeName { get; }
        public override int EventId => SyncEventsId.ScopeTableCreated.Id;
        public override string Source => Connection.Database;
        public override string Message => $"Scope Table [{ScopeType}] Created.";
    }

    public class ScopeTableDroppingArgs : ProgressArgs
    {
        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; set; }
        public DbScopeType ScopeType { get; }
        public string ScopeName { get; }

        public ScopeTableDroppingArgs(SyncContext context, string scopeName, DbScopeType scopeType, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context,  connection, transaction)
        {
            this.Command = command;
            this.ScopeType = scopeType;
            this.ScopeName = scopeName;
        }
        public override string Source => Connection.Database;
        public override string Message => $"Scope Table [{ScopeType}] Dropping.";
        public override int EventId => SyncEventsId.ScopeTableDropping.Id;

    }

    public class ScopeTableCreatingArgs : ProgressArgs
    {
        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; set; }
        public DbScopeType ScopeType { get; }
        public string ScopeName { get; }
        public ScopeTableCreatingArgs(SyncContext context, string scopeName, DbScopeType scopeType, DbCommand command, DbConnection connection = null, DbTransaction transaction = null) 
            : base(context, connection, transaction)
        {
            this.Command = command;
            this.ScopeType = scopeType;
            this.ScopeName = scopeName;
        }
        public override string Source => Connection.Database;
        public override string Message => $"Scope Table [{ScopeType}] Creating.";
        public override int EventId => SyncEventsId.ScopeTableCreating.Id;
    }

    public class ScopeLoadedArgs<T> : ProgressArgs where T : class
    {
        public DbScopeType ScopeType { get; }
        public string ScopeName { get; }
        public ScopeLoadedArgs(SyncContext context, string scopeName, DbScopeType scopeType, T scopeInfo, DbConnection connection = null, DbTransaction transaction = null) 
            : base(context, connection, transaction)
        {
            this.ScopeInfo = scopeInfo;
        }
        public override string Source => Connection.Database;

        public override string Message {
            get
            {
                return this.ScopeInfo switch
                {
                    ServerScopeInfo ssi => $"[{ssi?.Name}] [Version {ssi.Version}] Last cleanup Timestamp:{ssi?.LastCleanupTimestamp}.",
                    ScopeInfo si => $"[{si?.Name}] [Version {si.Version}] Last sync:{si?.LastSync} Last sync duration:{si?.LastSyncDurationString}.",
                    _ => base.Message
                };
            }
        }
        public T ScopeInfo { get; }
        public override int EventId => SyncEventsId.ScopeLoaded.Id;
    }

    public class ScopeLoadingArgs : ProgressArgs
    {
        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; set; }
        public DbScopeType ScopeType { get; set; }
        public string ScopeName { get; }
        public ScopeLoadingArgs(SyncContext context, string scopeName, DbScopeType scopeType, DbCommand command, DbConnection connection = null, DbTransaction transaction = null) 
            : base(context, connection, transaction)
        {
            this.Command = command;
            this.ScopeType = scopeType;
            this.ScopeName = scopeName;
        }
        public override int EventId => SyncEventsId.ScopeLoading.Id;
        public override string Source => Connection.Database;
        public override string Message => $"Scope Table [{ScopeType}] Loading.";
    }

    public class ScopeSavingArgs : ProgressArgs
    {
        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; set; }
        public DbScopeType ScopeType { get; }
        public string ScopeName { get; }

        public ScopeSavingArgs(SyncContext context, string scopeName, DbScopeType scopeType, object scopeInfo, DbCommand command, DbConnection connection = null, DbTransaction transaction = null) 
            : base(context, connection, transaction)
        {
            this.Command = command;
            this.ScopeType = scopeType;
            this.ScopeName = scopeName;
        }
        public override string Source => Connection.Database;
        public override string Message => $"Scope Table [{ScopeType}] Saving.";

        public object ScopeInfo { get; }
        public override int EventId => SyncEventsId.ScopeSaving.Id;
    }

    public class ScopeSavedArgs : ProgressArgs
    {
        public DbScopeType ScopeType { get; }
        public string ScopeName { get; }
        public ScopeSavedArgs(SyncContext context, string scopeName, DbScopeType scopeType, object scopeInfo, DbConnection connection = null, DbTransaction transaction = null) 
            : base(context, connection, transaction)
        {
            this.ScopeType = scopeType;
            this.ScopeInfo = scopeInfo;
            this.ScopeName = scopeName;
        }
        public override string Source => Connection.Database;
        public override string Message => $"Scope Table [{ScopeType}] Saved.";

        public object ScopeInfo { get; }
        public override int EventId => SyncEventsId.ScopeSaved.Id;
    }

    public static partial class InterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider action when a scope table is creating
        /// </summary>
        public static void OnScopeTableCreating(this BaseOrchestrator orchestrator, Action<ScopeTableCreatingArgs> action)
            => orchestrator.SetInterceptor(action);
        /// <summary>
        /// Intercept the provider action when a scope table is creating
        /// </summary>
        public static void OnScopeTableCreating(this BaseOrchestrator orchestrator, Func<ScopeTableCreatingArgs, Task> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope table is created
        /// </summary>
        public static void OnScopeTableCreated(this BaseOrchestrator orchestrator, Action<ScopeTableCreatedArgs> action)
            => orchestrator.SetInterceptor(action);
        /// <summary>
        /// Intercept the provider action when a scope table is created
        /// </summary>
        public static void OnScopeTableCreated(this BaseOrchestrator orchestrator, Func<ScopeTableCreatedArgs, Task> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope table is dropping
        /// </summary>
        public static void OnScopeTableDropping(this BaseOrchestrator orchestrator, Action<ScopeTableDroppingArgs> action)
            => orchestrator.SetInterceptor(action);
        /// <summary>
        /// Intercept the provider action when a scope table is dropping
        /// </summary>
        public static void OnScopeTableDropping(this BaseOrchestrator orchestrator, Func<ScopeTableDroppingArgs, Task> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope table is dropped
        /// </summary>
        public static void OnScopeTableDropped(this BaseOrchestrator orchestrator, Action<ScopeTableDroppedArgs> action)
            => orchestrator.SetInterceptor(action);
        /// <summary>
        /// Intercept the provider action when a scope table is dropped
        /// </summary>
        public static void OnScopeTableDropped(this BaseOrchestrator orchestrator, Func<ScopeTableDroppedArgs, Task> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope is about to be loaded from client database
        /// </summary>
        public static void OnScopeLoading(this LocalOrchestrator orchestrator, Action<ScopeLoadingArgs> action)
            => orchestrator.SetInterceptor(action);
        /// <summary>
        /// Intercept the provider action when a scope is about to be loaded from client database
        /// </summary>
        public static void OnScopeLoading(this LocalOrchestrator orchestrator, Func<ScopeLoadingArgs, Task> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope is about to be loaded from ServerScope database
        /// </summary>
        public static void OnServerScopeLoading(this RemoteOrchestrator orchestrator, Action<ScopeLoadingArgs> action)
            => orchestrator.SetInterceptor(action);
        /// <summary>
        /// Intercept the provider action when a scope is about to be loaded from ServerScope database
        /// </summary>
        public static void OnServerScopeLoading(this RemoteOrchestrator orchestrator, Func<ScopeLoadingArgs, Task> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope is loaded from client database
        /// </summary>
        public static void OnScopeLoaded(this LocalOrchestrator orchestrator, Action<ScopeLoadedArgs<ScopeInfo>> action)
            => orchestrator.SetInterceptor(action);
        /// <summary>
        /// Intercept the provider action when a scope is loaded from client database
        /// </summary>
        public static void OnScopeLoaded(this LocalOrchestrator orchestrator, Func<ScopeLoadedArgs<ScopeInfo>, Task> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope is loaded from Server database
        /// </summary>
        public static void OnServerScopeLoaded(this RemoteOrchestrator orchestrator, Action<ScopeLoadedArgs<ServerScopeInfo>> action)
            => orchestrator.SetInterceptor(action);
        /// <summary>
        /// Intercept the provider action when a scope is loaded from Server database
        /// </summary>
        public static void OnServerScopeLoaded(this RemoteOrchestrator orchestrator, Func<ScopeLoadedArgs<ServerScopeInfo>, Task> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope is saving
        /// </summary>
        public static void OnScopeSaving(this BaseOrchestrator orchestrator, Action<ScopeSavingArgs> action)
            => orchestrator.SetInterceptor(action);
        /// <summary>
        /// Intercept the provider action when a scope is saving
        /// </summary>
        public static void OnScopeSaving(this BaseOrchestrator orchestrator, Func<ScopeSavingArgs, Task> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope is saved
        /// </summary>
        public static void OnScopeSaved(this BaseOrchestrator orchestrator, Action<ScopeSavedArgs> action)
            => orchestrator.SetInterceptor(action);
        /// <summary>
        /// Intercept the provider action when a scope is saved
        /// </summary>
        public static void OnScopeSaved(this BaseOrchestrator orchestrator, Func<ScopeSavedArgs, Task> action)
            => orchestrator.SetInterceptor(action);

    }
    public static partial class SyncEventsId
    {
        public static EventId ScopeTableCreating => CreateEventId(7000, nameof(ScopeTableCreating));
        public static EventId ScopeTableCreated => CreateEventId(7050, nameof(ScopeTableCreated));
        public static EventId ScopeTableDropping => CreateEventId(7100, nameof(ScopeTableDropping));
        public static EventId ScopeTableDropped => CreateEventId(7150, nameof(ScopeTableDropped));
        public static EventId ScopeLoading => CreateEventId(7200, nameof(ScopeLoading));
        public static EventId ScopeLoaded => CreateEventId(7250, nameof(ScopeLoaded));
        public static EventId ServerScopeScopeLoading => CreateEventId(7300, nameof(ServerScopeScopeLoading));
        public static EventId ServerScopeScopeLoaded => CreateEventId(7350, nameof(ServerScopeScopeLoaded));
        public static EventId ScopeSaving => CreateEventId(7400, nameof(ScopeSaving));
        public static EventId ScopeSaved => CreateEventId(7450, nameof(ScopeSaved));
    }

}
