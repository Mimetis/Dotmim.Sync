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
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        public override string Source => Connection.Database;
        public override string Message => $"[{Connection.Database}] Scope Table [{ScopeType}] Dropped.";

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
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;
        public DbScopeType ScopeType { get; }
        public string ScopeName { get; }
        public override int EventId => SyncEventsId.ScopeTableCreated.Id;
        public override string Source => Connection.Database;
        public override string Message => $"[{Connection.Database}] Scope Table [{ScopeType}] Created.";
    }

    public class ScopeTableDroppingArgs : ProgressArgs
    {
        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; set; }
        public DbScopeType ScopeType { get; }
        public string ScopeName { get; }

        public ScopeTableDroppingArgs(SyncContext context, string scopeName, DbScopeType scopeType, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Command = command;
            this.ScopeType = scopeType;
            this.ScopeName = scopeName;
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;
        public override string Source => Connection.Database;
        public override string Message => $"[{Connection.Database}] Scope Table [{ScopeType}] Dropping.";
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
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;
        public override string Source => Connection.Database;
        public override string Message => $"[{Connection.Database}] Scope Table [{ScopeType}] Creating.";
        public override int EventId => SyncEventsId.ScopeTableCreating.Id;
    }

    public class ClientScopeInfoLoadedArgs : ProgressArgs
    {
        public string ScopeName { get; }
        public ClientScopeInfoLoadedArgs(SyncContext context, string scopeName, ClientScopeInfo clientScopeInfo, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.ScopeName = scopeName;
            this.ClientScopeInfo = clientScopeInfo;
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;
        public override string Source => Connection.Database;

        public override string Message => $"[{Connection.Database}] [{ClientScopeInfo?.Name}] [Version {ClientScopeInfo?.Version}] Last sync:{ClientScopeInfo?.LastSync} Last sync duration:{ClientScopeInfo?.LastSyncDurationString}.";
        
        public ClientScopeInfo ClientScopeInfo { get; set; }

        public override int EventId => SyncEventsId.ClientScopeScopeLoaded.Id;
    }

    public class ClientScopeInfoLoadingArgs : ProgressArgs
    {
        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; set; }
        public string ScopeName { get; }
        public ClientScopeInfoLoadingArgs(SyncContext context, string scopeName, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Command = command;
            this.ScopeName = scopeName;
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;
        public override int EventId => SyncEventsId.ClientScopeScopeLoading.Id;
        public override string Source => Connection.Database;
        public override string Message => $"[{Connection.Database}] Client Scope Table Loading.";
    }

    public class ServerScopeInfoLoadedArgs : ProgressArgs
    {
        public string ScopeName { get; }
        public ServerScopeInfoLoadedArgs(SyncContext context, string scopeName, ServerScopeInfo serverScopeInfo, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.ScopeName = scopeName;
            this.ServerScopeInfo = serverScopeInfo;
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;
        public override string Source => Connection.Database;

        public override string Message => $"[{Connection.Database}] [{ServerScopeInfo?.Name}] [Version {ServerScopeInfo?.Version}] Last cleanup timestamp:{ServerScopeInfo?.LastCleanupTimestamp}.";
        public ServerScopeInfo ServerScopeInfo { get; }
        public override int EventId => SyncEventsId.ServerScopeScopeLoaded.Id;
    }

    public class ServerScopeInfoLoadingArgs : ProgressArgs
    {
        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; set; }
        public string ScopeName { get; }
        public ServerScopeInfoLoadingArgs(SyncContext context, string scopeName, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Command = command;
            this.ScopeName = scopeName;
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;
        public override int EventId => SyncEventsId.ServerScopeScopeLoading.Id;
        public override string Source => Connection.Database;
        public override string Message => $"[{Connection.Database}] Server Scope Table Loading.";
    }


    public class ScopeSavingArgs : ProgressArgs
    {
        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; set; }
        public DbScopeType ScopeType { get; }
        public string ScopeName { get; }

        public ScopeSavingArgs(SyncContext context, string scopeName, DbScopeType scopeType, IScopeInfo scopeInfo, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Command = command;
            this.ScopeType = scopeType;
            this.ScopeName = scopeName;
            this.ScopeInfo = scopeInfo;
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;
        public override string Source => Connection.Database;
        public override string Message => $"[{Connection.Database}] Scope Table [{ScopeType}] Saving.";

        public IScopeInfo ScopeInfo { get; }
        public override int EventId => SyncEventsId.ScopeSaving.Id;
    }

    public class ScopeSavedArgs : ProgressArgs
    {
        public DbScopeType ScopeType { get; }
        public string ScopeName { get; }
        public ScopeSavedArgs(SyncContext context, string scopeName, DbScopeType scopeType, IScopeInfo scopeInfo, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.ScopeType = scopeType;
            this.ScopeInfo = scopeInfo;
            this.ScopeName = scopeName;
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;
        public override string Source => Connection.Database;
        public override string Message => $"[{Connection.Database}] Scope Table [{ScopeType}] Saved.";

        public IScopeInfo ScopeInfo { get; }
        public override int EventId => SyncEventsId.ScopeSaved.Id;
    }

    public static partial class InterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider action when a scope table is creating
        /// </summary>
        public static Guid OnScopeTableCreating(this BaseOrchestrator orchestrator, Action<ScopeTableCreatingArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider action when a scope table is creating
        /// </summary>
        public static Guid OnScopeTableCreating(this BaseOrchestrator orchestrator, Func<ScopeTableCreatingArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope table is created
        /// </summary>
        public static Guid OnScopeTableCreated(this BaseOrchestrator orchestrator, Action<ScopeTableCreatedArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider action when a scope table is created
        /// </summary>
        public static Guid OnScopeTableCreated(this BaseOrchestrator orchestrator, Func<ScopeTableCreatedArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope table is dropping
        /// </summary>
        public static Guid OnScopeTableDropping(this BaseOrchestrator orchestrator, Action<ScopeTableDroppingArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider action when a scope table is dropping
        /// </summary>
        public static Guid OnScopeTableDropping(this BaseOrchestrator orchestrator, Func<ScopeTableDroppingArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope table is dropped
        /// </summary>
        public static Guid OnScopeTableDropped(this BaseOrchestrator orchestrator, Action<ScopeTableDroppedArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider action when a scope table is dropped
        /// </summary>
        public static Guid OnScopeTableDropped(this BaseOrchestrator orchestrator, Func<ScopeTableDroppedArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a client scope is about to be loaded from client database
        /// </summary>
        public static Guid OnClientScopeInfoLoading(this LocalOrchestrator orchestrator, Action<ClientScopeInfoLoadingArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider action when a client scope is about to be loaded from client database
        /// </summary>
        public static Guid OnClientScopeInfoLoading(this LocalOrchestrator orchestrator, Func<ClientScopeInfoLoadingArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a server scope is about to be loaded from server database
        /// </summary>
        public static Guid OnServerScopeInfoLoading(this RemoteOrchestrator orchestrator, Action<ServerScopeInfoLoadingArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider action when a server scope is about to be loaded from server database
        /// </summary>
        public static Guid OnServerScopeInfoLoading(this RemoteOrchestrator orchestrator, Func<ServerScopeInfoLoadingArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope is loaded from client database
        /// </summary>
        public static Guid OnClientScopeInfoLoaded(this LocalOrchestrator orchestrator, Action<ClientScopeInfoLoadedArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider action when a scope is loaded from client database
        /// </summary>
        public static Guid OnClientScopeInfoLoaded(this LocalOrchestrator orchestrator, Func<ClientScopeInfoLoadedArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope is loaded from Server database
        /// </summary>
        public static Guid OnServerScopeInfoLoaded(this RemoteOrchestrator orchestrator, Action<ServerScopeInfoLoadedArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider action when a scope is loaded from Server database
        /// </summary>
        public static Guid OnServerScopeInfoLoaded(this RemoteOrchestrator orchestrator, Func<ServerScopeInfoLoadedArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope is saving
        /// </summary>
        public static Guid OnScopeSaving(this BaseOrchestrator orchestrator, Action<ScopeSavingArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider action when a scope is saving
        /// </summary>
        public static Guid OnScopeSaving(this BaseOrchestrator orchestrator, Func<ScopeSavingArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope is saved
        /// </summary>
        public static Guid OnScopeSaved(this BaseOrchestrator orchestrator, Action<ScopeSavedArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider action when a scope is saved
        /// </summary>
        public static Guid OnScopeSaved(this BaseOrchestrator orchestrator, Func<ScopeSavedArgs, Task> action)
            => orchestrator.AddInterceptor(action);

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
        public static EventId ClientScopeScopeLoading => CreateEventId(7301, nameof(ServerScopeScopeLoading));
        public static EventId ClientScopeScopeLoaded => CreateEventId(7351, nameof(ServerScopeScopeLoaded));
        public static EventId ScopeSaving => CreateEventId(7400, nameof(ScopeSaving));
        public static EventId ScopeSaved => CreateEventId(7450, nameof(ScopeSaved));
    }

}
