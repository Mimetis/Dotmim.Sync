using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

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

        public override int EventId => 45;
    }

    public class ScopeTableCreatedArgs : ScopeTableDroppedArgs
    {
        public ScopeTableCreatedArgs(SyncContext context, string scopeName, DbScopeType scopeType, DbConnection connection = null, DbTransaction transaction = null) : base(context, scopeName, scopeType, connection, transaction)
        {
        }
    }

    public class ScopeTableDroppingArgs : ScopeTableDroppedArgs
    {
        public bool Cancel { get; set; } = false;
        public DbCommand Command { get; }

        public ScopeTableDroppingArgs(SyncContext context, string scopeName, DbScopeType scopeType, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, scopeName, scopeType, connection, transaction)
        {
            this.Command = command;
        }
    }

    public class ScopeTableCreatingArgs : ScopeTableDroppingArgs
    {
        public ScopeTableCreatingArgs(SyncContext context, string scopeName, DbScopeType scopeType, DbCommand command, DbConnection connection = null, DbTransaction transaction = null) : base(context, scopeName, scopeType, command, connection, transaction)
        {
        }
    }

    public class ScopeLoadedArgs<T> : ScopeTableDroppedArgs where T : class
    {
        public ScopeLoadedArgs(SyncContext context, string scopeName, DbScopeType scopeType, T scopeInfo, DbConnection connection = null, DbTransaction transaction = null) : base(context, scopeName, scopeType, connection, transaction)
        {
            this.ScopeInfo = scopeInfo;
        }

        public override string Message {
            get
            {
                return this.ScopeInfo switch
                {
                    ServerScopeInfo ssi => $"[{Connection.Database}] [{ssi?.Name}] [Version {ssi.Version}] Last cleanup Timestamp:{ssi?.LastCleanupTimestamp} ",
                    ScopeInfo si => $"[{Connection.Database}] [{si?.Name}] [Version {si.Version}] Last sync:{si?.LastSync} Last sync duration:{si?.LastSyncDurationString} ",
                    _ => base.Message
                };
            }
        }
        public T ScopeInfo { get; }
    }

    public class ScopeLoadingArgs : ScopeTableDroppingArgs
    {
        public ScopeLoadingArgs(SyncContext context, string scopeName, DbScopeType scopeType, DbCommand command, DbConnection connection = null, DbTransaction transaction = null) : base(context, scopeName, scopeType, command, connection, transaction)
        {
        }
    }

    public class ScopeSavingArgs : ScopeTableDroppingArgs
    {

        public ScopeSavingArgs(SyncContext context, string scopeName, DbScopeType scopeType, object scopeInfo, DbCommand command, DbConnection connection = null, DbTransaction transaction = null) : base(context, scopeName, scopeType, command, connection, transaction)
        {
            this.ScopeInfo = scopeInfo;
        }

        public object ScopeInfo { get; }
    }

    public class ScopeSavedArgs : ScopeTableDroppedArgs
    {
        public ScopeSavedArgs(SyncContext context, string scopeName, DbScopeType scopeType, object scopeInfo, DbConnection connection = null, DbTransaction transaction = null) : base(context, scopeName, scopeType, connection, transaction)
        {
            this.ScopeInfo = scopeInfo;
        }
        public object ScopeInfo { get; }
    }

    public static partial class InterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider action when a scope table is creating
        /// </summary>
        public static void OnScopeTableCreating(this BaseOrchestrator orchestrator, Action<ScopeTableCreatingArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope table is created
        /// </summary>
        public static void OnScopeTableCreated(this BaseOrchestrator orchestrator, Action<ScopeTableCreatedArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope table is dropping
        /// </summary>
        public static void OnScopeTableDropping(this BaseOrchestrator orchestrator, Action<ScopeTableDroppingArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope table is dropped
        /// </summary>
        public static void OnScopeTableDropped(this BaseOrchestrator orchestrator, Action<ScopeTableDroppedArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope is about to be loaded from client database
        /// </summary>
        public static void OnScopeLoading(this BaseOrchestrator orchestrator, Action<ScopeLoadingArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope is about to be loaded from ServerScope database
        /// </summary>
        public static void OnServerScopeScopeLoading(this BaseOrchestrator orchestrator, Action<ScopeLoadingArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope is loaded from client database
        /// </summary>
        public static void OnScopeLoaded(this BaseOrchestrator orchestrator, Action<ScopeLoadedArgs<ScopeInfo>> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope is loaded from Server database
        /// </summary>
        public static void OnServerScopeLoaded(this BaseOrchestrator orchestrator, Action<ScopeLoadedArgs<ServerScopeInfo>> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope is saving
        /// </summary>
        public static void OnScopeSaving(this BaseOrchestrator orchestrator, Action<ScopeSavingArgs> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope is saved
        /// </summary>
        public static void OnScopeSaved(this BaseOrchestrator orchestrator, Action<ScopeSavedArgs> action)
            => orchestrator.SetInterceptor(action);

    }


}
