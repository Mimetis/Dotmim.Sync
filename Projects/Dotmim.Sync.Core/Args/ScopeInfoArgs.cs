using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Event args generated after a scope table is dropped.
    /// </summary>
    public class ScopeInfoTableDroppedArgs : ProgressArgs
    {

        /// <inheritdoc cref="ScopeInfoTableDroppedArgs" />
        public ScopeInfoTableDroppedArgs(SyncContext context, string scopeName, DbScopeType scopeType, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.ScopeType = scopeType;
            this.ScopeName = scopeName;
        }

        /// <summary>
        /// Gets the scope type.
        /// </summary>
        public DbScopeType ScopeType { get; }

        /// <summary>
        /// Gets the scope name.
        /// </summary>
        public string ScopeName { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel" />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override string Message => $"Scope Table [{this.ScopeType}] Dropped.";

        /// <inheritdoc cref="ProgressArgs.EventId" />
        public override int EventId => 7150;
    }

    /// <summary>
    /// Event args generated after a scope table is created.
    /// </summary>
    public class ScopeInfoTableCreatedArgs : ProgressArgs
    {
        /// <inheritdoc cref="ScopeInfoTableCreatedArgs" />
        public ScopeInfoTableCreatedArgs(SyncContext context, string scopeName, DbScopeType scopeType, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.ScopeType = scopeType;
            this.ScopeName = scopeName;
        }

        /// <summary>
        /// Gets the scope type.
        /// </summary>
        public DbScopeType ScopeType { get; }

        /// <summary>
        /// Gets the scope name.
        /// </summary>
        public string ScopeName { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel" />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override string Message => $"Scope Table [{this.ScopeType}] Created.";

        /// <inheritdoc cref="ProgressArgs.EventId" />
        public override int EventId => 7050;
    }

    /// <summary>
    /// Event args generated when a scope table is about to be dropped.
    /// </summary>
    public class ScopeInfoTableDroppingArgs : ProgressArgs
    {

        /// <inheritdoc cref="ScopeInfoTableDroppingArgs" />
        public ScopeInfoTableDroppingArgs(SyncContext context, string scopeName, DbScopeType scopeType, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Command = command;
            this.ScopeType = scopeType;
            this.ScopeName = scopeName;
        }

        /// <summary>
        /// Gets or sets a value indicating whether the operation should be canceled.
        /// </summary>
        public bool Cancel { get; set; }

        /// <summary>
        /// Gets or sets the command to be executed.
        /// </summary>
        public DbCommand Command { get; set; }

        /// <summary>
        /// Gets the scope type.
        /// </summary>
        public DbScopeType ScopeType { get; }

        /// <summary>
        /// Gets the scope name.
        /// </summary>
        public string ScopeName { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel" />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override string Message => $"Scope Table [{this.ScopeType}] Dropping.";

        /// <inheritdoc cref="ProgressArgs.EventId" />
        public override int EventId => 7100;
    }

    /// <summary>
    /// Event args generated when a scope table is about to be created.
    /// </summary>
    public class ScopeInfoTableCreatingArgs : ProgressArgs
    {

        /// <inheritdoc cref="ScopeInfoTableCreatingArgs" />
        public ScopeInfoTableCreatingArgs(SyncContext context, string scopeName, DbScopeType scopeType, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Command = command;
            this.ScopeType = scopeType;
            this.ScopeName = scopeName;
        }

        /// <summary>
        /// Gets or sets a value indicating whether the operation should be canceled.
        /// </summary>
        public bool Cancel { get; set; }

        /// <summary>
        /// Gets or sets the command to be executed.
        /// </summary>
        public DbCommand Command { get; set; }

        /// <summary>
        /// Gets the scope type.
        /// </summary>
        public DbScopeType ScopeType { get; }

        /// <summary>
        /// Gets the scope name.
        /// </summary>
        public string ScopeName { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel" />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Trace;

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override string Message => $"Scope Table [{this.ScopeType}] Creating.";

        /// <inheritdoc cref="ProgressArgs.EventId" />
        public override int EventId => 7000;
    }

    /// <summary>
    /// Event args generated when a scope is loaded from client or server database.
    /// </summary>
    public class ScopeInfoLoadedArgs : ProgressArgs
    {
        /// <inheritdoc cref="ScopeInfoLoadedArgs" />
        public ScopeInfoLoadedArgs(SyncContext context, ScopeInfo scopeInfo, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction) => this.ScopeInfo = scopeInfo;

        /// <summary>
        /// Gets or sets the scope info.
        /// </summary>
        public ScopeInfo ScopeInfo { get; set; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel" />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override string Message => $"[{this.ScopeInfo?.Name}] [Version {this.ScopeInfo?.Version}].";

        /// <inheritdoc cref="ProgressArgs.EventId" />
        public override int EventId => 7250;
    }

    /// <summary>
    /// Event args generated when a client scope is about to be loaded from client database.
    /// </summary>
    public class ScopeInfoLoadingArgs : ProgressArgs
    {

        /// <inheritdoc cref="ScopeInfoLoadingArgs" />
        public ScopeInfoLoadingArgs(SyncContext context, string scopeName, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Command = command;
            this.ScopeName = scopeName;
        }

        /// <summary>
        /// Gets or sets a value indicating whether the operation should be canceled.
        /// </summary>
        public bool Cancel { get; set; }

        /// <summary>
        /// Gets or sets the command to be executed.
        /// </summary>
        public DbCommand Command { get; set; }

        /// <summary>
        /// Gets the scope name.
        /// </summary>
        public string ScopeName { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel" />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override string Message => $"Client Scope Loading.";

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override int EventId => 7200;
    }

    /// <summary>
    /// Event args generated when a scope is about to be saved.
    /// </summary>
    public class ScopeInfoSavingArgs : ProgressArgs
    {
        /// <inheritdoc cref="ScopeInfoSavingArgs" />
        public ScopeInfoSavingArgs(SyncContext context, ScopeInfo scopeInfo, DbCommand command, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            this.Command = command;
            this.ScopeInfo = scopeInfo;
        }

        /// <summary>
        /// Gets or sets a value indicating whether the operation should be canceled.
        /// </summary>
        public bool Cancel { get; set; }

        /// <summary>
        /// Gets or sets the command to be executed.
        /// </summary>
        public DbCommand Command { get; set; }

        /// <summary>
        /// Gets the scope info.
        /// </summary>
        public ScopeInfo ScopeInfo { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel" />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override string Message => $"Scope Info Saving.";

        /// <inheritdoc cref="ProgressArgs.EventId" />
        public override int EventId => 7400;
    }

    /// <summary>
    /// Event args generated when a scope is saved.
    /// </summary>
    public class ScopeInfoSavedArgs : ProgressArgs
    {
        /// <inheritdoc cref="ScopeInfoSavedArgs" />
        public ScopeInfoSavedArgs(SyncContext context, ScopeInfo scopeInfo, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction) => this.ScopeInfo = scopeInfo;

        /// <summary>
        /// Gets the scope info.
        /// </summary>
        public ScopeInfo ScopeInfo { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel" />
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <inheritdoc cref="ProgressArgs.Message" />
        public override string Message => $"Scope Info Saved.";

        /// <inheritdoc cref="ProgressArgs.EventId" />
        public override int EventId => 7450;
    }

    /// <summary>
    /// Interceptor extension methods.
    /// </summary>
    public partial class InterceptorsExtensions
    {
        /// <summary>
        /// Intercept the provider action when a scope table is creating.
        /// </summary>
        public static Guid OnScopeInfoTableCreating(this BaseOrchestrator orchestrator, Action<ScopeInfoTableCreatingArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope table is creating.
        /// </summary>
        public static Guid OnScopeInfoTableCreating(this BaseOrchestrator orchestrator, Func<ScopeInfoTableCreatingArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope table is created.
        /// </summary>
        public static Guid OnScopeInfoTableCreated(this BaseOrchestrator orchestrator, Action<ScopeInfoTableCreatedArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope table is created.
        /// </summary>
        public static Guid OnScopeInfoTableCreated(this BaseOrchestrator orchestrator, Func<ScopeInfoTableCreatedArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope table is dropping.
        /// </summary>
        public static Guid OnScopeInfoTableDropping(this BaseOrchestrator orchestrator, Action<ScopeInfoTableDroppingArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope table is dropping.
        /// </summary>
        public static Guid OnScopeInfoTableDropping(this BaseOrchestrator orchestrator, Func<ScopeInfoTableDroppingArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope table is dropped.
        /// </summary>
        public static Guid OnScopeInfoTableDropped(this BaseOrchestrator orchestrator, Action<ScopeInfoTableDroppedArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope table is dropped.
        /// </summary>
        public static Guid OnScopeInfoTableDropped(this BaseOrchestrator orchestrator, Func<ScopeInfoTableDroppedArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a client scope is about to be loaded from client database.
        /// </summary>
        public static Guid OnScopeInfoLoading(this BaseOrchestrator orchestrator, Action<ScopeInfoLoadingArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a client scope is about to be loaded from client database.
        /// </summary>
        public static Guid OnScopeInfoLoading(this BaseOrchestrator orchestrator, Func<ScopeInfoLoadingArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope is loaded from client or server database.
        /// </summary>
        public static Guid OnScopeInfoLoaded(this BaseOrchestrator orchestrator, Action<ScopeInfoLoadedArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope is loaded from client or server database.
        /// </summary>
        public static Guid OnScopeInfoLoaded(this BaseOrchestrator orchestrator, Func<ScopeInfoLoadedArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope is saving.
        /// </summary>
        public static Guid OnScopeSaving(this BaseOrchestrator orchestrator, Action<ScopeInfoSavingArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope is saving.
        /// </summary>
        public static Guid OnScopeSaving(this BaseOrchestrator orchestrator, Func<ScopeInfoSavingArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope is saved.
        /// </summary>
        public static Guid OnScopeSaved(this BaseOrchestrator orchestrator, Action<ScopeInfoSavedArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider action when a scope is saved.
        /// </summary>
        public static Guid OnScopeSaved(this BaseOrchestrator orchestrator, Func<ScopeInfoSavedArgs, Task> action)
            => orchestrator.AddInterceptor(action);
    }
}