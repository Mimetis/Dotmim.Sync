using Dotmim.Sync.Enumerations;
using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Event args generated after a database has been provisioned.
    /// </summary>
    public class ProvisionedArgs : ProgressArgs
    {
        private readonly bool atLeastSomethingHasBeenCreated;

        /// <inheritdoc cref="ProvisionedArgs"/>
        public ProvisionedArgs(SyncContext context, SyncProvision provision, ScopeInfo scopeInfo, bool atLeastSomethingHasBeenCreated, DbConnection connection = null, DbTransaction transaction = null)
        : base(context, connection, transaction)
        {
            this.Provision = provision;
            this.ScopeInfo = scopeInfo;
            this.atLeastSomethingHasBeenCreated = atLeastSomethingHasBeenCreated;
        }

        /// <summary>
        /// Gets the provision type (Flag enum).
        /// </summary>
        public SyncProvision Provision { get; }

        /// <summary>
        /// Gets the scope info used to provision the database.
        /// </summary>
        public ScopeInfo ScopeInfo { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel"/>
        public override SyncProgressLevel ProgressLevel => this.atLeastSomethingHasBeenCreated ? SyncProgressLevel.Information : SyncProgressLevel.Debug;

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message => $"Provisioned {this.ScopeInfo.Schema.Tables.Count} Tables. Provision:{this.Provision}.";

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => 5050;
    }

    /// <summary>
    /// Event args generated before a database is provisioned.
    /// </summary>
    public class ProvisioningArgs : ProgressArgs
    {
        /// <inheritdoc cref="ProvisioningArgs"/>
        public ProvisioningArgs(SyncContext context, SyncProvision provision, ScopeInfo scopeInfo, DbConnection connection, DbTransaction transaction)
        : base(context, connection, transaction)
        {
            this.Provision = provision;
            this.ScopeInfo = scopeInfo;
        }

        /// <summary>
        /// Gets get the provision type (Flag enum).
        /// </summary>
        public SyncProvision Provision { get; }

        /// <summary>
        /// Gets the scope info used to provision the database.
        /// </summary>
        public ScopeInfo ScopeInfo { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel"/>
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message => $"Provisioning {this.ScopeInfo.Schema.Tables.Count} Tables. Provision:{this.Provision}.";

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => 5000;
    }

    /// <summary>
    /// Event args generated after a database has been deprovisioned.
    /// </summary>
    public class DeprovisionedArgs : ProgressArgs
    {
        private readonly bool atLeastSomethingHasBeenDropped;

        /// <inheritdoc cref="DeprovisionedArgs"/>
        public DeprovisionedArgs(SyncContext context, SyncProvision provision, SyncSetup setup, bool atLeastSomethingHasBeenDropped, DbConnection connection = null, DbTransaction transaction = null)
        : base(context, connection, transaction)
        {
            this.Provision = provision;
            this.atLeastSomethingHasBeenDropped = atLeastSomethingHasBeenDropped;
            this.Setup = setup;
        }

        /// <summary>
        /// Gets the provision type (Flag enum).
        /// </summary>
        public SyncProvision Provision { get; }

        /// <summary>
        /// Gets the schema that has been used to deprovision the database.
        /// </summary>
        public SyncSetup Setup { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel"/>
        public override SyncProgressLevel ProgressLevel => this.atLeastSomethingHasBeenDropped ? SyncProgressLevel.Information : SyncProgressLevel.Debug;

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message => $"Deprovisioned {this.Setup.Tables.Count} Tables. Deprovision:{this.Provision}.";

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => 5150;
    }

    /// <summary>
    /// Event args generated before a database is deprovisioned.
    /// </summary>
    public class DeprovisioningArgs : ProgressArgs
    {

        /// <inheritdoc cref="DeprovisioningArgs"/>
        public DeprovisioningArgs(SyncContext context, SyncProvision provision, SyncSetup setup, DbConnection connection, DbTransaction transaction)
        : base(context, connection, transaction)
        {
            this.Provision = provision;
            this.Setup = setup;
        }

        /// <summary>
        /// Gets get the provision type (Flag enum).
        /// </summary>
        public SyncProvision Provision { get; }

        /// <summary>
        /// Gets the schema to be applied in the database.
        /// </summary>
        public SyncSetup Setup { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel"/>
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message => $"Deprovisioning {this.Setup.Tables.Count} Tables. Deprovision:{this.Provision}.";

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => 5100;
    }

    /// <summary>
    /// Interceptors extensions.
    /// </summary>
    public partial class InterceptorsExtensions
    {

        /// <summary>
        /// Intercept the provider before it begins a database provisioning.
        /// </summary>
        public static Guid OnProvisioning(this BaseOrchestrator orchestrator, Action<ProvisioningArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider before it begins a database provisioning.
        /// </summary>
        public static Guid OnProvisioning(this BaseOrchestrator orchestrator, Func<ProvisioningArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider after it has provisioned a database.
        /// </summary>
        public static Guid OnProvisioned(this BaseOrchestrator orchestrator, Action<ProvisionedArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider after it has provisioned a database.
        /// </summary>
        public static Guid OnProvisioned(this BaseOrchestrator orchestrator, Func<ProvisionedArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider before it begins a database deprovisioning.
        /// </summary>
        public static Guid OnDeprovisioning(this BaseOrchestrator orchestrator, Action<DeprovisioningArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider before it begins a database deprovisioning.
        /// </summary>
        public static Guid OnDeprovisioning(this BaseOrchestrator orchestrator, Func<DeprovisioningArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider after it has deprovisioned a database.
        /// </summary>
        public static Guid OnDeprovisioned(this BaseOrchestrator orchestrator, Action<DeprovisionedArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider after it has deprovisioned a database.
        /// </summary>
        public static Guid OnDeprovisioned(this BaseOrchestrator orchestrator, Func<DeprovisionedArgs, Task> action)
            => orchestrator.AddInterceptor(action);
    }
}