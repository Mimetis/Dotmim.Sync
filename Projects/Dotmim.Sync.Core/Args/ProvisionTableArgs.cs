using Dotmim.Sync.Enumerations;
using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Event args generated after a table has been provisioned.
    /// </summary>
    public class ProvisionedTableArgs : ProgressArgs
    {
        private readonly bool atLeastSomethingHasBeenCreated;

        /// <inheritdoc cref="ProvisionedTableArgs"/>
        public ProvisionedTableArgs(SyncContext context, SyncProvision provision, ScopeInfo scopeInfo, SyncTable table, bool atLeastSomethingHasBeenCreated, DbConnection connection = null, DbTransaction transaction = null)
        : base(context, connection, transaction)
        {
            this.Provision = provision;
            this.ScopeInfo = scopeInfo;
            this.Table = table;
            this.atLeastSomethingHasBeenCreated = atLeastSomethingHasBeenCreated;
        }

        /// <summary>
        /// Gets the provision type (Flag enum).
        /// </summary>
        public SyncProvision Provision { get; }

        /// <summary>
        /// Gets the scope info that has been applied in the database.
        /// </summary>
        public ScopeInfo ScopeInfo { get; }

        /// <summary>
        /// Gets the table that has been provisioned.
        /// </summary>
        public SyncTable Table { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel"/>
        public override SyncProgressLevel ProgressLevel => this.atLeastSomethingHasBeenCreated ? SyncProgressLevel.Information : SyncProgressLevel.Debug;

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message => $"Provisioned {this.Table.GetFullName()}. Provision:{this.Provision}.";

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => 5051;
    }

    /// <summary>
    /// Event args generated before a table is provisioned.
    /// </summary>
    public class ProvisioningTableArgs : ProgressArgs
    {

        /// <inheritdoc cref="ProvisioningTableArgs"/>
        public ProvisioningTableArgs(SyncContext context, SyncProvision provision, ScopeInfo scopeInfo, SyncTable table, DbConnection connection, DbTransaction transaction)
        : base(context, connection, transaction)
        {
            this.Provision = provision;
            this.ScopeInfo = scopeInfo;
            this.Table = table;
        }

        /// <summary>
        /// Gets get the provision type (Flag enum).
        /// </summary>
        public SyncProvision Provision { get; }

        /// <summary>
        /// Gets the scope info to be applied in the database.
        /// </summary>
        public ScopeInfo ScopeInfo { get; }

        /// <summary>
        /// Gets the table to be provisioned.
        /// </summary>
        public SyncTable Table { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel"/>
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message => $"Provisioning {this.Table.GetFullName()}. Provision:{this.Provision}.";

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => 5001;
    }

    /// <summary>
    /// Event args generated after a table has been deprovisioned.
    /// </summary>
    public class DeprovisionedTableArgs : ProgressArgs
    {
        private readonly bool atLeastSomethingHasBeenDropped;

        /// <inheritdoc cref="DeprovisionedTableArgs"/>
        public DeprovisionedTableArgs(SyncContext context, SyncProvision provision, ScopeInfo scopeInfo, SyncTable table, bool atLeastSomethingHasBeenDropped, DbConnection connection = null, DbTransaction transaction = null)
        : base(context, connection, transaction)
        {
            this.Provision = provision;
            this.ScopeInfo = scopeInfo;
            this.atLeastSomethingHasBeenDropped = atLeastSomethingHasBeenDropped;
            this.Table = table;
        }

        /// <summary>
        /// Gets the provision type (Flag enum).
        /// </summary>
        public SyncProvision Provision { get; }

        /// <summary>
        /// Gets the scope info that has been deprovisioned in the database.
        /// </summary>
        public ScopeInfo ScopeInfo { get; }

        /// <summary>
        /// Gets the table that has been deprovisioned.
        /// </summary>
        public SyncTable Table { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel"/>
        public override SyncProgressLevel ProgressLevel => this.atLeastSomethingHasBeenDropped ? SyncProgressLevel.Information : SyncProgressLevel.Debug;

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message => $"Deprovisioned {this.Table.GetFullName()} Table. Deprovision:{this.Provision}.";

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => 5151;
    }

    /// <summary>
    /// Event args generated before a table is deprovisioned.
    /// </summary>
    public class DeprovisioningTableArgs : ProgressArgs
    {

        /// <inheritdoc cref="DeprovisioningTableArgs"/>
        public DeprovisioningTableArgs(SyncContext context, SyncProvision provision, ScopeInfo scopeInfo, SyncTable table, DbConnection connection, DbTransaction transaction)
        : base(context, connection, transaction)
        {
            this.Provision = provision;
            this.ScopeInfo = scopeInfo;
            this.Table = table;
        }

        /// <summary>
        /// Gets get the provision type (Flag enum).
        /// </summary>
        public SyncProvision Provision { get; }

        /// <summary>
        /// Gets the scope info to be deprisioned in the database.
        /// </summary>
        public ScopeInfo ScopeInfo { get; private set; }

        /// <summary>
        /// Gets the table to be deprovisioned.
        /// </summary>
        public SyncTable Table { get; }

        /// <inheritdoc cref="ProgressArgs.ProgressLevel"/>
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <inheritdoc cref="ProgressArgs.Message"/>
        public override string Message => $"Deprovisioning {this.Table.GetFullName()} Table. Deprovision:{this.Provision}.";

        /// <inheritdoc cref="ProgressArgs.EventId"/>
        public override int EventId => 5101;
    }

    /// <summary>
    /// Interceptors extensions.
    /// </summary>
    public partial class InterceptorsExtensions
    {

        /// <summary>
        /// Intercept the provider before it begins a table provisioning.
        /// </summary>
        public static Guid OnProvisioningTable(this BaseOrchestrator orchestrator, Action<ProvisioningTableArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider before it begins a table provisioning.
        /// </summary>
        public static Guid OnProvisioningTable(this BaseOrchestrator orchestrator, Func<ProvisioningTableArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider after it has provisioned a table.
        /// </summary>
        public static Guid OnProvisionedTable(this BaseOrchestrator orchestrator, Action<ProvisionedTableArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider after it has provisioned a table.
        /// </summary>
        public static Guid OnProvisionedTable(this BaseOrchestrator orchestrator, Func<ProvisionedTableArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider before it begins a table deprovisioning.
        /// </summary>
        public static Guid OnDeprovisioningTable(this BaseOrchestrator orchestrator, Action<DeprovisioningTableArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider before it begins a table deprovisioning.
        /// </summary>
        public static Guid OnDeprovisioningTable(this BaseOrchestrator orchestrator, Func<DeprovisioningTableArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider after it has deprovisioned a table.
        /// </summary>
        public static Guid OnDeprovisionedTable(this BaseOrchestrator orchestrator, Action<DeprovisionedTableArgs> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider after it has deprovisioned a table.
        /// </summary>
        public static Guid OnDeprovisionedTable(this BaseOrchestrator orchestrator, Func<DeprovisionedTableArgs, Task> action)
            => orchestrator.AddInterceptor(action);
    }
}