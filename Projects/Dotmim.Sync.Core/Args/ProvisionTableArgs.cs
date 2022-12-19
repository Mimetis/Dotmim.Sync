using System;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Microsoft.Extensions.Logging;

namespace Dotmim.Sync
{

    public class ProvisionedTableArgs : ProgressArgs
    {
        private readonly bool atLeastSomethingHasBeenCreated;

        public SyncProvision Provision { get; }
        public SyncSet Schema { get; }
        public SyncTable Table { get; }

        public ProvisionedTableArgs(SyncContext context, SyncProvision provision, SyncSet schema, SyncTable table, bool atLeastSomethingHasBeenCreated, DbConnection connection = null, DbTransaction transaction = null)
        : base(context, connection, transaction)

        {
            this.Provision = provision;
            this.Schema = schema;
            this.Table = table;
            this.atLeastSomethingHasBeenCreated = atLeastSomethingHasBeenCreated;
        }
        public override SyncProgressLevel ProgressLevel => this.atLeastSomethingHasBeenCreated?  SyncProgressLevel.Information : SyncProgressLevel.Debug;

        public override string Message => $"Provisioned {Table.GetFullName()}. Provision:{Provision}.";

        public override int EventId => SyncEventsId.ProvisionedTable.Id;
    }

    public class ProvisioningTableArgs : ProgressArgs
    {
        /// <summary>
        /// Get the provision type (Flag enum)
        /// </summary>
        public SyncProvision Provision { get; }

        /// <summary>
        /// Gets the schema to be applied in the database
        /// </summary>
        public SyncSet Schema { get; }
        public SyncTable Table { get; }

        public ProvisioningTableArgs(SyncContext context, SyncProvision provision, SyncSet schema, SyncTable table, DbConnection connection, DbTransaction transaction)
        : base(context, connection, transaction)

        {
            Provision = provision;
            Schema = schema;
            this.Table = table;
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        public override string Message => $"Provisioning {Table.GetFullName()}. Provision:{Provision}.";

        public override int EventId => SyncEventsId.ProvisioningTable.Id;
    }

    public class DeprovisionedTableArgs : ProgressArgs
    {
        private readonly bool atLeastSomethingHasBeenDropped;

        public SyncProvision Provision { get; }
        public SyncTable Table { get; }

        public DeprovisionedTableArgs(SyncContext context, SyncProvision provision, SyncTable table, bool atLeastSomethingHasBeenDropped, DbConnection connection = null, DbTransaction transaction = null)
        : base(context, connection, transaction)
        {
            this.Provision = provision;
            this.atLeastSomethingHasBeenDropped = atLeastSomethingHasBeenDropped;
            this.Table = table;
        }
        public override SyncProgressLevel ProgressLevel => this.atLeastSomethingHasBeenDropped ? SyncProgressLevel.Information : SyncProgressLevel.Debug;
        public override string Message => $"Deprovisioned {this.Table.GetFullName()} Table. Deprovision:{Provision}.";
        public override int EventId => SyncEventsId.DeprovisionedTable.Id;
    }

    public class DeprovisioningTableArgs : ProgressArgs
    {
        /// <summary>
        /// Get the provision type (Flag enum)
        /// </summary>
        public SyncProvision Provision { get; }

        public SyncTable Table { get; }

        public DeprovisioningTableArgs(SyncContext context, SyncProvision provision, SyncTable table, DbConnection connection, DbTransaction transaction)
        : base(context, connection, transaction)

        {
            Provision = provision;
            this.Table = table;
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;
        public override string Message => $"Deprovisioning {Table.GetFullName()} Table. Deprovision:{Provision}.";
        public override int EventId => SyncEventsId.DeprovisioningTable.Id;
    }

    public static partial class InterceptorsExtensions
    {

        /// <summary>
        /// Intercept the provider before it begins a table provisioning
        /// </summary>
        public static Guid OnProvisioningTable(this BaseOrchestrator orchestrator, Action<ProvisioningTableArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider before it begins a table provisioning
        /// </summary>
        public static Guid OnProvisioningTable(this BaseOrchestrator orchestrator, Func<ProvisioningTableArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider after it has provisioned a table
        /// </summary>
        public static Guid OnProvisionedTable(this BaseOrchestrator orchestrator, Action<ProvisionedTableArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider after it has provisioned a table
        /// </summary>
        public static Guid OnProvisionedTable(this BaseOrchestrator orchestrator, Func<ProvisionedTableArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider before it begins a table deprovisioning
        /// </summary>
        public static Guid OnDeprovisioningTable(this BaseOrchestrator orchestrator, Action<DeprovisioningTableArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider before it begins a table deprovisioning
        /// </summary>
        public static Guid OnDeprovisioningTable(this BaseOrchestrator orchestrator, Func<DeprovisioningTableArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider after it has deprovisioned a table
        /// </summary>
        public static Guid OnDeprovisionedTable(this BaseOrchestrator orchestrator, Action<DeprovisionedTableArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider after it has deprovisioned a table
        /// </summary>
        public static Guid OnDeprovisionedTable(this BaseOrchestrator orchestrator, Func<DeprovisionedTableArgs, Task> action)
            => orchestrator.AddInterceptor(action);

    }

    public static partial class SyncEventsId
    {
        public static EventId ProvisioningTable => CreateEventId(5001, nameof(ProvisioningTable));
        public static EventId ProvisionedTable => CreateEventId(5051, nameof(ProvisionedTable));
        public static EventId DeprovisioningTable => CreateEventId(5101, nameof(DeprovisioningTable));
        public static EventId DeprovisionedTable => CreateEventId(5151, nameof(DeprovisionedTable));
    }



}