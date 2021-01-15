using System;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Microsoft.Extensions.Logging;

namespace Dotmim.Sync
{

    public class ProvisionedArgs : ProgressArgs
    {
        public SyncProvision Provision { get; }
        public SyncSet Schema { get; }

        public ProvisionedArgs(SyncContext context, SyncProvision provision, SyncSet schema, DbConnection connection = null, DbTransaction transaction = null)
        : base(context, connection, transaction)

        {
            Provision = provision;
            Schema = schema;
        }

        public override string Message => $"[{Connection.Database}] Provisioned {Schema.Tables.Count} Tables. Provision:{Provision}.";

        public override int EventId => SyncEventsId.Provisioned.Id;
    }

    public class ProvisioningArgs : ProgressArgs
    {
        /// <summary>
        /// Get the provision type (Flag enum)
        /// </summary>
        public SyncProvision Provision { get; }

        /// <summary>
        /// Gets the schema to be applied in the database
        /// </summary>
        public SyncSet Schema { get; }

        public ProvisioningArgs(SyncContext context, SyncProvision provision, SyncSet schema, DbConnection connection, DbTransaction transaction)
        : base(context, connection, transaction)

        {
            Provision = provision;
            Schema = schema;
        }

        public override string Message => $"[{Connection.Database}] Provisioning {Schema.Tables.Count} Tables. Provision:{Provision}.";

        public override int EventId => SyncEventsId.Provisioning.Id;
    }

    public class DeprovisionedArgs : ProvisionedArgs
    {
        public DeprovisionedArgs(SyncContext context, SyncProvision provision, SyncSet schema, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, provision, schema, connection, transaction)
        {
        }
        public override int EventId => SyncEventsId.Deprovisioned.Id;
    }

    public class DeprovisioningArgs : ProvisioningArgs
    {
        public DeprovisioningArgs(SyncContext context, SyncProvision provision, SyncSet schema, DbConnection connection, DbTransaction transaction) : base(context, provision, schema, connection, transaction)
        {
        }
        public override int EventId => SyncEventsId.Deprovisioning.Id;
    }

    public static partial class InterceptorsExtensions
    {

        /// <summary>
        /// Intercept the provider before it begins a database provisioning
        /// </summary>
        public static void OnProvisioning(this BaseOrchestrator orchestrator, Action<ProvisioningArgs> action)
            => orchestrator.SetInterceptor(action);
        /// <summary>
        /// Intercept the provider before it begins a database provisioning
        /// </summary>
        public static void OnProvisioning(this BaseOrchestrator orchestrator, Func<ProvisioningArgs, Task> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider after it has provisioned a database
        /// </summary>
        public static void OnProvisioned(this BaseOrchestrator orchestrator, Action<ProvisionedArgs> action)
            => orchestrator.SetInterceptor(action);
        /// <summary>
        /// Intercept the provider after it has provisioned a database
        /// </summary>
        public static void OnProvisioned(this BaseOrchestrator orchestrator, Func<ProvisionedArgs, Task> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider before it begins a database deprovisioning
        /// </summary>
        public static void OnDeprovisioning(this BaseOrchestrator orchestrator, Action<DeprovisioningArgs> action)
            => orchestrator.SetInterceptor(action);
        /// <summary>
        /// Intercept the provider before it begins a database deprovisioning
        /// </summary>
        public static void OnDeprovisioning(this BaseOrchestrator orchestrator, Func<DeprovisioningArgs, Task> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the provider after it has deprovisioned a database
        /// </summary>
        public static void OnDeprovisioned(this BaseOrchestrator orchestrator, Action<DeprovisionedArgs> action)
            => orchestrator.SetInterceptor(action);
        /// <summary>
        /// Intercept the provider after it has deprovisioned a database
        /// </summary>
        public static void OnDeprovisioned(this BaseOrchestrator orchestrator, Func<DeprovisionedArgs, Task> action)
            => orchestrator.SetInterceptor(action);

    }

    public static partial class SyncEventsId
    {
        public static EventId Provisioning => CreateEventId(5000, nameof(Provisioning));
        public static EventId Provisioned => CreateEventId(5100, nameof(Provisioned));
        public static EventId Deprovisioning => CreateEventId(5200, nameof(Deprovisioning));
        public static EventId Deprovisioned => CreateEventId(5300, nameof(Deprovisioned));
    }



}