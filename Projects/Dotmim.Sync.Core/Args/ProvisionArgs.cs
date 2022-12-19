﻿using System;
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
        private readonly bool atLeastSomethingHasBeenCreated;

        public SyncProvision Provision { get; }
        public SyncSet Schema { get; }

        public ProvisionedArgs(SyncContext context, SyncProvision provision, SyncSet schema, bool atLeastSomethingHasBeenCreated, DbConnection connection = null, DbTransaction transaction = null)
        : base(context, connection, transaction)

        {
            this.Provision = provision;
            this.Schema = schema;
            this.atLeastSomethingHasBeenCreated = atLeastSomethingHasBeenCreated;
        }
        public override SyncProgressLevel ProgressLevel => this.atLeastSomethingHasBeenCreated?  SyncProgressLevel.Information : SyncProgressLevel.Debug;

        public override string Message => $"Provisioned {Schema.Tables.Count} Tables. Provision:{Provision}.";

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
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        public override string Message => $"Provisioning {Schema.Tables.Count} Tables. Provision:{Provision}.";

        public override int EventId => SyncEventsId.Provisioning.Id;
    }

    public class DeprovisionedArgs : ProgressArgs
    {
        private readonly bool atLeastSomethingHasBeenDropped;

        public SyncProvision Provision { get; }
        public SyncSetup Setup { get; }


        public DeprovisionedArgs(SyncContext context, SyncProvision provision, SyncSetup setup, bool atLeastSomethingHasBeenDropped, DbConnection connection = null, DbTransaction transaction = null)
        : base(context, connection, transaction)
        {
            this.Provision = provision;
            this.atLeastSomethingHasBeenDropped = atLeastSomethingHasBeenDropped;
            this.Setup = setup;
        }
        public override SyncProgressLevel ProgressLevel => this.atLeastSomethingHasBeenDropped ? SyncProgressLevel.Information : SyncProgressLevel.Debug;
        public override string Message => $"Deprovisioned {Setup.Tables.Count} Tables. Deprovision:{Provision}.";
        public override int EventId => SyncEventsId.Deprovisioned.Id;
    }

    public class DeprovisioningArgs : ProgressArgs
    {
        /// <summary>
        /// Get the provision type (Flag enum)
        /// </summary>
        public SyncProvision Provision { get; }

        /// <summary>
        /// Gets the schema to be applied in the database
        /// </summary>
        public SyncSetup Setup { get; }
        public DeprovisioningArgs(SyncContext context, SyncProvision provision, SyncSetup setup, DbConnection connection, DbTransaction transaction)
        : base(context, connection, transaction)

        {
            Provision = provision;
            Setup = setup;
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;
        public override string Message => $"Deprovisioning {Setup.Tables.Count} Tables. Deprovision:{Provision}.";
        public override int EventId => SyncEventsId.Deprovisioning.Id;
    }

    public static partial class InterceptorsExtensions
    {

        /// <summary>
        /// Intercept the provider before it begins a database provisioning
        /// </summary>
        public static Guid OnProvisioning(this BaseOrchestrator orchestrator, Action<ProvisioningArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider before it begins a database provisioning
        /// </summary>
        public static Guid OnProvisioning(this BaseOrchestrator orchestrator, Func<ProvisioningArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider after it has provisioned a database
        /// </summary>
        public static Guid OnProvisioned(this BaseOrchestrator orchestrator, Action<ProvisionedArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider after it has provisioned a database
        /// </summary>
        public static Guid OnProvisioned(this BaseOrchestrator orchestrator, Func<ProvisionedArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider before it begins a database deprovisioning
        /// </summary>
        public static Guid OnDeprovisioning(this BaseOrchestrator orchestrator, Action<DeprovisioningArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider before it begins a database deprovisioning
        /// </summary>
        public static Guid OnDeprovisioning(this BaseOrchestrator orchestrator, Func<DeprovisioningArgs, Task> action)
            => orchestrator.AddInterceptor(action);

        /// <summary>
        /// Intercept the provider after it has deprovisioned a database
        /// </summary>
        public static Guid OnDeprovisioned(this BaseOrchestrator orchestrator, Action<DeprovisionedArgs> action)
            => orchestrator.AddInterceptor(action);
        /// <summary>
        /// Intercept the provider after it has deprovisioned a database
        /// </summary>
        public static Guid OnDeprovisioned(this BaseOrchestrator orchestrator, Func<DeprovisionedArgs, Task> action)
            => orchestrator.AddInterceptor(action);

    }

    public static partial class SyncEventsId
    {
        public static EventId Provisioning => CreateEventId(5000, nameof(Provisioning));
        public static EventId Provisioned => CreateEventId(5050, nameof(Provisioned));
        public static EventId Deprovisioning => CreateEventId(5100, nameof(Deprovisioning));
        public static EventId Deprovisioned => CreateEventId(5150, nameof(Deprovisioned));
    }



}