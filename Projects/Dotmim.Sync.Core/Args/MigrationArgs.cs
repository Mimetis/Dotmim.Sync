using Dotmim.Sync.Enumerations;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Object representing a migration about to start
    /// </summary>
    public class MigratingArgs : ProgressArgs
    {
        public MigratingArgs(SyncContext context, SyncSet newSchema, SyncSetup oldSetup, SyncSetup newSetup, MigrationResults migrationResults, DbConnection connection, DbTransaction transaction) : base(context, connection, transaction)
        {
            this.NewSchema = newSchema;
            this.OldSetup = oldSetup;
            this.NewSetup = newSetup;
            this.MigrationResults = migrationResults;
        }

        public override string Source => Connection.Database;
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Debug;

        /// <summary>
        /// Gets message about migration
        /// </summary>
        public override string Message => $"Applying Migration.";

        /// <summary>
        /// Gets the schema used to apply migration
        /// </summary>
        public SyncSet NewSchema { get; }

        /// <summary>
        /// Gets the old setup to migrate
        /// </summary>
        public SyncSetup OldSetup { get; }

        /// <summary>
        /// Gets the new setup to apply
        /// </summary>
        public SyncSetup NewSetup { get; }
        public MigrationResults MigrationResults { get; }

        public override int EventId => SyncEventsId.DatabaseMigrating.Id;
    }

    /// <summary>
    /// Once migrated you have a new setup and schema available
    /// </summary>
    public class MigratedArgs : ProgressArgs
    {
        public MigratedArgs(SyncContext context, SyncSet schema, SyncSetup setup, MigrationResults migration, DbConnection connection = null, DbTransaction transaction = null) : base(context, connection, transaction)
        {
            this.Schema = schema;
            this.Setup = setup;
            this.Migration = migration;
        }
        public override SyncProgressLevel ProgressLevel => SyncProgressLevel.Information;

        public override string Source => Connection.Database;
        /// <summary>
        /// Gets message about migration
        /// </summary>
        public override string Message => $"Migrated. Tables:{Setup.Tables.Count}.";

        /// <summary>
        /// Gets the schema currently used
        /// </summary>
        public SyncSet Schema { get; }

        /// <summary>
        /// Gets the new setup applied
        /// </summary>
        public SyncSetup Setup { get; }

        /// <summary>
        /// Gets the Migration results
        /// </summary>
        public MigrationResults Migration { get; }

        public override int EventId => SyncEventsId.DatabaseMigrated.Id;
    }


    public static partial class InterceptorsExtensions
    {
        /// <summary>
        /// Intercept the orchestrator when migrating a Setup
        /// </summary>
        public static void OnMigrating(this BaseOrchestrator orchestrator, Action<MigratingArgs> action)
            => orchestrator.SetInterceptor(action);
        /// <summary>
        /// Intercept the orchestrator when migrating a Setup
        /// </summary>
        public static void OnMigrating(this BaseOrchestrator orchestrator, Func<MigratingArgs, Task> action)
            => orchestrator.SetInterceptor(action);

        /// <summary>
        /// Intercept the orchestrator when a Setup has been migrated
        /// </summary>
        public static void OnMigrated(this BaseOrchestrator orchestrator, Action<MigratedArgs> action)
            => orchestrator.SetInterceptor(action);
        /// <summary>
        /// Intercept the orchestrator when a Setup has been migrated
        /// </summary>
        public static void OnMigrated(this BaseOrchestrator orchestrator, Func<MigratedArgs, Task> action)
            => orchestrator.SetInterceptor(action);

    }


    public static partial class SyncEventsId
    {
        public static EventId DatabaseMigrating => CreateEventId(4000, nameof(DatabaseMigrating));
        public static EventId DatabaseMigrated => CreateEventId(4050, nameof(DatabaseMigrated));
    }

}
