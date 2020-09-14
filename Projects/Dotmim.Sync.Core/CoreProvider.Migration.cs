using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Manager;
using Dotmim.Sync.Serialization;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public abstract partial class CoreProvider
    {
        /// <summary>
        /// Migrate from a setup to another setup
        /// </summary>
        public virtual async Task<SyncContext> MigrationAsync(SyncContext context, SyncSet schema, SyncSetup oldSetup, SyncSetup newSetup, bool includeTable,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            // Create a new migration
            var migration = new Migration(newSetup, oldSetup);

            // get comparision results
            var migrationResults = migration.Compare();

            // get Database builder
            var builder = this.GetDatabaseBuilder();
            builder.UseChangeTracking = this.UseChangeTracking;

            // Deprovision
            // Generate a fake SyncSet since we don't need complete table schema
            foreach (var migrationTable in migrationResults.Tables)
            {
                this.Orchestrator.logger.LogDebug(SyncEventsId.Migration, migrationTable);

                var tableBuilder = this.GetTableBuilder(new SyncTable(migrationTable.SetupTable.TableName, migrationTable.SetupTable.SchemaName), oldSetup);

                // set if the builder supports creating the bulk operations proc stock
                tableBuilder.UseChangeTracking = this.UseChangeTracking;

                // Deprovision
                if (migrationTable.Triggers == MigrationAction.Drop || migrationTable.Triggers == MigrationAction.CreateOrRecreate)
                    await tableBuilder.DropTriggersAsync(connection, transaction);

                if (migrationTable.TrackingTable == MigrationAction.Drop || migrationTable.TrackingTable == MigrationAction.CreateOrRecreate)
                    await tableBuilder.DropTrackingTableAsync(connection, transaction);

                if (includeTable && (migrationTable.Table == MigrationAction.Drop || migrationTable.Table == MigrationAction.CreateOrRecreate))
                    await tableBuilder.DropTableAsync(connection, transaction);

            }

            // Provision
            // need the real SyncSet since we need columns definition
            foreach (var migrationTable in migrationResults.Tables)
            {
                var syncTable = schema.Tables[migrationTable.SetupTable.TableName, migrationTable.SetupTable.SchemaName];

                // a table that we drop of the setup
                if (syncTable == null)
                    continue;

                var tableBuilder = this.GetTableBuilder(syncTable, newSetup);

                // set if the builder supports creating the bulk operations proc stock
                tableBuilder.UseChangeTracking = this.UseChangeTracking;

                // Re provision table
                if (migrationTable.Table == MigrationAction.CreateOrRecreate && includeTable)
                {
                    await tableBuilder.DropTableAsync(connection, transaction);
                    await tableBuilder.CreateTableAsync(connection, transaction);
                }


                // Re provision tracking table
                if (migrationTable.TrackingTable == MigrationAction.Rename)
                {
                    var oldTable = oldSetup.Tables[migrationTable.SetupTable.TableName, migrationTable.SetupTable.SchemaName];

                    var oldTableBuilder = this.GetTableBuilder(new SyncTable(oldTable.TableName, oldTable.SchemaName), oldSetup);

                    var oldTableName = oldTableBuilder.TrackingTableName;

                    if (oldTable != null)
                        await tableBuilder.RenameTrackingTableAsync(oldTableName, connection, transaction);
                }
                else if (migrationTable.TrackingTable == MigrationAction.CreateOrRecreate)
                {
                    await tableBuilder.DropTrackingTableAsync(connection, transaction);
                    await tableBuilder.CreateTrackingTableAsync(connection, transaction);
                }

                // Re provision triggers
                if (migrationTable.Triggers == MigrationAction.CreateOrRecreate)
                {
                    await tableBuilder.DropTriggersAsync(connection, transaction);
                    await tableBuilder.CreateTriggersAsync(connection, transaction);
                }

               
            }
            return context;
        }


    }
}
