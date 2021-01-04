using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Manager;
using Dotmim.Sync.Serialization;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public abstract partial class BaseOrchestrator
    {

        /// <summary>
        /// Migrate from a setup to another setup
        /// </summary>
        internal async Task<SyncContext> InternalMigrationAsync(SyncContext context, SyncSet schema, SyncSetup oldSetup, SyncSetup newSetup, bool includeTable,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            // Launch InterceptAsync on Migrating
            await this.InterceptAsync(new MigratingArgs(context, schema, oldSetup, newSetup, connection, transaction), cancellationToken).ConfigureAwait(false);

            // Create a new migration
            var migration = new Migration(newSetup, oldSetup);

            // get comparision results
            var migrationResults = migration.Compare();

            // Deprovision
            // Generate a fake SyncSet since we don't need complete table schema
            foreach (var migrationTable in migrationResults.Tables)
            {
                var tableBuilder = this.Provider.GetTableBuilder(new SyncTable(migrationTable.SetupTable.TableName, migrationTable.SetupTable.SchemaName), oldSetup);

                // Deprovision stored procedures
                if (migrationTable.StoredProcedures == MigrationAction.Drop || migrationTable.StoredProcedures == MigrationAction.CreateOrRecreate)
                    await InternalDropStoredProceduresAsync(context, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                // Deprovision triggers
                if (migrationTable.Triggers == MigrationAction.Drop || migrationTable.Triggers == MigrationAction.CreateOrRecreate)
                {
                    var listTriggerType = Enum.GetValues(typeof(DbTriggerType));

                    foreach (DbTriggerType triggerType in listTriggerType)
                    {
                        var exists = await InternalExistsTriggerAsync(context, tableBuilder, triggerType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                        if (!exists)
                            continue;
                        await InternalDropTriggerAsync(context, tableBuilder, triggerType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                    }
                }

                if (migrationTable.TrackingTable == MigrationAction.Drop || migrationTable.TrackingTable == MigrationAction.CreateOrRecreate)
                {
                    var exists = await InternalExistsTrackingTableAsync(context, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (exists)
                        await InternalDropTrackingTableAsync(context, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                }

                if (includeTable && (migrationTable.Table == MigrationAction.Drop || migrationTable.Table == MigrationAction.CreateOrRecreate))
                {
                    var exists = await InternalExistsTableAsync(context, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (exists)
                        await InternalDropTableAsync(context, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                }

            }

            // Provision
            // need the real SyncSet since we need columns definition
            foreach (var migrationTable in migrationResults.Tables)
            {
                var syncTable = schema.Tables[migrationTable.SetupTable.TableName, migrationTable.SetupTable.SchemaName];

                // a table that we drop of the setup
                if (syncTable == null)
                    continue;

                var tableBuilder = this.Provider.GetTableBuilder(syncTable, newSetup);

                // Re provision table
                if (migrationTable.Table == MigrationAction.CreateOrRecreate && includeTable)
                {
                    var exists = await InternalExistsTableAsync(context, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (exists)
                        await InternalDropTableAsync(context, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    await InternalCreateTableAsync(context, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                }

                // Re provision tracking table
                if (migrationTable.TrackingTable == MigrationAction.Rename)
                {
                    var oldTable = oldSetup.Tables[migrationTable.SetupTable.TableName, migrationTable.SetupTable.SchemaName];

                    if (oldTable != null)
                    {
                        var (_, oldTableName) = this.Provider.GetParsers(new SyncTable(oldTable.TableName, oldTable.SchemaName), oldSetup);

                        await InternalRenameTrackingTableAsync(context, oldTableName, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                    }

                }
                else if (migrationTable.TrackingTable == MigrationAction.CreateOrRecreate)
                {
                    var exists = await InternalExistsTrackingTableAsync(context, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                    if (exists)
                        await InternalDropTrackingTableAsync(context, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    await InternalCreateTrackingTableAsync(context, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                }

                // Re provision stored procedures
                if (migrationTable.StoredProcedures == MigrationAction.CreateOrRecreate)
                {
                    await InternalCreateStoredProceduresAsync(context, true, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                }

                // Re provision triggers
                if (migrationTable.Triggers == MigrationAction.CreateOrRecreate)
                {
                    var listTriggerType = Enum.GetValues(typeof(DbTriggerType));

                    foreach (DbTriggerType triggerType in listTriggerType)
                    {
                        var exists = await InternalExistsTriggerAsync(context, tableBuilder, triggerType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // Drop trigger if already exists
                        if (exists)
                            await InternalDropTriggerAsync(context, tableBuilder, triggerType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        var shouldCreate = !exists;

                        if (!shouldCreate)
                            continue;

                        await InternalCreateTriggerAsync(context, tableBuilder, triggerType, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                    }
                }
            }

            // InterceptAsync Migrated
            var args = new MigratedArgs(context, schema, this.Setup);
            await this.InterceptAsync(args, cancellationToken).ConfigureAwait(false);
            this.ReportProgress(context, progress, args);

            return context;
        }


    }
}
