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

        ///// <summary>
        ///// Migrate from a setup to another setup
        ///// </summary>
        //internal async Task<SyncContext> InternalMigrationAsync(SyncContext context, ScopeInfo oldScopeInfo, ServerScopeInfo newScopeInfo,
        //                     DbConnection connection, DbTransaction transaction,
        //                     CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        //{
        //    // Create a new migration
        //    var migration = new Migration(oldScopeInfo, newScopeInfo);

        //    // get comparision results
        //    var migrationResults = migration.Compare();

        //    // Launch InterceptAsync on Migrating
        //    await this.InterceptAsync(new MigratingArgs(context, oldScopeInfo, newScopeInfo, migrationResults, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

        //    // Deprovision triggers stored procedures and tracking table if required
        //    foreach (var migrationTable in migrationResults.Tables)
        //    {
        //        // using a fake SyncTable based on oldSetup, since we don't need columns, but we need to have the filters
        //        var schemaTable = new SyncTable(migrationTable.SetupTable.TableName, migrationTable.SetupTable.SchemaName);

        //        // Create a temporary SyncSet for attaching to the schemaTable
        //        var tmpSchema = new SyncSet();

        //        // Add this table to schema
        //        tmpSchema.Tables.Add(schemaTable);

        //        tmpSchema.EnsureSchema();

        //        // copy filters from old setup
        //        foreach (var filter in oldScopeInfo.Setup.Filters)
        //            tmpSchema.Filters.Add(filter);

        //        // using a fake Synctable, since we don't need columns to deprovision

        //        // We don't uste GetTableBuilder since scopeName is already the new one and we need to deprovision from old one
        //        //var tableBuilder = this.GetTableBuilder(schemaTable, oldScopeInfo.Setup);

        //        var (tableName, trackingTableName) = this.Provider.GetParsers(schemaTable, oldScopeInfo.Setup);
        //        var tableBuilder = this.Provider.GetTableBuilder(schemaTable, tableName, trackingTableName, oldScopeInfo.Setup, oldScopeInfo.Name);

        //        // Deprovision stored procedures
        //        if (migrationTable.StoredProcedures == MigrationAction.Drop || migrationTable.StoredProcedures == MigrationAction.Create)
        //            await InternalDropStoredProceduresAsync(context, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

        //        // Deprovision triggers
        //        if (migrationTable.Triggers == MigrationAction.Drop || migrationTable.Triggers == MigrationAction.Create)
        //            await InternalDropTriggersAsync(context, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

        //        // Deprovision tracking table
        //        if (migrationTable.TrackingTable == MigrationAction.Drop || migrationTable.TrackingTable == MigrationAction.Create)
        //        {
        //            var exists = await InternalExistsTrackingTableAsync(context, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

        //            if (exists)
        //                await InternalDropTrackingTableAsync(context, oldScopeInfo.Setup, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
        //        }

        //        // Removing cached commands
        //        var syncAdapter = this.GetSyncAdapter(schemaTable, oldScopeInfo);
        //        syncAdapter.RemoveCommands();
        //    }

        //    // Provision table (create or alter), tracking tables, stored procedures and triggers
        //    // Need the real SyncSet since we need columns definition
        //    foreach (var migrationTable in migrationResults.Tables)
        //    {
        //        var syncTable = newScopeInfo.Schema.Tables[migrationTable.SetupTable.TableName, migrationTable.SetupTable.SchemaName];
        //        var oldTable = oldScopeInfo.Setup.Tables[migrationTable.SetupTable.TableName, migrationTable.SetupTable.SchemaName];

        //        if (syncTable == null)
        //            continue;

        //        var tableBuilder = this.GetTableBuilder(syncTable, newScopeInfo);

        //        // Re provision table
        //        if (migrationTable.Table == MigrationAction.Create)
        //        {
        //            // Check if we need to create a schema there
        //            var schemaExists = await InternalExistsSchemaAsync(context, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

        //            if (!schemaExists)
        //                await InternalCreateSchemaAsync(context, newScopeInfo.Setup, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

        //            var exists = await InternalExistsTableAsync(context, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

        //            if (!exists)
        //                await InternalCreateTableAsync(context, newScopeInfo.Setup, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
        //        }

        //        // Re provision table
        //        if (migrationTable.Table == MigrationAction.Alter)
        //        {
        //            var exists = await InternalExistsTableAsync(context, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

        //            if (!exists)
        //            {
        //                await InternalCreateTableAsync(context, newScopeInfo.Setup, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
        //            }
        //            else if (oldTable != null)
        //            {
        //                //get new columns to add
        //                var newColumns = syncTable.Columns.Where(c => !oldTable.Columns.Any(oldC => string.Equals(oldC, c.ColumnName, SyncGlobalization.DataSourceStringComparison)));

        //                if (newColumns != null)
        //                {
        //                    foreach (var newColumn in newColumns)
        //                    {
        //                        var columnExist = await InternalExistsColumnAsync(context, newColumn.ColumnName, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
        //                        if (!columnExist)
        //                            await InternalAddColumnAsync(context, newScopeInfo.Setup, newColumn.ColumnName, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
        //                    }
        //                }
        //            }
        //        }

        //        // Re provision tracking table
        //        if (migrationTable.TrackingTable == MigrationAction.Rename && oldTable != null)
        //        {
        //            var (_, oldTableName) = this.Provider.GetParsers(new SyncTable(oldTable.TableName, oldTable.SchemaName), oldScopeInfo.Setup);

        //            await InternalRenameTrackingTableAsync(context, newScopeInfo.Setup, oldTableName, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
        //        }
        //        else if (migrationTable.TrackingTable == MigrationAction.Create)
        //        {
        //            var exists = await InternalExistsTrackingTableAsync(context, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
        //            if (exists)
        //                await InternalDropTrackingTableAsync(context, newScopeInfo.Setup, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

        //            await InternalCreateTrackingTableAsync(context, newScopeInfo.Setup, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
        //        }

        //        // Re provision stored procedures
        //        if (migrationTable.StoredProcedures == MigrationAction.Create)
        //            await InternalCreateStoredProceduresAsync(context, true, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

        //        // Re provision triggers
        //        if (migrationTable.Triggers == MigrationAction.Create)
        //            await InternalCreateTriggersAsync(context, true, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
        //    }

        //    // InterceptAsync Migrated
        //    var args = new MigratedArgs(context, newScopeInfo.Schema, newScopeInfo.Setup, migrationResults, connection, transaction);
        //    await this.InterceptAsync(args, progress, cancellationToken).ConfigureAwait(false);

        //    return context;
        //}


    }
}
