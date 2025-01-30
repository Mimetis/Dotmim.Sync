﻿using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Contains internal provisioning methods.
    /// </summary>
    public abstract partial class BaseOrchestrator
    {
        /// <summary>
        /// Internal provision method.
        /// </summary>
        internal virtual async Task<(SyncContext Context, bool Provisioned)> InternalProvisionAsync(ScopeInfo scopeInfo, SyncContext context, bool overwrite, SyncProvision provision,
            DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            if (this.Provider == null)
                throw new MissingProviderException(nameof(this.InternalProvisionAsync));

            context.SyncStage = SyncStage.Provisioning;

            // If schema does not have any table, raise an exception
            if (scopeInfo.Schema == null || scopeInfo.Schema.Tables == null || !scopeInfo.Schema.HasTables)
                throw new MissingTablesException();

            await this.InterceptAsync(new ProvisioningArgs(context, provision, scopeInfo, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            try
            {
                // get Database builder
                var builder = this.Provider.GetDatabaseBuilder();
                await builder.EnsureDatabaseAsync(connection, transaction).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                throw this.GetSyncError(context, ex, "Error during EnsureDatabaseAsync");
            }

            try
            {
                // Check if we have at least something created
                var atLeastOneStoredProcedureHasBeenCreated = false;
                var atLeastOneTriggerHasBeenCreated = false;
                var atLeastOneTrackingTableBeenCreated = false;
                var atLeastOneTableBeenCreated = false;
                var atLeastOneSchemaTableBeenCreated = false;
                var atLeastOneScopeInfoTableBeenCreated = false;
                var atLeastOneScopeInfoClientTableBeenCreated = false;

                // Check if we have tables AND columns
                // If we don't have any columns it's most probably because user called method with the Setup only
                // So far we have only tables names, it's enough to get the schema
                if (scopeInfo.Schema.HasTables && !scopeInfo.Schema.HasColumns)
                    (context, scopeInfo.Schema) = await this.InternalGetSchemaAsync(context, scopeInfo.Setup, connection, transaction, progress, cancellationToken).ConfigureAwait(false);

                // Shoudl we create scope
                if (provision.HasFlag(SyncProvision.ScopeInfo))
                {
                    bool exists;
                    (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfo, connection, transaction, progress, cancellationToken).ConfigureAwait(false);

                    if (!exists)
                    {
                        var siCreated = false;
                        (context, siCreated) = await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.ScopeInfo, connection, transaction, progress, cancellationToken).ConfigureAwait(false);

                        if (siCreated && !atLeastOneScopeInfoTableBeenCreated)
                            atLeastOneScopeInfoTableBeenCreated = true;
                    }
                }

                if (provision.HasFlag(SyncProvision.ScopeInfoClient))
                {
                    bool exists;
                    (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfoClient, connection, transaction, progress, cancellationToken).ConfigureAwait(false);

                    if (!exists)
                    {
                        var sicCreated = false;
                        (context, sicCreated) = await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.ScopeInfoClient, connection, transaction, progress, cancellationToken).ConfigureAwait(false);

                        if (sicCreated && !atLeastOneScopeInfoClientTableBeenCreated)
                            atLeastOneScopeInfoClientTableBeenCreated = true;
                    }
                }

                // Sorting tables based on dependencies between them
                var schemaTables = scopeInfo.Schema.Tables
                    .SortByDependencies(tab => tab.GetRelations()
                        .Select(r => r.GetParentTable()));

                foreach (var schemaTable in schemaTables)
                {
                    var tableBuilder = this.GetSyncAdapter(schemaTable, scopeInfo).GetTableBuilder();

                    await this.InterceptAsync(new ProvisioningTableArgs(context, provision, scopeInfo, schemaTable, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                    // Check if we need to create a schema there
                    bool schemaExists;
                    (context, schemaExists) = await this.InternalExistsSchemaAsync(scopeInfo, context, tableBuilder, connection, transaction, progress, cancellationToken).ConfigureAwait(false);

                    var stCreated = false;
                    var tCreated = false;
                    var trackingTableExist = false;
                    var tgCreated = false;
                    var spCreated = false;

                    if (!schemaExists)
                    {
                        (context, stCreated) = await this.InternalCreateSchemaAsync(scopeInfo, context, tableBuilder, connection, transaction, progress, cancellationToken).ConfigureAwait(false);

                        if (stCreated && !atLeastOneSchemaTableBeenCreated)
                            atLeastOneSchemaTableBeenCreated = true;
                    }

                    if (provision.HasFlag(SyncProvision.Table))
                    {
                        bool tableExists;
                        (context, tableExists) = await this.InternalExistsTableAsync(scopeInfo, context, tableBuilder, connection, transaction, progress, cancellationToken).ConfigureAwait(false);

                        if (!tableExists)
                        {
                            (context, tCreated) = await this.InternalCreateTableAsync(scopeInfo, context, tableBuilder, connection, transaction, progress, cancellationToken).ConfigureAwait(false);

                            if (tCreated && !atLeastOneTableBeenCreated)
                                atLeastOneTableBeenCreated = true;
                        }
                    }

                    if (provision.HasFlag(SyncProvision.TrackingTable))
                    {
                        (context, trackingTableExist) = await this.InternalExistsTrackingTableAsync(scopeInfo, context, tableBuilder, connection, transaction, progress, cancellationToken).ConfigureAwait(false);

                        if (!trackingTableExist)
                        {
                            var ttCreated = false;
                            (context, ttCreated) = await this.InternalCreateTrackingTableAsync(scopeInfo, context, tableBuilder, connection, transaction, progress, cancellationToken).ConfigureAwait(false);

                            if (ttCreated && !atLeastOneTrackingTableBeenCreated)
                                atLeastOneTrackingTableBeenCreated = true;
                        }
                    }

                    if (provision.HasFlag(SyncProvision.Triggers))
                    {
                        (context, tgCreated) = await this.InternalCreateTriggersAsync(scopeInfo, context, overwrite, tableBuilder, connection, transaction, progress, cancellationToken).ConfigureAwait(false);

                        if (tgCreated && !atLeastOneTriggerHasBeenCreated)
                            atLeastOneTriggerHasBeenCreated = true;
                    }

                    if (provision.HasFlag(SyncProvision.StoredProcedures))
                    {
                        (context, spCreated) = await this.InternalCreateStoredProceduresAsync(scopeInfo, context, overwrite, tableBuilder, connection, transaction, progress, cancellationToken).ConfigureAwait(false);

                        if (spCreated && !atLeastOneStoredProcedureHasBeenCreated)
                            atLeastOneStoredProcedureHasBeenCreated = true;
                    }

                    // Check if we have created something on the current table.
                    var atLeastSomethingHasBeenCreatedOnThisTable = stCreated || tCreated || trackingTableExist || tgCreated || spCreated;

                    await this.InterceptAsync(new ProvisionedTableArgs(context, provision, scopeInfo, schemaTable, atLeastSomethingHasBeenCreatedOnThisTable, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
                }

                // Check if we have created something.
                var atLeastSomethingHasBeenCreated = atLeastOneSchemaTableBeenCreated || atLeastOneTableBeenCreated || atLeastOneTrackingTableBeenCreated || atLeastOneTriggerHasBeenCreated
                                                  || atLeastOneStoredProcedureHasBeenCreated || atLeastOneScopeInfoTableBeenCreated || atLeastOneScopeInfoClientTableBeenCreated;

                await this.InterceptAsync(new ProvisionedArgs(context, provision, scopeInfo, atLeastSomethingHasBeenCreated, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                return (context, true);
            }
            catch (Exception ex)
            {
                string message = null;

                message += $"Provision:{provision}.";
                message += $"Overwrite:{overwrite}.";

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Internal method to deprovision a scope.
        /// </summary>
        internal virtual async Task<(SyncContext Context, bool IsDeprovisioned)> InternalDeprovisionAsync(ScopeInfo scopeInfo, SyncContext context, SyncProvision provision, DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            try
            {
                if (this.Provider == null)
                    throw new MissingProviderException(nameof(this.InternalDeprovisionAsync));

                context.SyncStage = SyncStage.Deprovisioning;

                using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Deprovisioning, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    await this.InterceptAsync(new DeprovisioningArgs(context, provision, scopeInfo?.Setup, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    // get Database builder
                    var builder = this.Provider.GetDatabaseBuilder();

                    // Sorting tables based on dependencies between them
                    List<SyncTable> schemaTables;
                    if (scopeInfo == null)
                    {
                        schemaTables = [];
                    }
                    else
                    {
                        if (scopeInfo.Schema != null)
                        {
                            schemaTables = scopeInfo.Schema.Tables.SortByDependencies(tab => tab.GetRelations().Select(r => r.GetParentTable())).ToList();
                        }
                        else
                        {
                            schemaTables = [];
                            foreach (var setupTable in scopeInfo.Setup.Tables)
                                schemaTables.Add(new SyncTable(setupTable.TableName, setupTable.SchemaName));
                        }
                    }

                    // Disable check constraints
                    if (this.Options.DisableConstraintsOnApplyChanges)
                    {
                        foreach (var table in schemaTables.ToArray().Reverse())
                        {
                            var exists = false;
                            var tableBuilder = this.GetSyncAdapter(table, scopeInfo).GetTableBuilder();

                            (context, exists) = await this.InternalExistsTableAsync(scopeInfo, context, tableBuilder, runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                            if (exists)
                                await this.InternalDisableConstraintsAsync(scopeInfo, context, table, runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                        }
                    }

                    // Checking if we have to deprovision tables
                    var hasDeprovisionTableFlag = provision.HasFlag(SyncProvision.Table);

                    // Firstly, removing the flag from the provision, because we need to drop everything in correct order, then drop tables in reverse side
                    if (hasDeprovisionTableFlag)
                        provision ^= SyncProvision.Table;

                    // Check if we have at least dropped something
                    var atLeastOneStoredProcedureHasBeenDropped = false;
                    var atLeastOneTriggerHasBeenDropped = false;
                    var atLeastOneTrackingTableBeenDropped = false;
                    var atLeastOneTableBeenDropped = false;
                    var atLeastScopeInfoTableBeenDropped = false;
                    var atLeastScopeInfoClientTableBeenDropped = false;

                    foreach (var schemaTable in schemaTables)
                    {
                        var tableBuilder = this.GetSyncAdapter(schemaTable, scopeInfo).GetTableBuilder();

                        await this.InterceptAsync(new DeprovisioningTableArgs(context, provision, scopeInfo, schemaTable, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                        var spDropped = false;
                        var tgDropped = false;
                        var ttDropped = false;

                        if (provision.HasFlag(SyncProvision.StoredProcedures))
                        {
                            (context, spDropped) = await this.InternalDropStoredProceduresAsync(scopeInfo, context, tableBuilder, runner.Connection, runner.Transaction, progress, cancellationToken).ConfigureAwait(false);

                            // Removing cached commands
                            BaseOrchestrator.RemoveCommands();

                            if (spDropped && !atLeastOneStoredProcedureHasBeenDropped)
                                atLeastOneStoredProcedureHasBeenDropped = true;
                        }

                        if (provision.HasFlag(SyncProvision.Triggers))
                        {
                            (context, tgDropped) = await this.InternalDropTriggersAsync(scopeInfo, context, tableBuilder, runner.Connection, runner.Transaction, progress, cancellationToken).ConfigureAwait(false);

                            if (tgDropped && !atLeastOneTriggerHasBeenDropped)
                                atLeastOneTriggerHasBeenDropped = true;
                        }

                        if (provision.HasFlag(SyncProvision.TrackingTable))
                        {
                            bool exists;
                            (context, exists) = await this.InternalExistsTrackingTableAsync(scopeInfo, context, tableBuilder, runner.Connection, runner.Transaction, progress, cancellationToken).ConfigureAwait(false);

                            if (exists)
                            {
                                (context, ttDropped) = await this.InternalDropTrackingTableAsync(scopeInfo, context, tableBuilder, runner.Connection, runner.Transaction, progress, cancellationToken).ConfigureAwait(false);

                                if (ttDropped && !atLeastOneTrackingTableBeenDropped)
                                    atLeastOneTrackingTableBeenDropped = true;
                            }
                        }

                        var atLeastSomethingHasBeenDeprovisioned = spDropped || tgDropped || ttDropped;

                        await this.InterceptAsync(new DeprovisionedTableArgs(context, provision, scopeInfo, schemaTable, atLeastSomethingHasBeenDeprovisioned, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
                    }

                    // Eventually if we have the "Table" flag, then drop the table
                    if (hasDeprovisionTableFlag)
                    {
                        foreach (var schemaTable in schemaTables.ToArray().Reverse())
                        {
                            var tableBuilder = this.GetSyncAdapter(schemaTable, scopeInfo).GetTableBuilder();
                            bool exists;
                            (context, exists) = await this.InternalExistsTableAsync(scopeInfo, context, tableBuilder, runner.Connection, runner.Transaction, progress, cancellationToken).ConfigureAwait(false);

                            if (exists)
                            {
                                var tDropped = false;
                                (context, tDropped) = await this.InternalDropTableAsync(scopeInfo, context, tableBuilder, runner.Connection, runner.Transaction, progress, cancellationToken).ConfigureAwait(false);

                                if (tDropped && !atLeastOneTableBeenDropped)
                                    atLeastOneTableBeenDropped = true;
                            }
                        }
                    }

                    if (provision.HasFlag(SyncProvision.ScopeInfo))
                    {
                        bool exists;
                        (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfo, runner.Connection, runner.Transaction, progress, cancellationToken).ConfigureAwait(false);

                        if (exists)
                        {
                            var siDropped = false;
                            (context, siDropped) = await this.InternalDropScopeInfoTableAsync(context, DbScopeType.ScopeInfo, runner.Connection, runner.Transaction, progress, cancellationToken).ConfigureAwait(false);

                            if (siDropped && !atLeastScopeInfoTableBeenDropped)
                                atLeastScopeInfoTableBeenDropped = true;
                        }
                    }

                    if (provision.HasFlag(SyncProvision.ScopeInfoClient))
                    {
                        bool exists;
                        (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfoClient, runner.Connection, runner.Transaction, progress, cancellationToken).ConfigureAwait(false);

                        if (exists)
                        {
                            var sicDropped = false;
                            (context, sicDropped) = await this.InternalDropScopeInfoTableAsync(context, DbScopeType.ScopeInfoClient, runner.Connection, runner.Transaction, progress, cancellationToken).ConfigureAwait(false);

                            if (sicDropped && !atLeastScopeInfoClientTableBeenDropped)
                                atLeastScopeInfoClientTableBeenDropped = true;
                        }
                    }

                    // Disable check constraints
                    if (this.Options.DisableConstraintsOnApplyChanges && !hasDeprovisionTableFlag)
                    {
                        foreach (var table in schemaTables.ToArray().Reverse())
                        {
                            var exists = false;
                            var tableBuilder = this.GetSyncAdapter(table, scopeInfo).GetTableBuilder();

                            (context, exists) = await this.InternalExistsTableAsync(scopeInfo, context, tableBuilder, runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                            if (exists)
                                await this.InternalEnableConstraintsAsync(scopeInfo, context, table, runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                        }
                    }

                    var atLeastSomethingHasBeenDropped = atLeastScopeInfoTableBeenDropped || atLeastScopeInfoClientTableBeenDropped || atLeastOneTableBeenDropped || atLeastOneTrackingTableBeenDropped
                                                      || atLeastOneTriggerHasBeenDropped || atLeastOneStoredProcedureHasBeenDropped;

                    var args = new DeprovisionedArgs(context, provision, scopeInfo?.Setup, atLeastSomethingHasBeenDropped, runner.Connection, runner.Transaction);
                    await this.InterceptAsync(args, progress, cancellationToken).ConfigureAwait(false);

                    await runner.CommitAsync().ConfigureAwait(false);

                    return (context, true);
                }
            }
            catch (Exception ex)
            {
                string message = null;

                message += $"Provision:{provision}.";

                throw this.GetSyncError(context, ex, message);
            }
        }
    }
}