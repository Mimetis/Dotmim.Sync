using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Manager;
using Dotmim.Sync.Serialization;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
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
    public partial class RemoteOrchestrator
    {

        /// <summary>
        /// Check if we need to upgrade the Database Structure
        /// </summary>
        public virtual async Task<bool> NeedsToUpgradeAsync()
        {
            var context = new SyncContext(Guid.NewGuid(), SyncOptions.DefaultScopeName);
            try
            {
                var parsedName = ParserName.Parse(this.Options.ScopeInfoTableName);
                var scopeInfoServerTableName = $"{parsedName.Unquoted().Normalized()}_server";
                var scopeInfoServerHistoryTableName = $"{parsedName.Unquoted().Normalized()}_history";

                var sScopeInfoTableName = $"{parsedName.Unquoted().Normalized()}";
                var sScopeInfoClientTableName = $"{parsedName.Unquoted().Normalized()}_client";


                await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.Migrating).ConfigureAwait(false);
                // get Database builder
                var dbBuilder = this.Provider.GetDatabaseBuilder();

                // Initialize database if needed
                await dbBuilder.EnsureDatabaseAsync(runner.Connection, runner.Transaction).ConfigureAwait(false);

                // check old and new scopes tables
                var exist1 = await dbBuilder.ExistsTableAsync(scopeInfoServerTableName, null, runner.Connection, runner.Transaction);
                var exist2 = await dbBuilder.ExistsTableAsync(scopeInfoServerHistoryTableName, null, runner.Connection, runner.Transaction);
                var exist3 = await dbBuilder.ExistsTableAsync(sScopeInfoTableName, null, runner.Connection, runner.Transaction);
                var exist4 = await dbBuilder.ExistsTableAsync(sScopeInfoClientTableName, null, runner.Connection, runner.Transaction);

                if (!exist1 && !exist2 && !exist3 && !exist4)
                    return false;

                // Check if scope_info exists
                // If exists then we have already upgraded to last version
                bool exists;
                (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfo,
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                if (exists)
                    return false;

                await runner.CommitAsync().ConfigureAwait(false);

                return true;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Upgrade your <strong>server</strong> database to the last version <strong>0.9.6</strong>
        /// <para>
        /// As usual on the server side, the upgrade is manually, but pretty simple
        /// </para>
        /// <example>
        /// Here is an example where we are migrating a local server datasource where 2 scopes have been already used (1 with filters and the other one without filters)
        /// <code>
        /// var serverProvider = new SqlSyncProvider(serverCstring);
        /// var remoteOrchestrator = new RemoteOrchestrator(serverProvider, syncOptions);
        /// 
        /// var needsUpgrade = await remoteOrchestrator.NeedsToUpgradeAsync();
        /// 
        /// if (needsUpgrade)
        ///    var (scopeInfos, scopeInfoClients) = await remoteOrchestrator.UpgradeAsync(progress, evaluationOnly);
        /// </code>
        /// </example>
        /// </summary>
        /// <param name="progress">Progress of upgrade</param>
        /// <param name="evaluateOnly">if set to True, the upgrade will not be applied at the end. Usefull to test your upgrade and see if anything is breaking at some point.</param>
        /// <returns>
        /// The new scopeInfos and scopeInfoClients rows from your new upgrade sync config tables.
        /// </returns>
        public virtual async Task<(List<ScopeInfo> scopeInfos, List<ScopeInfoClient> scopeInfoClients)> UpgradeAsync(IProgress<ProgressArgs> progress = default, bool evaluateOnly = false)
        {
            var context = new SyncContext(Guid.NewGuid(), SyncOptions.DefaultScopeName);
            try
            {
                var parsedName = ParserName.Parse(this.Options.ScopeInfoTableName);
                var scopeInfoServerTableName = $"{parsedName.Unquoted().Normalized()}_server";
                var scopeInfoServerHistoryTableName = $"{parsedName.Unquoted().Normalized()}_history";

                var sScopeInfoTableName = $"{parsedName.Unquoted().Normalized()}";
                var sScopeInfoClientTableName = $"{parsedName.Unquoted().Normalized()}_client";


                await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Migrating, default, default, default, progress).ConfigureAwait(false);
                var dbBuilder = this.Provider.GetDatabaseBuilder();

                // Initialize database if needed
                await dbBuilder.EnsureDatabaseAsync(runner.Connection, runner.Transaction).ConfigureAwait(false);

                // ----------------------------------------------------
                // Step 1 : Drop scope_info_history table
                // ----------------------------------------------------
                await dbBuilder.DropsTableIfExistsAsync(scopeInfoServerHistoryTableName, null,
                    runner.Connection, runner.Transaction).ConfigureAwait(false);

                var message = $"- Drop {scopeInfoServerHistoryTableName} table.";
                await this.InterceptAsync(new UpgradeProgressArgs(context, message, SyncVersion.Current, runner.Connection, runner.Transaction), runner.Progress).ConfigureAwait(false);

                // ----------------------------------------------------
                // Step 2 : Get Rows form old scope_info_server table
                // ----------------------------------------------------
                var scopeInfoServerTable = await dbBuilder.GetTableAsync(scopeInfoServerTableName, default,
                    runner.Connection, runner.Transaction).ConfigureAwait(false);

                message = $"- Getting rows from old {scopeInfoServerTableName} table.";
                await this.InterceptAsync(new UpgradeProgressArgs(context, message, SyncVersion.Current, runner.Connection, runner.Transaction), runner.Progress).ConfigureAwait(false);


                // ----------------------------------------------------
                // Step 3 : Create scope_info 
                // ----------------------------------------------------
                bool existsCScopeInfo;
                (context, existsCScopeInfo) = await InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfo,
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                if (!existsCScopeInfo)
                    (context, _) = await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.ScopeInfo,
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                message = $"- Created new version of {sScopeInfoTableName} table.";
                await this.InterceptAsync(new UpgradeProgressArgs(context, message, SyncVersion.Current, runner.Connection, runner.Transaction), runner.Progress).ConfigureAwait(false);

                // ----------------------------------------------------
                // Step 4 : Create scope_info_client 
                // ----------------------------------------------------
                bool existsCScopeInfoClient;
                (context, existsCScopeInfoClient) = await InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfoClient,
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                if (!existsCScopeInfoClient)
                    (context, _) = await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.ScopeInfoClient,
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                message = $"- Created {sScopeInfoClientTableName} table.";
                await this.InterceptAsync(new UpgradeProgressArgs(context, message, SyncVersion.Current, runner.Connection, runner.Transaction), runner.Progress).ConfigureAwait(false);


                // ----------------------------------------------------
                // Step 5 : Migrate each scope to new table
                // ----------------------------------------------------
                foreach (var scopeInfoServerRow in scopeInfoServerTable.Rows)
                {
                    var setup = JsonConvert.DeserializeObject<SyncSetup>(scopeInfoServerRow["sync_scope_setup"].ToString());
                    var schema = JsonConvert.DeserializeObject<SyncSet>(scopeInfoServerRow["sync_scope_schema"].ToString());
                    var lastCleanup = (long)scopeInfoServerRow["sync_scope_last_clean_timestamp"];
                    var name = scopeInfoServerRow["sync_scope_name"].ToString();
                    name = string.IsNullOrEmpty(name) ? "DefaultScope" : name;

                    var sScopeInfo = new ScopeInfo
                    {
                        Name = name,
                        Setup = setup,
                        Schema = schema,
                        Version = SyncVersion.Current.ToString(),
                        LastCleanupTimestamp = lastCleanup,
                    };

                    await this.InternalSaveScopeInfoAsync(sScopeInfo, context,
                         runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                    message = $"- Saved scope_info {name} in {sScopeInfoTableName} table with a setup containing {setup.Tables.Count} tables.";
                    await this.InterceptAsync(new UpgradeProgressArgs(context, message, SyncVersion.Current, runner.Connection, runner.Transaction), runner.Progress).ConfigureAwait(false);
                }

                // ----------------------------------------------------
                // Step 6 : Drop old scope_info_server table
                // ----------------------------------------------------
                await dbBuilder.DropsTableIfExistsAsync(scopeInfoServerTableName, null,
                    runner.Connection, runner.Transaction).ConfigureAwait(false);

                message = $"- Drop old {scopeInfoServerTableName} table.";
                await this.InterceptAsync(new UpgradeProgressArgs(context, message, SyncVersion.Current, runner.Connection, runner.Transaction), runner.Progress).ConfigureAwait(false);


                // ----------------------------------------------------
                // Step 6: Deprovision all and re provision
                // ----------------------------------------------------
                List<ScopeInfo> sScopeInfos = null;

                // get scope infos
                (context, sScopeInfos) = await this.InternalLoadAllScopeInfosAsync(context,
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                // fallback to "try to drop an hypothetical default scope"
                if (sScopeInfos == null)
                    sScopeInfos = new List<ScopeInfo>();

                // Get all filters and fake them for the default scope
                var existingFilters = sScopeInfos?.SelectMany(si => si.Setup.Filters).ToList();

                var defaultCScopeInfo = new ScopeInfo
                {
                    Name = SyncOptions.DefaultScopeName,
                    Version = SyncVersion.Current.ToString(),
                };

                var defaultSetup = await dbBuilder.GetAllTablesAsync(runner.Connection, runner.Transaction).ConfigureAwait(false);

                // Considering removing tables with "_tracking" at the end
                var tables = defaultSetup.Tables.Where(setupTable => !setupTable.TableName.EndsWith("_tracking")).ToList();
                defaultSetup.Tables.Clear();
                defaultSetup.Tables.AddRange(tables);
                defaultCScopeInfo.Setup = defaultSetup;

                if (existingFilters != null && existingFilters.Count > 0)
                {
                    var filters = new SetupFilters();
                    foreach (var filter in existingFilters)
                        filters.Add(filter);

                    defaultCScopeInfo.Setup.Filters = filters;
                }

                sScopeInfos.Add(defaultCScopeInfo);

                // Deprovision old triggers & stored procedures
                var provision = SyncProvision.StoredProcedures | SyncProvision.Triggers;

                foreach (var sScopeInfo in sScopeInfos)
                {
                    if (sScopeInfo == null || sScopeInfo.Setup == null || sScopeInfo.Setup.Tables == null || sScopeInfo.Setup.Tables.Count <= 0)
                        continue;

                    await this.InternalDeprovisionAsync(sScopeInfo, context, provision,
                            runner.Connection, runner.Transaction, runner.CancellationToken, default).ConfigureAwait(false);

                    message = $"- Deprovision old scope {sScopeInfo.Name}.";
                    await this.InterceptAsync(new UpgradeProgressArgs(context, message, SyncVersion.Current, runner.Connection, runner.Transaction), runner.Progress).ConfigureAwait(false);
                }

                // ----------------------------------------------------
                // Step 7 : Provision again
                // ----------------------------------------------------
                (context, sScopeInfos) = await this.InternalLoadAllScopeInfosAsync(context,
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                foreach (var sScopeInfo in sScopeInfos)
                {
                    if (sScopeInfo == null || sScopeInfo.Setup == null || sScopeInfo.Setup.Tables == null || sScopeInfo.Setup.Tables.Count <= 0)
                        continue;

                    (context, _) = await InternalProvisionServerAsync(sScopeInfo, context, provision, false,
                        runner.Connection, runner.Transaction, runner.CancellationToken, default).ConfigureAwait(false);

                    message = $"- Provision new scope {sScopeInfo.Name}.";
                    await this.InterceptAsync(new UpgradeProgressArgs(context, message, SyncVersion.Current, runner.Connection, runner.Transaction), runner.Progress).ConfigureAwait(false);
                }


                // ----------------------------------------------------
                // Step 8 : Get final scope_info and scope_info_client 
                // ----------------------------------------------------
                List<ScopeInfo> sFinalScopeInfos = null;

                (context, sFinalScopeInfos) = await this.InternalLoadAllScopeInfosAsync(context,
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                var sFinalScopeInfoClients = await this.InternalLoadAllScopeInfoClientsAsync(context,
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);


                if (evaluateOnly)
                    await runner.RollbackAsync().ConfigureAwait(false);
                else
                    await runner.CommitAsync().ConfigureAwait(false);

                return (sFinalScopeInfos, sFinalScopeInfoClients);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }



        private async Task<Version> UpgdrateTo601Async(ScopeInfo serverScopeInfo, SyncContext context, DbConnection connection, DbTransaction transaction,
                        CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            var newVersion = new Version(0, 6, 1);
            // Sorting tables based on dependencies between them
            var schemaTables = serverScopeInfo.Schema.Tables
                .SortByDependencies(tab => tab.GetRelations()
                    .Select(r => r.GetParentTable()));

            foreach (var schemaTable in schemaTables)
            {
                var tableBuilder = this.GetTableBuilder(schemaTable, serverScopeInfo);
                (context, _) = await InternalCreateStoredProceduresAsync(serverScopeInfo, context, true, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                await this.InterceptAsync(new UpgradeProgressArgs(context, $"ALL Stored procedures for table {tableBuilder.TableDescription.GetFullName()} updated", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
            }

            var message = $"Upgrade to 0.6.1 done for scope {serverScopeInfo.Name}";
            await this.InterceptAsync(new UpgradeProgressArgs(context, message, newVersion, connection, transaction), progress, cancellationToken);

            return newVersion;
        }


        private async Task<Version> UpgdrateTo602Async(ScopeInfo serverScopeInfo, SyncContext context, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var newVersion = new Version(0, 6, 2);

            // Sorting tables based on dependencies between them
            var schemaTables = serverScopeInfo.Schema.Tables
                .SortByDependencies(tab => tab.GetRelations()
                    .Select(r => r.GetParentTable()));

            foreach (var schemaTable in schemaTables)
            {
                var tableBuilder = this.GetTableBuilder(schemaTable, serverScopeInfo);

                // Upgrade Select Initial Changes
                bool exists;
                (context, exists) = await InternalExistsTriggerAsync(serverScopeInfo, context, tableBuilder, DbTriggerType.Update, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (exists)
                    (context, _) = await InternalDropTriggerAsync(serverScopeInfo, context, tableBuilder, DbTriggerType.Update, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                (context, _) = await InternalCreateTriggerAsync(serverScopeInfo, context, tableBuilder, DbTriggerType.Update, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                await this.InterceptAsync(new UpgradeProgressArgs(context, $"Update Trigger for table {tableBuilder.TableDescription.GetFullName()} updated", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
            }

            var message = $"Upgrade to {newVersion} for scope {serverScopeInfo.Name}.";
            await this.InterceptAsync(new UpgradeProgressArgs(context, message, newVersion, connection, transaction), progress, cancellationToken);

            return newVersion;
        }

        private async Task<Version> UpgdrateTo700Async(ScopeInfo serverScopeInfo, SyncContext context, DbConnection connection, DbTransaction transaction,
                     CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            var newVersion = new Version(0, 7, 0);
            // Sorting tables based on dependencies between them

            var schemaTables = serverScopeInfo.Schema.Tables
                .SortByDependencies(tab => tab.GetRelations()
                    .Select(r => r.GetParentTable()));

            var message = $"Upgrade to {newVersion}:";
            await this.InterceptAsync(new UpgradeProgressArgs(context, message, newVersion, connection, transaction), progress, cancellationToken);

            foreach (var schemaTable in schemaTables)
            {
                var tableBuilder = this.GetTableBuilder(schemaTable, serverScopeInfo);

                // Upgrade Reset stored procedure
                bool exists;
                (context, exists) = await InternalExistsStoredProcedureAsync(serverScopeInfo, context, tableBuilder,
                    DbStoredProcedureType.Reset, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (exists)
                    (context, _) = await InternalDropStoredProcedureAsync(serverScopeInfo, context, tableBuilder, DbStoredProcedureType.Reset, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                (context, _) = await InternalCreateStoredProcedureAsync(serverScopeInfo, context, tableBuilder, DbStoredProcedureType.Reset, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                await this.InterceptAsync(new UpgradeProgressArgs(context, $"Reset stored procedure for table {tableBuilder.TableDescription.GetFullName()} updated", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                // Upgrade Update stored procedure
                bool existsUpdateSP;
                (context, existsUpdateSP) = await InternalExistsStoredProcedureAsync(serverScopeInfo, context, tableBuilder,
                    DbStoredProcedureType.UpdateRow, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (existsUpdateSP)
                    (context, _) = await InternalDropStoredProcedureAsync(serverScopeInfo, context, tableBuilder, DbStoredProcedureType.UpdateRow, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                (context, _) = await InternalCreateStoredProcedureAsync(serverScopeInfo, context, tableBuilder, DbStoredProcedureType.UpdateRow, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                await this.InterceptAsync(new UpgradeProgressArgs(context, $"Update stored procedure for table {tableBuilder.TableDescription.GetFullName()} updated", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                bool existsBulkUpdateSP;
                (context, existsBulkUpdateSP) = await InternalExistsStoredProcedureAsync(serverScopeInfo, context, tableBuilder,
                    DbStoredProcedureType.BulkUpdateRows, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (existsBulkUpdateSP)
                {
                    (context, _) = await InternalDropStoredProcedureAsync(serverScopeInfo, context, tableBuilder, DbStoredProcedureType.BulkUpdateRows, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                    (context, _) = await InternalCreateStoredProcedureAsync(serverScopeInfo, context, tableBuilder, DbStoredProcedureType.BulkUpdateRows, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                    await this.InterceptAsync(new UpgradeProgressArgs(context, $"Bulk Update stored procedure for table {tableBuilder.TableDescription.GetFullName()} updated", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
                }
            }
            message = $"Upgrade to {newVersion} for scope {serverScopeInfo.Name}.";
            await this.InterceptAsync(new UpgradeProgressArgs(context, message, newVersion, connection, transaction), progress, cancellationToken);
            return newVersion;
        }


        private async Task<Version> UpgdrateTo801Async(ScopeInfo serverScopeInfo, SyncContext context, DbConnection connection, DbTransaction transaction,
               CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            var newVersion = new Version(0, 8, 1);
            // Sorting tables based on dependencies between them

            var schemaTables = serverScopeInfo.Schema.Tables
                .SortByDependencies(tab => tab.GetRelations()
                    .Select(r => r.GetParentTable()));

            var message = $"Upgrade to {newVersion}:";
            await this.InterceptAsync(new UpgradeProgressArgs(context, message, newVersion, connection, transaction), progress, cancellationToken);

            foreach (var schemaTable in schemaTables)
            {
                var tableBuilder = this.GetTableBuilder(schemaTable, serverScopeInfo);
                await InternalCreateStoredProceduresAsync(serverScopeInfo, context, true, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                await this.InterceptAsync(new UpgradeProgressArgs(context, $"ALL Stored procedures for table {tableBuilder.TableDescription.GetFullName()} updated", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
            }
            message = $"Upgrade to {newVersion} for scope {serverScopeInfo.Name}.";
            await this.InterceptAsync(new UpgradeProgressArgs(context, message, newVersion, connection, transaction), progress, cancellationToken);

            return newVersion;
        }


        private async Task<Version> UpgdrateTo093Async(ScopeInfo serverScopeInfo, SyncContext context, DbConnection connection, DbTransaction transaction,
               CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            var newVersion = new Version(0, 9, 3);
            // Sorting tables based on dependencies between them

            var schemaTables = serverScopeInfo.Schema.Tables
                .SortByDependencies(tab => tab.GetRelations()
                    .Select(r => r.GetParentTable()));

            await this.InterceptAsync(new UpgradeProgressArgs(context, $"Upgrade to {newVersion}:", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            var provision = SyncProvision.StoredProcedures | SyncProvision.Triggers;

            await this.DeprovisionAsync(serverScopeInfo.Name, provision).ConfigureAwait(false);
            await this.ProvisionAsync(serverScopeInfo.Name, provision, false).ConfigureAwait(false);

            var message = $"Upgrade to {newVersion} for scope {serverScopeInfo.Name}.";
            await this.InterceptAsync(new UpgradeProgressArgs(context, message, newVersion, connection, transaction), progress, cancellationToken);

            return newVersion;
        }

        private async Task<Version> UpgdrateTo094Async(ScopeInfo serverScopeInfo, SyncContext context, DbConnection connection, DbTransaction transaction,
       CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            var newVersion = new Version(0, 9, 4);
            // Sorting tables based on dependencies between them

            var schemaTables = serverScopeInfo.Schema.Tables
                .SortByDependencies(tab => tab.GetRelations()
                    .Select(r => r.GetParentTable()));

            await this.InterceptAsync(new UpgradeProgressArgs(context, $"Upgrade to {newVersion}:", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            var provision = SyncProvision.StoredProcedures | SyncProvision.Triggers;

            await this.DeprovisionAsync(serverScopeInfo.Name, provision).ConfigureAwait(false);
            await this.ProvisionAsync(serverScopeInfo.Name, provision, false).ConfigureAwait(false);

            var message = $"Upgrade to {newVersion} for scope {serverScopeInfo.Name}.";
            await this.InterceptAsync(new UpgradeProgressArgs(context, message, newVersion, connection, transaction), progress, cancellationToken);

            return newVersion;
        }

        private async Task<Version> UpgdrateTo095Async(ScopeInfo scopeInfo, SyncContext context, DbConnection connection, DbTransaction transaction,
               CancellationToken cancellationToken, IProgress<ProgressArgs> progress)

        {
            var newVersion = new Version(0, 9, 5);

            await this.InterceptAsync(new UpgradeProgressArgs(context, $"Upgrade to {newVersion}:", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            // get scope info table name
            var scopeInfoTableName = ParserName.Parse(this.Options.ScopeInfoTableName);
            var tableName = $"{scopeInfoTableName.Unquoted().Normalized().ToString()}";
            var historyTableName = $"{tableName}_history";

            var syncTable = new SyncTable(historyTableName);
            var historyTableBuilder = this.GetTableBuilder(syncTable, scopeInfo);

            var pkeys = await historyTableBuilder.GetPrimaryKeysAsync(connection, transaction).ConfigureAwait(false);

            if (pkeys.Count() == 1)
            {
                if (this.Provider.GetProviderTypeName().Contains("Dotmim.Sync.SqlServer.SqlSyncProvider"))
                {
                    var commandText = @$"ALTER TABLE {historyTableName} DROP CONSTRAINT PK_{historyTableName};
                                        ALTER TABLE {historyTableName} ADD CONSTRAINT 
                                        PK_{historyTableName} PRIMARY KEY CLUSTERED (sync_scope_id, sync_scope_name);";


                    var command = connection.CreateCommand();
                    command.CommandText = commandText;
                    command.Transaction = transaction;

                    // Parametrized command timeout established if exist
                    if (Options.DbCommandTimeout.HasValue)
                    {
                        command.CommandTimeout = Options.DbCommandTimeout.Value;
                    }

                    await command.ExecuteNonQueryAsync();
                    await this.InterceptAsync(new UpgradeProgressArgs(context, $"{historyTableName} primary keys updated on SQL Server", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
                }

                if (this.Provider.GetProviderTypeName().Contains("MySqlSyncProvider"))
                {
                    var commandText = @$"ALTER TABLE `{historyTableName}` 
                                        CHANGE COLUMN `sync_scope_name` `sync_scope_name` VARCHAR(100) NOT NULL ,
                                        DROP PRIMARY KEY,
                                        ADD PRIMARY KEY (`sync_scope_id`, `sync_scope_name`);";

                    var command = connection.CreateCommand();
                    command.CommandText = commandText;
                    command.Transaction = transaction;

                    // Parametrized command timeout established if exist
                    if (Options.DbCommandTimeout.HasValue)
                    {
                        command.CommandTimeout = Options.DbCommandTimeout.Value;
                    }

                    await command.ExecuteNonQueryAsync();
                    await this.InterceptAsync(new UpgradeProgressArgs(context, $"{historyTableName} primary keys updated on MySql", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
                }


            }

            var provision = SyncProvision.StoredProcedures | SyncProvision.Triggers;
            await this.DeprovisionAsync(scopeInfo.Name, provision);
            // simulate 0.94 scope without scopename in sp
            await this.DeprovisionAsync(SyncOptions.DefaultScopeName, scopeInfo.Setup, provision);
            await this.InterceptAsync(new UpgradeProgressArgs(context, $"Deprovision scope {scopeInfo.Name}", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            await this.ProvisionAsync(scopeInfo.Name, provision, true);
            await this.InterceptAsync(new UpgradeProgressArgs(context, $"Provision scope {scopeInfo.Name}", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            return newVersion;
        }


        private async Task<Version> UpgdrateTo096Async(ScopeInfo scopeInfo, SyncContext context, DbConnection connection, DbTransaction transaction,
             CancellationToken cancellationToken, IProgress<ProgressArgs> progress)

        {
            var newVersion = new Version(0, 9, 6);

            await this.InterceptAsync(new UpgradeProgressArgs(context, $"Upgrade to {newVersion}:", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            // get scope info table name
            var scopeInfoTableName = ParserName.Parse(this.Options.ScopeInfoTableName);
            var tableName = $"{scopeInfoTableName.Unquoted().Normalized().ToString()}";
            var historyTableName = $"{tableName}_history";

            var syncTable = new SyncTable(historyTableName);
            var historyTableBuilder = this.GetTableBuilder(syncTable, scopeInfo);

            if (this.Provider.GetProviderTypeName().Contains("Dotmim.Sync.SqlServer.SqlSyncProvider"))
            {
                var commandText = @$"IF NOT EXISTS(SELECT col.name AS name FROM sys.columns as col
                                        INNER join sys.tables as tbl on tbl.object_id = col.object_id 
                                        WHERE tbl.name = '{historyTableName}' and col.name = 'scope_properties')
                                            ALTER TABLE {historyTableName} ADD scope_properties nvarchar(MAX) NULL;";


                var command = connection.CreateCommand();
                command.CommandText = commandText;
                command.Transaction = transaction;

                // Parametrized command timeout established if exist
                if (Options.DbCommandTimeout.HasValue)
                {
                    command.CommandTimeout = Options.DbCommandTimeout.Value;
                }

                await command.ExecuteNonQueryAsync();

                await this.InterceptAsync(new UpgradeProgressArgs(context, $"{historyTableName} primary keys updated on SQL Server", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
            }

            if (this.Provider.GetProviderTypeName().Contains("MySqlSyncProvider"))
            {
                var commandText = @$"";

                var command = connection.CreateCommand();
                command.CommandText = commandText;
                command.Transaction = transaction;

                // Parametrized command timeout established if exist
                if (Options.DbCommandTimeout.HasValue)
                {
                    command.CommandTimeout = Options.DbCommandTimeout.Value;
                }

                await command.ExecuteNonQueryAsync();
                await this.InterceptAsync(new UpgradeProgressArgs(context, $"{historyTableName} primary keys updated on MySql", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            }

            var provision = SyncProvision.StoredProcedures | SyncProvision.Triggers;
            await this.DeprovisionAsync(scopeInfo.Name, provision);
            // simulate 0.94 scope without scopename in sp
            await this.DeprovisionAsync(SyncOptions.DefaultScopeName, scopeInfo.Setup, provision);
            await this.InterceptAsync(new UpgradeProgressArgs(context, $"Deprovision scope {scopeInfo.Name}", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            await this.ProvisionAsync(scopeInfo.Name, provision, true);
            await this.InterceptAsync(new UpgradeProgressArgs(context, $"Provision scope {scopeInfo.Name}", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            return newVersion;
        }


        private async Task<Version> AutoUpgdrateToNewVersionAsync(ScopeInfo scopeInfo, SyncContext context, Version newVersion, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var message = $"Upgrade to {newVersion} for scope {scopeInfo.Name}.";
            await this.InterceptAsync(new UpgradeProgressArgs(context, message, newVersion, connection, transaction), progress, cancellationToken);

            return newVersion;
        }
    }
}
