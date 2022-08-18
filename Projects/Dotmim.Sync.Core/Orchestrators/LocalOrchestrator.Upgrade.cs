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
    public partial class LocalOrchestrator
    {

        public virtual async Task<bool> UpgradeAsync(DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), SyncOptions.DefaultScopeName);

            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.Migrating, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                var dbBuilder = this.Provider.GetDatabaseBuilder();

                // Initialize database if needed
                await dbBuilder.EnsureDatabaseAsync(runner.Connection, runner.Transaction).ConfigureAwait(false);

                List<ClientScopeInfo> clientScopeInfos;
                (context, clientScopeInfos) = await this.InternalLoadAllClientScopesInfoAsync(context, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (clientScopeInfos == null || clientScopeInfos.Count <= 0)
                    throw new MissingClientScopeInfoException();

                (context, _) = await this.InternalUpgradeAsync(clientScopeInfos, context, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return true;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Check if we need to upgrade the Database Structure
        /// </summary>
        public virtual async Task<bool> NeedsToUpgradeAsync(DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), SyncOptions.DefaultScopeName);
            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.Reading, SyncStage.Migrating, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                // get Database builder
                var dbBuilder = this.Provider.GetDatabaseBuilder();

                // Initialize database if needed
                await dbBuilder.EnsureDatabaseAsync(runner.Connection, runner.Transaction).ConfigureAwait(false);

                bool exists;
                (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.Client, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (!exists)
                    return false;

                List<ClientScopeInfo> clientScopeInfos;
                (context, clientScopeInfos) = await this.InternalLoadAllClientScopesInfoAsync(context, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (clientScopeInfos == null || clientScopeInfos.Count <= 0)
                    return false;

                await runner.CommitAsync().ConfigureAwait(false);

                return InternalNeedsToUpgrade(clientScopeInfos);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }
        internal virtual bool InternalNeedsToUpgrade(List<ClientScopeInfo> clientScopeInfos)
        {
            var version = SyncVersion.Current;

            // get the smallest version of all scope in the scope info server tables
            foreach (var clientScopeInfo in clientScopeInfos)
            {
                var tmpVersion = SyncVersion.EnsureVersion(clientScopeInfo.Version);

                if (tmpVersion < version)
                    version = tmpVersion;
            }

            return version < SyncVersion.Current;
        }


        internal virtual async Task<(SyncContext context, bool upgraded)> InternalUpgradeAsync(List<ClientScopeInfo> clientScopeInfos,
                        SyncContext context, DbConnection connection, DbTransaction transaction,
                        CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.Migrating, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
            try
            {

                foreach (var clientScopeInfo in clientScopeInfos)
                {
                    var version = SyncVersion.EnsureVersion(clientScopeInfo.Version);
                    var oldVersion = version.Clone() as Version;
                    // beta version
                    if (version.Major == 0)
                    {
                        if (version.Minor <= 5)
                            version = await AutoUpgdrateToNewVersionAsync(clientScopeInfo, context, new Version(0, 6, 0), runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                        if (version.Minor == 6 && version.Build == 0)
                            version = await UpgdrateTo601Async(clientScopeInfo, context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                        if (version.Minor == 6 && version.Build == 1)
                            version = await UpgdrateTo602Async(clientScopeInfo, context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                        if (version.Minor == 6 && version.Build >= 2)
                            version = await UpgdrateTo700Async(clientScopeInfo, context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                        if (version.Minor == 7 && version.Build == 0)
                            version = await AutoUpgdrateToNewVersionAsync(clientScopeInfo, context, new Version(0, 7, 1), runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                        if (version.Minor == 7 && version.Build == 1)
                            version = await AutoUpgdrateToNewVersionAsync(clientScopeInfo, context, new Version(0, 7, 2), runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                        if (version.Minor == 7 && version.Build == 2)
                            version = await AutoUpgdrateToNewVersionAsync(clientScopeInfo, context, new Version(0, 7, 3), runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                        if (version.Minor == 7 && version.Build >= 3)
                            version = await AutoUpgdrateToNewVersionAsync(clientScopeInfo, context, new Version(0, 8, 0), runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                        if (version.Minor == 8 && version.Build == 0)
                            version = await UpgdrateTo801Async(clientScopeInfo, context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                        if (version.Minor == 8 && version.Build == 1)
                            version = await AutoUpgdrateToNewVersionAsync(clientScopeInfo, context, new Version(0, 9, 0), runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                        if (version.Minor == 9 && version.Build == 0)
                            version = await AutoUpgdrateToNewVersionAsync(clientScopeInfo, context, new Version(0, 9, 1), runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                        if (version.Minor == 9 && version.Build == 1)
                            version = await UpgdrateTo093Async(clientScopeInfo, context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                        if (version.Minor == 9 && version.Build == 2)
                            version = await UpgdrateTo093Async(clientScopeInfo, context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                        if (version.Minor == 9 && version.Build == 3)
                            version = await UpgdrateTo094Async(clientScopeInfo, context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                        if (version.Minor == 9 && version.Build == 4)
                            version = await UpgdrateTo095Async(clientScopeInfo, context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);
                    }

                    if (oldVersion != version)
                    {
                        clientScopeInfo.Version = version.ToString();
                        (context, _) = await this.InternalSaveClientScopeInfoAsync(clientScopeInfo, context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);
                    }

                }

                await runner.CommitAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
            return (context, true);

        }

        private async Task<Version> UpgdrateTo601Async(IScopeInfo scopeInfo, SyncContext context, DbConnection connection, DbTransaction transaction,
                        CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            var newVersion = new Version(0, 6, 1);
            // Sorting tables based on dependencies between them
            var schemaTables = scopeInfo.Schema.Tables
                .SortByDependencies(tab => tab.GetRelations()
                    .Select(r => r.GetParentTable()));

            var message = $"Upgrade to {newVersion}:";
            await this.InterceptAsync(new UpgradeProgressArgs(context, message, newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            foreach (var schemaTable in schemaTables)
            {
                var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                // Upgrade Select Initial Changes
                bool exists;
                (context, exists) = await InternalExistsStoredProcedureAsync(scopeInfo, context, tableBuilder, DbStoredProcedureType.SelectInitializedChanges, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (exists)
                    await InternalDropStoredProcedureAsync(scopeInfo, context, tableBuilder, DbStoredProcedureType.SelectInitializedChanges, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                await InternalCreateStoredProcedureAsync(scopeInfo, context, tableBuilder, DbStoredProcedureType.SelectInitializedChanges, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                await this.InterceptAsync(new UpgradeProgressArgs(context, $"SelectInitializedChanges stored procedure for table {tableBuilder.TableDescription.GetFullName()} updated", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                // Upgrade Select Initial Changes With Filter
                if (tableBuilder.TableDescription.GetFilter() != null)
                {
                    bool existsWithFilter;
                    (context, existsWithFilter) = await InternalExistsStoredProcedureAsync(scopeInfo, context, tableBuilder, DbStoredProcedureType.SelectInitializedChangesWithFilters, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (existsWithFilter)
                        await InternalDropStoredProcedureAsync(scopeInfo, context, tableBuilder, DbStoredProcedureType.SelectInitializedChangesWithFilters, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    await InternalCreateStoredProcedureAsync(scopeInfo, context, tableBuilder, DbStoredProcedureType.SelectInitializedChangesWithFilters, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    await this.InterceptAsync(new UpgradeProgressArgs(context, $"SelectInitializedChangesWithFilters stored procedure for table {tableBuilder.TableDescription.GetFullName()} updated", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
                }

            }

            return newVersion;
        }

        private async Task<Version> UpgdrateTo602Async(IScopeInfo scopeInfo, SyncContext context, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            var newVersion = new Version(0, 6, 2);
            var message = $"Upgrade to {newVersion}:";
            await this.InterceptAsync(new UpgradeProgressArgs(context, message, newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            // Update the "Update trigger" for all tables

            // Sorting tables based on dependencies between them
            var schemaTables = scopeInfo.Schema.Tables
                .SortByDependencies(tab => tab.GetRelations()
                    .Select(r => r.GetParentTable()));

            foreach (var schemaTable in schemaTables)
            {
                var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                // Upgrade Select Initial Changes
                bool exists;
                (context, exists) = await InternalExistsTriggerAsync(scopeInfo, context, tableBuilder, DbTriggerType.Update, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (exists)
                    await InternalDropTriggerAsync(scopeInfo, context, tableBuilder, DbTriggerType.Update, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                await InternalCreateTriggerAsync(scopeInfo, context, tableBuilder, DbTriggerType.Update, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                await this.InterceptAsync(new UpgradeProgressArgs(context, $"Update Trigger for table {tableBuilder.TableDescription.GetFullName()} updated", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
            }

            message = $"Upgrade to {newVersion} done.";
            await this.InterceptAsync(new UpgradeProgressArgs(context, message, newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            return newVersion;
        }

        private async Task<Version> UpgdrateTo700Async(IScopeInfo scopeInfo, SyncContext context, DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            var newVersion = new Version(0, 7, 0);
            // Sorting tables based on dependencies between them

            var schemaTables = scopeInfo.Schema.Tables
                .SortByDependencies(tab => tab.GetRelations()
                    .Select(r => r.GetParentTable()));

            var message = $"Upgrade to {newVersion}:";
            await this.InterceptAsync(new UpgradeProgressArgs(context, message, newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            foreach (var schemaTable in schemaTables)
            {
                var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                // Upgrade Reset stored procedure
                bool exists;
                (context, exists) = await InternalExistsStoredProcedureAsync(scopeInfo, context, tableBuilder,
                    DbStoredProcedureType.Reset, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (exists)
                    await InternalDropStoredProcedureAsync(scopeInfo, context, tableBuilder, DbStoredProcedureType.Reset, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                await InternalCreateStoredProcedureAsync(scopeInfo, context, tableBuilder, DbStoredProcedureType.Reset, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                await this.InterceptAsync(new UpgradeProgressArgs(context, $"Reset stored procedure for table {tableBuilder.TableDescription.GetFullName()} updated", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                // Upgrade Update stored procedure
                bool existsUpdateSP;
                (context, existsUpdateSP) = await InternalExistsStoredProcedureAsync(scopeInfo, context, tableBuilder,
                    DbStoredProcedureType.UpdateRow, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (existsUpdateSP)
                    await InternalDropStoredProcedureAsync(scopeInfo, context, tableBuilder, DbStoredProcedureType.UpdateRow, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                await InternalCreateStoredProcedureAsync(scopeInfo, context, tableBuilder, DbStoredProcedureType.UpdateRow, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                await this.InterceptAsync(new UpgradeProgressArgs(context, $"Update stored procedure for table {tableBuilder.TableDescription.GetFullName()} updated", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                // Upgrade Bulk Update stored procedure
                bool existsBulkUpdateSP;
                (context, existsBulkUpdateSP) = await InternalExistsStoredProcedureAsync(scopeInfo, context, tableBuilder,
                    DbStoredProcedureType.BulkUpdateRows, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (existsBulkUpdateSP)
                {
                    await InternalDropStoredProcedureAsync(scopeInfo, context, tableBuilder, DbStoredProcedureType.BulkUpdateRows, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                    await InternalCreateStoredProcedureAsync(scopeInfo, context, tableBuilder, DbStoredProcedureType.BulkUpdateRows, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    await this.InterceptAsync(new UpgradeProgressArgs(context, $"Bulk Update stored procedure for table {tableBuilder.TableDescription.GetFullName()} updated", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
                }
            }
            return newVersion;
        }


        private async Task<Version> UpgdrateTo801Async(IScopeInfo scopeInfo, SyncContext context, DbConnection connection, DbTransaction transaction,
                       CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            var newVersion = new Version(0, 8, 1);
            // Sorting tables based on dependencies between them

            var schemaTables = scopeInfo.Schema.Tables
                .SortByDependencies(tab => tab.GetRelations()
                    .Select(r => r.GetParentTable()));

            var message = $"Upgrade to {newVersion}:";
            await this.InterceptAsync(new UpgradeProgressArgs(context, message, newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            foreach (var schemaTable in schemaTables)
            {
                var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);
                await InternalCreateStoredProceduresAsync(scopeInfo, context, true, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                await this.InterceptAsync(new UpgradeProgressArgs(context, $"ALL Stored procedures for table {tableBuilder.TableDescription.GetFullName()} updated", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
            }

            return newVersion;
        }

        private async Task<Version> UpgdrateTo093Async(ClientScopeInfo clientScopeInfo, SyncContext context, DbConnection connection, DbTransaction transaction,
                        CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var newVersion = new Version(0, 9, 3);
            // Sorting tables based on dependencies between them

            var schemaTables = clientScopeInfo.Schema.Tables
                .SortByDependencies(tab => tab.GetRelations()
                    .Select(r => r.GetParentTable()));

            var message = $"Upgrade to {newVersion}:";
            await this.InterceptAsync(new UpgradeProgressArgs(context, message, newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            var provision = SyncProvision.StoredProcedures | SyncProvision.Triggers;

            await this.DeprovisionAsync(clientScopeInfo.Name, provision, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            var clientScope = await this.GetClientScopeInfoAsync(clientScopeInfo.Name, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
            var serverScope = new ServerScopeInfo
            {
                Schema = clientScope.Schema,
                Setup = clientScope.Setup,
                Version = clientScope.Version
            };

            clientScope = await this.ProvisionAsync(serverScope, provision, true, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            return newVersion;
        }
        private async Task<Version> UpgdrateTo094Async(ClientScopeInfo clientScopeInfo, SyncContext context, DbConnection connection, DbTransaction transaction,
                        CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var newVersion = new Version(0, 9, 4);
            // Sorting tables based on dependencies between them

            var schemaTables = clientScopeInfo.Schema.Tables
                .SortByDependencies(tab => tab.GetRelations()
                    .Select(r => r.GetParentTable()));

            var message = $"Upgrade to {newVersion}:";
            await this.InterceptAsync(new UpgradeProgressArgs(context, message, newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            var provision = SyncProvision.StoredProcedures | SyncProvision.Triggers;

            await this.DeprovisionAsync(clientScopeInfo.Name, provision, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            var clientScope = await this.GetClientScopeInfoAsync(clientScopeInfo.Name, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
            var serverScope = new ServerScopeInfo
            {
                Schema = clientScope.Schema,
                Setup = clientScope.Setup,
                Version = clientScope.Version
            };

            clientScope = await this.ProvisionAsync(serverScope, provision, true, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            return newVersion;
        }

        private async Task<Version> UpgdrateTo095Async(ClientScopeInfo scopeInfo, SyncContext context, DbConnection connection, DbTransaction transaction,
                CancellationToken cancellationToken, IProgress<ProgressArgs> progress)

        {

            await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.Migrating, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            var newVersion = new Version(0, 9, 5);

            await this.InterceptAsync(new UpgradeProgressArgs(context, $"Upgrade client scope {scopeInfo.Name} to {newVersion}:", newVersion, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

            // get scope info table name
            var parsedName = ParserName.Parse(this.Options.ScopeInfoTableName);
            var scopeClientInfoTableName = $"{parsedName.Unquoted().Normalized().ToString()}";

            var syncTable = new SyncTable(scopeClientInfoTableName);
            var scopeClientInfoTableBuilder = this.GetTableBuilder(syncTable, scopeInfo);
            var pkeys = await scopeClientInfoTableBuilder.GetPrimaryKeysAsync(runner.Connection, runner.Transaction).ConfigureAwait(false);

            if (pkeys.Count() == 1)
            {
                if (this.Provider.GetProviderTypeName().Contains("Dotmim.Sync.SqlServer.SqlSyncProvider"))
                {
                    var commandText = @$"ALTER TABLE dbo.{scopeClientInfoTableName} DROP CONSTRAINT PK_{scopeClientInfoTableName};
                                        ALTER TABLE dbo.{scopeClientInfoTableName} ADD CONSTRAINT 
                                        PK_{scopeClientInfoTableName} PRIMARY KEY CLUSTERED (sync_scope_id, sync_scope_name);";

                    var command = runner.Connection.CreateCommand();
                    command.CommandText = commandText;
                    command.Transaction = runner.Transaction;

                    command.CommandTimeout = Options.SqlCommandTimeout;

                    await command.ExecuteNonQueryAsync();

                    await this.InterceptAsync(new UpgradeProgressArgs(context, $"{scopeClientInfoTableName} primary keys updated on SQL Server", newVersion, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                }

                if (this.Provider.GetProviderTypeName().Contains("Dotmim.Sync.MySql.MySqlSyncProvider"))
                {
                    var commandText = @$"ALTER TABLE `{scopeClientInfoTableName}` 
                                        CHANGE COLUMN `sync_scope_name` `sync_scope_name` VARCHAR(100) NOT NULL ,
                                        DROP PRIMARY KEY,
                                        ADD PRIMARY KEY (`sync_scope_id`, `sync_scope_name`);";
                    var command = runner.Connection.CreateCommand();
                    command.CommandText = commandText;
                    command.Transaction = runner.Transaction;

                    command.CommandTimeout = Options.SqlCommandTimeout;

                    await command.ExecuteNonQueryAsync();
                }
                if (this.Provider.GetProviderTypeName().Contains("Dotmim.Sync.Sqlite.SqliteSyncProvider"))
                {
                    var commandText = @$"
                                        BEGIN TRANSACTION;

                                        ALTER TABLE [{scopeClientInfoTableName}] RENAME TO old_table_{scopeClientInfoTableName};
                                        CREATE TABLE [{scopeClientInfoTableName}](
                                                    sync_scope_id blob NOT NULL,
	                                                sync_scope_name text NOT NULL,
	                                                sync_scope_schema text NULL,
	                                                sync_scope_setup text NULL,
	                                                sync_scope_version text NULL,
                                                    scope_last_server_sync_timestamp integer NULL,
                                                    scope_last_sync_timestamp integer NULL,
                                                    scope_last_sync_duration integer NULL,
                                                    scope_last_sync datetime NULL,
                                                    CONSTRAINT PK_{scopeClientInfoTableName} PRIMARY KEY(sync_scope_id, sync_scope_name));

                                        INSERT INTO [{scopeClientInfoTableName}] SELECT * FROM old_table_{scopeClientInfoTableName};
                                        COMMIT;

                                        DROP TABLE old_table_{scopeClientInfoTableName};";

                    var command = runner.Connection.CreateCommand();
                    command.CommandText = commandText;
                    command.Transaction = runner.Transaction;

                    command.CommandTimeout = Options.SqlCommandTimeout;

                    await command.ExecuteNonQueryAsync();
                }
            }


            List<ClientScopeInfo> clientScopeInfos;
            (context, clientScopeInfos) = await this.InternalLoadAllClientScopesInfoAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

            var defaultClientScopeInfo = clientScopeInfos.FirstOrDefault(csi => csi.Name == "DefaultScope");
            if (defaultClientScopeInfo == null)
                defaultClientScopeInfo = clientScopeInfos[0];

            var provision = SyncProvision.StoredProcedures | SyncProvision.Triggers;

            await this.DeprovisionAsync(scopeInfo.Name, provision, runner.Connection, runner.Transaction, progress: progress);

            // simulate 0.94 scope without scopename in sp
            // Creating a fake scope info
            var fakeClientScopeInfo = this.InternalCreateScopeInfo(SyncOptions.DefaultScopeName, DbScopeType.Client);
            fakeClientScopeInfo.Setup = scopeInfo.Setup;

            await this.InternalDeprovisionAsync(fakeClientScopeInfo, context, provision, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);
            await this.InterceptAsync(new UpgradeProgressArgs(context, $"Deprovision scope {scopeInfo.Name}", newVersion, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

            // Delete scope info to be able to recreate it with correct primary keys
            await this.InternalDeleteClientScopeInfoAsync(scopeInfo, context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

            scopeInfo.Id = defaultClientScopeInfo.Id;
            await this.InternalSaveClientScopeInfoAsync(scopeInfo, context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

            // provision with a fake server scope
            var serverScope = new ServerScopeInfo
            {
                Schema = scopeInfo.Schema,
                Setup = scopeInfo.Setup,
                Version = scopeInfo.Version,
                Name = scopeInfo.Name,
                IsNewScope = scopeInfo.IsNewScope,
            };

            await this.ProvisionAsync(serverScope, provision, true, runner.Connection, runner.Transaction, progress: runner.Progress);
            await this.InterceptAsync(new UpgradeProgressArgs(context, $"Provision scope {scopeInfo.Name}", newVersion, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

            await runner.CommitAsync().ConfigureAwait(false);

            return newVersion;
        }
        private async Task<Version> AutoUpgdrateToNewVersionAsync(IScopeInfo scopeInfo, SyncContext context, Version newVersion, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var message = $"Upgrade to {newVersion}:";

            await this.InterceptAsync(new UpgradeProgressArgs(context, message, newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
            return newVersion;
        }
    }
}
