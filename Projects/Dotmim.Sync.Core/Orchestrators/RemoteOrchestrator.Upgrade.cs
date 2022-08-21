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
    public partial class RemoteOrchestrator
    {
        /// <summary>
        /// Upgrade the database structure to reach the last DMS version
        /// </summary>
        public virtual async Task<bool> UpgradeAsync(DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), SyncOptions.DefaultScopeName);
            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Provisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                var dbBuilder = this.Provider.GetDatabaseBuilder();

                // Initialize database if needed
                await dbBuilder.EnsureDatabaseAsync(runner.Connection, runner.Transaction).ConfigureAwait(false);

                List<ServerScopeInfo> serverScopeInfos;
                (context, serverScopeInfos) = await this.InternalLoadAllServerScopesInfosAsync(context, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (serverScopeInfos == null || serverScopeInfos.Count <= 0)
                    throw new MissingServerScopeInfoException();

                var r = await this.InternalUpgradeAsync(serverScopeInfos, context, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

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
                await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.Provisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                // get Database builder
                var dbBuilder = this.Provider.GetDatabaseBuilder();

                // Initialize database if needed
                await dbBuilder.EnsureDatabaseAsync(runner.Connection, runner.Transaction).ConfigureAwait(false);

                bool exists;
                (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.Server, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (!exists)
                    return false;

                List<ServerScopeInfo> serverScopeInfos;
                (context, serverScopeInfos) = await this.InternalLoadAllServerScopesInfosAsync(context, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (serverScopeInfos == null || serverScopeInfos.Count <= 0)
                    return false;

                await runner.CommitAsync().ConfigureAwait(false);

                return InternalNeedsToUpgrade(serverScopeInfos);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        internal virtual bool InternalNeedsToUpgrade(List<ServerScopeInfo> serverScopeInfos)
        {
            var version = SyncVersion.Current;

            // get the smallest version of all scope in the scope info server tables
            foreach (var serverScopeInfo in serverScopeInfos)
            {
                var tmpVersion = SyncVersion.EnsureVersion(serverScopeInfo.Version);

                if (tmpVersion < version)
                    version = tmpVersion;
            }

            return version < SyncVersion.Current;
        }


        internal virtual async Task<(SyncContext context, bool upgraded)> InternalUpgradeAsync(List<ServerScopeInfo> serverScopeInfos, SyncContext context, DbConnection connection, DbTransaction transaction,
                        CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            // get the smallest version of all scope in the scope info server tables
            foreach (var serverScopeInfo in serverScopeInfos)
            {
                var version = SyncVersion.EnsureVersion(serverScopeInfo.Version);

                // beta version
                if (version.Major == 0)
                {
                    if (version.Minor <= 5)
                        version = await AutoUpgdrateToNewVersionAsync(serverScopeInfo, context, new Version(0, 6, 0), connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (version.Minor == 6 && version.Build == 0)
                        version = await UpgdrateTo601Async(serverScopeInfo, context, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (version.Minor == 6 && version.Build == 1)
                        version = await UpgdrateTo602Async(serverScopeInfo, context, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (version.Minor == 6 && version.Build >= 2)
                        version = await UpgdrateTo700Async(serverScopeInfo, context, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (version.Minor == 7 && version.Build == 0)
                        version = await AutoUpgdrateToNewVersionAsync(serverScopeInfo, context, new Version(0, 7, 1), connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (version.Minor == 7 && version.Build == 1)
                        version = await AutoUpgdrateToNewVersionAsync(serverScopeInfo, context, new Version(0, 7, 2), connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (version.Minor == 7 && version.Build == 2)
                        version = await AutoUpgdrateToNewVersionAsync(serverScopeInfo, context, new Version(0, 7, 3), connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (version.Minor == 7 && version.Build >= 3)
                        version = await AutoUpgdrateToNewVersionAsync(serverScopeInfo, context, new Version(0, 8, 0), connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (version.Minor == 8 && version.Build == 0)
                        version = await UpgdrateTo801Async(serverScopeInfo, context, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (version.Minor == 8 && version.Build >= 1)
                        version = await AutoUpgdrateToNewVersionAsync(serverScopeInfo, context, new Version(0, 9, 0), connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (version.Minor == 9 && version.Build == 0)
                        version = await AutoUpgdrateToNewVersionAsync(serverScopeInfo, context, new Version(0, 9, 1), connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (version.Minor == 9 && version.Build == 1)
                        version = await UpgdrateTo093Async(serverScopeInfo, context, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (version.Minor == 9 && version.Build == 2)
                        version = await UpgdrateTo093Async(serverScopeInfo, context, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (version.Minor == 9 && version.Build == 3)
                        version = await UpgdrateTo094Async(serverScopeInfo, context, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (version.Minor == 9 && version.Build == 4)
                        version = await UpgdrateTo095Async(serverScopeInfo, context, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                }

                serverScopeInfo.Version = version.ToString();
                (context, _) = await this.InternalSaveServerScopeInfoAsync(serverScopeInfo, context, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            }

            return (context, true);

        }

        private async Task<Version> UpgdrateTo601Async(IScopeInfo serverScopeInfo, SyncContext context, DbConnection connection, DbTransaction transaction,
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


        private async Task<Version> UpgdrateTo602Async(IScopeInfo serverScopeInfo, SyncContext context, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
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

        private async Task<Version> UpgdrateTo700Async(IScopeInfo serverScopeInfo, SyncContext context, DbConnection connection, DbTransaction transaction,
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


        private async Task<Version> UpgdrateTo801Async(IScopeInfo serverScopeInfo, SyncContext context, DbConnection connection, DbTransaction transaction,
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


        private async Task<Version> UpgdrateTo093Async(IScopeInfo serverScopeInfo, SyncContext context, DbConnection connection, DbTransaction transaction,
               CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            var newVersion = new Version(0, 9, 3);
            // Sorting tables based on dependencies between them

            var schemaTables = serverScopeInfo.Schema.Tables
                .SortByDependencies(tab => tab.GetRelations()
                    .Select(r => r.GetParentTable()));

            await this.InterceptAsync(new UpgradeProgressArgs(context, $"Upgrade to {newVersion}:", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            var provision = SyncProvision.StoredProcedures | SyncProvision.Triggers;

            await this.DeprovisionAsync(serverScopeInfo.Name, provision, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
            await this.ProvisionAsync(serverScopeInfo.Name, provision, false, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            var message = $"Upgrade to {newVersion} for scope {serverScopeInfo.Name}.";
            await this.InterceptAsync(new UpgradeProgressArgs(context, message, newVersion, connection, transaction), progress, cancellationToken);

            return newVersion;
        }

        private async Task<Version> UpgdrateTo094Async(IScopeInfo serverScopeInfo, SyncContext context, DbConnection connection, DbTransaction transaction,
       CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            var newVersion = new Version(0, 9, 4);
            // Sorting tables based on dependencies between them

            var schemaTables = serverScopeInfo.Schema.Tables
                .SortByDependencies(tab => tab.GetRelations()
                    .Select(r => r.GetParentTable()));

            await this.InterceptAsync(new UpgradeProgressArgs(context, $"Upgrade to {newVersion}:", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            var provision = SyncProvision.StoredProcedures | SyncProvision.Triggers;

            await this.DeprovisionAsync(serverScopeInfo.Name, provision, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
            await this.ProvisionAsync(serverScopeInfo.Name, provision, false, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            var message = $"Upgrade to {newVersion} for scope {serverScopeInfo.Name}.";
            await this.InterceptAsync(new UpgradeProgressArgs(context, message, newVersion, connection, transaction), progress, cancellationToken);

            return newVersion;
        }

        private async Task<Version> UpgdrateTo095Async(IScopeInfo scopeInfo, SyncContext context, DbConnection connection, DbTransaction transaction,
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
                    var commandText = @$"ALTER TABLE dbo.{historyTableName} DROP CONSTRAINT PK_{historyTableName};
                                        ALTER TABLE dbo.{historyTableName} ADD CONSTRAINT 
                                        PK_{historyTableName} PRIMARY KEY CLUSTERED (sync_scope_id, sync_scope_name);";


                    var command = connection.CreateCommand();
                    command.CommandText = commandText;
                    command.Transaction = transaction;
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
                    await command.ExecuteNonQueryAsync();
                    await this.InterceptAsync(new UpgradeProgressArgs(context, $"{historyTableName} primary keys updated on MySql", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
                }


            }

            var provision = SyncProvision.StoredProcedures | SyncProvision.Triggers;
            await this.DeprovisionAsync(scopeInfo.Name, provision, connection, transaction, progress: progress);
            // simulate 0.94 scope without scopename in sp
            await this.DeprovisionAsync(SyncOptions.DefaultScopeName, scopeInfo.Setup, provision, connection, transaction, progress: progress);
            await this.InterceptAsync(new UpgradeProgressArgs(context, $"Deprovision scope {scopeInfo.Name}", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            await this.ProvisionAsync(scopeInfo.Name, provision, true, connection, transaction, progress: progress);
            await this.InterceptAsync(new UpgradeProgressArgs(context, $"Provision scope {scopeInfo.Name}", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            return newVersion;
        }


        private async Task<Version> UpgdrateTo096Async(IScopeInfo scopeInfo, SyncContext context, DbConnection connection, DbTransaction transaction,
             CancellationToken cancellationToken, IProgress<ProgressArgs> progress)

        {
            var newVersion = new Version(0, 9, 9);

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
                await command.ExecuteNonQueryAsync();
                await this.InterceptAsync(new UpgradeProgressArgs(context, $"{historyTableName} primary keys updated on SQL Server", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
            }

            if (this.Provider.GetProviderTypeName().Contains("MySqlSyncProvider"))
            {
                var commandText = @$"";

                var command = connection.CreateCommand();
                command.CommandText = commandText;
                command.Transaction = transaction;
                await command.ExecuteNonQueryAsync();
                await this.InterceptAsync(new UpgradeProgressArgs(context, $"{historyTableName} primary keys updated on MySql", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            }

            var provision = SyncProvision.StoredProcedures | SyncProvision.Triggers;
            await this.DeprovisionAsync(scopeInfo.Name, provision, connection, transaction, progress: progress);
            // simulate 0.94 scope without scopename in sp
            await this.DeprovisionAsync(SyncOptions.DefaultScopeName, scopeInfo.Setup, provision, connection, transaction, progress: progress);
            await this.InterceptAsync(new UpgradeProgressArgs(context, $"Deprovision scope {scopeInfo.Name}", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            await this.ProvisionAsync(scopeInfo.Name, provision, true, connection, transaction, progress: progress);
            await this.InterceptAsync(new UpgradeProgressArgs(context, $"Provision scope {scopeInfo.Name}", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            return newVersion;
        }


        private async Task<Version> AutoUpgdrateToNewVersionAsync(IScopeInfo scopeInfo, SyncContext context, Version newVersion, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var message = $"Upgrade to {newVersion} for scope {scopeInfo.Name}.";
            await this.InterceptAsync(new UpgradeProgressArgs(context, message, newVersion, connection, transaction), progress, cancellationToken);

            return newVersion;
        }
    }
}
