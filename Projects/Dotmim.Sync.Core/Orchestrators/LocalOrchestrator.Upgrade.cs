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

        public virtual async Task<ClientScopeInfo> UpgradeAsync(ClientScopeInfo clientScopeInfo, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), clientScopeInfo.Name);

            try
            {
                Guard.Against.Null(clientScopeInfo);

                await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.ChangesApplying, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                (context, clientScopeInfo) = await this.InternalUpgradeAsync(clientScopeInfo, context, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return clientScopeInfo;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }
        public virtual async Task<ClientScopeInfo> UpgradeAsync(string scopeName, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            Guard.Against.NullOrWhiteSpace(scopeName);
            var context = new SyncContext(Guid.NewGuid(), scopeName);
            try
            {

                await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.ChangesApplying, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                ClientScopeInfo scopeInfo;
                (context, scopeInfo) = await this.InternalLoadClientScopeInfoAsync(context, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (scopeInfo == null || scopeInfo.Schema == null || scopeInfo.Schema.Tables == null || scopeInfo.Schema.Tables.Count <= 0 || !scopeInfo.Schema.HasColumns)
                    return null;

                (context, scopeInfo )= await this.InternalUpgradeAsync(scopeInfo, context, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return scopeInfo;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Check if we need to upgrade the Database Structure
        /// </summary>
        public virtual async Task<bool> NeedsToUpgradeAsync(string scopeName, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);
            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.Reading, SyncStage.Provisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                ClientScopeInfo clientScopeInfo;
                (context, clientScopeInfo) = await this.InternalLoadClientScopeInfoAsync(context, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (clientScopeInfo == null || clientScopeInfo.Schema == null || clientScopeInfo.Schema.Tables == null || clientScopeInfo.Schema.Tables.Count <= 0 || !clientScopeInfo.Schema.HasColumns)
                    return false;


                return InternalNeedsToUpgrade(clientScopeInfo);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        internal virtual bool InternalNeedsToUpgrade(IScopeInfo scopeInfo)
        {
            var version = SyncVersion.EnsureVersion(scopeInfo.Version);
            return version < SyncVersion.Current;
        }

        internal virtual async Task<(SyncContext context, ClientScopeInfo clientScopeInfo)> InternalUpgradeAsync(ClientScopeInfo clientScopeInfo,
                        SyncContext context, DbConnection connection, DbTransaction transaction,
                        CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            var version = SyncVersion.EnsureVersion(clientScopeInfo.Version);
            var oldVersion = version.Clone() as Version;
            // beta version
            if (version.Major == 0)
            {
                if (version.Minor <= 5)
                    version = await AutoUpgdrateToNewVersionAsync(clientScopeInfo, context, new Version(0, 6, 0), connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (version.Minor == 6 && version.Build == 0)
                    version = await UpgdrateTo601Async(clientScopeInfo, context, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (version.Minor == 6 && version.Build == 1)
                    version = await UpgdrateTo602Async(clientScopeInfo, context, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (version.Minor == 6 && version.Build >= 2)
                    version = await UpgdrateTo700Async(clientScopeInfo, context, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (version.Minor == 7 && version.Build == 0)
                    version = await AutoUpgdrateToNewVersionAsync(clientScopeInfo, context, new Version(0, 7, 1), connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (version.Minor == 7 && version.Build == 1)
                    version = await AutoUpgdrateToNewVersionAsync(clientScopeInfo, context, new Version(0, 7, 2), connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (version.Minor == 7 && version.Build == 2)
                    version = await AutoUpgdrateToNewVersionAsync(clientScopeInfo, context, new Version(0, 7, 3), connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (version.Minor == 7 && version.Build >= 3)
                    version = await AutoUpgdrateToNewVersionAsync(clientScopeInfo, context, new Version(0, 8, 0), connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (version.Minor == 8 && version.Build == 0)
                    version = await UpgdrateTo801Async(clientScopeInfo, context, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (version.Minor == 8 && version.Build == 1)
                    version = await AutoUpgdrateToNewVersionAsync(clientScopeInfo, context, new Version(0, 9, 0), connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (version.Minor == 9 && version.Build == 0)
                    version = await AutoUpgdrateToNewVersionAsync(clientScopeInfo, context, new Version(0, 9, 1), connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (version.Minor == 9 && version.Build == 1)
                    version = await UpgdrateTo093Async(clientScopeInfo, context, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (version.Minor == 9 && version.Build == 2)
                    version = await UpgdrateTo093Async(clientScopeInfo, context, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (version.Minor == 9 && version.Build == 3)
                    version = await UpgdrateTo094Async(clientScopeInfo, context, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
            }

            if (oldVersion != version)
            {
                clientScopeInfo.Version = version.ToString();
                (context, clientScopeInfo) = await this.InternalSaveClientScopeInfoAsync(clientScopeInfo, context, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
            }
            return (context, clientScopeInfo);

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

            await this.DeprovisionAsync(clientScopeInfo, provision, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
            (context, _) = await this.InternalProvisionClientAsync(context.ScopeName, context, provision, false, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

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

            await this.DeprovisionAsync(clientScopeInfo, provision, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
            (context, _) = await this.InternalProvisionClientAsync(context.ScopeName, context, provision, false, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            return newVersion;
        }

        private async Task<Version> UpgdrateTo095Async(IScopeInfo scopeInfo, SyncContext context, DbConnection connection, DbTransaction transaction,
                CancellationToken cancellationToken, IProgress<ProgressArgs> progress)

        {
            var newVersion = new Version(0, 9, 4);

            // get scope info table name
            var scopeInfoTableName = ParserName.Parse(this.Options.ScopeInfoTableName);
            var tableName = $"{scopeInfoTableName.Unquoted().Normalized().ToString()}";

            var alterScopeInfoTableCommandTextSql = @$"
            ALTER TABLE [dbo].[{tableName}] DROP CONSTRAINT [PK_{tableName}];
            ALTER TABLE dbo.scope_info ADD CONSTRAINT
                [PK_{tableName}] PRIMARY KEY CLUSTERED (sync_scope_id, sync_scope_name) ON[PRIMARY];";

            var alterScopeInfoTableCommandTextSqlite = @$"
            PRAGMA foreign_keys=off;
            BEGIN TRANSACTION;

            ALTER TABLE [{tableName}] RENAME TO old_table_{tableName};
            CREATE TABLE [{tableName}](
                        sync_scope_id blob NOT NULL,
	                    sync_scope_name text NOT NULL,
	                    sync_scope_schema text NULL,
	                    sync_scope_setup text NULL,
	                    sync_scope_version text NULL,
                        scope_last_server_sync_timestamp integer NULL,
                        scope_last_sync_timestamp integer NULL,
                        scope_last_sync_duration integer NULL,
                        scope_last_sync datetime NULL,
                        CONSTRAINT PK_{tableName} PRIMARY KEY(sync_scope_id, sync_scope_name));

            INSERT INTO [{tableName}] SELECT * FROM old_table_{tableName};
            COMMIT;

            DROP TABLE old_table_{tableName};

            PRAGMA foreign_keys=on;";



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
