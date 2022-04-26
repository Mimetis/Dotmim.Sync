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
            try
            {
                await using var runner = await this.GetConnectionAsync(SyncOptions.DefaultScopeName, SyncMode.Writing, SyncStage.Provisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                var dbBuilder = this.Provider.GetDatabaseBuilder();

                // Initialize database if needed
                await dbBuilder.EnsureDatabaseAsync(runner.Connection, runner.Transaction).ConfigureAwait(false);

                var serverScopeInfos = await this.InternalGetAllScopesAsync(SyncOptions.DefaultScopeName, DbScopeType.Server, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (serverScopeInfos == null || serverScopeInfos.Count <= 0)
                    throw new MissingServerScopeInfoException();

                var r = await this.InternalUpgradeAsync(serverScopeInfos, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return true;
            }
            catch (Exception ex)
            {
                throw GetSyncError(SyncOptions.DefaultScopeName, ex);
            }
        }


        /// <summary>
        /// Check if we need to upgrade the Database Structure
        /// </summary>
        public virtual async Task<bool> NeedsToUpgradeAsync(string scopeName, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(scopeName, SyncMode.Reading, SyncStage.Provisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                // get Database builder
                var dbBuilder = this.Provider.GetDatabaseBuilder();

                // Initialize database if needed
                await dbBuilder.EnsureDatabaseAsync(runner.Connection, runner.Transaction).ConfigureAwait(false);

                var exists = await this.InternalExistsScopeInfoTableAsync(scopeName, DbScopeType.Server, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (!exists)
                    return false;

                var scopes = await this.InternalGetAllScopesAsync(scopeName, DbScopeType.Server, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (scopes == null || scopes.Count <= 0)
                    return false;

                await runner.CommitAsync().ConfigureAwait(false);

                return InternalNeedsToUpgrade(scopes);
            }
            catch (Exception ex)
            {
                throw GetSyncError(scopeName, ex);
            }
        }

        internal virtual bool InternalNeedsToUpgrade(List<IScopeInfo> serverScopeInfos)
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


        internal virtual async Task<bool> InternalUpgradeAsync(List<IScopeInfo> serverScopeInfos, DbConnection connection, DbTransaction transaction,
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
                        version = await AutoUpgdrateToNewVersionAsync(serverScopeInfo, new Version(0, 6, 0), connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (version.Minor == 6 && version.Build == 0)
                        version = await UpgdrateTo601Async(serverScopeInfo, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                    
                    if (version.Minor == 6 && version.Build == 1)
                        version = await UpgdrateTo602Async(serverScopeInfo, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (version.Minor == 6 && version.Build >= 2)
                        version = await UpgdrateTo700Async(serverScopeInfo, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (version.Minor == 7 && version.Build == 0)
                        version = await AutoUpgdrateToNewVersionAsync(serverScopeInfo, new Version(0, 7, 1), connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (version.Minor == 7 && version.Build == 1)
                        version = await AutoUpgdrateToNewVersionAsync(serverScopeInfo, new Version(0, 7, 2), connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (version.Minor == 7 && version.Build == 2)
                        version = await AutoUpgdrateToNewVersionAsync(serverScopeInfo, new Version(0, 7, 3), connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (version.Minor == 7 && version.Build >= 3)
                        version = await AutoUpgdrateToNewVersionAsync(serverScopeInfo, new Version(0, 8, 0), connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (version.Minor == 8 && version.Build == 0)
                        version = await UpgdrateTo801Async(serverScopeInfo, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (version.Minor == 8 && version.Build >= 1)
                        version = await AutoUpgdrateToNewVersionAsync(serverScopeInfo, new Version(0, 9, 0), connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (version.Minor == 9 && version.Build == 0)
                        version = await AutoUpgdrateToNewVersionAsync(serverScopeInfo, new Version(0, 9, 1), connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (version.Minor == 9 && version.Build == 1)
                        version = await UpgdrateTo093Async(serverScopeInfo, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (version.Minor == 9 && version.Build == 2)
                        version = await UpgdrateTo093Async(serverScopeInfo, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (version.Minor == 9 && version.Build == 3)
                        version = await UpgdrateTo094Async(serverScopeInfo, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                }

                serverScopeInfo.Version = version.ToString();
                await this.InternalSaveScopeAsync(serverScopeInfo, DbScopeType.Server, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            }

            return true;

        }

        private async Task<Version> UpgdrateTo601Async(IScopeInfo serverScopeInfo, DbConnection connection, DbTransaction transaction,
                        CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            var newVersion = new Version(0, 6, 1);
            // Sorting tables based on dependencies between them
            var schemaTables = serverScopeInfo.Schema.Tables
                .SortByDependencies(tab => tab.GetRelations()
                    .Select(r => r.GetParentTable()));

            var context = this.GetContext(serverScopeInfo.Name);

            foreach (var schemaTable in schemaTables)
            {
                var tableBuilder = this.GetTableBuilder(schemaTable, serverScopeInfo);
                await InternalCreateStoredProceduresAsync(serverScopeInfo, true, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                await this.InterceptAsync(new UpgradeProgressArgs(context, $"ALL Stored procedures for table {tableBuilder.TableDescription.GetFullName()} updated", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
            }

            var message = $"Upgrade to 0.6.1 done for scope {serverScopeInfo.Name}";
            await this.InterceptAsync(new UpgradeProgressArgs(context, message, newVersion, connection, transaction), progress, cancellationToken);

            return newVersion;
        }


        private async Task<Version> UpgdrateTo602Async(IScopeInfo serverScopeInfo, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var newVersion = new Version(0, 6, 2);

            // Sorting tables based on dependencies between them
            var schemaTables = serverScopeInfo.Schema.Tables
                .SortByDependencies(tab => tab.GetRelations()
                    .Select(r => r.GetParentTable()));

            var context = this.GetContext(serverScopeInfo.Name);
            foreach (var schemaTable in schemaTables)
            {
                var tableBuilder = this.GetTableBuilder(schemaTable, serverScopeInfo);

                // Upgrade Select Initial Changes
                var exists = await InternalExistsTriggerAsync(serverScopeInfo, tableBuilder, DbTriggerType.Update, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                if (exists)
                    await InternalDropTriggerAsync(serverScopeInfo, tableBuilder, DbTriggerType.Update, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                await InternalCreateTriggerAsync(serverScopeInfo, tableBuilder, DbTriggerType.Update, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                await this.InterceptAsync(new UpgradeProgressArgs(context, $"Update Trigger for table {tableBuilder.TableDescription.GetFullName()} updated", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
            }

            var message = $"Upgrade to {newVersion} for scope {serverScopeInfo.Name}.";
            await this.InterceptAsync(new UpgradeProgressArgs(context, message, newVersion, connection, transaction), progress, cancellationToken);

            return newVersion;
        }

        private async Task<Version> UpgdrateTo700Async(IScopeInfo serverScopeInfo, DbConnection connection, DbTransaction transaction,
                     CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            var newVersion = new Version(0, 7, 0);
            // Sorting tables based on dependencies between them

            var schemaTables = serverScopeInfo.Schema.Tables
                .SortByDependencies(tab => tab.GetRelations()
                    .Select(r => r.GetParentTable()));
            var context = this.GetContext(serverScopeInfo.Name);

            var message = $"Upgrade to {newVersion}:";
            await this.InterceptAsync(new UpgradeProgressArgs(context, message, newVersion, connection, transaction), progress, cancellationToken);

            foreach (var schemaTable in schemaTables)
            {
                var tableBuilder = this.GetTableBuilder(schemaTable, serverScopeInfo);

                // Upgrade Reset stored procedure
                var exists = await InternalExistsStoredProcedureAsync(serverScopeInfo, tableBuilder,
                    DbStoredProcedureType.Reset, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                if (exists)
                    await InternalDropStoredProcedureAsync(serverScopeInfo, tableBuilder, DbStoredProcedureType.Reset, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                await InternalCreateStoredProcedureAsync(serverScopeInfo, tableBuilder, DbStoredProcedureType.Reset, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                await this.InterceptAsync(new UpgradeProgressArgs(context, $"Reset stored procedure for table {tableBuilder.TableDescription.GetFullName()} updated", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                // Upgrade Update stored procedure
                var existsUpdateSP = await InternalExistsStoredProcedureAsync(serverScopeInfo, tableBuilder,
                    DbStoredProcedureType.UpdateRow, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                if (existsUpdateSP)
                    await InternalDropStoredProcedureAsync(serverScopeInfo, tableBuilder, DbStoredProcedureType.UpdateRow, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                await InternalCreateStoredProcedureAsync(serverScopeInfo, tableBuilder, DbStoredProcedureType.UpdateRow, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                await this.InterceptAsync(new UpgradeProgressArgs(context, $"Update stored procedure for table {tableBuilder.TableDescription.GetFullName()} updated", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                var existsBulkUpdateSP = await InternalExistsStoredProcedureAsync(serverScopeInfo, tableBuilder,
                    DbStoredProcedureType.BulkUpdateRows, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (existsBulkUpdateSP)
                {
                    await InternalDropStoredProcedureAsync(serverScopeInfo, tableBuilder, DbStoredProcedureType.BulkUpdateRows, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                    await InternalCreateStoredProcedureAsync(serverScopeInfo, tableBuilder, DbStoredProcedureType.BulkUpdateRows, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                    await this.InterceptAsync(new UpgradeProgressArgs(context, $"Bulk Update stored procedure for table {tableBuilder.TableDescription.GetFullName()} updated", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
                }
            }
            message = $"Upgrade to {newVersion} for scope {serverScopeInfo.Name}.";
            await this.InterceptAsync(new UpgradeProgressArgs(context, message, newVersion, connection, transaction), progress, cancellationToken);
            return newVersion;
        }


        private async Task<Version> UpgdrateTo801Async(IScopeInfo serverScopeInfo, DbConnection connection, DbTransaction transaction,
               CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            var newVersion = new Version(0, 8, 1);
            // Sorting tables based on dependencies between them

            var context = this.GetContext(serverScopeInfo.Name);
            var schemaTables = serverScopeInfo.Schema.Tables
                .SortByDependencies(tab => tab.GetRelations()
                    .Select(r => r.GetParentTable()));

            var message = $"Upgrade to {newVersion}:";
            await this.InterceptAsync(new UpgradeProgressArgs(context, message, newVersion, connection, transaction), progress, cancellationToken);

            foreach (var schemaTable in schemaTables)
            {
                var tableBuilder = this.GetTableBuilder(schemaTable, serverScopeInfo);
                await InternalCreateStoredProceduresAsync(serverScopeInfo, true, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                await this.InterceptAsync(new UpgradeProgressArgs(context, $"ALL Stored procedures for table {tableBuilder.TableDescription.GetFullName()} updated", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
            }
            message = $"Upgrade to {newVersion} for scope {serverScopeInfo.Name}.";
            await this.InterceptAsync(new UpgradeProgressArgs(context, message, newVersion, connection, transaction), progress, cancellationToken);

            return newVersion;
        }


        private async Task<Version> UpgdrateTo093Async(IScopeInfo serverScopeInfo, DbConnection connection, DbTransaction transaction,
               CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            var newVersion = new Version(0, 9, 3);
            // Sorting tables based on dependencies between them

            var context = this.GetContext(serverScopeInfo.Name);
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

        private async Task<Version> UpgdrateTo094Async(IScopeInfo serverScopeInfo, DbConnection connection, DbTransaction transaction,
       CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            var newVersion = new Version(0, 9, 4);
            // Sorting tables based on dependencies between them

            var context = this.GetContext(serverScopeInfo.Name);
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

        private async Task<Version> AutoUpgdrateToNewVersionAsync(IScopeInfo scopeInfo, Version newVersion, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var context = this.GetContext(scopeInfo.Name);
            var message = $"Upgrade to {newVersion} for scope {scopeInfo.Name}.";
            await this.InterceptAsync(new UpgradeProgressArgs(context, message, newVersion, connection, transaction), progress, cancellationToken);

            return newVersion;
        }
    }
}
