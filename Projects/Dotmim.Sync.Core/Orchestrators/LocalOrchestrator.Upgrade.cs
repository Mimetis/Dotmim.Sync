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

        public virtual async Task<ScopeInfo> UpgradeAsync(ScopeInfo scopeInfo, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                Guard.Against.Null(scopeInfo);

                await using var runner = await this.GetConnectionAsync(scopeInfo.Name, SyncMode.Writing, SyncStage.ChangesApplying, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                scopeInfo = await this.InternalUpgradeAsync(scopeInfo, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return scopeInfo;
            }
            catch (Exception ex)
            {
                throw GetSyncError(scopeInfo.Name, ex);
            }
        }
        public virtual async Task<ScopeInfo> UpgradeAsync(string scopeName, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                Guard.Against.NullOrWhiteSpace(scopeName);

                await using var runner = await this.GetConnectionAsync(scopeName, SyncMode.Writing, SyncStage.ChangesApplying, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var scopeInfo = await this.InternalGetScopeAsync(scopeName, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false) as ScopeInfo;

                if (scopeInfo == null || scopeInfo.Schema == null || scopeInfo.Schema.Tables == null || scopeInfo.Schema.Tables.Count <= 0 || !scopeInfo.Schema.HasColumns)
                    return null;

                scopeInfo = await this.InternalUpgradeAsync(scopeInfo, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return scopeInfo;
            }
            catch (Exception ex)
            {
                throw GetSyncError(scopeName, ex);
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

                var scopeInfo = await this.InternalGetScopeAsync(scopeName, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (scopeInfo == null || scopeInfo.Schema == null || scopeInfo.Schema.Tables == null || scopeInfo.Schema.Tables.Count <= 0 || !scopeInfo.Schema.HasColumns)
                    return false;


                return InternalNeedsToUpgrade(scopeInfo);
            }
            catch (Exception ex)
            {
                throw GetSyncError(scopeName, ex);
            }
        }

        internal virtual bool InternalNeedsToUpgrade(IScopeInfo scopeInfo)
        {
            var version = SyncVersion.EnsureVersion(scopeInfo.Version);
            return version < SyncVersion.Current;
        }

        internal virtual async Task<ScopeInfo> InternalUpgradeAsync(ScopeInfo scopeInfo, DbConnection connection, DbTransaction transaction,
                        CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            var version = SyncVersion.EnsureVersion(scopeInfo.Version);
            var oldVersion = version.Clone() as Version;
            // beta version
            if (version.Major == 0)
            {
                if (version.Minor <= 5)
                    version = await AutoUpgdrateToNewVersionAsync(scopeInfo, new Version(0, 6, 0), connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (version.Minor == 6 && version.Build == 0)
                    version = await UpgdrateTo601Async(scopeInfo, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (version.Minor == 6 && version.Build == 1)
                    version = await UpgdrateTo602Async(scopeInfo, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (version.Minor == 6 && version.Build >= 2)
                    version = await UpgdrateTo700Async(scopeInfo, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (version.Minor == 7 && version.Build == 0)
                    version = await AutoUpgdrateToNewVersionAsync(scopeInfo, new Version(0, 7, 1), connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (version.Minor == 7 && version.Build == 1)
                    version = await AutoUpgdrateToNewVersionAsync(scopeInfo, new Version(0, 7, 2), connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (version.Minor == 7 && version.Build == 2)
                    version = await AutoUpgdrateToNewVersionAsync(scopeInfo, new Version(0, 7, 3), connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (version.Minor == 7 && version.Build >= 3)
                    version = await AutoUpgdrateToNewVersionAsync(scopeInfo, new Version(0, 8, 0), connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (version.Minor == 8 && version.Build == 0)
                    version = await UpgdrateTo801Async(scopeInfo, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (version.Minor == 8 && version.Build == 1)
                    version = await AutoUpgdrateToNewVersionAsync(scopeInfo, new Version(0, 9, 0), connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (version.Minor == 9 && version.Build == 0)
                    version = await AutoUpgdrateToNewVersionAsync(scopeInfo, new Version(0, 9, 1), connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (version.Minor == 9 && version.Build == 1)
                    version = await UpgdrateTo093Async(scopeInfo, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (version.Minor == 9 && version.Build == 2)
                    version = await UpgdrateTo093Async(scopeInfo, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (version.Minor == 9 && version.Build == 3)
                    version = await UpgdrateTo094Async(scopeInfo, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
            }

            if (oldVersion != version)
            {
                scopeInfo.Version = version.ToString();
                scopeInfo = await this.InternalSaveScopeAsync(scopeInfo, DbScopeType.Client, connection, transaction, cancellationToken, progress).ConfigureAwait(false) as ScopeInfo;
            }
            return scopeInfo;

        }

        private async Task<Version> UpgdrateTo601Async(IScopeInfo scopeInfo, DbConnection connection, DbTransaction transaction,
                        CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            var context = this.GetContext(scopeInfo.Name);
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
                var exists = await InternalExistsStoredProcedureAsync(scopeInfo, tableBuilder, DbStoredProcedureType.SelectInitializedChanges, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                if (exists)
                    await InternalDropStoredProcedureAsync(scopeInfo, tableBuilder, DbStoredProcedureType.SelectInitializedChanges, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                await InternalCreateStoredProcedureAsync(scopeInfo, tableBuilder, DbStoredProcedureType.SelectInitializedChanges, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                await this.InterceptAsync(new UpgradeProgressArgs(context, $"SelectInitializedChanges stored procedure for table {tableBuilder.TableDescription.GetFullName()} updated", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                // Upgrade Select Initial Changes With Filter
                if (tableBuilder.TableDescription.GetFilter() != null)
                {
                    var existsWithFilter = await InternalExistsStoredProcedureAsync(scopeInfo, tableBuilder, DbStoredProcedureType.SelectInitializedChangesWithFilters, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                    if (existsWithFilter)
                        await InternalDropStoredProcedureAsync(scopeInfo, tableBuilder, DbStoredProcedureType.SelectInitializedChangesWithFilters, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                    await InternalCreateStoredProcedureAsync(scopeInfo, tableBuilder, DbStoredProcedureType.SelectInitializedChangesWithFilters, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    await this.InterceptAsync(new UpgradeProgressArgs(context, $"SelectInitializedChangesWithFilters stored procedure for table {tableBuilder.TableDescription.GetFullName()} updated", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
                }

            }

            return newVersion;
        }

        private async Task<Version> UpgdrateTo602Async(IScopeInfo scopeInfo, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            var newVersion = new Version(0, 6, 2);
            var context = this.GetContext(scopeInfo.Name);
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
                var exists = await InternalExistsTriggerAsync(scopeInfo, tableBuilder, DbTriggerType.Update, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                if (exists)
                    await InternalDropTriggerAsync(scopeInfo, tableBuilder, DbTriggerType.Update, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                await InternalCreateTriggerAsync(scopeInfo, tableBuilder, DbTriggerType.Update, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                await this.InterceptAsync(new UpgradeProgressArgs(context, $"Update Trigger for table {tableBuilder.TableDescription.GetFullName()} updated", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
            }

            message = $"Upgrade to {newVersion} done.";
            await this.InterceptAsync(new UpgradeProgressArgs(context, message, newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            return newVersion;
        }

        private async Task<Version> UpgdrateTo700Async(IScopeInfo scopeInfo, DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            var newVersion = new Version(0, 7, 0);
            var context = this.GetContext(scopeInfo.Name);
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
                var exists = await InternalExistsStoredProcedureAsync(scopeInfo, tableBuilder,
                    DbStoredProcedureType.Reset, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                if (exists)
                    await InternalDropStoredProcedureAsync(scopeInfo, tableBuilder, DbStoredProcedureType.Reset, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                await InternalCreateStoredProcedureAsync(scopeInfo, tableBuilder, DbStoredProcedureType.Reset, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                await this.InterceptAsync(new UpgradeProgressArgs(context, $"Reset stored procedure for table {tableBuilder.TableDescription.GetFullName()} updated", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                // Upgrade Update stored procedure
                var existsUpdateSP = await InternalExistsStoredProcedureAsync(scopeInfo, tableBuilder,
                    DbStoredProcedureType.UpdateRow, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                if (existsUpdateSP)
                    await InternalDropStoredProcedureAsync(scopeInfo, tableBuilder, DbStoredProcedureType.UpdateRow, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                await InternalCreateStoredProcedureAsync(scopeInfo, tableBuilder, DbStoredProcedureType.UpdateRow, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                await this.InterceptAsync(new UpgradeProgressArgs(context, $"Update stored procedure for table {tableBuilder.TableDescription.GetFullName()} updated", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

                // Upgrade Bulk Update stored procedure
                var existsBulkUpdateSP = await InternalExistsStoredProcedureAsync(scopeInfo, tableBuilder,
                    DbStoredProcedureType.BulkUpdateRows, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (existsBulkUpdateSP)
                {
                    await InternalDropStoredProcedureAsync(scopeInfo, tableBuilder, DbStoredProcedureType.BulkUpdateRows, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                    await InternalCreateStoredProcedureAsync(scopeInfo, tableBuilder, DbStoredProcedureType.BulkUpdateRows, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    await this.InterceptAsync(new UpgradeProgressArgs(context, $"Bulk Update stored procedure for table {tableBuilder.TableDescription.GetFullName()} updated", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
                }
            }
            return newVersion;
        }


        private async Task<Version> UpgdrateTo801Async(IScopeInfo scopeInfo, DbConnection connection, DbTransaction transaction,
                       CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            var context = this.GetContext(scopeInfo.Name);
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
                await InternalCreateStoredProceduresAsync(scopeInfo, true, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                await this.InterceptAsync(new UpgradeProgressArgs(context, $"ALL Stored procedures for table {tableBuilder.TableDescription.GetFullName()} updated", newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
            }

            return newVersion;
        }

        private async Task<Version> UpgdrateTo093Async(IScopeInfo scopeInfo, DbConnection connection, DbTransaction transaction,
                        CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var context = this.GetContext(scopeInfo.Name);
            var newVersion = new Version(0, 9, 3);
            // Sorting tables based on dependencies between them

            var schemaTables = scopeInfo.Schema.Tables
                .SortByDependencies(tab => tab.GetRelations()
                    .Select(r => r.GetParentTable()));

            var message = $"Upgrade to {newVersion}:";
            await this.InterceptAsync(new UpgradeProgressArgs(context, message, newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            var provision = SyncProvision.StoredProcedures | SyncProvision.Triggers;

            await this.DeprovisionAsync(scopeInfo, provision, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
            await this.ProvisionAsync(scopeInfo, provision, false, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            return newVersion;
        }
        private async Task<Version> UpgdrateTo094Async(IScopeInfo scopeInfo, DbConnection connection, DbTransaction transaction,
                        CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var context = this.GetContext(scopeInfo.Name);
            var newVersion = new Version(0, 9, 4);
            // Sorting tables based on dependencies between them

            var schemaTables = scopeInfo.Schema.Tables
                .SortByDependencies(tab => tab.GetRelations()
                    .Select(r => r.GetParentTable()));

            var message = $"Upgrade to {newVersion}:";
            await this.InterceptAsync(new UpgradeProgressArgs(context, message, newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            var provision = SyncProvision.StoredProcedures | SyncProvision.Triggers;

            await this.DeprovisionAsync(scopeInfo, provision, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
            await this.ProvisionAsync(scopeInfo, provision, false, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            return newVersion;
        }
        private async Task<Version> AutoUpgdrateToNewVersionAsync(IScopeInfo scopeInfo, Version newVersion, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var message = $"Upgrade to {newVersion}:";
            var context = this.GetContext(scopeInfo.Name);

            await this.InterceptAsync(new UpgradeProgressArgs(context, message, newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
            return newVersion;
        }
    }
}
