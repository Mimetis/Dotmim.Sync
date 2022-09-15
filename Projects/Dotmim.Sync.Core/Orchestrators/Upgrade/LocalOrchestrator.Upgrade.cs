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
    public partial class LocalOrchestrator
    {
        /// <summary>
        /// Check if we need to upgrade the Database Structure
        /// </summary>
        public virtual async Task<bool> NeedsToUpgradeAsync()
        {
            var context = new SyncContext(Guid.NewGuid(), SyncOptions.DefaultScopeName);
            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.Migrating).ConfigureAwait(false);
                // get Database builder
                var dbBuilder = this.Provider.GetDatabaseBuilder();

                // Initialize database if needed
                await dbBuilder.EnsureDatabaseAsync(runner.Connection, runner.Transaction).ConfigureAwait(false);

                bool cScopeInfoExists;
                (context, cScopeInfoExists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfo,
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                bool cScopeInfoClientExists;
                (context, cScopeInfoClientExists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfoClient,
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                if (!cScopeInfoExists && !cScopeInfoClientExists)
                    return false;

                int cScopeInfoTableColumns = 10;

                if (cScopeInfoExists)
                {
                    var scopeInfoTable = await dbBuilder.GetTableColumnsAsync(this.Options.ScopeInfoTableName, default, runner.Connection, runner.Transaction).ConfigureAwait(false);

                    // 6 columns => version == 0.9.6
                    // > 6 columns => version <= 0.9.5
                    cScopeInfoTableColumns = scopeInfoTable.Rows.Count;
                }

                // migrated
                if (cScopeInfoTableColumns == 6 && cScopeInfoClientExists)
                    return false;

                return true;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Upgrade your client database to the last version
        /// <para>
        /// This upgrade is manually since <strong>we need the parameters</strong> you are using, to <strong>save them</strong> in the client database.
        /// Please provide all the entries (scope name + sync parameters) you are using <strong>locally</strong>, to sync to server
        /// </para>
        /// <example>
        /// Here is an example where we are migrating a local datasource where 2 scopes have been already used (1 with filters and the other one without filters)
        /// <code>
        /// var entries = new List[ScopeInfoClientUpgrade]();
        /// var entry = new ScopeInfoClientUpgrade
        /// {
        ///    Parameters = new SyncParameters(("ProductCategoryId", new Guid("Your_GUID_Filter_Value"))),
        ///    ScopeName = "v1"
        /// };
        /// entries.Add(entry);
        /// entry = new ScopeInfoClientUpgrade {ScopeName = "v2"};
        /// entries.Add(entry);
        /// 
        /// var (scopeInfos, scopeInfoClients) = await localOrchestrator.UpgradeAsync(entries, progress, evaluationOnly);
        /// </code>
        /// </example>
        /// </summary>
        /// <param name="entries">List of all entries you are using locally (an entry is a scope name + syncparameters with value) you are using to sync to server</param>
        /// <param name="progress">Progress of upgrade</param>
        /// <param name="evaluateOnly">if set to True, the upgrade will not be applied at the end. Usefull to test your upgrade and see if anything is breaking at some point.</param>
        /// <returns>
        /// The new scopeInfos and scopeInfoClients rows from your new upgrade sync config tables
        /// </returns>
        public virtual async Task<(List<ScopeInfo> scopeInfos, List<ScopeInfoClient> scopeInfoClients)> UpgradeAsync(List<ScopeInfoClientUpgrade> entries, IProgress<ProgressArgs> progress = default, bool evaluateOnly = false)
        {
            if (entries == null)
                throw new Exception("Please provide at least one entry (even without parameters)");

            var context = new SyncContext(Guid.NewGuid(), SyncOptions.DefaultScopeName);

            try
            {
                // scope info table name (and tmp table name)
                var parsedName = ParserName.Parse(this.Options.ScopeInfoTableName);
                var cScopeInfoTableName = $"{parsedName.Unquoted().Normalized()}";
                var cScopeInfoClientTableName = $"{parsedName.Unquoted().Normalized()}_client";
                var tmpCScopeInfoTableName = $"tmp{cScopeInfoTableName}";

                await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Migrating, default, default, default, progress).ConfigureAwait(false);
                var dbBuilder = this.Provider.GetDatabaseBuilder();

                // Initialize database if needed
                await dbBuilder.EnsureDatabaseAsync(runner.Connection, runner.Transaction).ConfigureAwait(false);

                // ----------------------------------------------------
                // Step 1 : Renaming scope_info to tmpscope_info
                // ----------------------------------------------------
                var tmpScopeInfoClientExists = await dbBuilder.ExistsTableAsync(tmpCScopeInfoTableName, null,
                    runner.Connection, runner.Transaction).ConfigureAwait(false);

                if (!tmpScopeInfoClientExists)
                    await dbBuilder.RenameTableAsync(cScopeInfoTableName, null, tmpCScopeInfoTableName, null,
                        runner.Connection, runner.Transaction).ConfigureAwait(false);

                var message = $"- Temporary renamed {cScopeInfoTableName} to {tmpCScopeInfoTableName}.";
                await this.InterceptAsync(new UpgradeProgressArgs(context, message, SyncVersion.Current, runner.Connection, runner.Transaction), runner.Progress).ConfigureAwait(false);


                // ----------------------------------------------------
                // Step 2 : Create scope_info 
                // ----------------------------------------------------
                bool existsCScopeInfo;
                (context, existsCScopeInfo) = await InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfo,
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                if (!existsCScopeInfo)
                    (context, _) = await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.ScopeInfo,
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                message = $"- Created new version of {cScopeInfoTableName} table.";
                await this.InterceptAsync(new UpgradeProgressArgs(context, message, SyncVersion.Current, runner.Connection, runner.Transaction), runner.Progress).ConfigureAwait(false);

                // ----------------------------------------------------
                // Step 3 : Create scope_info_client 
                // ----------------------------------------------------
                bool existsCScopeInfoClient;
                (context, existsCScopeInfoClient) = await InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfoClient,
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                if (!existsCScopeInfoClient)
                    (context, _) = await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.ScopeInfoClient,
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                message = $"- Created {cScopeInfoClientTableName} table.";
                await this.InterceptAsync(new UpgradeProgressArgs(context, message, SyncVersion.Current, runner.Connection, runner.Transaction), runner.Progress).ConfigureAwait(false);

                // ----------------------------------------------------
                // Step 4 : Read rows from tmpscope_info & create scope_info & scope_info_client rows
                // ----------------------------------------------------

                // load all scope_info lines
                var tmpScopeInfoClientTable = await dbBuilder.GetTableAsync(tmpCScopeInfoTableName, null,
                        runner.Connection, runner.Transaction).ConfigureAwait(false);

                // Create scope_info and scope_info_client for each pre version scope_info lines
                foreach (var scopeInfoRow in tmpScopeInfoClientTable.Rows)
                {
                    var setup = JsonConvert.DeserializeObject<SyncSetup>(scopeInfoRow["sync_scope_setup"].ToString());
                    var schema = JsonConvert.DeserializeObject<SyncSet>(scopeInfoRow["sync_scope_schema"].ToString());
                    var scopeName = scopeInfoRow["sync_scope_name"].ToString();

                    var setupParameters = setup.Filters.SelectMany(filter => filter.Parameters)?.GroupBy(sfp => sfp.Name).Select(g => g.Key).ToList();

                    // Get the entry parameters, provided by user with the parameters and their values
                    var entryParameters = entries.FirstOrDefault(e =>
                    {
                        var entryScopeName = string.IsNullOrEmpty(e.ScopeName) ? "DefaultScope" : e.ScopeName;
                        return entryScopeName == scopeName;
                    });

                    var parameters = entryParameters?.Parameters;

                    if (entryParameters != null && entryParameters.Parameters != null && entryParameters.Parameters.Count > 0)
                    {
                        if (setupParameters != null && setupParameters.Count > 0 && (parameters.Count != setupParameters.Count))
                            throw new Exception("Your setup to migrate contains one or more filters. Please use a SyncParameters argument when calling UpgradeAsync() (or SynchronizeAsync()).");

                        if (parameters.Count > 0 && (setupParameters == null || parameters.Count != setupParameters.Count))
                            throw new Exception("You specified a SyncParameters argument, but it seems your setup to migrate does not contains any filters.");

                        if (setupParameters != null)
                        {
                            if (setupParameters.Count != parameters.Count)
                                throw new Exception("Your setup to migrate contains one or more filters. Please use a SyncParameters argument when calling UpgradeAsync() (or SynchronizeAsync()).");

                            foreach (var setupParameter in setupParameters)
                                if (!parameters.Any(p => p.Name.ToLowerInvariant() == setupParameter.ToLowerInvariant()))
                                    throw new Exception("Your setup filters contains at least one parameter that is not available from SyncParameters argument.");

                            foreach (var parameter in parameters)
                                if (!setupParameters.Any(n => n.ToLowerInvariant() == parameter.Name.ToLowerInvariant()))
                                    throw new Exception("Your SyncParameters argument contains a parameter that is not contained in your setup filters.");
                        }
                    }

                    // Step 3 : create new scope_info and scope_info_client
                    var cScopeInfo = new ScopeInfo
                    {
                        Name = scopeName,
                        Setup = setup,
                        Schema = schema,
                        Version = SyncVersion.Current.ToString()
                    };
                    await this.InternalSaveScopeInfoAsync(cScopeInfo, context,
                         runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                    message = $"- Saved scope_info {scopeName} in {cScopeInfoTableName} table with a setup containing {setup.Tables.Count} tables.";
                    await this.InterceptAsync(new UpgradeProgressArgs(context, message, SyncVersion.Current, runner.Connection, runner.Transaction), runner.Progress).ConfigureAwait(false);


                    // create scope info client
                    var cScopeInfoClient = new ScopeInfoClient
                    {
                        LastSyncDuration = (long)scopeInfoRow["scope_last_sync_duration"],
                        LastSync = (DateTime)scopeInfoRow["scope_last_sync"],
                        Id = (Guid)scopeInfoRow["sync_scope_id"],
                        Hash = parameters != null ? parameters.GetHash() : SyncParameters.DefaultScopeHash,
                        LastServerSyncTimestamp = (long)scopeInfoRow["scope_last_server_sync_timestamp"],
                        LastSyncTimestamp = (long)scopeInfoRow["scope_last_sync_timestamp"],
                        Name = scopeInfoRow["sync_scope_name"].ToString(),
                        Parameters = parameters,
                    };

                    await this.InternalSaveScopeInfoClientAsync(cScopeInfoClient, context,
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                    message = $"- Saved scope_info_client {scopeName} in {cScopeInfoClientTableName} table with a LastServerSyncTimestamp fixed to {cScopeInfoClient.LastServerSyncTimestamp}";
                    await this.InterceptAsync(new UpgradeProgressArgs(context, message, SyncVersion.Current, runner.Connection, runner.Transaction), runner.Progress).ConfigureAwait(false);
                }

                // ----------------------------------------------------
                // Step 5 : Drop tmp table
                // ----------------------------------------------------
                await dbBuilder.DropsTableIfExistsAsync(tmpCScopeInfoTableName, null,
                    runner.Connection, runner.Transaction).ConfigureAwait(false);

                message = $"- Drop temporary {tmpCScopeInfoTableName} table.";
                await this.InterceptAsync(new UpgradeProgressArgs(context, message, SyncVersion.Current, runner.Connection, runner.Transaction), runner.Progress).ConfigureAwait(false);

                // ----------------------------------------------------
                // Step 6: Deprovision all and re provision
                // ----------------------------------------------------
                List<ScopeInfo> cScopeInfos = null;

                // get scope infos
                (context, cScopeInfos) = await this.InternalLoadAllScopeInfosAsync(context,
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                // fallback to "try to drop an hypothetical default scope"
                if (cScopeInfos == null)
                    cScopeInfos = new List<ScopeInfo>();

                // Get all filters and fake them for the default scope
                var existingFilters = cScopeInfos?.SelectMany(si => si.Setup.Filters).ToList();

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

                cScopeInfos.Add(defaultCScopeInfo);

                // Deprovision old triggers & stored procedures
                var provision = SyncProvision.StoredProcedures | SyncProvision.Triggers;

                foreach (var cScopeInfo in cScopeInfos)
                {
                    if (cScopeInfo == null || cScopeInfo.Setup == null || cScopeInfo.Setup.Tables == null || cScopeInfo.Setup.Tables.Count <= 0)
                        continue;

                    await this.InternalDeprovisionAsync(cScopeInfo, context, provision,
                            runner.Connection, runner.Transaction, runner.CancellationToken, default).ConfigureAwait(false);

                    message = $"- Deprovision old scope {cScopeInfo.Name}.";
                    await this.InterceptAsync(new UpgradeProgressArgs(context, message, SyncVersion.Current, runner.Connection, runner.Transaction), runner.Progress).ConfigureAwait(false);
                }

                // ----------------------------------------------------
                // Step 7 : Provision again
                // ----------------------------------------------------
                (context, cScopeInfos) = await this.InternalLoadAllScopeInfosAsync(context,
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                foreach (var cScopeInfo in cScopeInfos)
                {
                    if (cScopeInfo == null || cScopeInfo.Setup == null || cScopeInfo.Setup.Tables == null || cScopeInfo.Setup.Tables.Count <= 0)
                        continue;

                    (context, _) = await InternalProvisionClientAsync(cScopeInfo, cScopeInfo, context, provision, false,
                        runner.Connection, runner.Transaction, runner.CancellationToken, default).ConfigureAwait(false);

                    message = $"- Provision new scope {cScopeInfo.Name}.";
                    await this.InterceptAsync(new UpgradeProgressArgs(context, message, SyncVersion.Current, runner.Connection, runner.Transaction), runner.Progress).ConfigureAwait(false);
                }


                // ----------------------------------------------------
                // Step 8 : Get final scope_info and scope_info_client 
                // ----------------------------------------------------
                List<ScopeInfo> cFinalScopeInfos = null;

                (context, cFinalScopeInfos) = await this.InternalLoadAllScopeInfosAsync(context,
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                var cFinalScopeInfoClients = await this.InternalLoadAllScopeInfoClientsAsync(context,
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);


                if (evaluateOnly)
                    await runner.RollbackAsync().ConfigureAwait(false);
                else
                    await runner.CommitAsync().ConfigureAwait(false);

                return (cFinalScopeInfos, cFinalScopeInfoClients);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }


        internal virtual async Task<(SyncContext context, bool upgraded)> InternalUpgradeAsync(List<ScopeInfo> clientScopeInfos,
                        SyncContext context, DbConnection connection, DbTransaction transaction,
                        CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Migrating, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
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
                        (context, _) = await this.InternalSaveScopeInfoAsync(clientScopeInfo, context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);
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

        private async Task<Version> UpgdrateTo601Async(ScopeInfo scopeInfo, SyncContext context, DbConnection connection, DbTransaction transaction,
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

        private async Task<Version> UpgdrateTo602Async(ScopeInfo scopeInfo, SyncContext context, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
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

        private async Task<Version> UpgdrateTo700Async(ScopeInfo scopeInfo, SyncContext context, DbConnection connection, DbTransaction transaction,
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


        private async Task<Version> UpgdrateTo801Async(ScopeInfo scopeInfo, SyncContext context, DbConnection connection, DbTransaction transaction,
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

        private async Task<Version> UpgdrateTo093Async(ScopeInfo clientScopeInfo, SyncContext context, DbConnection connection, DbTransaction transaction,
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

            await this.DeprovisionAsync(clientScopeInfo.Name, provision).ConfigureAwait(false);

            var clientScope = await this.GetScopeInfoAsync(clientScopeInfo.Name).ConfigureAwait(false);
            var serverScope = new ScopeInfo
            {
                Schema = clientScope.Schema,
                Setup = clientScope.Setup,
                Version = clientScope.Version
            };

            clientScope = await this.ProvisionAsync(serverScope, provision, true).ConfigureAwait(false);

            return newVersion;
        }

        private async Task<Version> UpgdrateTo094Async(ScopeInfo clientScopeInfo, SyncContext context, DbConnection connection, DbTransaction transaction,
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

            await this.DeprovisionAsync(clientScopeInfo.Name, provision).ConfigureAwait(false);

            var clientScope = await this.GetScopeInfoAsync(clientScopeInfo.Name).ConfigureAwait(false);
            var serverScope = new ScopeInfo
            {
                Schema = clientScope.Schema,
                Setup = clientScope.Setup,
                Version = clientScope.Version
            };

            clientScope = await this.ProvisionAsync(serverScope, provision, true).ConfigureAwait(false);

            return newVersion;
        }

        private async Task<Version> UpgdrateTo095Async(ScopeInfo scopeInfo, SyncContext context, DbConnection connection, DbTransaction transaction,
                CancellationToken cancellationToken, IProgress<ProgressArgs> progress)

        {

            await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Migrating, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

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

                    // Parametrized command timeout established if exist
                    if (Options.DbCommandTimeout.HasValue)
                        command.CommandTimeout = Options.DbCommandTimeout.Value;

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

                    // Parametrized command timeout established if exist
                    if (Options.DbCommandTimeout.HasValue)
                    {
                        command.CommandTimeout = Options.DbCommandTimeout.Value;
                    }

                    await command.ExecuteNonQueryAsync();
                }
                if (this.Provider.GetProviderTypeName().Contains("Dotmim.Sync.Sqlite.SqliteSyncProvider"))
                {
                    var commandText = @$"BEGIN TRANSACTION;


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
                                        DROP TABLE old_table_{scopeClientInfoTableName};
                                        COMMIT;";

                    var command = runner.Connection.CreateCommand();
                    command.CommandText = commandText;
                    command.Transaction = runner.Transaction;

                    // Parametrized command timeout established if exist
                    if (Options.DbCommandTimeout.HasValue)
                    {
                        command.CommandTimeout = Options.DbCommandTimeout.Value;
                    }

                    await command.ExecuteNonQueryAsync();
                }
            }


            List<ScopeInfo> clientScopeInfos;
            (context, clientScopeInfos) = await this.InternalLoadAllScopeInfosAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

            var defaultClientScopeInfo = clientScopeInfos.FirstOrDefault(csi => csi.Name == "DefaultScope");
            if (defaultClientScopeInfo == null)
                defaultClientScopeInfo = clientScopeInfos[0];

            var provision = SyncProvision.StoredProcedures | SyncProvision.Triggers;

            await this.DeprovisionAsync(scopeInfo.Name, provision);

            // simulate 0.94 scope without scopename in sp
            // Creating a fake scope info
            var fakeClientScopeInfo = this.InternalCreateScopeInfo(SyncOptions.DefaultScopeName);
            fakeClientScopeInfo.Setup = scopeInfo.Setup;

            await this.InternalDeprovisionAsync(fakeClientScopeInfo, context, provision, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);
            await this.InterceptAsync(new UpgradeProgressArgs(context, $"Deprovision scope {scopeInfo.Name}", newVersion, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

            // Delete scope info to be able to recreate it with correct primary keys
            await this.InternalDeleteScopeInfoAsync(scopeInfo, context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

            await this.InternalSaveScopeInfoAsync(scopeInfo, context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

            // provision with a fake server scope
            var serverScope = new ScopeInfo
            {
                Schema = scopeInfo.Schema,
                Setup = scopeInfo.Setup,
                Version = scopeInfo.Version,
                Name = scopeInfo.Name
            };

            await this.ProvisionAsync(serverScope, provision, true);
            await this.InterceptAsync(new UpgradeProgressArgs(context, $"Provision scope {scopeInfo.Name}", newVersion, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

            await runner.CommitAsync().ConfigureAwait(false);

            return newVersion;
        }

        private async Task<Version> AutoUpgdrateToNewVersionAsync(ScopeInfo scopeInfo, SyncContext context, Version newVersion, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var message = $"Upgrade to {newVersion}:";

            await this.InterceptAsync(new UpgradeProgressArgs(context, message, newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
            return newVersion;
        }
    }
}
