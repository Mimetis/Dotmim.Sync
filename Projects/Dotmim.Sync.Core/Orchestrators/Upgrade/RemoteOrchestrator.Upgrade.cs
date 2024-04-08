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
        public virtual async Task<bool> NeedsToUpgradeAsync(SyncContext context)
        {
            try
            {

                await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.Migrating).ConfigureAwait(false);

                bool cScopeInfoExists;
                (context, cScopeInfoExists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfo,
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                // no scopes info table. Just a new sync
                if (!cScopeInfoExists)
                    return false;

                // get Database builder
                var dbBuilder = this.Provider.GetDatabaseBuilder();

                // get lines
                var scopeInfos = await dbBuilder.GetTableAsync(this.Options.ScopeInfoTableName, default,
                    runner.Connection, runner.Transaction).ConfigureAwait(false);

                // if empty, no need to upgrade
                if (scopeInfos == null || scopeInfos.Rows.Count == 0)
                    return false;

                // If we have a scope info, we can get the version
                if (scopeInfos != null && scopeInfos.Rows.Count > 0)
                {
                    // Get the first row and check sync_scope_version value
                    var row = scopeInfos.Rows[0];
                    var versionString = row["sync_scope_version"] == DBNull.Value ? null : row["sync_scope_version"].ToString();
                    var version = SyncVersion.EnsureVersion(versionString);

                    if (version != SyncVersion.Current)
                        return true;
                }

                return false;
                //await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.Migrating).ConfigureAwait(false);

                //var parsedName = ParserName.Parse(this.Options.ScopeInfoTableName);
                //var scopeInfoServerTableName = $"{parsedName.Unquoted().Normalized()}_server";
                //var scopeInfoServerHistoryTableName = $"{parsedName.Unquoted().Normalized()}_history";

                //var sScopeInfoTableName = $"{parsedName.Unquoted().Normalized()}";
                //var sScopeInfoClientTableName = $"{parsedName.Unquoted().Normalized()}_client";

                //// get Database builder
                //var dbBuilder = this.Provider.GetDatabaseBuilder();

                //// Initialize database if needed
                //await dbBuilder.EnsureDatabaseAsync(runner.Connection, runner.Transaction).ConfigureAwait(false);

                //// check old and new scopes tables
                //var exist1 = await dbBuilder.ExistsTableAsync(scopeInfoServerTableName, null, runner.Connection, runner.Transaction);
                //var exist2 = await dbBuilder.ExistsTableAsync(scopeInfoServerHistoryTableName, null, runner.Connection, runner.Transaction);
                //var exist3 = await dbBuilder.ExistsTableAsync(sScopeInfoTableName, null, runner.Connection, runner.Transaction);
                //var exist4 = await dbBuilder.ExistsTableAsync(sScopeInfoClientTableName, null, runner.Connection, runner.Transaction);

                //if (!exist1 && !exist2 && !exist3 && !exist4)
                //    return false;

                //// Check if scope_info exists
                //// If exists then we have already upgraded to last version
                //bool exists;
                //(context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfo,
                //    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                //if (exists)
                //    return false;

                //await runner.CommitAsync().ConfigureAwait(false);

                // return true;
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
        /// <returns>
        /// The new scopeInfos and scopeInfoClients rows from your new upgrade sync config tables.
        /// </returns>
        internal virtual async Task<bool> InternalUpgradeAsync(
            SyncContext context, DbConnection connection = default, DbTransaction transaction = default,
                        CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = default)
        {

            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.Migrating,
                    connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                // get Database builder
                var dbBuilder = this.Provider.GetDatabaseBuilder();

                // get the scope info lines
                var scopeInfos = await dbBuilder.GetTableAsync(this.Options.ScopeInfoTableName, default,
                    runner.Connection, runner.Transaction).ConfigureAwait(false);

                // if empty, no need to upgrade
                if (scopeInfos == null || scopeInfos.Rows.Count == 0)
                    return true;

                Version version = SyncVersion.Current;

                // If we have a scope info, we can get the version
                if (scopeInfos != null && scopeInfos.Rows.Count > 0)
                {
                    // Get the first row and check sync_scope_version value
                    var versionString = scopeInfos.Rows[0]["sync_scope_version"] == DBNull.Value ? null : scopeInfos.Rows[0]["sync_scope_version"].ToString();
                    version = SyncVersion.EnsureVersion(versionString);
                }

                if (version.Major == 0 && version.Minor == 9 && version.Build >= 6)
                    await UpgradeAutoToLastVersion(context, version, scopeInfos, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                else if (version.Major == 0 && (version.Minor <= 9 && version.Build <= 5 || version.Minor <= 8))
                    await UpgradeOldestVersions(context, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                return true;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }


        }
        internal virtual async Task<bool> UpgradeOldestVersions(
         SyncContext context, DbConnection connection = default, DbTransaction transaction = default,
                     CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = default)
        {
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
                // Step 2 : Create scope_info 
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
                // Step 3 : Create scope_info_client 
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
                // Step 4 : Migrate each scope to new table
                // ----------------------------------------------------

                // ----------------------------------------------------
                // Step 2 : Get Rows form old scope_info_server table
                // ----------------------------------------------------
                var existScopeInfoServerTable = await dbBuilder.ExistsTableAsync(scopeInfoServerTableName, null, runner.Connection, runner.Transaction);

                if (existScopeInfoServerTable)
                {

                    var scopeInfoServerTable = await dbBuilder.GetTableAsync(scopeInfoServerTableName, default,
                        runner.Connection, runner.Transaction).ConfigureAwait(false);

                    message = $"- Getting rows from old {scopeInfoServerTableName} table.";
                    await this.InterceptAsync(new UpgradeProgressArgs(context, message, SyncVersion.Current, runner.Connection, runner.Transaction), runner.Progress).ConfigureAwait(false);

                    foreach (var scopeInfoServerRow in scopeInfoServerTable.Rows)
                    {
                        var setup = serializer.Deserialize<SyncSetup>(scopeInfoServerRow["sync_scope_setup"].ToString());
                        var schema = serializer.Deserialize<SyncSet>(scopeInfoServerRow["sync_scope_schema"].ToString());
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

                    sScopeInfo.Version = SyncVersion.Current.ToString();

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

                await runner.CommitAsync().ConfigureAwait(false);

                return true;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }


    }
}
