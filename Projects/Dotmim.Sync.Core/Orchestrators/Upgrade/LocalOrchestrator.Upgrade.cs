using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Manager;
using Dotmim.Sync.Serialization;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
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

        internal virtual async Task<bool> NeedsToUpgradeAsync(SyncContext context)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.Migrating).ConfigureAwait(false);
                // get Database builder
                var dbBuilder = this.Provider.GetDatabaseBuilder();

                bool cScopeInfoExists;
                (context, cScopeInfoExists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfo,
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                // no scopes info table. Just a new sync
                if (!cScopeInfoExists)
                    return false;

                // check schema is valid
                var schemaIsValid = await this.IsScopeInfoSchemaValidAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                // if columns are not the same, we need to upgrade
                if (!schemaIsValid)
                    return true;

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

                // Check if we have a client scope info table
                bool cScopeInfoClientExists;
                (context, cScopeInfoClientExists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfoClient,
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                // no scopes info client table but we have a scope info table. should not happens.
                // anyway table will be created
                if (!cScopeInfoClientExists)
                    return false;

                // check schema is valid
                var scopeInfoClientschemaIsValid = await this.IsScopeInfoClientSchemaValidAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                // if columns are not the same, we need to upgrade
                if (!scopeInfoClientschemaIsValid)
                    return true;

                return false;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }


        internal virtual async Task<bool> InternalUpgradeAsync(SyncContext context,
                        DbConnection connection = default, DbTransaction transaction = default,
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
                    await UpgdrateFromNoWhereTo098Async(context, version, scopeInfos, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                else
                    await UpgradeToLastVersionAsync(context, version, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                return true;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }


        private async Task<Version> UpgradeToLastVersionAsync(SyncContext context, Version version,
      DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)

        {
            await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Migrating, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            string message = string.Empty;
            var dbBuilder = this.Provider.GetDatabaseBuilder();
            // scope info client table name (and tmp table name)
            var parsedName = ParserName.Parse(this.Options.ScopeInfoTableName);
            var cScopeInfoTableName = $"{parsedName.Unquoted().Normalized()}";
            var cScopeInfoClientTableName = $"{parsedName.Unquoted().Normalized()}_client";

            // ----------------------------------------------------
            // Step 1: Deprovision all and re provision
            // ----------------------------------------------------
            List<ScopeInfo> cScopeInfos = null;

            // get scope infos
            (context, cScopeInfos) = await this.InternalLoadAllScopeInfosAsync(context,
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

            // Deprovision old triggers & stored procedures
            var provision = SyncProvision.StoredProcedures | SyncProvision.Triggers;

            foreach (var cScopeInfo in cScopeInfos)
            {
                if (cScopeInfo == null || cScopeInfo.Setup == null || cScopeInfo.Setup.Tables == null || cScopeInfo.Setup.Tables.Count <= 0)
                    continue;

                await this.InternalDeprovisionAsync(cScopeInfo, context, provision,
                        runner.Connection, runner.Transaction, runner.CancellationToken, default).ConfigureAwait(false);

                message = $"- Deprovision client old scope {cScopeInfo.Name}.";
                await this.InterceptAsync(new UpgradeProgressArgs(context, message, SyncVersion.Current, runner.Connection, runner.Transaction), runner.Progress).ConfigureAwait(false);
            }

            // ----------------------------------------------------
            // Step 2 : Provision again
            // ----------------------------------------------------
            (context, cScopeInfos) = await this.InternalLoadAllScopeInfosAsync(context,
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

            foreach (var cScopeInfo in cScopeInfos)
            {
                if (cScopeInfo == null || cScopeInfo.Setup == null || cScopeInfo.Setup.Tables == null || cScopeInfo.Setup.Tables.Count <= 0)
                    continue;

                // set correct version
                cScopeInfo.Version = SyncVersion.Current.ToString();

                (context, _) = await InternalProvisionClientAsync(cScopeInfo, cScopeInfo, context, provision, false,
                    runner.Connection, runner.Transaction, runner.CancellationToken, default).ConfigureAwait(false);

                message = $"- Provision client new scope {cScopeInfo.Name}.";
                await this.InterceptAsync(new UpgradeProgressArgs(context, message, SyncVersion.Current, runner.Connection, runner.Transaction), runner.Progress).ConfigureAwait(false);
            }

            await this.InterceptAsync(new UpgradeProgressArgs(context, $"Upgrade client from version {version} to {SyncVersion.Current} done.", SyncVersion.Current, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

            await runner.CommitAsync().ConfigureAwait(false);

            return SyncVersion.Current;
        }



        private async Task<Version> UpgdrateFromNoWhereTo098Async(SyncContext context, Version version, SyncTable scopeInfos,
            DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)

        {
            await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Migrating, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            var newVersion = new Version(0, 9, 8);

            // If we have only one scope without paramaters, we can upgrade it automatically
            bool hasFilters = false;
            foreach (var row in scopeInfos.Rows)
            {
                var setupJson = row["sync_scope_setup"].ToString();

                if (setupJson == null)
                    break;

                JObject setup = JToken.Parse(setupJson) as JObject;

                // Raise an error if we have more than one scope with filters
                if (setup != null && setup.TryGetValue("fils", out var fils))
                {
                    var filters = fils.ToObject<List<SetupFilter>>();
                    hasFilters = filters != null && filters.Count > 0;
                    if (hasFilters)
                    {
                        StringBuilder stringBuilder = new StringBuilder();
                        stringBuilder.AppendLine($"Your version is {version} and you need to manually upgrade your client to be able to use the current verison {SyncVersion.Current}.");
                        stringBuilder.AppendLine($"Your {this.Options.ScopeInfoTableName} table contains setup with filters that need to be migrated manually, as new version needs the parameters values saved in the {this.Options.ScopeInfoTableName} table and they are not present in the {this.Options.ScopeInfoTableName} table version {version}.");
                        stringBuilder.AppendLine($"Please see this discussion on how to migrate to your version to the last one : https://github.com/Mimetis/Dotmim.Sync/discussions/802#discussioncomment-3594681");
                        throw new Exception(stringBuilder.ToString());

                    }
                }
            }

            string message = string.Empty;
            var dbBuilder = this.Provider.GetDatabaseBuilder();
            // ----------------------------------------------------
            // Step 1 : Migrate scope_info and scope_info_client
            // ----------------------------------------------------

            // Migrate scope_info
            var oldScopeInfoTable = await MigrateScopeInfoTableAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);
            // Migrate scope_info_client
            var oldScopeInfoClientTable = await MigrateScopeInfoClientTableAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

            // ----------------------------------------------------
            // Step 2 : Read rows from tmpscope_info & create scope_info & scope_info_client rows
            // ----------------------------------------------------

            Guid? scope_info_client_id = null; // this scope id will be unique and will determined by the first row read.

            // scope info client table name (and tmp table name)
            var parsedName = ParserName.Parse(this.Options.ScopeInfoTableName);
            var cScopeInfoTableName = $"{parsedName.Unquoted().Normalized()}";
            var cScopeInfoClientTableName = $"{parsedName.Unquoted().Normalized()}_client";

            // Create scope_info and scope_info_client for each pre version scope_info lines
            foreach (var scopeInfoRow in oldScopeInfoTable.Rows)
            {
                // Get setup schema and scope name from old scope info table
                var setup = JsonConvert.DeserializeObject<SyncSetup>(scopeInfoRow["sync_scope_setup"].ToString());
                var schema = JsonConvert.DeserializeObject<SyncSet>(scopeInfoRow["sync_scope_schema"].ToString());
                var scopeName = scopeInfoRow["sync_scope_name"].ToString();

                // scope info client id should be unique.
                scope_info_client_id = scope_info_client_id.HasValue ? scope_info_client_id : (Guid)scopeInfoRow["sync_scope_id"];

                // Create new scope_info and scope_info_client
                var cScopeInfo = new ScopeInfo { Name = scopeName, Setup = setup, Schema = schema, Version = SyncVersion.Current.ToString() };

                // Save this scope to new scope info table
                await this.InternalSaveScopeInfoAsync(cScopeInfo, context,
                     runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                // Raise message about migrating current scope
                message = $"- Saved scope_info {scopeName} in the new {cScopeInfoTableName} table version {SyncVersion.Current} with a setup containing {setup.Tables.Count} tables.";
                await this.InterceptAsync(new UpgradeProgressArgs(context, message, newVersion, runner.Connection, runner.Transaction),
                    runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                // Create scope info client
                var cScopeInfoClient = new ScopeInfoClient
                {
                    Id = scope_info_client_id.Value,
                    Name = scopeName,
                    LastSyncDuration = (long)scopeInfoRow["scope_last_sync_duration"],
                    LastSync = (DateTime)scopeInfoRow["scope_last_sync"],
                    Hash = SyncParameters.DefaultScopeHash, // as we see that we don't have any parameters
                    LastServerSyncTimestamp = (long)scopeInfoRow["scope_last_server_sync_timestamp"],
                    LastSyncTimestamp = (long)scopeInfoRow["scope_last_sync_timestamp"],
                };

                await this.InternalSaveScopeInfoClientAsync(cScopeInfoClient, context,
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                message = $"- Saved scope_info_client {scopeName} in new {cScopeInfoClientTableName} table version {SyncVersion.Current} with a LastServerSyncTimestamp fixed to {cScopeInfoClient.LastServerSyncTimestamp}";
                await this.InterceptAsync(new UpgradeProgressArgs(context, message, SyncVersion.Current, runner.Connection, runner.Transaction), runner.Progress).ConfigureAwait(false);
            }


            //----------------------------------------------------
            //Step 3 : Drop tmp tables
            //----------------------------------------------------

            if (oldScopeInfoTable != null)
            {
                await dbBuilder.DropsTableIfExistsAsync(oldScopeInfoTable.TableName, null, runner.Connection, runner.Transaction).ConfigureAwait(false);

                message = $"- Drop temporary {oldScopeInfoTable.GetFullName()} table.";
                await this.InterceptAsync(new UpgradeProgressArgs(context, message, SyncVersion.Current, runner.Connection, runner.Transaction), runner.Progress).ConfigureAwait(false);
            }

            if (oldScopeInfoClientTable != null)
            {
                await dbBuilder.DropsTableIfExistsAsync(oldScopeInfoClientTable.TableName, null, runner.Connection, runner.Transaction).ConfigureAwait(false);

                message = $"- Drop temporary {oldScopeInfoClientTable.GetFullName()} table.";
                await this.InterceptAsync(new UpgradeProgressArgs(context, message, SyncVersion.Current, runner.Connection, runner.Transaction), runner.Progress).ConfigureAwait(false);
            }


            // ----------------------------------------------------
            // Step 4: Deprovision all and re provision
            // ----------------------------------------------------
            List<ScopeInfo> cScopeInfos = null;

            // get scope infos
            (context, cScopeInfos) = await this.InternalLoadAllScopeInfosAsync(context,
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

            // fallback to "try to drop an hypothetical default scope"
            cScopeInfos ??= new List<ScopeInfo>();

            var defaultCScopeInfo = new ScopeInfo
            {
                Name = "DMS Generated",
                Version = SyncVersion.Current.ToString(),
            };

            var defaultSetup = await dbBuilder.GetAllTablesAsync(runner.Connection, runner.Transaction).ConfigureAwait(false);

            // Considering removing tables with "_tracking" at the end
            var tables = defaultSetup.Tables.Where(setupTable => !setupTable.TableName.EndsWith("_tracking") && setupTable.TableName != cScopeInfoTableName && setupTable.TableName != cScopeInfoClientTableName).ToList();
            defaultSetup.Tables.Clear();
            defaultSetup.Tables.AddRange(tables);
            defaultCScopeInfo.Setup = defaultSetup;

            if (defaultCScopeInfo.Setup != null && defaultCScopeInfo.Setup.Tables.Count > 0)
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


            await this.InterceptAsync(new UpgradeProgressArgs(context, $"Upgrade from version {version} to {SyncVersion.Current} done.", newVersion, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

            await runner.CommitAsync().ConfigureAwait(false);

            return newVersion;
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
        /// var (scopeInfos, scopeInfoClients) = await localOrchestrator.ManualUpgradeWithFiltersParameterAsync(entries, progress, evaluationOnly);
        /// </code>
        /// </example>
        /// </summary>
        /// <param name="entries">List of all entries you are using locally (an entry is a scope name + syncparameters with value) you are using to sync to server</param>
        /// <param name="progress">Progress of upgrade</param>
        /// <param name="evaluateOnly">if set to True, the upgrade will not be applied at the end. Usefull to test your upgrade and see if anything is breaking at some point.</param>
        /// <returns>
        /// The new scopeInfos and scopeInfoClients rows from your new upgrade sync config tables
        /// </returns>
        public virtual async Task<(List<ScopeInfo> scopeInfos, List<ScopeInfoClient> scopeInfoClients)> ManualUpgradeWithFiltersParameterAsync(List<ScopeInfoClientUpgrade> entries, IProgress<ProgressArgs> progress = default, bool evaluateOnly = false)
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
                            throw new Exception("Your setup to migrate contains one or more filters. Please use a SyncParameters argument when calling ManualUpgradeWithFiltersParameterAsync() (or SynchronizeAsync()).");

                        if (parameters.Count > 0 && (setupParameters == null || parameters.Count != setupParameters.Count))
                            throw new Exception("You specified a SyncParameters argument, but it seems your setup to migrate does not contains any filters.");

                        if (setupParameters != null)
                        {
                            if (setupParameters.Count != parameters.Count)
                                throw new Exception("Your setup to migrate contains one or more filters. Please use a SyncParameters argument when calling ManualUpgradeWithFiltersParameterAsync() (or SynchronizeAsync()).");

                            foreach (var setupParameter in setupParameters)
                                if (!parameters.Any(p => string.Equals(p.Name, setupParameter, SyncGlobalization.DataSourceStringComparison)))
                                    throw new Exception("Your setup filters contains at least one parameter that is not available from SyncParameters argument.");

                            foreach (var parameter in parameters)
                                if (!setupParameters.Any(n => string.Equals(n, parameter.Name, SyncGlobalization.DataSourceStringComparison)))
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
                    await runner.RollbackAsync("Evaluate only").ConfigureAwait(false);
                else
                    await runner.CommitAsync().ConfigureAwait(false);

                return (cFinalScopeInfos, cFinalScopeInfoClients);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        private async Task<Version> AutoUpgdrateToNewVersionAsync(ScopeInfo scopeInfo, SyncContext context, Version newVersion, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            var message = $"Upgrade to {newVersion}:";

            await this.InterceptAsync(new UpgradeProgressArgs(context, message, newVersion, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
            return newVersion;
        }
    }
}
