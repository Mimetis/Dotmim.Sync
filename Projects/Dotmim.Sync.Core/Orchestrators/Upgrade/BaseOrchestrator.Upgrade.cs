using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using System;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Contains the logic to upgrade the database schema to the last version.
    /// </summary>
    public partial class BaseOrchestrator
    {
        /// <summary>
        /// Returns if the scope info schema is valid.
        /// </summary>
        internal async Task<bool> IsScopeInfoSchemaValidAsync(SyncContext context, DbConnection connection = default, DbTransaction transaction = default,
                        IProgress<ProgressArgs> progress = default, CancellationToken cancellationToken = default)
        {
            using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.Migrating,
                connection, transaction, progress, cancellationToken).ConfigureAwait(false);
            await using (runner.ConfigureAwait(false))
            {
                var scopeInfoTableName = this.Provider.GetParsers(new SyncTable(this.Options.ScopeInfoTableName), new SyncSetup()).TableName;
                var tableName = scopeInfoTableName.Unquoted().Normalized().ToString();
                var tableBuilder = this.GetTableBuilder(new SyncTable(tableName), new ScopeInfo { Setup = new SyncSetup() });

                // check columns
                var columns = (await tableBuilder.GetColumnsAsync(runner.Connection, runner.Transaction).ConfigureAwait(false)).ToList();

                if (columns.Count != 6)
                    return false;

                if (columns[0].ColumnName != "sync_scope_name")
                    return false;
                if (columns[1].ColumnName != "sync_scope_schema")
                    return false;
                if (columns[2].ColumnName != "sync_scope_setup")
                    return false;
                if (columns[3].ColumnName != "sync_scope_version")
                    return false;
                if (columns[4].ColumnName != "sync_scope_last_clean_timestamp")
                    return false;
                if (columns[5].ColumnName != "sync_scope_properties")
                    return false;

                return true;
            }
        }

        /// <summary>
        /// Returns if the scope info client schema is valid.
        /// </summary>
        internal async Task<bool> IsScopeInfoClientSchemaValidAsync(SyncContext context, DbConnection connection = default, DbTransaction transaction = default,
                IProgress<ProgressArgs> progress = default, CancellationToken cancellationToken = default)
        {
            using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.Migrating,
            connection, transaction, progress, cancellationToken).ConfigureAwait(false);

            await using (runner.ConfigureAwait(false))
            {
                var scopeInfoTableName = this.Provider.GetParsers(new SyncTable(this.Options.ScopeInfoTableName), new SyncSetup()).TableName;
                var tableName = $"{scopeInfoTableName.Unquoted().Normalized()}_client";
                var tableBuilder = this.GetTableBuilder(new SyncTable(tableName), new ScopeInfo { Setup = new SyncSetup() });

                // check columns
                var columns = (await tableBuilder.GetColumnsAsync(runner.Connection, runner.Transaction).ConfigureAwait(false)).ToList();

                if (columns.Count != 10)
                    return false;

                if (columns[0].ColumnName != "sync_scope_id")
                    return false;
                if (columns[1].ColumnName != "sync_scope_name")
                    return false;
                if (columns[2].ColumnName != "sync_scope_hash")
                    return false;
                if (columns[3].ColumnName != "sync_scope_parameters")
                    return false;
                if (columns[4].ColumnName != "scope_last_sync_timestamp")
                    return false;
                if (columns[5].ColumnName != "scope_last_server_sync_timestamp")
                    return false;
                if (columns[6].ColumnName != "scope_last_sync_duration")
                    return false;
                if (columns[7].ColumnName != "scope_last_sync")
                    return false;
                if (columns[8].ColumnName != "sync_scope_errors")
                    return false;
                if (columns[9].ColumnName != "sync_scope_properties")
                    return false;

                return true;
            }
        }

        /// <summary>
        /// Migrate the scope info client table to the new version.
        /// </summary>
        internal virtual async Task<SyncTable> MigrateScopeInfoClientTableAsync(SyncContext context, DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {

            using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Migrating, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
            await using (runner.ConfigureAwait(false))
            {
                var dbBuilder = this.Provider.GetDatabaseBuilder();

                // scope info client table name (and tmp table name)
                var parsedName = ParserName.Parse(this.Options.ScopeInfoTableName);
                var cScopeInfoClientTableName = $"{parsedName.Unquoted().Normalized()}_client";
                var tmpCScopeInfoClientTableName = $"tmp{cScopeInfoClientTableName}";
                var message = string.Empty;

                // ----------------------------------------------------
                // Step 1 : Renaming scope_info_client to tmpscope_info_client
                // ----------------------------------------------------
                var tmpScopeInfoClientExists = await dbBuilder.ExistsTableAsync(tmpCScopeInfoClientTableName, null,
                    runner.Connection, runner.Transaction).ConfigureAwait(false);

                bool existsCScopeInfoClient;
                (context, existsCScopeInfoClient) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfoClient,
                    runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                if (!tmpScopeInfoClientExists && existsCScopeInfoClient)
                {
                    await dbBuilder.RenameTableAsync(cScopeInfoClientTableName, null, tmpCScopeInfoClientTableName, null,
                        runner.Connection, runner.Transaction).ConfigureAwait(false);

                    message = $"- Temporary renamed {cScopeInfoClientTableName} to {tmpCScopeInfoClientTableName}.";
                    await this.InterceptAsync(new UpgradeProgressArgs(context, message, SyncVersion.Current, runner.Connection, runner.Transaction), runner.Progress, cancellationToken).ConfigureAwait(false);
                }

                // ----------------------------------------------------
                // Step 3 : Create scope_info_client
                // ----------------------------------------------------
                (context, existsCScopeInfoClient) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfoClient,
                    runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                if (!existsCScopeInfoClient)
                {
                    (context, _) = await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.ScopeInfoClient,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                }

                message = $"- Created {cScopeInfoClientTableName} table.";
                await this.InterceptAsync(new UpgradeProgressArgs(context, message, SyncVersion.Current, runner.Connection, runner.Transaction), runner.Progress, cancellationToken).ConfigureAwait(false);

                tmpScopeInfoClientExists = await dbBuilder.ExistsTableAsync(tmpCScopeInfoClientTableName, null,
                    runner.Connection, runner.Transaction).ConfigureAwait(false);

                SyncTable table = null;

                if (tmpScopeInfoClientExists)
                    table = await dbBuilder.GetTableAsync(tmpCScopeInfoClientTableName, null, runner.Connection, runner.Transaction).ConfigureAwait(false);

                return table;
            }
        }

        /// <summary>
        /// Migrate the scope info table to the new version.
        /// </summary>
        internal virtual async Task<SyncTable> MigrateScopeInfoTableAsync(SyncContext context, DbConnection connection, DbTransaction transaction,
                        IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Migrating, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
            await using (runner.ConfigureAwait(false))
            {
                var dbBuilder = this.Provider.GetDatabaseBuilder();

                // scope info table name (and tmp table name)
                var parsedName = ParserName.Parse(this.Options.ScopeInfoTableName);
                var cScopeInfoTableName = $"{parsedName.Unquoted().Normalized()}";
                var cScopeInfoClientTableName = $"{parsedName.Unquoted().Normalized()}_client";
                var tmpCScopeInfoTableName = $"tmp{cScopeInfoTableName}";
                var message = string.Empty;

                // Initialize database if needed
                await dbBuilder.EnsureDatabaseAsync(runner.Connection, runner.Transaction).ConfigureAwait(false);

                // ----------------------------------------------------
                // Step 1 : Renaming scope_info to tmpscope_info
                // ----------------------------------------------------
                var tmpScopeInfoExists = await dbBuilder.ExistsTableAsync(tmpCScopeInfoTableName, null,
                    runner.Connection, runner.Transaction).ConfigureAwait(false);

                var existsCScopeInfo = await dbBuilder.ExistsTableAsync(cScopeInfoTableName, null,
                    runner.Connection, runner.Transaction).ConfigureAwait(false);

                if (!tmpScopeInfoExists && existsCScopeInfo)
                {
                    await dbBuilder.RenameTableAsync(cScopeInfoTableName, null, tmpCScopeInfoTableName, null,
                        runner.Connection, runner.Transaction).ConfigureAwait(false);

                    message = $"- Temporary renamed {cScopeInfoTableName} to {tmpCScopeInfoTableName}.";
                    await this.InterceptAsync(new UpgradeProgressArgs(context, message, SyncVersion.Current, runner.Connection, runner.Transaction), runner.Progress, cancellationToken).ConfigureAwait(false);
                }

                // ----------------------------------------------------
                // Step 2 : Create scope_info
                // ----------------------------------------------------
                existsCScopeInfo = await dbBuilder.ExistsTableAsync(cScopeInfoTableName, null,
                    runner.Connection, runner.Transaction).ConfigureAwait(false);

                if (!existsCScopeInfo)
                {
                    (context, _) = await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.ScopeInfo,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                }

                message = $"- Created new version of {cScopeInfoTableName} table.";
                await this.InterceptAsync(new UpgradeProgressArgs(context, message, SyncVersion.Current, runner.Connection, runner.Transaction), runner.Progress, cancellationToken).ConfigureAwait(false);

                var oldScopeInfotable = await dbBuilder.GetTableAsync(tmpCScopeInfoTableName, null, runner.Connection, runner.Transaction).ConfigureAwait(false);

                return oldScopeInfotable;
            }
        }

        /// <summary>
        /// Auto upgrade the database schema to the last version.
        /// </summary>
        internal virtual async Task<Version> UpgradeAutoToLastVersion(SyncContext context, Version version, SyncTable scopeInfos,
                DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Migrating, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
            await using (runner.ConfigureAwait(false))
            {
                var newVersion = SyncVersion.Current;

                // Create scope_info and scope_info_client for each pre version scope_info lines
                foreach (var scopeInfoRow in scopeInfos.Rows)
                {
                    // Get setup schema and scope name from old scope info table
                    var scopeName = scopeInfoRow["sync_scope_name"] as string;
                    var schema = Serializer.Deserialize<SyncSet>(scopeInfoRow["sync_scope_schema"].ToString());
                    var setup = Serializer.Deserialize<SyncSetup>(scopeInfoRow["sync_scope_setup"].ToString());
                    var lastCleanUpTimestamp = scopeInfoRow["sync_scope_last_clean_timestamp"] != null && scopeInfoRow["sync_scope_last_clean_timestamp"] != DBNull.Value ? (long?)scopeInfoRow["sync_scope_last_clean_timestamp"] : null;
                    var scopeProperties = scopeInfoRow["sync_scope_properties"] as string;

                    // Create new scope_info and scope_info_client
                    var cScopeInfo = new ScopeInfo
                    {
                        Name = scopeName,
                        Setup = setup,
                        Schema = schema,
                        LastCleanupTimestamp = lastCleanUpTimestamp,
                        Properties = scopeProperties,
                        Version = SyncVersion.Current.ToString(),
                    };

                    // Save this scope to new scope info table
                    await this.InternalSaveScopeInfoAsync(cScopeInfo, context,
                         runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    // Raise message about migrating current scope
                    var message = $"- Saved scope_info {scopeName} with version {SyncVersion.Current} with a setup containing {setup.Tables.Count} tables.";
                    await this.InterceptAsync(
                        new UpgradeProgressArgs(context, message, newVersion, runner.Connection, runner.Transaction),
                        runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                }

                await this.InterceptAsync(new UpgradeProgressArgs(context, $"Upgrade from version {version} to {SyncVersion.Current} done.", newVersion, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return newVersion;
            }
        }
    }
}