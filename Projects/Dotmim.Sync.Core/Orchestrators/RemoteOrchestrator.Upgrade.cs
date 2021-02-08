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
        /// Check if we need to upgrade the Database Structure
        /// </summary>
        public virtual Task<bool> NeedsToUpgradeAsync(DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        => RunInTransactionAsync(SyncStage.Provisioning, async (ctx, connection, transaction) =>
        {
            if (this.Setup == null)
                return false;

            var builder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            var scopes = await this.InternalGetAllScopesAsync<ServerScopeInfo>(ctx, DbScopeType.Server, this.ScopeName, builder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            if (scopes == null || scopes.Count <= 0)
                return false;

            return InternalNeedsToUpgrade(ctx, scopes);

        }, connection, transaction, cancellationToken);

        internal virtual bool InternalNeedsToUpgrade(SyncContext context, List<ServerScopeInfo> serverScopeInfos)
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

        /// <summary>
        /// Upgrade the database structure to reach the last DMS version
        /// </summary>
        public virtual Task<bool> UpgradeAsync(DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        => RunInTransactionAsync(SyncStage.Provisioning, async (ctx, connection, transaction) =>
        {
            if (this.Setup == null)
                return false;

            // get Database builder
            var dbBuilder = this.Provider.GetDatabaseBuilder();

            // Initialize database if needed
            await dbBuilder.EnsureDatabaseAsync(connection, transaction).ConfigureAwait(false);

            // Get schema
            var schema = await this.InternalGetSchemaAsync(ctx, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            // If schema does not have any table, raise an exception
            if (schema == null || schema.Tables == null || !schema.HasTables)
                throw new MissingTablesException();

            var builder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);

            var serverScopeInfos = await this.InternalGetAllScopesAsync<ServerScopeInfo>(ctx, DbScopeType.Server, this.ScopeName, builder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            if (serverScopeInfos == null || serverScopeInfos.Count <= 0)
                throw new MissingServerScopeInfoException();

            return await this.InternalUpgradeAsync(ctx, schema, serverScopeInfos, builder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

        }, connection, transaction, cancellationToken);


        internal virtual async Task<bool> InternalUpgradeAsync(SyncContext context, SyncSet schema, List<ServerScopeInfo> serverScopeInfos, DbScopeBuilder builder, DbConnection connection, DbTransaction transaction,
                        CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            var version = SyncVersion.Current;

            // get the smallest version of all scope in the scope info server tables
            foreach (var serverScopeInfo in serverScopeInfos)
            {
                var tmpVersion = SyncVersion.EnsureVersion(serverScopeInfo.Version);

                if (tmpVersion < version)
                    version = tmpVersion;
            }

            // beta version
            if (version.Major == 0)
            {
                // Migrate from 0.5.x to 0.6.0
                if (version.Minor <= 5)
                {
                    version = await UpgdrateTo600Async(context, schema, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                }

                // Migrate from 0.6.0 to 0.6.1
                if (version.Minor <= 6 && version.Build <= 0)
                {
                    version = await UpgdrateTo601Async(context, schema, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                }

            }

            foreach (var serverScopeInfo in serverScopeInfos)
            {
                var oldVersion = SyncVersion.EnsureVersion(serverScopeInfo.Version);
                if (oldVersion != version)
                {
                    serverScopeInfo.Version = version.ToString();
                    await this.InternalSaveScopeAsync(context, DbScopeType.Server, serverScopeInfo, builder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                }
            }

            return version == SyncVersion.Current;

        }

        private Task<Version> UpgdrateTo600Async(SyncContext context, SyncSet schema, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            var newVersion = new Version(0, 6, 0);

            return Task.FromResult(newVersion);
        }

        private async Task<Version> UpgdrateTo601Async(SyncContext context, SyncSet schema, DbConnection connection, DbTransaction transaction,
                        CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            // Sorting tables based on dependencies between them
            var schemaTables = schema.Tables
                .SortByDependencies(tab => tab.GetRelations()
                    .Select(r => r.GetParentTable()));

            foreach (var schemaTable in schemaTables)
            {
                var tableBuilder = this.GetTableBuilder(schemaTable, this.Setup);


                // Upgrade Select Initial Changes
                var exists = await InternalExistsStoredProcedureAsync(context, tableBuilder, DbStoredProcedureType.SelectInitializedChanges, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                if (exists)
                    await InternalDropStoredProcedureAsync(context, tableBuilder, DbStoredProcedureType.SelectInitializedChanges, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                await InternalCreateStoredProcedureAsync(context, tableBuilder, DbStoredProcedureType.SelectInitializedChanges, connection, transaction, cancellationToken, progress).ConfigureAwait(false);


                // Upgrade Select Initial Changes With Filter
                if (tableBuilder.TableDescription.GetFilter() != null)
                {
                    var existsWithFilter = await InternalExistsStoredProcedureAsync(context, tableBuilder, DbStoredProcedureType.SelectInitializedChangesWithFilters, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                    if (existsWithFilter)
                        await InternalDropStoredProcedureAsync(context, tableBuilder, DbStoredProcedureType.SelectInitializedChangesWithFilters, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                    await InternalCreateStoredProcedureAsync(context, tableBuilder, DbStoredProcedureType.SelectInitializedChangesWithFilters, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                }

            }

            return new Version(0, 6, 1);
        }
    }
}
