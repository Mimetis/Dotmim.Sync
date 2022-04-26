using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public abstract partial class BaseOrchestrator
    {
        internal async Task<bool> InternalProvisionAsync(IScopeInfo scopeInfo, bool overwrite, SyncProvision provision, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            // If schema does not have any table, raise an exception
            if (scopeInfo.Schema == null || scopeInfo.Schema.Tables == null || !scopeInfo.Schema.HasTables)
                throw new MissingTablesException();

            var ctx = this.GetContext(scopeInfo.Name);

            await this.InterceptAsync(new ProvisioningArgs(ctx, provision, scopeInfo.Schema, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            // get Database builder
            var builder = this.Provider.GetDatabaseBuilder();

            // Initialize database if needed
            await builder.EnsureDatabaseAsync(connection, transaction).ConfigureAwait(false);

            // Check if we have tables AND columns
            // If we don't have any columns it's most probably because user called method with the Setup only
            // So far we have only tables names, it's enough to get the schema
            if (scopeInfo.Schema.HasTables && !scopeInfo.Schema.HasColumns)
                scopeInfo.Schema = await this.InternalGetSchemaAsync(scopeInfo.Name, scopeInfo.Setup, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            // Shoudl we create scope
            if (provision.HasFlag(SyncProvision.ClientScope))
            {
                var exists = await this.InternalExistsScopeInfoTableAsync(scopeInfo.Name, DbScopeType.Client, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(scopeInfo.Name, DbScopeType.Client, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            }

            // if scope is not null, so obviously we have create the table before, so no need to test
            if (provision.HasFlag(SyncProvision.ServerScope))
            {
                var exists = await this.InternalExistsScopeInfoTableAsync(scopeInfo.Name, DbScopeType.Server, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(scopeInfo.Name, DbScopeType.Server, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
            }

            if (provision.HasFlag(SyncProvision.ServerHistoryScope))
            {
                var exists = await this.InternalExistsScopeInfoTableAsync(scopeInfo.Name, DbScopeType.ServerHistory, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(scopeInfo.Name, DbScopeType.ServerHistory, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
            }

            // Sorting tables based on dependencies between them
            var schemaTables = scopeInfo.Schema.Tables
                .SortByDependencies(tab => tab.GetRelations()
                    .Select(r => r.GetParentTable()));

            foreach (var schemaTable in schemaTables)
            {
                var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                // Check if we need to create a schema there
                var schemaExists = await InternalExistsSchemaAsync(scopeInfo, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (!schemaExists)
                    await InternalCreateSchemaAsync(scopeInfo, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (provision.HasFlag(SyncProvision.Table))
                {
                    var tableExistst = await this.InternalExistsTableAsync(scopeInfo, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (!tableExistst)
                        await this.InternalCreateTableAsync(scopeInfo, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                }

                if (provision.HasFlag(SyncProvision.TrackingTable))
                {
                    var trackingTableExistst = await this.InternalExistsTrackingTableAsync(scopeInfo, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (!trackingTableExistst)
                        await this.InternalCreateTrackingTableAsync(scopeInfo, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                }

                if (provision.HasFlag(SyncProvision.Triggers))
                    await this.InternalCreateTriggersAsync(scopeInfo, overwrite, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (provision.HasFlag(SyncProvision.StoredProcedures))
                    await this.InternalCreateStoredProceduresAsync(scopeInfo, overwrite, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            }

            //// save scope
            //if (this is LocalOrchestrator)
            //{
            //    if (clientScopeInfo == null)
            //        clientScopeInfo = await this.InternalCreateScopeAsync(scopeInfo.Name, DbScopeType.Client, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            //    var clientScopeInfo = scopeInfo as ScopeInfo;

            //    clientScopeInfo.Schema = schema;
            //    clientScopeInfo.Setup = this.Setup;
            //    clientScopeInfo.Name = this.ScopeName;

            //    await this.InternalSaveScopeAsync(ctx, DbScopeType.Client, clientScopeInfo, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            //    var args = new ProvisionedArgs(ctx, provision, schema, connection);
            //    await this.InterceptAsync(args, progress, cancellationToken).ConfigureAwait(false);
            //    return clientScopeInfo;

            //}
            //else if (this is RemoteOrchestrator)
            //{
            //    var serverScopeInfo = scope as ServerScopeInfo;

            //    if (serverScopeInfo == null)
            //        serverScopeInfo = await this.InternalCreateScopeAsync<ServerScopeInfo>(this.GetContext(), DbScopeType.Client, this.ScopeName, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            //    serverScopeInfo.Schema = schema;
            //    serverScopeInfo.Setup = this.Setup;
            //    serverScopeInfo.Name = this.ScopeName;

            //    await this.InternalSaveScopeAsync(ctx, DbScopeType.Server, serverScopeInfo, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            //    var args = new ProvisionedArgs(ctx, provision, schema, connection);
            //    await this.InterceptAsync(args, progress, cancellationToken).ConfigureAwait(false);
            //    return serverScopeInfo;
            //}

            return true;

        }

        internal async Task<bool> InternalDeprovisionAsync(IScopeInfo scopeInfo, SyncProvision provision,DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            // If schema does not have any table, raise an exception
            if (scopeInfo.Schema == null || scopeInfo.Schema.Tables == null || !scopeInfo.Schema.HasTables)
                throw new MissingTablesException();

            var ctx = this.GetContext(scopeInfo.Name);

            await this.InterceptAsync(new DeprovisioningArgs(ctx, provision, scopeInfo.Schema, connection, transaction), progress, cancellationToken).ConfigureAwait(false);

            // get Database builder
            var builder = this.Provider.GetDatabaseBuilder();

            // Sorting tables based on dependencies between them
            var schemaTables = scopeInfo.Schema.Tables
                .SortByDependencies(tab => tab.GetRelations()
                    .Select(r => r.GetParentTable()));

            // Disable check constraints
            if (this.Options.DisableConstraintsOnApplyChanges)
                foreach (var table in schemaTables.Reverse())
                    await this.InternalDisableConstraintsAsync(scopeInfo, this.GetSyncAdapter(table, scopeInfo), connection, transaction).ConfigureAwait(false);


            // Checking if we have to deprovision tables
            bool hasDeprovisionTableFlag = provision.HasFlag(SyncProvision.Table);

            // Firstly, removing the flag from the provision, because we need to drop everything in correct order, then drop tables in reverse side
            if (hasDeprovisionTableFlag)
                provision ^= SyncProvision.Table;

            foreach (var schemaTable in schemaTables)
            {
                var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                if (provision.HasFlag(SyncProvision.StoredProcedures))
                {
                    await InternalDropStoredProceduresAsync(scopeInfo, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    // Removing cached commands
                    var syncAdapter = this.GetSyncAdapter(schemaTable, scopeInfo);
                    syncAdapter.RemoveCommands();
                }

                if (provision.HasFlag(SyncProvision.Triggers))
                {
                    await InternalDropTriggersAsync(scopeInfo, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                }

                if (provision.HasFlag(SyncProvision.TrackingTable))
                {
                    var exists = await InternalExistsTrackingTableAsync(scopeInfo, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (exists)
                        await this.InternalDropTrackingTableAsync(scopeInfo, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                }
            }

            // Eventually if we have the "Table" flag, then drop the table
            if (hasDeprovisionTableFlag)
            {
                foreach (var schemaTable in schemaTables.Reverse())
                {
                    var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                    var exists = await InternalExistsTableAsync(scopeInfo, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                    if (exists)
                        await this.InternalDropTableAsync(scopeInfo, tableBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                }
            }

            bool hasDeleteClientScopeTable = false;
            bool hasDeleteServerScopeTable = false;
            if (provision.HasFlag(SyncProvision.ClientScope))
            {
                var exists = await this.InternalExistsScopeInfoTableAsync(scopeInfo.Name, DbScopeType.Client, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (exists)
                {
                    await this.InternalDropScopeInfoTableAsync(scopeInfo.Name, DbScopeType.Client, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                    hasDeleteClientScopeTable = true;
                }
            }

            if (provision.HasFlag(SyncProvision.ServerScope))
            {
                var exists = await this.InternalExistsScopeInfoTableAsync(scopeInfo.Name, DbScopeType.Server, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (exists)
                {
                    await this.InternalDropScopeInfoTableAsync(scopeInfo.Name, DbScopeType.Server, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                    hasDeleteServerScopeTable = true;
                }
            }

            if (provision.HasFlag(SyncProvision.ServerHistoryScope))
            {
                var exists = await this.InternalExistsScopeInfoTableAsync(scopeInfo.Name, DbScopeType.ServerHistory, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (exists)
                    await this.InternalDropScopeInfoTableAsync(scopeInfo.Name, DbScopeType.ServerHistory, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
            }

            //// save scope
            //if (this is LocalOrchestrator && !hasDeleteClientScopeTable && scope != null)
            //{
            //    var clientScopeInfo = scope as ScopeInfo;
            //    clientScopeInfo.Schema = null;
            //    clientScopeInfo.Setup = null;

            //    var exists = await this.InternalExistsScopeInfoTableAsync(scopeInfo.Name, DbScopeType.Client, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            //    if (exists)
            //        await this.InternalSaveScopeAsync(scopeInfo, DbScopeType.Client, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
            //}
            //else if (this is RemoteOrchestrator && !hasDeleteServerScopeTable && scope != null)
            //{
            //    var serverScopeInfo = scope as ServerScopeInfo;
            //    serverScopeInfo.Schema = schema;
            //    serverScopeInfo.Setup = setup;

            //    var exists = await this.InternalExistsScopeInfoTableAsync(ctx, DbScopeType.Server, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            //    if (exists)
            //        await this.InternalSaveScopeAsync(ctx, DbScopeType.Server, serverScopeInfo, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
            //}

            var args = new DeprovisionedArgs(ctx, provision, scopeInfo.Schema, connection);
            await this.InterceptAsync(args, progress, cancellationToken).ConfigureAwait(false);

            return true;
        }

    }
}
