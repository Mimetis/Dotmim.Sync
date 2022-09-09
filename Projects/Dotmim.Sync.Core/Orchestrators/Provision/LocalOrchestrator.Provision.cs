using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;


namespace Dotmim.Sync
{
    public partial class LocalOrchestrator : BaseOrchestrator
    {

        /// <summary>
        /// Provision a local datasource (<strong>triggers</strong>, <strong>stored procedures</strong> (if supported), <strong>tracking tables</strong> and <strong>tables</strong> if needed. Create also <strong>scope_info</strong> and <strong>scope_info_client</strong> tables.
        /// <para>
        /// The <paramref name="provision" /> argument specify the objects to provision. See <see cref="SyncProvision" /> enumeration.
        /// </para>
        /// <para>
        /// The <paramref name="sScopeInfo"/> argument contains the schema to apply and should be retrieved from a <c>scope_info</c> table (most of the time from your server datasource)
        /// </para>
        /// <para>
        /// <example>
        /// <code>
        /// var remoteOrchestrator = new RemoteOrchestrator(serverProvider);
        /// var sScopeInfo = await remoteOrchestrator.GetScopeInfoAsync();
        /// var cScopeInfo = await localOrchestrator.ProvisionAsync(sScopeInfo);
        /// </code>
        /// </example>
        /// </para>
        /// </summary>
        /// <param name="sScopeInfo">A <see cref="ScopeInfo "/> instance coming from your server datasource or your client datasource (if exists).</param>
        /// <param name="provision">If you do not specify <c>provision</c>, a default value <c>SyncProvision.Table | SyncProvision.StoredProcedures | SyncProvision.Triggers | SyncProvision.TrackingTable</c> is used.</param>
        /// <param name="overwrite">If specified, all metadatas are generated and overwritten even if they already exists</param>
        /// <param name="connection">optional connection</param>
        /// <param name="transaction">optional transaction</param>
        /// <returns>
        /// A <see cref="ScopeInfo"/> instance, saved locally in the client datasource.
        /// </returns> 
        public async Task<ScopeInfo> ProvisionAsync(ScopeInfo sScopeInfo, SyncProvision provision = default, bool overwrite = true, DbConnection connection = null, DbTransaction transaction = null)
        {
            var context = new SyncContext(Guid.NewGuid(), sScopeInfo.Name);
            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Provisioning, connection, transaction).ConfigureAwait(false);

                ScopeInfo clientScopeInfo;
                (context, clientScopeInfo) = await InternalEnsureScopeInfoAsync(context, 
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                (context, clientScopeInfo) = await InternalProvisionClientAsync(sScopeInfo, clientScopeInfo, context, provision, overwrite, 
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return clientScopeInfo;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }


        /// <summary>
        /// Deprovision your client datasource.
        /// <example>
        /// Deprovision a client database:
        /// <code>
        /// var localOrchestrator = new LocalOrchestrator(clientProvider);
        /// await localOrchestrator.DeprovisionAsync();
        /// </code>
        /// </example>
        /// </summary>
        /// <remarks>
        /// By default, <strong>DMS</strong> will never deprovision a table, if not explicitly set with the <c>provision</c> argument. <strong>scope_info</strong> and <strong>scope_info_client</strong> tables
        /// are not deprovisioned by default to preserve existing configurations
        /// </remarks>
        /// <param name="provision">If you do not specify <c>provision</c>, a default value <c>SyncProvision.StoredProcedures | SyncProvision.Triggers</c> is used.</param>
        /// <param name="connection">optional connection</param>
        /// <param name="transaction">optional transaction</param>
        /// <returns></returns>
        public Task<bool> DeprovisionAsync(SyncProvision provision = default, DbConnection connection = null, DbTransaction transaction = null) 
            => DeprovisionAsync(SyncOptions.DefaultScopeName, provision, connection, transaction);


        /// <inheritdoc cref="DeprovisionAsync(SyncProvision, DbConnection, DbTransaction)" />
        public virtual async Task<bool> DeprovisionAsync(string scopeName, SyncProvision provision = default, DbConnection connection = null, DbTransaction transaction = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);
            try
            {
                if (provision == default)
                    provision = SyncProvision.StoredProcedures | SyncProvision.Triggers;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Deprovisioning, connection, transaction).ConfigureAwait(false);

                // get client scope
                ScopeInfo cScopeInfo = null;
                bool exists;
                (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfo, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                if (exists)
                    (context, cScopeInfo) = await this.InternalLoadScopeInfoAsync(context, 
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                bool isDeprovisioned;
                (context, isDeprovisioned) = await InternalDeprovisionAsync(cScopeInfo, context, provision, 
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return isDeprovisioned;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <inheritdoc cref="DeprovisionAsync(string, SyncSetup, SyncProvision, DbConnection, DbTransaction)" />
        public virtual Task<bool> DeprovisionAsync(SyncSetup setup, SyncProvision provision = default, DbConnection connection = null, DbTransaction transaction = null) 
            => DeprovisionAsync(SyncOptions.DefaultScopeName, setup, provision, connection, transaction);

        /// <summary>
        /// Deprovision your client datasource.
        /// <example>
        /// Deprovision a client database:
        /// <code>
        /// var localOrchestrator = new LocalOrchestrator(clientProvider);
        /// var setup = new SyncSetup("ProductCategory", "Product");
        /// await localOrchestrator.DeprovisionAsync(setup);
        /// </code>
        /// </example>
        /// </summary>
        /// <remarks>
        /// By default, <strong>DMS</strong> will never deprovision a table, if not explicitly set with the <c>provision</c> argument. <strong>scope_info</strong> and <strong>scope_info_client</strong> tables
        /// are not deprovisioned by default to preserve existing configurations
        /// </remarks>
        /// <param name="scopeName">scopeName. If not defined, SyncOptions.DefaultScopeName is used</param>
        /// <param name="setup">Setup containing tables to deprovision</param>
        /// <param name="provision">If you do not specify <c>provision</c>, a default value <c>SyncProvision.StoredProcedures | SyncProvision.Triggers</c> is used.</param>
        /// <param name="connection">Optional Connection</param>
        /// <param name="transaction">Optional Transaction</param>
        /// <returns></returns>
        public virtual async Task<bool> DeprovisionAsync(string scopeName, SyncSetup setup, SyncProvision provision = default, DbConnection connection = null, DbTransaction transaction = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);
            try
            {
                if (provision == default)
                    provision = SyncProvision.ScopeInfo | SyncProvision.ScopeInfoClient | SyncProvision.StoredProcedures | SyncProvision.Triggers | SyncProvision.TrackingTable;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Deprovisioning, connection, transaction).ConfigureAwait(false);

                // Creating a fake scope info
                var cScopeInfo = this.InternalCreateScopeInfo(scopeName);
                cScopeInfo.Setup = setup;
                cScopeInfo.Schema = new SyncSet(setup);

                bool isDeprovisioned;
                (context, isDeprovisioned) = await InternalDeprovisionAsync(cScopeInfo, context, provision,
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return isDeprovisioned;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }

        }

        /// <summary>
        /// Drop everything related to DMS. Tracking tables, triggers, tracking tables, sync_scope and sync_scope_client tables
        /// <example>
        /// Deprovision a client database:
        /// <code>
        /// var localOrchestrator = new LocalOrchestrator(clientProvider);
        /// await localOrchestrator.DropAllAsync();
        /// </code>
        /// </example>
        /// </summary>
        public virtual async Task DropAllAsync(DbConnection connection = null, DbTransaction transaction = null)
        {
            var context = new SyncContext(Guid.NewGuid(), SyncOptions.DefaultScopeName);
            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Deprovisioning, connection, transaction).ConfigureAwait(false);

                // get client scope and create tables / row if needed

                List<ScopeInfo> cScopeInfos = null;
                bool exists;
                (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfo,
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                if (exists)
                    (context, cScopeInfos) = await this.InternalLoadAllScopeInfosAsync(context, runner.Connection, 
                        runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                // fallback to "try to drop an hypothetical default scope"
                if (cScopeInfos == null)
                    cScopeInfos = new List<ScopeInfo>();

                // try to get some filters
                var existingFilters = cScopeInfos?.SelectMany(si => si.Setup.Filters).ToList();

                var defaultClientScopeInfo = this.InternalCreateScopeInfo(SyncOptions.DefaultScopeName);
                SyncSetup setup;
                (context, setup) = await this.InternalGetAllTablesAsync(context, 
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                // Considering removing tables with "_tracking" at the end
                var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);
                var scopeInfoTableName = scopeBuilder.ScopeInfoTableName.Unquoted().ToString();
                var tables = setup.Tables.Where(setupTable => !setupTable.TableName.EndsWith("_tracking") && setupTable.TableName != scopeInfoTableName).ToList();
                setup.Tables.Clear();
                setup.Tables.AddRange(tables);
                defaultClientScopeInfo.Setup = setup;

                // add any random filters, to try to delete them
                if (existingFilters != null && existingFilters.Count > 0)
                {
                    var filters = new SetupFilters();
                    foreach (var filter in existingFilters)
                        filters.Add(filter);

                    defaultClientScopeInfo.Setup.Filters = filters;
                }

                cScopeInfos.Add(defaultClientScopeInfo);

                var provision = SyncProvision.StoredProcedures | SyncProvision.Triggers | SyncProvision.TrackingTable | SyncProvision.ScopeInfo | SyncProvision.ScopeInfoClient;

                foreach (var clientScopeInfo in cScopeInfos)
                {
                    if (clientScopeInfo == null || clientScopeInfo.Setup == null || clientScopeInfo.Setup.Tables == null || clientScopeInfo.Setup.Tables.Count <= 0)
                        continue;

                    (context, _) = await InternalDeprovisionAsync(clientScopeInfo, context, provision, 
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);
                }

                await runner.CommitAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Provision the local database based on the scope info parameter.
        /// Scope info parameter should contains Schema and Setup properties
        /// </summary>
        internal virtual async Task<(SyncContext context, ScopeInfo cScopeInfo)>
                    InternalProvisionClientAsync(ScopeInfo serverScopeInfo, ScopeInfo clientScopeInfo, SyncContext context, SyncProvision provision = default, bool overwrite = true, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                if (serverScopeInfo.Schema == null)
                    throw new Exception($"No Schema in your server scope info {serverScopeInfo.Name}");

                if (serverScopeInfo.Schema == null)
                    throw new Exception($"No Setup in your server scope info {serverScopeInfo.Name}");

                await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Provisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                // 2) Provision
                if (provision == SyncProvision.NotSet)
                    provision = SyncProvision.Table | SyncProvision.StoredProcedures | SyncProvision.Triggers | SyncProvision.TrackingTable;

                (context, _) = await this.InternalProvisionAsync(serverScopeInfo, context, overwrite, provision, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                // set client scope setup and schema
                clientScopeInfo.Setup = serverScopeInfo.Setup;
                clientScopeInfo.Schema = serverScopeInfo.Schema;

                // Write scopes locally
                (context, clientScopeInfo) = await this.InternalSaveScopeInfoAsync(clientScopeInfo, context, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return (context, clientScopeInfo);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

    }
}
