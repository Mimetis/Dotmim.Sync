using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Contains all methods related to provisioning a local database.
    /// </summary>
    public partial class LocalOrchestrator : BaseOrchestrator
    {

        /// <summary>
        /// Provision a local datasource (<strong>triggers</strong>, <strong>stored procedures</strong> (if supported), <strong>tracking tables</strong> and <strong>tables</strong> if needed. Create also <strong>scope_info</strong> and <strong>scope_info_client</strong> tables.
        /// <para>
        /// The <paramref name="provision" /> argument specify the objects to provision. See <see cref="SyncProvision" /> enumeration.
        /// </para>
        /// <para>
        /// The <paramref name="sScopeInfo"/> argument contains the schema to apply and should be retrieved from a <c>scope_info</c> table (most of the time from your server datasource).
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
        /// <param name="overwrite">If specified, all metadatas are generated and overwritten even if they already exists.</param>
        /// <param name="connection">optional connection.</param>
        /// <param name="transaction">optional transaction.</param>
        /// <param name="progress">optional IProgress of ProgressArgs token.</param>
        /// <param name="cancellationToken">optional cancellation token.</param>
        /// <returns>
        /// A <see cref="ScopeInfo"/> instance, saved locally in the client datasource.
        /// </returns>
        public async Task<ScopeInfo> ProvisionAsync(ScopeInfo sScopeInfo, SyncProvision provision = default, bool overwrite = true, DbConnection connection = null, DbTransaction transaction = null, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
        {
            Guard.ThrowIfNull(sScopeInfo);

            var context = new SyncContext(Guid.NewGuid(), sScopeInfo.Name);
            try
            {
                using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Provisioning, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    ScopeInfo clientScopeInfo;
                    (context, clientScopeInfo) = await this.InternalEnsureScopeInfoAsync(
                        context,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    (context, clientScopeInfo) = await this.InternalProvisionClientAsync(sScopeInfo, clientScopeInfo, context, provision, overwrite,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    await runner.CommitAsync().ConfigureAwait(false);

                    return clientScopeInfo;
                }
            }
            catch (Exception ex)
            {
                string message = null;

                message += $"Provision:{provision}.";
                message += $"Overwrite:{overwrite}.";

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Deprovision your client datasource.
        /// <example>
        /// <code>
        /// var localOrchestrator = new LocalOrchestrator(clientProvider);
        /// await localOrchestrator.DeprovisionAsync();
        /// </code>
        /// </example>
        /// </summary>
        /// <remarks>
        /// By default, <strong>DMS</strong> will never deprovision a table, if not explicitly set with the <c>provision</c> argument. <strong>scope_info</strong> and <strong>scope_info_client</strong> tables
        /// are not deprovisioned by default to preserve existing configurations.
        /// </remarks>
        /// <param name="provision">If you do not specify <c>provision</c>, a default value <c>SyncProvision.StoredProcedures | SyncProvision.Triggers</c> is used.</param>
        /// <param name="connection">optional connection.</param>
        /// <param name="transaction">optional transaction.</param>
        /// <param name="progress">option IProgress{ProgressArgs}.</param>
        /// <param name="cancellationToken">optional cancellation token.</param>
        public Task<bool> DeprovisionAsync(SyncProvision provision = default, DbConnection connection = null, DbTransaction transaction = null, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
            => this.DeprovisionAsync(SyncOptions.DefaultScopeName, provision, connection, transaction, progress, cancellationToken);

        /// <inheritdoc cref="DeprovisionAsync(SyncProvision, DbConnection, DbTransaction, IProgress{ProgressArgs}, CancellationToken)" />
        public virtual async Task<bool> DeprovisionAsync(string scopeName, SyncProvision provision = default,
            DbConnection connection = null, DbTransaction transaction = null, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);
            try
            {
                if (provision == default)
                    provision = SyncProvision.StoredProcedures | SyncProvision.Triggers;

                using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Deprovisioning, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    // get client scope
                    ScopeInfo cScopeInfo = null;
                    bool exists;
                    (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfo, runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    if (exists)
                    {
                        (context, cScopeInfo) = await this.InternalLoadScopeInfoAsync(
                            context,
                            runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                    }

                    bool isDeprovisioned;
                    (context, isDeprovisioned) = await this.InternalDeprovisionAsync(cScopeInfo, context, provision,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    await runner.CommitAsync().ConfigureAwait(false);

                    return isDeprovisioned;
                }
            }
            catch (Exception ex)
            {
                string message = null;

                message += $"Provision:{provision}.";

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <inheritdoc cref="DeprovisionAsync(string, SyncSetup, SyncProvision, DbConnection, DbTransaction, IProgress{ProgressArgs}, CancellationToken)" />
        public virtual Task<bool> DeprovisionAsync(SyncSetup setup, SyncProvision provision = default,
            DbConnection connection = null, DbTransaction transaction = null, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
            => this.DeprovisionAsync(SyncOptions.DefaultScopeName, setup, provision, connection, transaction, progress, cancellationToken);

        /// <summary>
        /// Deprovision your client datasource.
        /// <example>
        /// <code>
        /// var localOrchestrator = new LocalOrchestrator(clientProvider);
        /// var setup = new SyncSetup("ProductCategory", "Product");
        /// await localOrchestrator.DeprovisionAsync(setup);
        /// </code>
        /// </example>
        /// </summary>
        /// <remarks>
        /// By default, <strong>DMS</strong> will never deprovision a table, if not explicitly set with the <c>provision</c> argument. <strong>scope_info</strong> and <strong>scope_info_client</strong> tables
        /// are not deprovisioned by default to preserve existing configurations.
        /// </remarks>
        /// <param name="scopeName">scopeName. If not defined, SyncOptions.DefaultScopeName is used.</param>
        /// <param name="setup">Setup containing tables to deprovision.</param>
        /// <param name="provision">If you do not specify <c>provision</c>, a default value <c>SyncProvision.StoredProcedures | SyncProvision.Triggers</c> is used.</param>
        /// <param name="connection">Optional Connection.</param>
        /// <param name="transaction">Optional Transaction.</param>
        /// <param name="progress">option IProgress{ProgressArgs}.</param>
        /// <param name="cancellationToken">optional cancellation token.</param>
        public virtual async Task<bool> DeprovisionAsync(string scopeName, SyncSetup setup, SyncProvision provision = default,
            DbConnection connection = null, DbTransaction transaction = null, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);
            try
            {
                if (provision == default)
                    provision = SyncProvision.ScopeInfo | SyncProvision.ScopeInfoClient | SyncProvision.StoredProcedures | SyncProvision.Triggers | SyncProvision.TrackingTable;

                using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Deprovisioning, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    // Creating a fake scope info
                    var cScopeInfo = InternalCreateScopeInfo(scopeName);
                    cScopeInfo.Setup = setup;
                    cScopeInfo.Schema = new SyncSet(setup);

                    bool isDeprovisioned;
                    (context, isDeprovisioned) = await this.InternalDeprovisionAsync(cScopeInfo, context, provision,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    await runner.CommitAsync().ConfigureAwait(false);

                    return isDeprovisioned;
                }
            }
            catch (Exception ex)
            {
                string message = null;

                message += $"Provision:{provision}.";

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <summary>
        /// Drop everything related to DMS. Tracking tables, triggers, tracking tables, sync_scope and sync_scope_client tables.
        /// <example>
        /// Deprovision a client database:
        /// <code>
        /// var localOrchestrator = new LocalOrchestrator(clientProvider);
        /// await localOrchestrator.DropAllAsync();
        /// </code>
        /// </example>
        /// </summary>
        public virtual async Task DropAllAsync(bool dropTables = false, DbConnection connection = null, DbTransaction transaction = null, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
        {
            var context = new SyncContext(Guid.NewGuid(), SyncOptions.DefaultScopeName);
            try
            {
                using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Deprovisioning, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    // get client scope and create tables / row if needed
                    List<ScopeInfo> cScopeInfos = null;
                    bool exists;
                    (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfo,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    if (exists)
                    {

                        (context, cScopeInfos) = await this.InternalLoadAllScopeInfosAsync(context, runner.Connection,
                            runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                    }

                    // fallback to "try to drop an hypothetical default scope"
                    cScopeInfos ??= [];

                    // try to get some filters
                    var existingFilters = cScopeInfos.SelectMany(si => si.Setup == null ? [] : si.Setup.Filters).ToList();

                    var defaultClientScopeInfo = InternalCreateScopeInfo(SyncOptions.DefaultScopeName);
                    SyncSetup setup;
                    (context, setup) = await this.InternalGetAllTablesAsync(
                        context, runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    // Considering removing tables with "_tracking" at the end
                    var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);
                    var scopeInfoTableName = scopeBuilder.ScopeInfoTableName.Unquoted().ToString();
                    var scopeInfoClientTableName = $"{scopeBuilder.ScopeInfoTableName.Unquoted()}_client";

                    var tables = setup.Tables.Where(setupTable => !setupTable.TableName.EndsWith("_tracking", SyncGlobalization.DataSourceStringComparison) && setupTable.TableName != scopeInfoTableName && setupTable.TableName != scopeInfoClientTableName).ToList();
                    setup.Tables.Clear();
                    setup.Tables.AddRange(tables);
                    defaultClientScopeInfo.Setup = setup;

                    if (defaultClientScopeInfo.Setup != null && defaultClientScopeInfo.Setup.Tables.Count > 0)
                    {
                        var (_, defaultSchema) = await this.InternalGetSchemaAsync(context, defaultClientScopeInfo.Setup,
                            runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                        defaultClientScopeInfo.Schema = defaultSchema;

                        // add any random filters, to try to delete them
                        if (existingFilters != null && existingFilters.Count > 0)
                        {
                            var filters = new SetupFilters();
                            foreach (var filter in existingFilters)
                                filters.Add(filter);

                            defaultClientScopeInfo.Setup.Filters = filters;
                        }

                        cScopeInfos.Add(defaultClientScopeInfo);
                    }

                    var provision = SyncProvision.StoredProcedures | SyncProvision.Triggers | SyncProvision.TrackingTable | SyncProvision.ScopeInfo | SyncProvision.ScopeInfoClient;

                    if (dropTables)
                    {
                        var dropAllArgs = new DropAllArgs(context, connection, transaction);
                        await this.InterceptAsync(dropAllArgs, progress, cancellationToken).ConfigureAwait(false);

                        var confirm = dropAllArgs.Confirm();

                        if (confirm)
                            provision |= SyncProvision.Table;
                    }

                    foreach (var clientScopeInfo in cScopeInfos)
                    {
                        if (clientScopeInfo == null || clientScopeInfo.Setup == null || clientScopeInfo.Setup.Tables == null || clientScopeInfo.Setup.Tables.Count <= 0)
                            continue;

                        (context, _) = await this.InternalDeprovisionAsync(clientScopeInfo, context, provision,
                            runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                    }

                    await runner.CommitAsync().ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                throw this.GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Provision the local database based on the scope info parameter.
        /// Scope info parameter should contains Schema and Setup properties.
        /// </summary>
        internal virtual async Task<(SyncContext Context, ScopeInfo CScopeInfo)>
                    InternalProvisionClientAsync(ScopeInfo serverScopeInfo, ScopeInfo clientScopeInfo, SyncContext context, SyncProvision provision, bool overwrite,
                            DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            try
            {
                Guard.ThrowIfNull(serverScopeInfo);
                Guard.ThrowIfNull(serverScopeInfo.Setup, $"No Setup in your server scope info {serverScopeInfo.Name}");
                Guard.ThrowIfNull(serverScopeInfo.Schema, $"No Schema in your server scope info {serverScopeInfo.Name}");

                using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Provisioning, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    // 2) Provision
                    if (provision == SyncProvision.NotSet)
                        provision = SyncProvision.Table | SyncProvision.StoredProcedures | SyncProvision.Triggers | SyncProvision.TrackingTable;

                    (context, _) = await this.InternalProvisionAsync(serverScopeInfo, context, overwrite, provision, runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    // set client scope setup and schema
                    clientScopeInfo.Setup = serverScopeInfo.Setup;
                    clientScopeInfo.Schema = serverScopeInfo.Schema;

                    // Write scopes locally
                    (context, clientScopeInfo) = await this.InternalSaveScopeInfoAsync(clientScopeInfo, context, runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    await runner.CommitAsync().ConfigureAwait(false);

                    return (context, clientScopeInfo);
                }
            }
            catch (Exception ex)
            {
                string message = null;

                message += $"Provision:{provision}.";
                message += $"Overwrite:{overwrite}.";

                throw this.GetSyncError(context, ex, message);
            }
        }
    }
}