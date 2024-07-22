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
    /// Contains remote orchestrator provisioning methods.
    /// </summary>
    public partial class RemoteOrchestrator : BaseOrchestrator
    {
        /// <summary>
        /// Provision a server datasource (<strong>triggers</strong>, <strong>stored procedures</strong> (if supported) and <strong>tracking tables</strong> if needed. Create also <strong>scope_info</strong> and <strong>scope_info_client</strong> tables.
        /// <para>
        /// The <paramref name="provision" /> argument specify the objects to provision. See <see cref="SyncProvision" /> enumeration.
        /// </para>
        /// <para>
        /// If The <paramref name="setup" /> argument is not specified, setup is retrieved from the scope_info table. Means that you have done a provision before.
        /// </para>
        /// <para>
        /// <example>
        /// <code>
        /// var remoteOrchestrator = new RemoteOrchestrator(serverProvider);
        /// var setup = new SyncSetup("ProductCategory", "Product");
        /// var sScopeInfo = await remoteOrchestrator.ProvisionAsync(setup);
        /// </code>
        /// </example>
        /// </para>
        /// </summary>
        /// <param name="scopeName">Scope name.</param>
        /// <param name="setup">Setup containing all tables to provision on the server side.</param>
        /// <param name="provision">If you do not specify <c>provision</c>, a default value <c>SyncProvision.StoredProcedures | SyncProvision.Triggers | SyncProvision.TrackingTable</c> is used.</param>
        /// <param name="overwrite">If specified, all metadatas are generated and overwritten even if they already exists.</param>
        /// <param name="connection">Optional Connection.</param>
        /// <param name="transaction">Optional Transaction.</param>
        /// <param name="progress">option IProgress{ProgressArgs}.</param>
        /// <param name="cancellationToken">optional cancellation token.</param>
        /// <returns>
        /// A <see cref="ScopeInfo"/> instance, saved locally in the server datasource.
        /// </returns>
        public virtual async Task<ScopeInfo> ProvisionAsync(string scopeName, SyncSetup setup = null, SyncProvision provision = default, bool overwrite = false,
            DbConnection connection = null, DbTransaction transaction = null, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);
            try
            {
                using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Provisioning, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    ScopeInfo sScopeInfo;
                    (context, sScopeInfo, _) = await this.InternalEnsureScopeInfoAsync(context, setup, overwrite,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    if (sScopeInfo.Setup == null || sScopeInfo.Schema == null)
                        throw new MissingServerScopeTablesException(scopeName);

                    // 2) Provision
                    if (provision == SyncProvision.NotSet)
                        provision = SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;

                    (context, sScopeInfo) = await this.InternalProvisionServerAsync(sScopeInfo, context, provision, overwrite,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    await runner.CommitAsync().ConfigureAwait(false);

                    return sScopeInfo;
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
        /// Provision a server datasource (<strong>triggers</strong>, <strong>stored procedures</strong> (if supported) and <strong>tracking tables</strong> if needed. Create also <strong>scope_info</strong> and <strong>scope_info_client</strong> tables.
        /// <para>
        /// The <paramref name="provision" /> argument specify the objects to provision. See <see cref="SyncProvision" /> enumeration.
        /// </para>
        /// <para>
        /// <example>
        /// <code>
        /// var remoteOrchestrator = new RemoteOrchestrator(serverProvider);
        /// var serverScope = await remoteOrchestrator.GetScopeInfoAsync();
        /// var schema = await remoteOrchestrator.GetSchemaAsync(setup);
        /// serverScope.Schema = schema;
        /// serverScope.Setup = setup;
        /// var sScopeInfo = await localOrchestrator.ProvisionAsync(serverScope);
        /// </code>
        /// </example>
        /// </para>
        /// </summary>
        /// <param name="serverScopeInfo"><see cref="ScopeInfo"/> instance to provision on server side.</param>
        /// <param name="provision">If you do not specify <c>provision</c>, a default value <c>SyncProvision.StoredProcedures | SyncProvision.Triggers | SyncProvision.TrackingTable</c> is used.</param>
        /// <param name="overwrite">If specified, all metadatas are generated and overwritten even if they already exists.</param>
        /// <param name="connection">Optional Connection.</param>
        /// <param name="transaction">Optional Transaction.</param>
        /// <param name="progress">option IProgress{ProgressArgs}.</param>
        /// <param name="cancellationToken">optional cancellation token.</param>
        /// <returns>
        /// A <see cref="ScopeInfo"/> instance, saved locally in the server datasource.
        /// </returns>
        public virtual async Task<ScopeInfo> ProvisionAsync(ScopeInfo serverScopeInfo, SyncProvision provision = default, bool overwrite = false,
            DbConnection connection = null, DbTransaction transaction = null, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
        {
            Guard.ThrowIfNull(serverScopeInfo);

            var context = new SyncContext(Guid.NewGuid(), serverScopeInfo.Name);
            try
            {
                (_, serverScopeInfo) = await this.InternalProvisionServerAsync(serverScopeInfo, context, provision, overwrite,
                    connection, transaction, progress, cancellationToken).ConfigureAwait(false);
                return serverScopeInfo;
            }
            catch (Exception ex)
            {
                string message = null;

                message += $"Provision:{provision}.";
                message += $"Overwrite:{overwrite}.";

                throw this.GetSyncError(context, ex, message);
            }
        }

        /// <inheritdoc cref="ProvisionAsync(string, SyncSetup, SyncProvision, bool, DbConnection, DbTransaction, IProgress{ProgressArgs}, CancellationToken)"/>
        public virtual Task<ScopeInfo> ProvisionAsync(SyncProvision provision = default, bool overwrite = false,
            DbConnection connection = null, DbTransaction transaction = null, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
            => this.ProvisionAsync(SyncOptions.DefaultScopeName, provision, overwrite, connection, transaction, progress, cancellationToken);

        /// <inheritdoc cref="ProvisionAsync(string, SyncSetup, SyncProvision, bool, DbConnection, DbTransaction, IProgress{ProgressArgs}, CancellationToken)"/>
        public virtual Task<ScopeInfo> ProvisionAsync(string scopeName, SyncProvision provision = default, bool overwrite = false,
            DbConnection connection = null, DbTransaction transaction = null, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
        {
            if (provision == SyncProvision.NotSet)
                provision = SyncProvision.ScopeInfo | SyncProvision.ScopeInfoClient | SyncProvision.StoredProcedures | SyncProvision.Triggers | SyncProvision.TrackingTable;

            return this.ProvisionAsync(scopeName, null, provision, overwrite, connection, transaction, progress, cancellationToken);
        }

        /// <inheritdoc cref="ProvisionAsync(string, SyncSetup, SyncProvision, bool, DbConnection, DbTransaction, IProgress{ProgressArgs}, CancellationToken)"/>
        public virtual Task<ScopeInfo> ProvisionAsync(SyncSetup setup, SyncProvision provision = default, bool overwrite = false,
            DbConnection connection = null, DbTransaction transaction = null, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
            => this.ProvisionAsync(SyncOptions.DefaultScopeName, setup, provision, overwrite, connection, transaction, progress, cancellationToken);

        /// <summary>
        /// Deprovision your server datasource.
        /// <example>
        /// Deprovision a server database:
        /// <code>
        /// var remoteOrchestrator = new RemoteOrchestrator(serverProvider);
        /// await remoteOrchestrator.DeprovisionAsync();
        /// </code>
        /// </example>
        /// </summary>
        /// <remarks>
        /// By default, <strong>DMS</strong> will never deprovision a table, if not explicitly set with the <c>provision</c> argument. <strong>scope_info</strong> and <strong>scope_info_client</strong> tables
        /// are not deprovisioned by default to preserve existing configurations.
        /// </remarks>
        /// <param name="provision">If you do not specify <c>provision</c>, a default value <c>SyncProvision.StoredProcedures | SyncProvision.Triggers</c> is used.</param>
        /// <param name="connection">Optional Connection.</param>
        /// <param name="transaction">Optional Transaction.</param>
        /// <param name="progress">option IProgress{ProgressArgs}.</param>
        /// <param name="cancellationToken">optional cancellation token.</param>
        public virtual Task<bool> DeprovisionAsync(SyncProvision provision = default, DbConnection connection = null, DbTransaction transaction = null, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
            => this.DeprovisionAsync(SyncOptions.DefaultScopeName, provision, connection, transaction, progress, cancellationToken);

        /// <inheritdoc cref="DeprovisionAsync(SyncProvision, DbConnection, DbTransaction,  IProgress{ProgressArgs}, CancellationToken)"/>
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
                    ScopeInfo serverScopeInfo = null;
                    bool exists;
                    (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfo,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    if (exists)
                    {

                        (context, serverScopeInfo) = await this.InternalLoadScopeInfoAsync(
                            context,
                            runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                    }

                    bool isDeprovisioned;
                    (context, isDeprovisioned) = await this.InternalDeprovisionAsync(serverScopeInfo, context, provision,
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

        /// <inheritdoc cref="DeprovisionAsync(string, SyncSetup, SyncProvision, DbConnection, DbTransaction, IProgress{ProgressArgs}, CancellationToken)"/>
        public virtual Task<bool> DeprovisionAsync(SyncSetup setup, SyncProvision provision = default, DbConnection connection = null, DbTransaction transaction = null, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
            => this.DeprovisionAsync(SyncOptions.DefaultScopeName, setup, provision, connection, transaction, progress, cancellationToken);

        /// <summary>
        /// Deprovision your client datasource.
        /// <example>
        /// Deprovision a client database:
        /// <code>
        /// var remoteOrchestrator = new RemoteOrchestrator(serverProvider);
        /// var setup = new SyncSetup("ProductCategory", "Product");
        /// await remoteOrchestrator.DeprovisionAsync(setup);
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
        /// <param name="progress">optional IProgress{ProgressArgs}.</param>
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
                    var serverScopeInfo = InternalCreateScopeInfo(scopeName);
                    serverScopeInfo.Setup = setup;
                    serverScopeInfo.Schema = new SyncSet(setup);

                    bool isDeprovisioned;
                    (context, isDeprovisioned) = await this.InternalDeprovisionAsync(serverScopeInfo, context, provision,
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
        /// Deprovision a server database:
        /// <code>
        /// var remoteOrchestrator = new RemoteOrchestrator(serverProvider);
        /// await remoteOrchestrator.DropAllAsync();
        /// </code>
        /// </example>
        /// </summary>
        public virtual async Task DropAllAsync(DbConnection connection = null, DbTransaction transaction = null, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
        {
            var context = new SyncContext(Guid.NewGuid(), SyncOptions.DefaultScopeName);
            try
            {
                using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Deprovisioning, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    List<ScopeInfo> serverScopeInfos = null;
                    bool exists;
                    (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfo,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    if (exists)
                    {

                        (context, serverScopeInfos) = await this.InternalLoadAllScopeInfosAsync(
                            context,
                            runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                    }

                    // fallback to "try to drop an hypothetical default scope"
                    serverScopeInfos ??= [];

                    var existingFilters = serverScopeInfos?.SelectMany(si => si.Setup == null ? [] : si.Setup.Filters).ToList();

                    var defaultServerScopeInfo = InternalCreateScopeInfo(SyncOptions.DefaultScopeName);

                    SyncSetup setup;
                    (context, setup) = await this.InternalGetAllTablesAsync(
                        context,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);
                    var scopeInfoTableName = scopeBuilder.ScopeInfoTableName.Unquoted().ToString();
                    var scopeInfoClientTableName = $"{scopeBuilder.ScopeInfoTableName.Unquoted()}_client";

                    // Considering removing tables with "_tracking" at the end
                    var tables = setup.Tables.Where(setupTable => !setupTable.TableName.EndsWith("_tracking", SyncGlobalization.DataSourceStringComparison) && setupTable.TableName != scopeInfoTableName && setupTable.TableName != scopeInfoClientTableName).ToList();
                    setup.Tables.Clear();
                    setup.Tables.AddRange(tables);
                    defaultServerScopeInfo.Setup = setup;

                    if (defaultServerScopeInfo.Setup != null && defaultServerScopeInfo.Setup.Tables.Count > 0)
                    {
                        var (_, defaultSchema) = await this.InternalGetSchemaAsync(context, defaultServerScopeInfo.Setup,
                        runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                        defaultServerScopeInfo.Schema = defaultSchema;

                        // add any random filters, to try to delete them
                        if (existingFilters != null && existingFilters.Count > 0)
                        {
                            var filters = new SetupFilters();
                            foreach (var filter in existingFilters)
                                filters.Add(filter);

                            defaultServerScopeInfo.Setup.Filters = filters;
                        }

                        serverScopeInfos.Add(defaultServerScopeInfo);
                    }

                    var provision = SyncProvision.StoredProcedures | SyncProvision.Triggers | SyncProvision.TrackingTable | SyncProvision.ScopeInfo | SyncProvision.ScopeInfoClient;

                    foreach (var serverScopeInfo in serverScopeInfos)
                    {
                        if (serverScopeInfo == null || serverScopeInfo.Setup == null || serverScopeInfo.Setup.Tables == null || serverScopeInfo.Setup.Tables.Count <= 0)
                            continue;

                        (context, _) = await this.InternalDeprovisionAsync(serverScopeInfo, context, provision,
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
        /// Check if the server datasource should be provisioned.
        /// </summary>
        internal virtual async Task<(SyncContext Context, ScopeInfo ServerScopeInfo)> InternalProvisionServerAsync(ScopeInfo sScopeInfo, SyncContext context,
                                SyncProvision provision, bool overwrite,
                                DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            try
            {
                Guard.ThrowIfNull(sScopeInfo);
                Guard.ThrowIfNull(sScopeInfo.Setup, $"No Setup in your server scopeInfo {sScopeInfo.Name}");
                Guard.ThrowIfNull(sScopeInfo.Schema, $"No Schema in your server scopeInfo {sScopeInfo.Name}");

                using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Provisioning, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    if (provision == SyncProvision.NotSet)
                        provision = SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;

                    (context, _) = await this.InternalProvisionAsync(sScopeInfo, context, overwrite, provision, runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    // Write scopes locally
                    (context, sScopeInfo) = await this.InternalSaveScopeInfoAsync(sScopeInfo, context, runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    await runner.CommitAsync().ConfigureAwait(false);

                    return (context, sScopeInfo);
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
        /// Check if the server datasource should be provisioned.
        /// </summary>
        internal virtual async Task<bool> InternalShouldProvisionServerAsync(ScopeInfo sScopeInfo, SyncContext context,
                                DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            try
            {
                using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.Provisioning, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    var scopeInfoClients = await this.InternalLoadAllScopeInfoClientsAsync(context, runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    if (scopeInfoClients == null || scopeInfoClients.Count <= 0)
                        return true;

                    var scopeInfoClientsForThisScopeNameAlreadyExists = scopeInfoClients.Any(sic => sic.Name == sScopeInfo.Name);

                    return !scopeInfoClientsForThisScopeNameAlreadyExists;
                }
            }
            catch (Exception ex)
            {
                throw this.GetSyncError(context, ex);
            }
        }
    }
}