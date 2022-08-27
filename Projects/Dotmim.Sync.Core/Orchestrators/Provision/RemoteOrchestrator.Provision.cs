using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
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
    public partial class RemoteOrchestrator : BaseOrchestrator
    {


        public virtual Task<ScopeInfo> ProvisionAsync(SyncProvision provision = default, bool overwrite = false, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => ProvisionAsync(SyncOptions.DefaultScopeName, provision, overwrite, connection, transaction, cancellationToken, progress);

        /// <summary>
        /// Provision the remote database 
        /// </summary>
        /// <param name="overwrite">Overwrite existing objects</param>
        public virtual Task<ScopeInfo> ProvisionAsync(string scopeName, SyncProvision provision = default, bool overwrite = false, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (provision == SyncProvision.None)
                provision = SyncProvision.ScopeInfo | SyncProvision.ScopeInfoClient | SyncProvision.StoredProcedures | SyncProvision.Triggers | SyncProvision.TrackingTable;

            return this.ProvisionAsync(scopeName, null, provision, overwrite, connection, transaction, cancellationToken, progress);
        }


        public virtual Task<ScopeInfo> ProvisionAsync(SyncSetup setup, SyncProvision provision = default, bool overwrite = false, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => ProvisionAsync(SyncOptions.DefaultScopeName, setup, provision, overwrite, connection, transaction, cancellationToken, progress);


        /// <summary>
        /// Provision the remote database based on the Setup parameter, and the provision enumeration
        /// </summary>
        /// <param name="provision">Provision enumeration to determine which components to apply</param>
        /// <param name="serverScopeInfo">server scope. Will be saved once provision is done</param>
        /// <returns>Full schema with table and columns properties</returns>
        public virtual async Task<ScopeInfo> ProvisionAsync(string scopeName, SyncSetup setup = null, SyncProvision provision = default, bool overwrite = false, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);
            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Provisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                ScopeInfo serverScopeInfo;
                (context, serverScopeInfo, _) = await this.InternalEnsureScopeInfoAsync(context, setup, overwrite, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                // 2) Provision
                if (provision == SyncProvision.None)
                    provision = SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;

                (context, serverScopeInfo) = await this.InternalProvisionServerAsync(serverScopeInfo, context, provision, overwrite, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return serverScopeInfo;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        public virtual async Task<ScopeInfo> ProvisionAsync(ScopeInfo serverScopeInfo, SyncProvision provision = default, bool overwrite = false, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), serverScopeInfo.Name);
            try
            {
                (_, serverScopeInfo) = await InternalProvisionServerAsync(serverScopeInfo, context, provision, overwrite, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                return serverScopeInfo;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }

        }


        internal virtual async Task<(SyncContext context, ScopeInfo serverScopeInfo)>
          InternalProvisionServerAsync(ScopeInfo sScopeInfo, SyncContext context, SyncProvision provision = default, bool overwrite = false, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                if (sScopeInfo.Schema == null)
                    throw new Exception($"No Schema in your server scopeInfo {sScopeInfo.Name}");

                if (sScopeInfo.Schema == null)
                    throw new Exception($"No Setup in your server scopeInfo {sScopeInfo.Name}");

                await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Provisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                if (provision == SyncProvision.None)
                    provision = SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;

                (context, _) = await this.InternalProvisionAsync(sScopeInfo, context, overwrite, provision, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                // Write scopes locally
                (context, sScopeInfo) = await this.InternalSaveScopeInfoAsync(sScopeInfo, context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return (context, sScopeInfo);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }

        }

        /// <summary>
        /// Deprovision the remote database. Schema tables are retrieved through the default scope.
        /// </summary>
        public virtual Task<bool>
               DeprovisionAsync(SyncProvision provision = default, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => DeprovisionAsync(SyncOptions.DefaultScopeName, provision, connection, transaction, cancellationToken, progress);

        /// <summary>
        /// Deprovision the remote database. Schema tables are retrieved through scope name.
        /// </summary>
        public virtual async Task<bool>
            DeprovisionAsync(string scopeName, SyncProvision provision = default, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);
            try
            {
                if (provision == default)
                    provision = SyncProvision.StoredProcedures | SyncProvision.Triggers;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Deprovisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                ScopeInfo serverScopeInfo = null;
                bool exists;
                (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfo, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                if (exists)
                    (context, serverScopeInfo) = await this.InternalLoadScopeInfoAsync(context, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                bool isDeprovisioned;
                (context, isDeprovisioned) = await InternalDeprovisionAsync(serverScopeInfo, context, provision, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return isDeprovisioned;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }

        }

        public virtual Task<bool> DeprovisionAsync(SyncSetup setup, SyncProvision provision = default, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => DeprovisionAsync(SyncOptions.DefaultScopeName, setup, provision, connection, transaction, cancellationToken, progress);

        /// <summary>
        /// Deprovision the remote database. Schema tables are retrieved through setup in parameter.
        /// </summary>
        public virtual async Task<bool> DeprovisionAsync(string scopeName, SyncSetup setup, SyncProvision provision = default, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);
            try
            {
                if (provision == default)
                    provision = SyncProvision.ScopeInfo | SyncProvision.ScopeInfoClient | SyncProvision.StoredProcedures | SyncProvision.Triggers | SyncProvision.TrackingTable;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Deprovisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                // Creating a fake scope info
                var serverScopeInfo = this.InternalCreateScopeInfo(scopeName);
                serverScopeInfo.Setup = setup;

                bool isDeprovisioned;
                (context, isDeprovisioned) = await InternalDeprovisionAsync(serverScopeInfo, context, provision, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return isDeprovisioned;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }

        }


        /// <summary>
        /// Drop everything related to DMS
        /// </summary>
        public virtual async Task DropAllAsync(DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), SyncOptions.DefaultScopeName);
            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Deprovisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                List<ScopeInfo> serverScopeInfos = null;
                bool exists;
                (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfo, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                if (exists)
                    (context, serverScopeInfos) = await this.InternalLoadAllScopeInfosAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                // fallback to "try to drop an hypothetical default scope"
                if (serverScopeInfos == null)
                    serverScopeInfos = new List<ScopeInfo>();

                var defaultServerScopeInfo = this.InternalCreateScopeInfo(SyncOptions.DefaultScopeName);

                SyncSetup setup;
                (context, setup) = await this.InternalGetAllTablesAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                // Considering removing tables with "_tracking" at the end
                var tables = setup.Tables.Where(setupTable => !setupTable.TableName.EndsWith("_tracking")).ToList();
                setup.Tables.Clear();
                setup.Tables.AddRange(tables);
                defaultServerScopeInfo.Setup = setup;

                serverScopeInfos.Add(defaultServerScopeInfo);

                var provision = SyncProvision.StoredProcedures | SyncProvision.Triggers | SyncProvision.TrackingTable | SyncProvision.ScopeInfo | SyncProvision.ScopeInfoClient;

                foreach (var serverScopeInfo in serverScopeInfos)
                {
                    if (serverScopeInfo == null || serverScopeInfo.Setup == null || serverScopeInfo.Setup.Tables == null || serverScopeInfo.Setup.Tables.Count <= 0)
                        continue;

                    (context, _) = await InternalDeprovisionAsync(serverScopeInfo, context, provision, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
                }

                await runner.CommitAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

    }
}
