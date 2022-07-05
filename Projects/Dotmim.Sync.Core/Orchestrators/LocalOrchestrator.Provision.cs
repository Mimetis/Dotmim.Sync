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
    public partial class LocalOrchestrator : BaseOrchestrator
    {

        public virtual async Task<ClientScopeInfo>
            ProvisionAsync(ServerScopeInfo serverScopeInfo, SyncProvision provision = default, bool overwrite = true, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), serverScopeInfo.Name);
            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.Provisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                ClientScopeInfo clientScopeInfo;
                (context, clientScopeInfo) = await InternalGetClientScopeInfoAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);
                (context, clientScopeInfo) = await InternalProvisionClientAsync(serverScopeInfo, clientScopeInfo, context, provision, overwrite, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return clientScopeInfo;
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
        public virtual async Task<(SyncContext context, ClientScopeInfo clientScopeInfo)>
                    InternalProvisionClientAsync(ServerScopeInfo serverScopeInfo, ClientScopeInfo clientScopeInfo, SyncContext context, SyncProvision provision = default, bool overwrite = true, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                if (serverScopeInfo.Schema == null)
                    throw new Exception($"No Schema in your server scope info {serverScopeInfo.Name}");

                if (serverScopeInfo.Schema == null)
                    throw new Exception($"No Setup in your server scope info {serverScopeInfo.Name}");

                await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.Provisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                // Check incompatibility with the flags
                if (provision.HasFlag(SyncProvision.ServerHistoryScope) || provision.HasFlag(SyncProvision.ServerScope))
                    throw new InvalidProvisionForLocalOrchestratorException();

                // 2) Provision
                if (provision == SyncProvision.None)
                    provision = SyncProvision.Table | SyncProvision.StoredProcedures | SyncProvision.Triggers | SyncProvision.TrackingTable;

                (context, _) = await this.InternalProvisionAsync(serverScopeInfo, context, overwrite, provision, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                // set client scope setup and schema
                clientScopeInfo.Setup = serverScopeInfo.Setup;
                clientScopeInfo.Schema = serverScopeInfo.Schema;

                // Write scopes locally
                (context, clientScopeInfo) = await this.InternalSaveClientScopeInfoAsync(clientScopeInfo, context, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return (context, clientScopeInfo);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }





        /// <summary>
        /// Deprovision the orchestrator database based the provision enumeration
        /// Default Deprovision objects are StoredProcedures & Triggers
        /// </summary>
        public virtual Task<bool> DeprovisionAsync(SyncProvision provision = default, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => DeprovisionAsync(SyncOptions.DefaultScopeName, provision, connection, transaction, cancellationToken);

        /// <summary>
        /// Deprovision the orchestrator database based the provision enumeration
        /// </summary>
        public virtual async Task<bool> DeprovisionAsync(string scopeName, SyncProvision provision = default, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);
            try
            {
                if (provision == default)
                    provision = SyncProvision.StoredProcedures | SyncProvision.Triggers;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.Deprovisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                // get client scope and create tables / row if needed

                ClientScopeInfo clientScopeInfo;
                (context, clientScopeInfo) = await this.InternalLoadClientScopeInfoAsync(context, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (clientScopeInfo == null || clientScopeInfo.Schema == null || !clientScopeInfo.Schema.HasTables || !clientScopeInfo.Schema.HasColumns)
                    return false;

                bool isDeprovisioned;
                (context, isDeprovisioned) = await InternalDeprovisionAsync(clientScopeInfo, context, provision, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

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
                await using var runner = await this.GetConnectionAsync(context, SyncMode.Writing, SyncStage.Deprovisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                // get client scope and create tables / row if needed

                List<ClientScopeInfo> clientScopeInfos;
                (context, clientScopeInfos) = await this.InternalLoadAllClientScopesInfoAsync(context, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);

                if (clientScopeInfos == null || clientScopeInfos.Count == 0)
                    return;

                var provision = SyncProvision.StoredProcedures | SyncProvision.Triggers | SyncProvision.TrackingTable | SyncProvision.ClientScope;

                foreach (var clientScopeInfo in clientScopeInfos)
                {
                    if (clientScopeInfo == null || clientScopeInfo.Schema == null || !clientScopeInfo.Schema.HasTables || !clientScopeInfo.Schema.HasColumns)
                        continue;

                    (context, _) = await InternalDeprovisionAsync(clientScopeInfo, context, provision, runner.Connection, runner.Transaction, cancellationToken, progress).ConfigureAwait(false);
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
