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

        /// <summary>
        /// Provision the local database based on the orchestrator setup, and the provision enumeration
        /// </summary>
        /// <param name="provision">Provision enumeration to determine which components to apply</param>
        public virtual Task<SyncSet> ProvisionAsync(SyncProvision provision, bool overwrite = false, ScopeInfo clientScopeInfo = null, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => this.ProvisionAsync(new SyncSet(this.Setup), provision, overwrite, clientScopeInfo, connection, transaction, cancellationToken, progress);


        /// <summary>
        /// Provision the local database based on the schema parameter, and the provision enumeration
        /// </summary>
        /// <param name="schema">Schema to be applied to the database managed by the orchestrator, through the provider.</param>
        /// <param name="provision">Provision enumeration to determine which components to apply</param>
        /// <param name="clientScopeInfo">client scope. Will be saved once provision is done</param>
        /// <returns>Full schema with table and columns properties</returns>
        public virtual Task<SyncSet> ProvisionAsync(SyncSet schema, SyncProvision provision, bool overwrite = false, ScopeInfo clientScopeInfo = null, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => RunInTransactionAsync(SyncStage.Provisioning, async (ctx, connection, transaction) =>
            {
                // Check incompatibility with the flags
                if (provision.HasFlag(SyncProvision.ServerHistoryScope) || provision.HasFlag(SyncProvision.ServerScope))
                    throw new InvalidProvisionForLocalOrchestratorException();

                // Get client scope if not supplied
                if (clientScopeInfo == null)
                    clientScopeInfo = await this.GetClientScopeAsync(connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                schema = await InternalProvisionAsync(ctx, overwrite, schema, provision, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                // Affect good values
                clientScopeInfo.Schema = schema;
                clientScopeInfo.Setup = this.Setup;

                // Save scope
                var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);
                await this.InternalSaveScopeAsync(ctx, DbScopeType.Client, clientScopeInfo, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                return schema;

            }, connection, transaction, cancellationToken);

    }
}
