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
        /// <summary>
        /// Provision the remote database based on the Setup parameter, and the provision enumeration
        /// </summary>
        /// <param name="provision">Provision enumeration to determine which components to apply</param>
        /// <param name="serverScopeInfo">server scope. Will be saved once provision is done</param>
        /// <returns>Full schema with table and columns properties</returns>
        public virtual Task<SyncSet> ProvisionAsync(SyncProvision provision, bool overwrite = false, ServerScopeInfo serverScopeInfo = null, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => RunInTransactionAsync(SyncStage.Provisioning, async (ctx, connection, transaction) =>
            {
                // Check incompatibility with the flags
                if (provision.HasFlag(SyncProvision.ClientScope))
                    throw new InvalidProvisionForRemoteOrchestratorException();

                // Get server scope if not supplied
                if (serverScopeInfo == null)
                    serverScopeInfo = await this.GetServerScopeAsync(connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                var schema = new SyncSet(this.Setup);

                schema = await InternalProvisionAsync(ctx, overwrite, schema, provision, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                // Affect good values
                serverScopeInfo.Schema = schema;
                serverScopeInfo.Setup = this.Setup;

                // Save scope
                var scopeBuilder = this.GetScopeBuilder(this.Options.ScopeInfoTableName);
                await this.InternalSaveScopeAsync(ctx, DbScopeType.Server, serverScopeInfo, scopeBuilder, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                return schema;

            }, connection, transaction, cancellationToken);

    }
}
