using Dotmim.Sync.Args;
using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;


namespace Dotmim.Sync
{
    public partial class LocalOrchestrator : BaseOrchestrator
    {

        public virtual Task<ScopeInfo> GetScopeInfoAsync(DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => GetScopeInfoAsync(SyncOptions.DefaultScopeName, connection, transaction, cancellationToken, progress);

        public virtual async Task<ScopeInfo>
            GetScopeInfoAsync(string scopeName, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);

            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.ScopeLoading, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                ScopeInfo cScopeInfo;
                (context, cScopeInfo) = await InternalEnsureScopeInfoAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                await runner.CommitAsync().ConfigureAwait(false);

                return cScopeInfo;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }




        /// <summary>
        /// Check 
        /// </summary>
        public virtual async Task<(SyncContext, bool, ScopeInfo, ScopeInfo)> IsConflictingSetupAsync(
            SyncContext context, SyncSetup inputSetup, ScopeInfo clientScopeInfo, ScopeInfo serverScopeInfo, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            //if (clientScopeInfo.IsNewScope || clientScopeInfo.Schema == null)
            if (clientScopeInfo.Schema == null)
            {
                return (context, false, clientScopeInfo, serverScopeInfo);
            }

            if (inputSetup != null && clientScopeInfo.Setup != null && !clientScopeInfo.Setup.EqualsByProperties(inputSetup))
            {

                var conflictingSetupArgs = new ConflictingSetupArgs(context, inputSetup, clientScopeInfo, serverScopeInfo);
                await this.InterceptAsync(conflictingSetupArgs, progress, cancellationToken).ConfigureAwait(false);

                if (conflictingSetupArgs.Action == ConflictingSetupAction.Rollback)
                {
                    throw new Exception("Seems you are trying another Setup tables that what is stored in your client scope database. Please create a new scope or deprovision and provision again your client scope.");
                }
                if (conflictingSetupArgs.Action == ConflictingSetupAction.Abort)
                {
                    return (context, true, clientScopeInfo, serverScopeInfo);
                }

                // re affect scope infos
                clientScopeInfo = conflictingSetupArgs.ClientScopeInfo;
                serverScopeInfo = conflictingSetupArgs.ServerScopeInfo;
            }

            if (clientScopeInfo.Setup != null && serverScopeInfo.Setup != null && !clientScopeInfo.Setup.EqualsByProperties(serverScopeInfo.Setup))
            {
                var conflictingSetupArgs = new ConflictingSetupArgs(context, inputSetup, clientScopeInfo, serverScopeInfo);
                await this.InterceptAsync(conflictingSetupArgs, progress, cancellationToken).ConfigureAwait(false);

                if (conflictingSetupArgs.Action == ConflictingSetupAction.Rollback)
                {
                    throw new Exception("Seems your client setup is different from your server setup. Please create a new scope or deprovision and provision again your client scope with the server scope.");
                }
                if (conflictingSetupArgs.Action == ConflictingSetupAction.Abort)
                {
                    return (context, true, clientScopeInfo, serverScopeInfo);
                }

                // re affect scope infos
                clientScopeInfo = conflictingSetupArgs.ClientScopeInfo;
                serverScopeInfo = conflictingSetupArgs.ServerScopeInfo;
            }

            // We gave 2 chances to user to edit the setup and fill correct values.
            // Final check, but if not valid, raise an error
            if (clientScopeInfo.Setup != null && serverScopeInfo.Setup != null && !clientScopeInfo.Setup.EqualsByProperties(serverScopeInfo.Setup))
                throw new Exception("Seems your client setup is different from your server setup. Please create a new scope or deprovision and provision again your client scope with the server scope.");

            return (context, false, clientScopeInfo, serverScopeInfo);
        }


        internal virtual async Task<(SyncContext context, ScopeInfo serverScopeInfo)>
             InternalEnsureScopeInfoAsync(SyncContext context, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            try
            {
                await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.ScopeLoading, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                bool exists;
                (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfo, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);
                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.ScopeInfo, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfoClient, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);
                if (!exists)
                    await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.ScopeInfoClient, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                ScopeInfo cScopeInfo;
                bool cScopeInfoExists;
                (context, cScopeInfoExists) = await this.InternalExistsScopeInfoAsync(context.ScopeName, context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                if (!cScopeInfoExists)
                {
                    cScopeInfo = this.InternalCreateScopeInfo(context.ScopeName);
                    (context, cScopeInfo) = await this.InternalSaveScopeInfoAsync(cScopeInfo, context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);
                }
                else
                {
                    (context, cScopeInfo) = await this.InternalLoadScopeInfoAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);
                }

                await runner.CommitAsync().ConfigureAwait(false);

                return (context, cScopeInfo);
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }


    }
}
