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
    public partial class RemoteOrchestrator : BaseOrchestrator
    {

        public virtual async Task<ScopeInfo> GetScopeInfoAsync(string scopeName = SyncOptions.DefaultScopeName)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);

            try
            {
                ScopeInfo sScopeInfo;
                (context, sScopeInfo, _) = await InternalEnsureScopeInfoAsync(context, default, false, default, default, default, default).ConfigureAwait(false);
                return sScopeInfo;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        public virtual Task<ScopeInfo> GetScopeInfoAsync(SyncSetup setup) => GetScopeInfoAsync(SyncOptions.DefaultScopeName, setup);

        public virtual async Task<ScopeInfo> GetScopeInfoAsync(string scopeName, SyncSetup setup)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);

            try
            {
                ScopeInfo sScopeInfo;
                (context, sScopeInfo, _) = await InternalEnsureScopeInfoAsync(context, setup, false, default, default, default, default).ConfigureAwait(false);

                return sScopeInfo;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }



        /// <summary>
        /// Check 
        /// </summary>
        internal virtual async Task<(SyncContext, bool, ScopeInfo)> InternalIsConflictingSetupAsync(SyncContext context, SyncSetup inputSetup, ScopeInfo sScopeInfo, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (sScopeInfo.Schema == null)
                return (context, false, sScopeInfo);

            if (inputSetup != null && sScopeInfo.Setup != null && !sScopeInfo.Setup.EqualsByProperties(inputSetup))
            {
                var conflictingSetupArgs = new ConflictingSetupArgs(context, inputSetup, null, sScopeInfo);
                await this.InterceptAsync(conflictingSetupArgs, progress, cancellationToken).ConfigureAwait(false);

                if (conflictingSetupArgs.Action == ConflictingSetupAction.Rollback)
                    throw new Exception("Seems you are trying another Setup tables that what is stored in your server scope database. Please create a new scope or deprovision and provision again your server scope.");

                if (conflictingSetupArgs.Action == ConflictingSetupAction.Abort)
                    return (context, true, sScopeInfo);

                // re affect scope infos
                sScopeInfo = conflictingSetupArgs.ServerScopeInfo;
            }

            // We gave 2 chances to user to edit the setup and fill correct values.
            // Final check, but if not valid, raise an error
            if (inputSetup != null && sScopeInfo.Setup != null && !sScopeInfo.Setup.EqualsByProperties(inputSetup))
                throw new Exception("Seems you are trying another Setup tables that what is stored in your server scope database. Please make a migration or create a new scope");

            return (context, false, sScopeInfo);
        }


        internal virtual async Task<(SyncContext context, ScopeInfo serverScopeInfo, bool shouldProvision)>
             InternalEnsureScopeInfoAsync(SyncContext context, SyncSetup setup, bool overwrite, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            bool shouldProvision = false;
            await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.ScopeLoading, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            bool exists;
            (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfo, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);
            if (!exists)
                await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.ScopeInfo, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

            (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfoClient, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);
            if (!exists)
                await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.ScopeInfoClient, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

            ScopeInfo sScopeInfo;
            bool sScopeInfoExists;
            (context, sScopeInfoExists) = await this.InternalExistsScopeInfoAsync(context.ScopeName, context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

            bool shouldSave = false;

            if (!sScopeInfoExists)
            {
                sScopeInfo = this.InternalCreateScopeInfo(context.ScopeName);
                shouldSave = true;
            }
            else
            {
                (context, sScopeInfo) = await this.InternalLoadScopeInfoAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);
            }

            // if serverscopeinfo is a new, because we never run any sync before, grab schema and affect setup
            if (setup != null && setup.Tables.Count > 0)
            {
                if ((sScopeInfo.Setup == null && sScopeInfo.Schema == null) || overwrite)
                {
                    SyncSet schema;
                    (context, schema) = await this.InternalGetSchemaAsync(context, setup, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);
                    sScopeInfo.Setup = setup;
                    sScopeInfo.Schema = schema;

                    // Checking if we have already some scopes
                    // Then gets the first scope to get the tracking tables & sp prefixes
                    List<ScopeInfo> allScopes;
                    (context, allScopes) = await this.InternalLoadAllScopeInfosAsync(context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                    if (allScopes.Count > 0)
                    {
                        // Get the first scope with an existing setup
                        var firstScope = allScopes.FirstOrDefault(sc => sc.Setup != null);

                        if (firstScope != null)
                        {
                            if (sScopeInfo.Setup.TrackingTablesPrefix != firstScope.Setup.TrackingTablesPrefix)
                                throw new Exception($"Can't add a new setup with different tracking table prefix. Please use same tracking table prefix as your first setup ([\"{firstScope.Setup.TrackingTablesPrefix}\"])");

                            if (sScopeInfo.Setup.TrackingTablesSuffix != firstScope.Setup.TrackingTablesSuffix)
                                throw new Exception($"Can't add a new setup with different tracking table suffix. Please use same tracking table suffix as your first setup ([\"{firstScope.Setup.TrackingTablesSuffix}\"])");

                            if (sScopeInfo.Setup.TriggersPrefix != firstScope.Setup.TriggersPrefix)
                                throw new Exception($"Can't add a new setup with different trigger prefix. Please use same trigger prefix as your first setup ([\"{firstScope.Setup.TriggersPrefix}\"])");

                            if (sScopeInfo.Setup.TriggersSuffix != firstScope.Setup.TriggersSuffix)
                                throw new Exception($"Can't add a new setup with different trigger suffix. Please use same trigger suffix as your first setup ([\"{firstScope.Setup.TriggersSuffix}\"])");
                        }
                    }

                    shouldSave = true;
                    shouldProvision = true;
                }
            }

            if (shouldSave)
                (context, sScopeInfo) = await this.InternalSaveScopeInfoAsync(sScopeInfo, context, runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

            await runner.CommitAsync().ConfigureAwait(false);

            return (context, sScopeInfo, shouldProvision);
        }
    }
}
