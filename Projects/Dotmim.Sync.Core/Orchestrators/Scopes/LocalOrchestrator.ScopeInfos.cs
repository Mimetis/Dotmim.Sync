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

        /// <summary>
        /// Get a scope info from the remote data source. A scope contains the <see cref="SyncSetup"/> setup and the <see cref="SyncSet"/> schema.
        /// <para>
        /// If the <strong>scope_info</strong> table is not existing, it will be created. If no scope is found, an empty scope will be created with empty schema and setup properties.
        /// </para>
        /// <example>
        /// <code>
        ///  var localOrchestrator = new LocalOrchestrator(clientProvider);
        ///  var scopeInfo = await localOrchestrator.GetScopeInfoAsync();
        ///  foreach (var schemaTable in scopeInfo.Schema.Tables)
        ///  {
        ///    Console.WriteLine($"Table Name: {schemaTable.TableName}");
        ///       
        ///    foreach (var column in schemaTable.Columns)
        ///          Console.WriteLine($"Column Name: {column.ColumnName}");
        ///  }
        /// </code>
        /// </example>
        /// </summary>
        /// <returns><see cref="ScopeInfo"/> instance.</returns>
        public virtual async Task<ScopeInfo> GetScopeInfoAsync(string scopeName = SyncOptions.DefaultScopeName)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);

            try
            {
                ScopeInfo cScopeInfo;
                (context, cScopeInfo) = await InternalEnsureScopeInfoAsync(context, default, default, default, default).ConfigureAwait(false);
                return cScopeInfo;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }


        internal virtual async Task<(SyncContext context, ScopeInfo serverScopeInfo)> InternalEnsureScopeInfoAsync(SyncContext context,
            DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
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

        /// <summary>
        /// Check 
        /// </summary>
        internal virtual async Task<(SyncContext, bool, ScopeInfo, ScopeInfo)> InternalIsConflictingSetupAsync(SyncContext context, SyncSetup inputSetup, ScopeInfo clientScopeInfo, ScopeInfo serverScopeInfo,
            DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (clientScopeInfo.Schema == null)
                return (context, false, clientScopeInfo, serverScopeInfo);

            if (inputSetup != null && clientScopeInfo.Setup != null && !clientScopeInfo.Setup.EqualsByProperties(inputSetup))
            {
                var conflictingSetupArgs = new ConflictingSetupArgs(context, inputSetup, clientScopeInfo, serverScopeInfo);
                await this.InterceptAsync(conflictingSetupArgs, progress, cancellationToken).ConfigureAwait(false);

                if (conflictingSetupArgs.Action == ConflictingSetupAction.Rollback)
                    throw new Exception("Seems you are trying another Setup tables that what is stored in your client scope database. Please create a new scope or deprovision and provision again your client scope.");

                if (conflictingSetupArgs.Action == ConflictingSetupAction.Abort)
                    return (context, true, clientScopeInfo, serverScopeInfo);

                // re affect scope infos
                clientScopeInfo = conflictingSetupArgs.ClientScopeInfo;
                serverScopeInfo = conflictingSetupArgs.ServerScopeInfo;
            }

            if (clientScopeInfo.Setup != null && serverScopeInfo.Setup != null && !clientScopeInfo.Setup.EqualsByProperties(serverScopeInfo.Setup))
            {
                var conflictingSetupArgs = new ConflictingSetupArgs(context, inputSetup, clientScopeInfo, serverScopeInfo);
                await this.InterceptAsync(conflictingSetupArgs, progress, cancellationToken).ConfigureAwait(false);

                if (conflictingSetupArgs.Action == ConflictingSetupAction.Rollback)
                    throw new Exception("Seems your client setup is different from your server setup. Please create a new scope or deprovision and provision again your client scope with the server scope.");

                if (conflictingSetupArgs.Action == ConflictingSetupAction.Abort)
                    return (context, true, clientScopeInfo, serverScopeInfo);

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


    }
}
