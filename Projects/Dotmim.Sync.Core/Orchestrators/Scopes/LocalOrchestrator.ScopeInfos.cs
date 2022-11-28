
using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
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
        public virtual async Task<ScopeInfo> GetScopeInfoAsync(string scopeName, DbConnection connection = null, DbTransaction transaction = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);

            try
            {
                ScopeInfo cScopeInfo;
                (context, cScopeInfo) = await InternalEnsureScopeInfoAsync(context, connection, transaction, default, default).ConfigureAwait(false);
                return cScopeInfo;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <inheritdoc cref="GetScopeInfoAsync(string, DbConnection, DbTransaction)"/>
        public virtual Task<ScopeInfo> GetScopeInfoAsync(DbConnection connection = null, DbTransaction transaction = null)
            => GetScopeInfoAsync(SyncOptions.DefaultScopeName, connection, transaction);



        internal virtual async Task<(SyncContext context, ScopeInfo serverScopeInfo)> InternalEnsureScopeInfoAsync(SyncContext context,
            DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
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

        /// <summary>
        /// Check 
        /// </summary>
        internal virtual async Task<(SyncContext, bool, ScopeInfo, ScopeInfo)> InternalIsConflictingSetupAsync(SyncContext context, SyncSetup inputSetup, ScopeInfo clientScopeInfo, ScopeInfo serverScopeInfo,
            DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (clientScopeInfo.Schema == null)
                return (context, false, clientScopeInfo, serverScopeInfo);

            try
            {
                if (inputSetup != null && clientScopeInfo.Setup != null && !clientScopeInfo.Setup.EqualsByProperties(inputSetup))
                {
                    var conflictingSetupArgs = new ConflictingSetupArgs(context, inputSetup, clientScopeInfo, serverScopeInfo);
                    await this.InterceptAsync(conflictingSetupArgs, progress, cancellationToken).ConfigureAwait(false);

                    if (conflictingSetupArgs.Action == ConflictingSetupAction.Rollback)
                        throw new SetupConflictOnClientException(inputSetup, clientScopeInfo.Setup);

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
                        throw new SetupConflictOnClientException(clientScopeInfo.Setup, clientScopeInfo.Setup);

                    if (conflictingSetupArgs.Action == ConflictingSetupAction.Abort)
                        return (context, true, clientScopeInfo, serverScopeInfo);

                    // re affect scope infos
                    clientScopeInfo = conflictingSetupArgs.ClientScopeInfo;
                    serverScopeInfo = conflictingSetupArgs.ServerScopeInfo;
                }

                // We gave 2 chances to user to edit the setup and fill correct values.
                // Final check, but if not valid, raise an error
                if (clientScopeInfo.Setup != null && serverScopeInfo.Setup != null && !clientScopeInfo.Setup.EqualsByProperties(serverScopeInfo.Setup))
                    throw new SetupConflictOnClientException(clientScopeInfo.Setup, clientScopeInfo.Setup);

                return (context, false, clientScopeInfo, serverScopeInfo);

            }
            catch (SetupConflictOnClientException)
            {
                // direct throw because message is already really long and we don't want to duplicate it
                throw;
            }
            catch (Exception ex)
            {
                string message = null;

                if (inputSetup != null)
                    message += $"Input Setup:{JsonConvert.SerializeObject(inputSetup)}.";

                if (clientScopeInfo != null && clientScopeInfo.Setup != null)
                    message += $"Client Scope Setup:{JsonConvert.SerializeObject(clientScopeInfo.Setup)}.";

                if (serverScopeInfo != null && serverScopeInfo.Setup != null)
                    message += $"Server Scope Setup:{JsonConvert.SerializeObject(serverScopeInfo.Setup)}.";

                throw GetSyncError(context, ex, message);
            }
        }
    }
}
