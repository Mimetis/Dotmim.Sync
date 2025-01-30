﻿using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Extensions;
using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Contains methods to get a scope info from the remote data source. A scope contains the <see cref="SyncSetup"/> setup and the <see cref="SyncSet"/> schema.
    /// </summary>
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
        ///    Console.WriteLine($"Table Name: {schemaTable.ObjectName}");
        ///
        ///    foreach (var column in schemaTable.Columns)
        ///          Console.WriteLine($"Column Name: {column.ObjectName}");
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
                (context, cScopeInfo) = await this.InternalEnsureScopeInfoAsync(context, connection, transaction, default, default).ConfigureAwait(false);
                return cScopeInfo;
            }
            catch (Exception ex)
            {
                throw this.GetSyncError(context, ex);
            }
        }

        /// <inheritdoc cref="GetScopeInfoAsync(string, DbConnection, DbTransaction)"/>
        public virtual Task<ScopeInfo> GetScopeInfoAsync(DbConnection connection = null, DbTransaction transaction = null)
            => this.GetScopeInfoAsync(SyncOptions.DefaultScopeName, connection, transaction);

        /// <summary>
        /// Ensure the scope info is created on the client side. If the scope info table is not existing, it will be created.
        /// </summary>
        internal virtual async Task<(SyncContext Context, ScopeInfo ServerScopeInfo)> InternalEnsureScopeInfoAsync(
            SyncContext context,
            DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            try
            {
                using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.ScopeLoading, connection, transaction, progress, cancellationToken).ConfigureAwait(false);
                await using (runner.ConfigureAwait(false))
                {
                    bool exists;
                    (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfo, runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                    if (!exists)
                        await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.ScopeInfo, runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    (context, exists) = await this.InternalExistsScopeInfoTableAsync(context, DbScopeType.ScopeInfoClient, runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                    if (!exists)
                        await this.InternalCreateScopeInfoTableAsync(context, DbScopeType.ScopeInfoClient, runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    ScopeInfo cScopeInfo;
                    bool cScopeInfoExists;
                    (context, cScopeInfoExists) = await this.InternalExistsScopeInfoAsync(context.ScopeName, context, runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    if (!cScopeInfoExists)
                    {
                        cScopeInfo = InternalCreateScopeInfo(context.ScopeName);
                        (context, cScopeInfo) = await this.InternalSaveScopeInfoAsync(cScopeInfo, context, runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        (context, cScopeInfo) = await this.InternalLoadScopeInfoAsync(context, runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                    }

                    await runner.CommitAsync().ConfigureAwait(false);

                    return (context, cScopeInfo);
                }
            }
            catch (Exception ex)
            {
                throw this.GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Check if a setup is conflicting with the current setup on the client side.
        /// </summary>
        internal virtual async Task<(SyncContext Context, bool IsConflicting, ScopeInfo ClientScopeInfo, ScopeInfo ServerScopeInfo)> InternalIsConflictingSetupAsync(SyncContext context, SyncSetup inputSetup, ScopeInfo clientScopeInfo, ScopeInfo serverScopeInfo,
            DbConnection connection = default, DbTransaction transaction = default, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
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
                        throw new SetupConflictOnClientException(serverScopeInfo.Setup, clientScopeInfo.Setup);

                    if (conflictingSetupArgs.Action == ConflictingSetupAction.Abort)
                        return (context, true, clientScopeInfo, serverScopeInfo);

                    // re affect scope infos
                    clientScopeInfo = conflictingSetupArgs.ClientScopeInfo;
                    serverScopeInfo = conflictingSetupArgs.ServerScopeInfo;
                }

                // We gave 2 chances to user to edit the setup and fill correct values.
                // Final check, but if not valid, raise an error
                if (clientScopeInfo.Setup != null && serverScopeInfo.Setup != null && !clientScopeInfo.Setup.EqualsByProperties(serverScopeInfo.Setup))
                    throw new SetupConflictOnClientException(serverScopeInfo.Setup, clientScopeInfo.Setup);

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
                    message += $"Input Setup:{Serializer.Serialize(inputSetup).ToUtf8String()}.";

                if (clientScopeInfo != null && clientScopeInfo.Setup != null)
                    message += $"Client Scope Setup:{Serializer.Serialize(clientScopeInfo.Setup).ToUtf8String()}.";

                if (serverScopeInfo != null && serverScopeInfo.Setup != null)
                    message += $"Server Scope Setup:{Serializer.Serialize(serverScopeInfo.Setup).ToUtf8String()}.";

                throw this.GetSyncError(context, ex, message);
            }
        }
    }
}