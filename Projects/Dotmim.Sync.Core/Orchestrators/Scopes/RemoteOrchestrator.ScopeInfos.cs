using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Extensions;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public partial class RemoteOrchestrator : BaseOrchestrator
    {
        /// <summary>
        /// Get a scope info from the remote data source. A scope contains the <see cref="SyncSetup"/> setup and the <see cref="SyncSet"/> schema.
        /// <para>
        /// If the <strong>scope_info</strong> table is not existing, it will be created. If no scope is found, an empty scope will be created with empty schema and setup properties.
        /// </para>
        /// <example>
        /// <code>
        ///  var remoteOrchestrator = new RemoteOrchestrator(clientProvider);
        ///  var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync();
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
        /// <param name="scopeName"></param>
        /// <param name="connection">Optional Connection</param>
        /// <param name="transaction">Optional Transaction</param>
        /// <returns></returns>
        public virtual async Task<ScopeInfo> GetScopeInfoAsync(string scopeName, DbConnection connection = default, DbTransaction transaction = default)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);

            try
            {
                ScopeInfo sScopeInfo;
                (context, sScopeInfo, _) = await InternalEnsureScopeInfoAsync(context, default, false, connection, transaction, default, default).ConfigureAwait(false);
                return sScopeInfo;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <inheritdoc cref="GetScopeInfoAsync(string, DbConnection, DbTransaction)"/>
        public virtual Task<ScopeInfo> GetScopeInfoAsync(DbConnection connection = default, DbTransaction transaction = default)
            => GetScopeInfoAsync(SyncOptions.DefaultScopeName, connection, transaction);


        /// <summary>
        /// Get a scope info from the remote data source. A scope contains the <see cref="SyncSetup"/> setup and the <see cref="SyncSet"/> schema.
        /// <para>
        /// If the <strong>scope_info</strong> table is not existing, it will be created. The setup argument is used to get the tables schema and fill the <strong>Setup</strong> and <strong>Schema</strong> properties.
        /// </para>
        /// <example>
        /// <code>
        ///  var remoteOrchestrator = new RemoteOrchestrator(clientProvider);
        ///  var setup = new SyncSetup("Product, ProductCategory");
        ///  var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);
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
        /// <param name="setup"></param>
        /// <param name="connection">Optional Connection</param>
        /// <param name="transaction">Optional Transaction</param>
        /// <returns></returns>
        public virtual Task<ScopeInfo> GetScopeInfoAsync(SyncSetup setup, DbConnection connection = default, DbTransaction transaction = default)
            => GetScopeInfoAsync(SyncOptions.DefaultScopeName, setup, connection, transaction);


        /// <inheritdoc cref="GetScopeInfoAsync(SyncSetup, DbConnection, DbTransaction)"/>
        public virtual async Task<ScopeInfo> GetScopeInfoAsync(string scopeName, SyncSetup setup, DbConnection connection = default, DbTransaction transaction = default)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeName);

            try
            {
                ScopeInfo sScopeInfo;
                (context, sScopeInfo, _) = await InternalEnsureScopeInfoAsync(context, setup, false, connection, transaction, default, default).ConfigureAwait(false);

                return sScopeInfo;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        internal virtual async Task<(SyncContext context, ScopeInfo serverScopeInfo, bool shouldProvision)> InternalEnsureScopeInfoAsync(SyncContext context, SyncSetup setup, bool overwrite,
            DbConnection connection, DbTransaction transaction, IProgress<ProgressArgs> progress, CancellationToken cancellationToken)
        {
            try
            {
                bool shouldProvision = false;

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

                    ScopeInfo sScopeInfo;
                    bool sScopeInfoExists;
                    (context, sScopeInfoExists) = await this.InternalExistsScopeInfoAsync(context.ScopeName, context, runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    bool shouldSave = false;

                    if (!sScopeInfoExists)
                    {
                        sScopeInfo = this.InternalCreateScopeInfo(context.ScopeName);
                        shouldSave = true;
                    }
                    else
                    {
                        (context, sScopeInfo) = await this.InternalLoadScopeInfoAsync(context, runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                    }

                    // if serverscopeinfo is a new, because we never run any sync before, grab schema and affect setup
                    if (setup != null && setup.Tables.Count > 0)
                    {
                        if ((sScopeInfo.Setup == null && sScopeInfo.Schema == null) || overwrite)
                        {
                            SyncSet schema;
                            (context, schema) = await this.InternalGetSchemaAsync(context, setup, runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);
                            sScopeInfo.Setup = setup;
                            sScopeInfo.Schema = schema;

                            // Checking if we have already some scopes
                            // Then gets the first scope to get the tracking tables & sp prefixes
                            List<ScopeInfo> allScopes;
                            (context, allScopes) = await this.InternalLoadAllScopeInfosAsync(context, runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

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
                        (context, sScopeInfo) = await this.InternalSaveScopeInfoAsync(sScopeInfo, context, runner.Connection, runner.Transaction, runner.Progress, runner.CancellationToken).ConfigureAwait(false);

                    await runner.CommitAsync().ConfigureAwait(false);

                    return (context, sScopeInfo, shouldProvision);
                }
            }
            catch (Exception ex)
            {
                string message = null;

                message += $"Overwrite:{overwrite}.";

                throw GetSyncError(context, ex, message);
            }
        }


        /// <summary>
        /// Check 
        /// </summary>
        internal virtual async Task<(SyncContext, bool, ScopeInfo)> InternalIsConflictingSetupAsync(SyncContext context, SyncSetup inputSetup, ScopeInfo sScopeInfo,
            DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (sScopeInfo.Schema == null)
                return (context, false, sScopeInfo);

            try
            {
                if (inputSetup != null && sScopeInfo.Setup != null && !sScopeInfo.Setup.EqualsByProperties(inputSetup))
                {
                    var conflictingSetupArgs = new ConflictingSetupArgs(context, inputSetup, null, sScopeInfo, connection, transaction);
                    await this.InterceptAsync(conflictingSetupArgs, progress, cancellationToken).ConfigureAwait(false);

                    if (conflictingSetupArgs.Action == ConflictingSetupAction.Rollback)
                        throw new SetupConflictOnServerException(inputSetup, sScopeInfo.Setup);

                    if (conflictingSetupArgs.Action == ConflictingSetupAction.Abort)
                        return (context, true, sScopeInfo);

                    // re affect scope infos
                    sScopeInfo = conflictingSetupArgs.ServerScopeInfo;
                }

                // We gave 2 chances to user to edit the setup and fill correct values.
                // Final check, but if not valid, raise an error
                if (inputSetup != null && sScopeInfo.Setup != null && !sScopeInfo.Setup.EqualsByProperties(inputSetup))
                    throw new SetupConflictOnServerException(inputSetup, sScopeInfo.Setup);

                return (context, false, sScopeInfo);
            }
            catch (SetupConflictOnServerException)
            {
                // direct throw because message is already really long and we don't want to duplicate it
                throw;
            }
            catch (Exception ex)
            {
                string message = null;

                if (inputSetup != null)
                    message += $"Input Setup:{Serializer.Serialize(inputSetup).ToUtf8String()}.";

                if (sScopeInfo != null && sScopeInfo.Setup != null)
                    message += $"Server Setup:{Serializer.Serialize(sScopeInfo.Setup).ToUtf8String()}.";
                throw GetSyncError(context, ex, message);
            }
        }
    }
}
