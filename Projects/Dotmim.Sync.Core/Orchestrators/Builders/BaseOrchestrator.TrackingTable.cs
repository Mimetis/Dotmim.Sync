
using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public abstract partial class BaseOrchestrator
    {

        /// <summary>
        /// Create a <strong>Tracking Table</strong> for a given table present in an existing scopeInfo.
        /// <example>
        /// <code>
        /// var remoteOrchestrator = new RemoteOrchestrator(sqlProvider, options);
        /// var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);
        /// await remoteOrchestrator.CreateTrackingTableAsync(scopeInfo, "Employee");
        /// </code>
        /// </example>
        /// </summary>
        /// <param name="scopeInfo">ScopeInfo instance used to defines tracking table generation (name, prefix, suffix...).</param>
        /// <param name="tableName"><strong>Table Name</strong>. Should exists in ScopeInfo instance.</param>
        /// <param name="schemaName">Optional <strong>Schema Name</strong>. Only available for <strong>Sql Server</strong>.</param>
        /// <param name="overwrite">If specified the tracking table is dropped, if exists, then created.</param>
        /// <param name="connection">Optional Connection</param>
        /// <param name="transaction">Optional Transaction</param>
        public async Task<bool> CreateTrackingTableAsync(ScopeInfo scopeInfo, string tableName, string schemaName = default, bool overwrite = false, 
            DbConnection connection = null, DbTransaction transaction = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            try
            {
                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return false;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return false;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Provisioning, connection, transaction).ConfigureAwait(false);
                bool hasBeenCreated = false;

                // Get table builder
                var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                bool schemaExists;
                (context, schemaExists) = await InternalExistsSchemaAsync(scopeInfo, context, tableBuilder, 
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                if (!schemaExists)
                    (context, _) = await InternalCreateSchemaAsync(scopeInfo,context, tableBuilder, 
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                bool exists;
                (context, exists) = await InternalExistsTrackingTableAsync(scopeInfo, context, tableBuilder, 
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                // should create only if not exists OR if overwrite has been set
                var shouldCreate = !exists || overwrite;

                if (shouldCreate)
                {
                    // Drop if already exists and we need to overwrite
                    if (exists && overwrite)
                        (context, _) = await InternalDropTrackingTableAsync(scopeInfo, context, tableBuilder, 
                            runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                    (context, hasBeenCreated) = await InternalCreateTrackingTableAsync(scopeInfo, context, tableBuilder, 
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);
                }

                await runner.CommitAsync().ConfigureAwait(false);

                return hasBeenCreated;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }


        /// <summary>
        /// Check if a <strong>Tracking Table</strong> exists, for a given table present in an existing scopeInfo.
        /// <example>
        /// <code>
        /// var remoteOrchestrator = new RemoteOrchestrator(sqlProvider, options);
        /// var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);
        /// var exists = await remoteOrchestrator.ExistTrackingTableAsync(scopeInfo, "Employee");
        /// </code>
        /// </example>
        /// </summary>
        /// <param name="scopeInfo">ScopeInfo instance used to defines tracking table generation (name, prefix, suffix...).</param>
        /// <param name="tableName"><strong>Table Name</strong>. Should exists in ScopeInfo instance.</param>
        /// <param name="schemaName">Optional <strong>Schema Name</strong>. Only available for <strong>Sql Server</strong>.</param>
        /// <param name="connection">Optional Connection</param>
        /// <param name="transaction">Optional Transaction</param>
        public async Task<bool> ExistTrackingTableAsync(ScopeInfo scopeInfo, string tableName, string schemaName = default,
            DbConnection connection = null, DbTransaction transaction = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            try
            {
                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return false;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return false;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.None, connection, transaction).ConfigureAwait(false);

                // Get table builder
                var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                bool exists;
                (context, exists) = await InternalExistsTrackingTableAsync(scopeInfo, context, tableBuilder,
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                return exists;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Create all <strong>Tracking Tables</strong> for a given table present in an existing scopeInfo.
        /// <example>
        /// <code>
        /// var remoteOrchestrator = new RemoteOrchestrator(sqlProvider, options);
        /// var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);
        /// await remoteOrchestrator.CreateTrackingTablesAsync(scopeInfo);
        /// </code>
        /// </example>
        /// </summary>
        /// <param name="scopeInfo">ScopeInfo instance used to defines tracking table generation (name, prefix, suffix...).</param>
        /// <param name="overwrite">If specified the tracking table is dropped, if exists, then created.</param>
        /// <param name="connection">Optional Connection</param>
        /// <param name="transaction">Optional Transaction</param>
        public async Task<bool> CreateTrackingTablesAsync(ScopeInfo scopeInfo, bool overwrite = false,
            DbConnection connection = null, DbTransaction transaction = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            try
            {
                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return false;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Provisioning, connection, transaction).ConfigureAwait(false);
                var atLeastOneHasBeenCreated = false;

                foreach (var schemaTable in scopeInfo.Schema.Tables)
                {
                    // Get table builder
                    var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                    bool schemaExists;
                    (context, schemaExists) = await InternalExistsSchemaAsync(scopeInfo, context, tableBuilder, 
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                    if (!schemaExists)
                        (context, _) = await InternalCreateSchemaAsync(scopeInfo, context, tableBuilder, 
                            runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                    bool exists;
                    (context, exists) = await InternalExistsTrackingTableAsync(scopeInfo, context, tableBuilder, 
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                    // should create only if not exists OR if overwrite has been set
                    var shouldCreate = !exists || overwrite;

                    if (shouldCreate)
                    {
                        // Drop if already exists and we need to overwrite
                        if (exists && overwrite)
                           (context, _) =  await InternalDropTrackingTableAsync(scopeInfo, context, tableBuilder, 
                               runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                        bool hasBeenCreated;
                        (context, hasBeenCreated) = await InternalCreateTrackingTableAsync(scopeInfo, context, tableBuilder, 
                            runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                        if (hasBeenCreated)
                            atLeastOneHasBeenCreated = true;
                    }
                }
                await runner.CommitAsync().ConfigureAwait(false);

                return atLeastOneHasBeenCreated;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }


        /// <summary>
        /// Drop a tracking table
        /// </summary>
        public async Task<bool> DropTrackingTableAsync(ScopeInfo scopeInfo, string tableName, string schemaName = default,
            DbConnection connection = null, DbTransaction transaction = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);

            try
            {
                if (scopeInfo.Schema == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return false;

                var schemaTable = scopeInfo.Schema.Tables[tableName, schemaName];

                if (schemaTable == null)
                    return false;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.Deprovisioning, connection, transaction).ConfigureAwait(false);
                bool hasBeenDropped = false;

                var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                bool exists;
                (context, exists) = await InternalExistsTrackingTableAsync(scopeInfo, context, tableBuilder, 
                    runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                if (exists)
                    (context, hasBeenDropped) = await InternalDropTrackingTableAsync(scopeInfo, context, tableBuilder, 
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                return hasBeenDropped;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }

        }

        /// <summary>
        /// Drop all <strong>Tracking Tables</strong> for a given table present in an existing scopeInfo.
        /// <example>
        /// <code>
        /// var remoteOrchestrator = new RemoteOrchestrator(sqlProvider, options);
        /// var scopeInfo = await remoteOrchestrator.GetScopeInfoAsync(setup);
        /// await remoteOrchestrator.DropTrackingTablesAsync(scopeInfo);
        /// </code>
        /// </example>
        /// </summary>
        /// <param name="scopeInfo">ScopeInfo instance used to defines tracking table generation (name, prefix, suffix...).</param>
        /// <param name="connection">Optional Connection</param>
        /// <param name="transaction">Optional Transaction</param>
        public async Task<bool> DropTrackingTablesAsync(ScopeInfo scopeInfo, DbConnection connection = null, DbTransaction transaction = null)
        {
            var context = new SyncContext(Guid.NewGuid(), scopeInfo.Name);
            try
            {
                if (scopeInfo.Schema == null || scopeInfo.Schema.Tables == null || !scopeInfo.Schema.HasTables || !scopeInfo.Schema.HasColumns)
                    return false;

                await using var runner = await this.GetConnectionAsync(context, SyncMode.WithTransaction, SyncStage.Deprovisioning, connection, transaction).ConfigureAwait(false);
                bool atLeastOneTrackingTableHasBeenDropped = false;

                foreach (var schemaTable in scopeInfo.Schema.Tables.Reverse())
                {
                    // Get table builder
                    var tableBuilder = this.GetTableBuilder(schemaTable, scopeInfo);

                    bool exists;
                    (context, exists) = await InternalExistsTrackingTableAsync(scopeInfo, context, tableBuilder, 
                        runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);

                    if (exists)
                        (context, atLeastOneTrackingTableHasBeenDropped) = await InternalDropTrackingTableAsync(scopeInfo, context, tableBuilder, 
                            runner.Connection, runner.Transaction, runner.CancellationToken, runner.Progress).ConfigureAwait(false);
                }

                await runner.CommitAsync().ConfigureAwait(false);

                return atLeastOneTrackingTableHasBeenDropped;
            }
            catch (Exception ex)
            {
                throw GetSyncError(context, ex);
            }
        }

        /// <summary>
        /// Internal create tracking table routine
        /// </summary>
        internal virtual async Task<(SyncContext context, bool crated)> InternalCreateTrackingTableAsync(
            ScopeInfo scopeInfo, SyncContext context, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            if (Provider == null)
                throw new MissingProviderException(nameof(InternalCreateTrackingTableAsync));

            await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.Provisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            if (tableBuilder.TableDescription.Columns.Count <= 0)
                throw new MissingsColumnException(tableBuilder.TableDescription.GetFullName());

            if (tableBuilder.TableDescription.PrimaryKeys.Count <= 0)
                throw new MissingPrimaryKeyException(tableBuilder.TableDescription.GetFullName());

            using var command = await tableBuilder.GetCreateTrackingTableCommandAsync(runner.Connection, runner.Transaction).ConfigureAwait(false);

            if (command == null)
                return (context, false);

            var (_, trackingTableName) = this.Provider.GetParsers(tableBuilder.TableDescription, scopeInfo.Setup);

            var action = await this.InterceptAsync(new TrackingTableCreatingArgs(context, tableBuilder.TableDescription, trackingTableName, command, runner.Connection, runner.Transaction), 
                runner.Progress, runner.CancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return (context, false);

            // Parametrized command timeout established if exist
            if (this.Options.DbCommandTimeout.HasValue)
                action.Command.CommandTimeout = this.Options.DbCommandTimeout.Value;

            await this.InterceptAsync(new ExecuteCommandArgs(context, action.Command, default, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

            await action.Command.ExecuteNonQueryAsync().ConfigureAwait(false);

            await this.InterceptAsync(new TrackingTableCreatedArgs(context, tableBuilder.TableDescription, trackingTableName, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

            action.Command.Dispose();

            return (context, true);
        }

        /// <summary>
        /// Internal rename tracking table routine
        /// </summary>
        internal virtual async Task<(SyncContext context, bool renamed)> InternalRenameTrackingTableAsync(
            ScopeInfo scopeInfo, SyncContext context, ParserName oldTrackingTableName, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.Provisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            if (tableBuilder.TableDescription.Columns.Count <= 0)
                throw new MissingsColumnException(tableBuilder.TableDescription.GetFullName());

            if (tableBuilder.TableDescription.PrimaryKeys.Count <= 0)
                throw new MissingPrimaryKeyException(tableBuilder.TableDescription.GetFullName());

            using var command = await tableBuilder.GetRenameTrackingTableCommandAsync(oldTrackingTableName, runner.Connection, runner.Transaction).ConfigureAwait(false);

            if (command == null)
                return (context, false);

            var (_, trackingTableName) = this.Provider.GetParsers(tableBuilder.TableDescription, scopeInfo.Setup);


            var action = await this.InterceptAsync(new TrackingTableRenamingArgs(context, tableBuilder.TableDescription, trackingTableName, oldTrackingTableName, command, runner.Connection, runner.Transaction),
                runner.Progress, runner.CancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return (context, false);

            // Parametrized command timeout established if exist
            if (Options.DbCommandTimeout.HasValue)
                action.Command.CommandTimeout = Options.DbCommandTimeout.Value;
            
            await this.InterceptAsync(new ExecuteCommandArgs(context, action.Command, default, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

            await action.Command.ExecuteNonQueryAsync().ConfigureAwait(false);

            await this.InterceptAsync(new TrackingTableRenamedArgs(context, tableBuilder.TableDescription, trackingTableName, oldTrackingTableName, runner.Connection, runner.Transaction), runner.Progress, runner.CancellationToken).ConfigureAwait(false);

            action.Command.Dispose();

            return (context, true);
        }

        /// <summary>
        /// Internal drop tracking table routine
        /// </summary>
        internal virtual async Task<(SyncContext context, bool dropped)> InternalDropTrackingTableAsync(
            ScopeInfo scopeInfo, SyncContext context, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.Deprovisioning, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

            using var command = await tableBuilder.GetDropTrackingTableCommandAsync(connection, transaction).ConfigureAwait(false);

            if (command == null)
                return (context, false);

            var (_, trackingTableName) = this.Provider.GetParsers(tableBuilder.TableDescription, scopeInfo.Setup);

            var action = await this.InterceptAsync(new TrackingTableDroppingArgs(context, tableBuilder.TableDescription, trackingTableName, command, runner.Connection, runner.Transaction), progress, cancellationToken).ConfigureAwait(false);

            if (action.Cancel || action.Command == null)
                return (context, false);

            // Parametrized command timeout established if exist
            if (Options.DbCommandTimeout.HasValue)
                action.Command.CommandTimeout = Options.DbCommandTimeout.Value;

            await this.InterceptAsync(new ExecuteCommandArgs(context, action.Command, default, runner.Connection, runner.Transaction), progress, cancellationToken).ConfigureAwait(false);

            await action.Command.ExecuteNonQueryAsync().ConfigureAwait(false);
            await this.InterceptAsync(new TrackingTableDroppedArgs(context, tableBuilder.TableDescription, trackingTableName, runner.Connection, runner.Transaction), progress, cancellationToken).ConfigureAwait(false);

            action.Command.Dispose();

            return (context, true);
        }

        /// <summary>
        /// Internal exists tracking table procedure routine
        /// </summary>
        internal virtual async Task<(SyncContext context, bool exists)> InternalExistsTrackingTableAsync(ScopeInfo scopeInfo, SyncContext context, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            await using var runner = await this.GetConnectionAsync(context, SyncMode.NoTransaction, SyncStage.None, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
            // Get exists command
            using var existsCommand = await tableBuilder.GetExistsTrackingTableCommandAsync(runner.Connection, runner.Transaction).ConfigureAwait(false);

            if (existsCommand == null)
                return (context, false);

            await this.InterceptAsync(new ExecuteCommandArgs(context, existsCommand, default, runner.Connection, runner.Transaction), progress, cancellationToken).ConfigureAwait(false);

            // Parametrized command timeout established if exist
            if (Options.DbCommandTimeout.HasValue)
            {
                existsCommand.CommandTimeout = Options.DbCommandTimeout.Value;
            }

            var existsResultObject = await existsCommand.ExecuteScalarAsync().ConfigureAwait(false);
            var exists = Convert.ToInt32(existsResultObject) > 0;
            await runner.CommitAsync().ConfigureAwait(false);
            return (context, exists);

        }
    }
}
