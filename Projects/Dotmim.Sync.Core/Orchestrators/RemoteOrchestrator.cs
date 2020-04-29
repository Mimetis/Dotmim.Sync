﻿using Dotmim.Sync.Batch;
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
    public class RemoteOrchestrator : BaseOrchestrator
    {

        /// <summary>
        /// Gets the sync side of this Orchestrator. RemoteOrchestrator is always used on server side
        /// </summary>
        public override SyncSide Side => SyncSide.ServerSide;


        /// <summary>
        /// Create a remote orchestrator, used to orchestrates the whole sync on the server side
        /// </summary>
        public RemoteOrchestrator(CoreProvider provider, SyncOptions options, SyncSetup setup, string scopeName = SyncOptions.DefaultScopeName)
           : base(provider, options, setup, scopeName)
        {
        }



        /// <summary>
        /// Get the server scope histories
        /// </summary>
        public virtual async Task<List<ServerHistoryScopeInfo>> GetServerHistoryScopes(CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            // Get context or create a new one
            var ctx = this.GetContext();
            List<ServerHistoryScopeInfo> serverHistoryScopes = null;

            using (var connection = this.Provider.CreateConnection())
            {
                try
                {
                    ctx.SyncStage = SyncStage.ScopeLoading;

                    // Open connection
                    await this.OpenConnectionAsync(connection, cancellationToken).ConfigureAwait(false);

                    DbTransaction transaction;
                    // Create a transaction
                    using (transaction = connection.BeginTransaction())
                    {
                        await this.InterceptAsync(new TransactionOpenedArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                        // Ensore scope history table exists
                        ctx = await this.Provider.EnsureServerHistoryScopeAsync(ctx, this.Options.ScopeInfoTableName, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // Get scope if exists
                        (ctx, serverHistoryScopes) = await this.Provider.GetServerHistoryScopesAsync(ctx, this.Options.ScopeInfoTableName, this.ScopeName, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        await this.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                        transaction.Commit();
                    }

                    ctx.SyncStage = SyncStage.ScopeLoaded;

                    await this.CloseConnectionAsync(connection, cancellationToken).ConfigureAwait(false);

                }
                catch (Exception ex)
                {
                    RaiseError(ex);
                }
                finally
                {
                    if (connection != null && connection.State == ConnectionState.Open)
                        connection.Close();
                }

                return serverHistoryScopes;
            }
        }


        /// <summary>
        /// Write a server scope 
        /// </summary>
        public virtual async Task<ServerScopeInfo> WriteServerScopeAsync(ServerScopeInfo serverScopeInfo, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            // Get context or create a new one
            var ctx = this.GetContext();
            
            using (var connection = this.Provider.CreateConnection())
            {
                try
                {
                    ctx.SyncStage = SyncStage.ScopeWriting;

                    // Open connection
                    await this.OpenConnectionAsync(connection, cancellationToken).ConfigureAwait(false);

                    DbTransaction transaction;
                    // Create a transaction
                    using (transaction = connection.BeginTransaction())
                    {
                        await this.InterceptAsync(new TransactionOpenedArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                        // await this.InterceptAsync(new ServerScopeLoadingArgs(ctx, this.ScopeName, this.Options.ScopeInfoTableName, connection, transaction), cancellationToken).ConfigureAwait(false);

                        // Create scope server
                        ctx = await this.Provider.EnsureServerScopeAsync(ctx, this.Options.ScopeInfoTableName, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // Write scopes locally
                        ctx = await this.Provider.WriteServerScopeAsync(ctx, this.Options.ScopeInfoTableName, serverScopeInfo, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        await this.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                        transaction.Commit();
                    }

                    ctx.SyncStage = SyncStage.ScopeWrited;

                    await this.CloseConnectionAsync(connection, cancellationToken).ConfigureAwait(false);

                    //var scopeArgs = new ServerScopeLoadedArgs(ctx, serverScopeInfo, connection, transaction);
                    //await this.InterceptAsync(scopeArgs, cancellationToken).ConfigureAwait(false);
                    //this.ReportProgress(ctx, progress, scopeArgs);

                }
                catch (Exception ex)
                {
                    RaiseError(ex);
                }
                finally
                {
                    if (connection != null && connection.State == ConnectionState.Open)
                        connection.Close();
                }

                this.logger.LogInformation(SyncEventsId.WriteServerScope, serverScopeInfo);

                return serverScopeInfo;
            }
        }

        /// <summary>
        /// Get the local configuration, ensures the local scope is created
        /// </summary>
        /// <returns>Server scope info, containing all scopes names, version, setup and related schema infos</returns>
        public virtual async Task<ServerScopeInfo> GetServerScopeAsync(CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            // Get context or create a new one
            var ctx = this.GetContext();
            ServerScopeInfo serverScopeInfo = null;

            using (var connection = this.Provider.CreateConnection())
            {
                try
                {
                    ctx.SyncStage = SyncStage.ScopeLoading;

                    // Open connection
                    await this.OpenConnectionAsync(connection, cancellationToken).ConfigureAwait(false);

                    DbTransaction transaction;
                    // Create a transaction
                    using (transaction = connection.BeginTransaction())
                    {
                        await this.InterceptAsync(new TransactionOpenedArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                        await this.InterceptAsync(new ServerScopeLoadingArgs(ctx, this.ScopeName, this.Options.ScopeInfoTableName, connection, transaction), cancellationToken).ConfigureAwait(false);

                        // Create scope server
                        ctx = await this.Provider.EnsureServerScopeAsync(ctx, this.Options.ScopeInfoTableName, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // Create scope history
                        ctx = await this.Provider.EnsureServerHistoryScopeAsync(ctx, this.Options.ScopeInfoTableName, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // Get scope if exists
                        (ctx, serverScopeInfo) = await this.Provider.GetServerScopeAsync(ctx, this.Options.ScopeInfoTableName, this.ScopeName, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // Compare serverscope setup with current
                        if (serverScopeInfo.Setup != this.Setup)
                        {
                            this.logger.LogDebug(SyncEventsId.GetServerScope, $"[{ctx.SyncStage}] database {connection.Database}. serverScopeInfo.Setup != this.Setup. Need migrations ");

                            SyncSet schema;
                            // 1) Get Schema from remote provider
                            (ctx, schema) = await this.Provider.GetSchemaAsync(ctx, this.Setup, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                            schema.EnsureSchema();

                            // Launch InterceptAsync on Migrating
                            await this.InterceptAsync(new DatabaseMigratingArgs(ctx, schema, serverScopeInfo.Setup, this.Setup, connection, transaction), cancellationToken).ConfigureAwait(false);

                            // Migrate the old setup (serverScopeInfo.Setup) to the new setup (this.Setup) based on the new schema 
                            await this.Provider.MigrationAsync(ctx, schema, serverScopeInfo.Setup, this.Setup, false, connection, transaction, cancellationToken, progress);

                            // Now call the ProvisionAsync() to provision new tables
                            var provision = SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;

                            // Provision new tables if needed
                            await this.Provider.ProvisionAsync(ctx, schema, this.Setup, provision, this.Options.ScopeInfoTableName, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                            serverScopeInfo.Setup = this.Setup;
                            serverScopeInfo.Schema = schema;

                            // Write scopes locally
                            ctx = await this.Provider.WriteServerScopeAsync(ctx, this.Options.ScopeInfoTableName, serverScopeInfo, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                            // InterceptAsync Migrated
                            var args = new DatabaseMigratedArgs(ctx, schema, this.Setup, connection, transaction);
                            await this.InterceptAsync(args, cancellationToken).ConfigureAwait(false);
                            this.ReportProgress(ctx, progress, args);
                        }

                        await this.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                        transaction.Commit();
                    }

                    ctx.SyncStage = SyncStage.ScopeLoaded;

                    await this.CloseConnectionAsync(connection, cancellationToken).ConfigureAwait(false);

                    var scopeArgs = new ServerScopeLoadedArgs(ctx, serverScopeInfo, connection, transaction);
                    await this.InterceptAsync(scopeArgs, cancellationToken).ConfigureAwait(false);
                    this.ReportProgress(ctx, progress, scopeArgs);

                }
                catch (Exception ex)
                {
                    RaiseError(ex);
                }
                finally
                {
                    if (connection != null && connection.State == ConnectionState.Open)
                        connection.Close();
                }

                this.logger.LogInformation(SyncEventsId.GetServerScope, serverScopeInfo);

                return serverScopeInfo;
            }
        }

        /// <summary>
        /// Ensure the schema is readed from the server, based on the Setup instance.
        /// Creates all required tables (server_scope tables) and provision all tables (tracking, stored proc, triggers and so on...)
        /// Then return the schema readed
        /// </summary>
        /// <returns>current context, the local scope info created or get from the database and the configuration from the client if changed </returns>
        public virtual async Task<ServerScopeInfo> EnsureSchemaAsync(CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            // Get context or create a new one
            var ctx = this.GetContext();

            ServerScopeInfo serverScopeInfo = null;
            using (var connection = this.Provider.CreateConnection())
            {
                try
                {
                    // starting with scope loading
                    ctx.SyncStage = SyncStage.ScopeLoading;

                    // Open connection
                    await this.OpenConnectionAsync(connection, cancellationToken).ConfigureAwait(false);

                    DbTransaction transaction;
                    // Create a transaction
                    using (transaction = connection.BeginTransaction())
                    {
                        // Open the connection
                        // Interceptors
                        await this.InterceptAsync(new TransactionOpenedArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                        // raise scope loading
                        await this.InterceptAsync(new ScopeLoadingArgs(ctx, this.ScopeName, this.Options.ScopeInfoTableName, connection, transaction), cancellationToken).ConfigureAwait(false);

                        // Create scope server
                        ctx = await this.Provider.EnsureServerScopeAsync(ctx, this.Options.ScopeInfoTableName, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // Create scope history
                        ctx = await this.Provider.EnsureServerHistoryScopeAsync(ctx, this.Options.ScopeInfoTableName, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // Get scope if exists
                        (ctx, serverScopeInfo) = await this.Provider.GetServerScopeAsync(ctx, this.Options.ScopeInfoTableName, this.ScopeName, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        this.logger.LogInformation(SyncEventsId.GetServerScope, serverScopeInfo);

                        // raise scope loaded
                        ctx.SyncStage = SyncStage.ScopeLoaded;
                        var scopeArgs = new ServerScopeLoadedArgs(ctx, serverScopeInfo, connection, transaction);
                        await this.InterceptAsync(scopeArgs, cancellationToken).ConfigureAwait(false);
                        this.ReportProgress(ctx, progress, scopeArgs);

                        SyncSet schema;
                        // Let's compare this serverScopeInfo with the current Setup
                        // If schema is null :
                        // - Read the schema from database based on this.Setup
                        // - Provision the database with this schema
                        // - Write the scope with this.Setup and schema
                        // If schema is not null :
                        // - Compare saved setup with current setup
                        // - If not equals:
                        // - Read schema from database based on this.Setup
                        if (serverScopeInfo.Schema == null)
                        {
                            // So far, we don't have already a database provisionned
                            ctx.SyncStage = SyncStage.Provisioning;

                            this.logger.LogInformation(SyncEventsId.GetSchema, this.Setup);

                            // 1) Get Schema from remote provider
                            (ctx, schema) = await this.Provider.GetSchemaAsync(ctx, this.Setup, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                            schema.EnsureSchema();

                            // 2) Ensure databases are ready
                            var provision = SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;
                            await this.InterceptAsync(new DatabaseProvisioningArgs(ctx, provision, schema, connection, transaction), cancellationToken).ConfigureAwait(false);

                            ctx = await this.Provider.ProvisionAsync(ctx, schema, this.Setup, provision, this.Options.ScopeInfoTableName, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                            // So far, we don't have already a database provisionned
                            ctx.SyncStage = SyncStage.Provisioned;

                            // Generated the first serverscope to be updated
                            serverScopeInfo.LastCleanupTimestamp = 0;
                            serverScopeInfo.Schema = schema;
                            serverScopeInfo.Setup = this.Setup;
                            serverScopeInfo.Version = "1";

                            // 3) Update server scope
                            ctx = await this.Provider.WriteServerScopeAsync(ctx, this.Options.ScopeInfoTableName, serverScopeInfo, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                            var args = new DatabaseProvisionedArgs(ctx, provision, schema, connection, transaction);
                            await this.InterceptAsync(args, cancellationToken).ConfigureAwait(false);
                            this.ReportProgress(ctx, progress, args);
                        }
                        else
                        {
                            // Setup stored on local or remote is different from the one provided.
                            // So, we can migrate
                            if (serverScopeInfo.Setup != this.Setup)
                            {
                                // 1) Get Schema from remote provider
                                (ctx, schema) = await this.Provider.GetSchemaAsync(ctx, this.Setup, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                                schema.EnsureSchema();

                                // Launch InterceptAsync on Migrating
                                await this.InterceptAsync(new DatabaseMigratingArgs(ctx, schema, serverScopeInfo.Setup, this.Setup, connection, transaction), cancellationToken).ConfigureAwait(false);

                                // Migrate the old setup (serverScopeInfo.Setup) to the new setup (this.Setup) based on the new schema 
                                await this.Provider.MigrationAsync(ctx, schema, serverScopeInfo.Setup, this.Setup, false, connection, transaction, cancellationToken, progress);

                                // Now call the ProvisionAsync() to provision new tables
                                var provision = SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;

                                // Provision new tables if needed
                                await this.Provider.ProvisionAsync(ctx, schema, this.Setup, provision, this.Options.ScopeInfoTableName, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                                serverScopeInfo.Setup = this.Setup;
                                serverScopeInfo.Schema = schema;
                                
                                // Write scopes locally
                                ctx = await this.Provider.WriteServerScopeAsync(ctx, this.Options.ScopeInfoTableName, serverScopeInfo, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                                // InterceptAsync Migrated
                                var args = new DatabaseMigratedArgs(ctx, schema, this.Setup, connection, transaction);
                                await this.InterceptAsync(args, cancellationToken).ConfigureAwait(false);
                                this.ReportProgress(ctx, progress, args);
                            }

                            // Get the schema saved on server
                            schema = serverScopeInfo.Schema;
                        }

                        await this.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                        transaction.Commit();
                    }
                    await this.CloseConnectionAsync(connection, cancellationToken).ConfigureAwait(false);

                }
                catch (Exception ex)
                {
                    RaiseError(ex);
                }
                finally
                {
                    if (connection != null && connection.State == ConnectionState.Open)
                        connection.Close();
                }

                this.logger.LogInformation(SyncEventsId.EnsureSchema, serverScopeInfo);

                return serverScopeInfo;
            }
        }


        /// <summary>
        /// Migrate an old setup configuration to a new one. This method is usefull if you are changing your SyncSetup when a database has been already configured previously
        /// </summary>
        public virtual async Task MigrationAsync(SyncSetup oldSetup, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            // Get context or create a new one
            var ctx = this.GetContext();
            SyncSet schema;

            using (var connection = this.Provider.CreateConnection())
            {
                try
                {
                    ctx.SyncStage = SyncStage.Migrating;

                    // Open connection
                    await this.OpenConnectionAsync(connection, cancellationToken).ConfigureAwait(false);

                    // Create a transaction
                    using (var transaction = connection.BeginTransaction())
                    {
                        await this.InterceptAsync(new TransactionOpenedArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                        // Get Schema from remote provider
                        (ctx, schema) = await this.Provider.GetSchemaAsync(ctx, this.Setup, connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                        schema.EnsureSchema();

                        // Launch InterceptAsync on Migrating
                        await this.InterceptAsync(new DatabaseMigratingArgs(ctx, schema, oldSetup, this.Setup, connection, transaction), cancellationToken).ConfigureAwait(false);

                        // Migrate the db structure
                        await this.Provider.MigrationAsync(ctx, schema, oldSetup, this.Setup, false, connection, transaction, cancellationToken, progress);

                        // Now call the ProvisionAsync() to provision new tables
                        var provision = SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;

                        // Provision new tables if needed
                        await this.Provider.ProvisionAsync(ctx, schema, this.Setup, provision, this.Options.ScopeInfoTableName, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        ServerScopeInfo remoteScope = null;

                        (ctx, remoteScope) = await this.Provider.GetServerScopeAsync(ctx, this.Options.ScopeInfoTableName, this.ScopeName, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        remoteScope.Setup = this.Setup;
                        remoteScope.Schema = schema;

                        // Write scopes locally
                        ctx = await this.Provider.WriteServerScopeAsync(ctx, this.Options.ScopeInfoTableName, remoteScope, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        await this.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                        transaction.Commit();
                    }

                    ctx.SyncStage = SyncStage.Migrated;

                    await this.CloseConnectionAsync(connection, cancellationToken).ConfigureAwait(false);

                    // InterceptAsync Migrated
                    var args = new DatabaseMigratedArgs(ctx, schema, this.Setup);
                    await this.InterceptAsync(args, cancellationToken).ConfigureAwait(false);
                    this.ReportProgress(ctx, progress, args);

                }
                catch (Exception ex)
                {
                    RaiseError(ex);
                }
                finally
                {
                    if (connection != null && connection.State == ConnectionState.Open)
                        connection.Close();
                }
            }
        }



        /// <summary>
        /// Apply changes on remote provider
        /// </summary>
        internal virtual async Task<(long RemoteClientTimestamp, BatchInfo ServerBatchInfo, ConflictResolutionPolicy ServerPolicy, DatabaseChangesApplied ClientChangesApplied, DatabaseChangesSelected ServerChangesSelected)>
            ApplyThenGetChangesAsync(ScopeInfo clientScope, BatchInfo clientBatchInfo, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            // Get context or create a new one
            var ctx = this.GetContext();

            long remoteClientTimestamp = 0L;
            DatabaseChangesSelected serverChangesSelected = null;
            DatabaseChangesApplied clientChangesApplied = null;
            BatchInfo serverBatchInfo = null;
            SyncSet schema = null;

            using (var connection = this.Provider.CreateConnection())
            {
                try
                {

                    ctx.SyncStage = SyncStage.ChangesApplying;

                    //Direction set to Upload
                    ctx.SyncWay = SyncWay.Upload;

                    // Open connection
                    await this.OpenConnectionAsync(connection, cancellationToken).ConfigureAwait(false);

                    DbTransaction transaction;
                    // Create two transactions
                    // First one to commit changes
                    // Second one to get changes now that everything is commited
                    using (transaction = connection.BeginTransaction())
                    {
                        await this.InterceptAsync(new TransactionOpenedArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                        ServerScopeInfo serverScopeInfo;
                        // Maybe here, get the schema from server, issue from client scope name
                        // Maybe then compare the schema version from client scope with schema version issued from server
                        // Maybe if different, raise an error ?
                        // Get scope if exists

                        // Getting server scope assumes we have already created the schema on server
                        (ctx, serverScopeInfo) = await this.Provider.GetServerScopeAsync(ctx, this.Options.ScopeInfoTableName, clientScope.Name,
                                            connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // Should we ?
                        if (serverScopeInfo.Schema == null)
                            throw new MissingRemoteOrchestratorSchemaException();

                        // Check if we have a version to control
                        if (!serverScopeInfo.Version.Equals(clientScope.Version, SyncGlobalization.DataSourceStringComparison))
                            throw new ArgumentException("Server schema version does not match client schema version");

                        // deserialiaze schema
                        schema = serverScopeInfo.Schema;

                        // Create message containing everything we need to apply on server side
                        var applyChanges = new MessageApplyChanges(Guid.Empty, clientScope.Id, false, clientScope.LastServerSyncTimestamp, schema, this.Setup, this.Options.ConflictResolutionPolicy,
                                        this.Options.DisableConstraintsOnApplyChanges, this.Options.UseBulkOperations, this.Options.CleanMetadatas, this.Options.CleanFolder, clientBatchInfo);

                        // call interceptor
                        await this.InterceptAsync(new DatabaseChangesApplyingArgs(ctx, applyChanges, connection, transaction), cancellationToken).ConfigureAwait(false);

                        // Call provider to apply changes
                        (ctx, clientChangesApplied) = await this.Provider.ApplyChangesAsync(ctx, applyChanges, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        await this.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                        // commit first transaction
                        transaction.Commit();
                    }

                    ctx.SyncStage = SyncStage.ChangesApplied;


                    this.logger.LogInformation(SyncEventsId.ApplyChanges, clientChangesApplied);

                    var databaseChangesAppliedArgs = new DatabaseChangesAppliedArgs(ctx, clientChangesApplied, connection, transaction);
                    await this.InterceptAsync(databaseChangesAppliedArgs, cancellationToken).ConfigureAwait(false);
                    this.ReportProgress(ctx, progress, databaseChangesAppliedArgs, connection, transaction);

                    ctx.SyncStage = SyncStage.ChangesSelecting;

                    using (transaction = connection.BeginTransaction())
                    {
                        await this.InterceptAsync(new TransactionOpenedArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                        //Direction set to Download
                        ctx.SyncWay = SyncWay.Download;

                        // JUST Before get changes, get the timestamp, to be sure to 
                        // get rows inserted / updated elsewhere since the sync is not over
                        remoteClientTimestamp = await this.Provider.GetLocalTimestampAsync(ctx, connection, transaction, cancellationToken, progress);

                        // Get if we need to get all rows from the datasource
                        var fromScratch = clientScope.IsNewScope || ctx.SyncType == SyncType.Reinitialize || ctx.SyncType == SyncType.ReinitializeWithUpload;

                        var message = new MessageGetChangesBatch(clientScope.Id, Guid.Empty, fromScratch, clientScope.LastServerSyncTimestamp, schema, this.Setup, this.Options.BatchSize, this.Options.BatchDirectory);

                        // Call interceptor
                        await this.InterceptAsync(new DatabaseChangesSelectingArgs(ctx, message, connection, transaction), cancellationToken).ConfigureAwait(false);

                        // When we get the chnages from server, we create the batches if it's requested by the client
                        // the batch decision comes from batchsize from client
                        (ctx, serverBatchInfo, serverChangesSelected) =
                            await this.Provider.GetChangeBatchAsync(ctx, message, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        if (cancellationToken.IsCancellationRequested)
                            cancellationToken.ThrowIfCancellationRequested();

                        // generate the new scope item
                        this.CompleteTime = DateTime.UtcNow;

                        var scopeHistory = new ServerHistoryScopeInfo
                        {
                            Id = clientScope.Id,
                            Name = clientScope.Name,
                            LastSyncTimestamp = remoteClientTimestamp,
                            LastSync = this.CompleteTime,
                            LastSyncDuration = this.CompleteTime.Value.Subtract(this.StartTime.Value).Ticks,
                        };

                        // Write scopes locally
                        ctx = await this.Provider.WriteServerHistoryScopeAsync(ctx, this.Options.ScopeInfoTableName, scopeHistory, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // Commit second transaction for getting changes
                        await this.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                        transaction.Commit();
                    }

                    // Event progress & interceptor
                    ctx.SyncStage = SyncStage.ChangesSelected;

                    await this.CloseConnectionAsync(connection, cancellationToken).ConfigureAwait(false);

                    this.logger.LogInformation(SyncEventsId.GetChanges, serverChangesSelected);

                    var tableChangesSelectedArgs = new DatabaseChangesSelectedArgs(ctx, remoteClientTimestamp, serverBatchInfo, serverChangesSelected, connection, transaction);
                    this.ReportProgress(ctx, progress, tableChangesSelectedArgs);
                    await this.InterceptAsync(tableChangesSelectedArgs, cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    RaiseError(ex);
                }
                finally
                {
                    if (connection != null && connection.State != ConnectionState.Closed)
                        connection.Close();
                }
                return (remoteClientTimestamp, serverBatchInfo, this.Options.ConflictResolutionPolicy, clientChangesApplied, serverChangesSelected);
            }

        }

        /// <summary>
        /// Get changes from remote database
        /// </summary>
        /// <returns></returns>
        public virtual async Task<(long RemoteClientTimestamp, BatchInfo ServerBatchInfo, DatabaseChangesSelected ServerChangesSelected)>
            GetChangesAsync(ScopeInfo clientScope, ServerScopeInfo serverScope = null, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            // Output
            long remoteClientTimestamp = 0L;
            BatchInfo serverBatchInfo = null;
            DatabaseChangesSelected serverChangesSelected = null;

            // Get context or create a new one
            var ctx = this.GetContext();
            using (var connection = this.Provider.CreateConnection())
            {
                try
                {
                    ctx.SyncStage = SyncStage.ChangesSelecting;

                    // Open connection
                    await this.OpenConnectionAsync(connection, cancellationToken).ConfigureAwait(false);

                    using (var transaction = connection.BeginTransaction())
                    {
                        await this.InterceptAsync(new TransactionOpenedArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                        //Direction set to Download
                        ctx.SyncWay = SyncWay.Download;

                        // Getting server scope assumes we have already created the schema on server
                        if (serverScope == null)
                            (ctx, serverScope) = await this.Provider.GetServerScopeAsync(ctx, this.Options.ScopeInfoTableName, clientScope.Name,
                                                connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // Should we ?
                        if (serverScope.Schema == null)
                            throw new MissingRemoteOrchestratorSchemaException();


                        // JUST Before get changes, get the timestamp, to be sure to 
                        // get rows inserted / updated elsewhere since the sync is not over
                        remoteClientTimestamp = await this.Provider.GetLocalTimestampAsync(ctx, connection, transaction, cancellationToken, progress);

                        // Get if we need to get all rows from the datasource
                        var fromScratch = clientScope.IsNewScope || ctx.SyncType == SyncType.Reinitialize || ctx.SyncType == SyncType.ReinitializeWithUpload;

                        var message = new MessageGetChangesBatch(clientScope.Id, Guid.Empty, fromScratch, clientScope.LastServerSyncTimestamp, serverScope.Schema, this.Setup, this.Options.BatchSize, this.Options.BatchDirectory);

                        // Call interceptor
                        await this.InterceptAsync(new DatabaseChangesSelectingArgs(ctx, message, connection, transaction), cancellationToken).ConfigureAwait(false);

                        // When we get the chnages from server, we create the batches if it's requested by the client
                        // the batch decision comes from batchsize from client
                        (ctx, serverBatchInfo, serverChangesSelected) =
                            await this.Provider.GetChangeBatchAsync(ctx, message, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // Commit second transaction for getting changes
                        await this.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                        transaction.Commit();
                    }


                    // Event progress & interceptor
                    ctx.SyncStage = SyncStage.ChangesSelected;

                    await this.CloseConnectionAsync(connection, cancellationToken).ConfigureAwait(false);

                    this.logger.LogInformation(SyncEventsId.GetChanges, serverChangesSelected);

                    var tableChangesSelectedArgs = new DatabaseChangesSelectedArgs(ctx, remoteClientTimestamp, serverBatchInfo, serverChangesSelected, connection);
                    this.ReportProgress(ctx, progress, tableChangesSelectedArgs);
                    await this.InterceptAsync(tableChangesSelectedArgs, cancellationToken).ConfigureAwait(false);

                }
                catch (Exception ex)
                {
                    RaiseError(ex);
                }
                finally
                {
                    if (connection != null && connection.State != ConnectionState.Closed)
                        connection.Close();
                }

                return (remoteClientTimestamp, serverBatchInfo, serverChangesSelected);
            }
        }

        /// <summary>
        /// Create a snapshot, based on the Setup object. 
        /// </summary>
        /// <param name="syncParameters">if not parameters are found in the SyncContext instance, will use thes sync parameters instead</param>
        /// <returns>Instance containing all information regarding the snapshot</returns>
        public virtual async Task<BatchInfo> CreateSnapshotAsync(SyncParameters syncParameters = null, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            // Get context or create a new one
            var ctx = this.GetContext();

            SyncSet schema = null;
            BatchInfo batchInfo = null;

            using (var connection = this.Provider.CreateConnection())
            {
                try
                {
                    ctx.SyncStage = SyncStage.SnapshotCreating;

                    if (string.IsNullOrEmpty(this.Options.SnapshotsDirectory) || this.Options.BatchSize <= 0)
                        throw new SnapshotMissingMandatariesOptionsException();

                    // check parameters
                    // If context has no parameters specified, and user specifies a parameter collection we switch them
                    if ((ctx.Parameters == null || ctx.Parameters.Count <= 0) && syncParameters != null && syncParameters.Count > 0)
                        ctx.Parameters = syncParameters;

                    // Open connection
                    await this.OpenConnectionAsync(connection, cancellationToken).ConfigureAwait(false);

                    // Create a transaction
                    using (var transaction = connection.BeginTransaction())
                    {
                        await this.InterceptAsync(new TransactionOpenedArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                        // 1) Get Schema from remote provider
                        (ctx, schema) = await this.Provider.GetSchemaAsync(ctx, this.Setup, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // 2) Ensure databases are ready
                        var provision = SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;

                        await this.InterceptAsync(new DatabaseProvisioningArgs(ctx, provision, schema, connection, transaction), cancellationToken).ConfigureAwait(false);

                        ctx = await this.Provider.ProvisionAsync(ctx, schema, this.Setup, provision, this.Options.ScopeInfoTableName, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // 3) Getting the most accurate timestamp
                        var remoteClientTimestamp = await this.Provider.GetLocalTimestampAsync(ctx, connection, transaction, cancellationToken, progress);

                        await this.InterceptAsync(new SnapshotCreatingArgs(ctx, schema, this.Options.SnapshotsDirectory, this.Options.BatchSize, remoteClientTimestamp, connection, transaction), cancellationToken).ConfigureAwait(false);

                        // 4) Create the snapshot
                        (ctx, batchInfo) = await this.Provider.CreateSnapshotAsync(ctx, schema, this.Setup, connection, transaction, this.Options.SnapshotsDirectory,
                                this.Options.BatchSize, remoteClientTimestamp, cancellationToken, progress).ConfigureAwait(false);

                        await this.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                        transaction.Commit();
                    }

                    ctx.SyncStage = SyncStage.SnapshotCreated;

                    await this.CloseConnectionAsync(connection, cancellationToken).ConfigureAwait(false);

                    this.logger.LogInformation(SyncEventsId.CreateSnapshot, batchInfo);

                    var snapshotCreated = new SnapshotCreatedArgs(ctx, schema, batchInfo, connection);
                    await this.InterceptAsync(snapshotCreated, cancellationToken).ConfigureAwait(false);
                    this.ReportProgress(ctx, progress, snapshotCreated);

                }
                catch (Exception ex)
                {
                    RaiseError(ex);
                }
                finally
                {
                    if (connection != null && connection.State != ConnectionState.Closed)
                        connection.Close();
                }
                return batchInfo;
            }
        }


        /// <summary>
        /// Get a snapshot
        /// </summary>
        public virtual async Task<(long RemoteClientTimestamp, BatchInfo ServerBatchInfo)> GetSnapshotAsync(SyncSet schema = null, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {

            // TODO: Get snapshot based on version and scopename

            // Get context or create a new one
            var ctx = this.GetContext();

            BatchInfo serverBatchInfo = null;
            try
            {
                var connection = this.Provider.CreateConnection();

                if (string.IsNullOrEmpty(this.Options.SnapshotsDirectory))
                    return (0, null);

                //Direction set to Download
                ctx.SyncWay = SyncWay.Download;

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // Get Schema from remote provider if no schema passed from args
                if (schema == null)
                {
                    var serverScopeInfo = await this.EnsureSchemaAsync(cancellationToken, progress).ConfigureAwait(false);
                    schema = serverScopeInfo.Schema;
                }

                // When we get the chnages from server, we create the batches if it's requested by the client
                // the batch decision comes from batchsize from client
                (ctx, serverBatchInfo) =
                    await this.Provider.GetSnapshotAsync(ctx, schema, this.Options.SnapshotsDirectory, cancellationToken, progress);

            }
            catch (Exception ex)
            {
                RaiseError(ex);
            }

            if (serverBatchInfo == null)
                return (0, null);

            return (serverBatchInfo.Timestamp, serverBatchInfo);
        }


        /// <summary>
        /// Check if we can reach the underlying provider database
        /// </summary>
        /// <returns></returns>
        public async Task<(string DatabaseName, string Version)> GetHelloAsync(CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            // Get context or create a new one
            var ctx = this.GetContext();
            string databaseName = null;
            string version = null;
            using (var connection = this.Provider.CreateConnection())
            {
                try
                {

                    // Open connection
                    await this.OpenConnectionAsync(connection, cancellationToken).ConfigureAwait(false);

                    // Create a transaction
                    using (var transaction = connection.BeginTransaction())
                    {
                        await this.InterceptAsync(new TransactionOpenedArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                        (ctx, databaseName, version) = await this.Provider.GetHelloAsync(ctx, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        await this.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                        transaction.Commit();
                    }

                    await this.CloseConnectionAsync(connection, cancellationToken).ConfigureAwait(false);

                }
                catch (Exception ex)
                {
                    RaiseError(ex);
                }
                finally
                {
                    if (connection != null && connection.State != ConnectionState.Closed)
                        connection.Close();
                }

                return (databaseName, version);
            }
        }


        /// <summary>
        /// Delete all metadatas from tracking tables, based on min timestamp from history client table
        /// </summary>
        public virtual async Task<DatabaseMetadatasCleaned> DeleteMetadatasAsync(CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            var ctx = this.GetContext();

            // Get the min timestamp, where we can without any problem, delete metadatas
            var histories = await this.GetServerHistoryScopes(cancellationToken, progress);

            if (histories == null || histories.Count == 0)
                return new DatabaseMetadatasCleaned();

            var minTimestamp = histories.Min(shsi => shsi.LastSyncTimestamp);

            if (minTimestamp == 0)
                return new DatabaseMetadatasCleaned();

            return await this.DeleteMetadatasAsync(minTimestamp, cancellationToken, progress);
        }

        /// <summary>
        /// Delete metadatas items from tracking tables
        /// </summary>
        /// <param name="timeStampStart">Timestamp start. Used to limit the delete metadatas rows from now to this timestamp</param>
        public override async Task<DatabaseMetadatasCleaned> DeleteMetadatasAsync(long timeStampStart, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            DatabaseMetadatasCleaned databaseMetadatasCleaned = null;
            // Get context or create a new one
            var ctx = this.GetContext();
            using (var connection = this.Provider.CreateConnection())
            {
                try
                {
                    ctx.SyncStage = SyncStage.MetadataCleaning;

                    // Open connection
                    await this.OpenConnectionAsync(connection, cancellationToken).ConfigureAwait(false);

                    // Create a transaction
                    using (var transaction = connection.BeginTransaction())
                    {
                        await this.InterceptAsync(new TransactionOpenedArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                        await this.InterceptAsync(new MetadataCleaningArgs(ctx, this.Setup, timeStampStart, connection, transaction), cancellationToken).ConfigureAwait(false);

                        // Create a dummy schema to be able to call the DeprovisionAsync method on the provider
                        // No need columns or primary keys to be able to deprovision a table
                        SyncSet schema = new SyncSet(this.Setup);

                        (ctx, databaseMetadatasCleaned) = await this.Provider.DeleteMetadatasAsync(ctx, schema, this.Setup, timeStampStart, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // Update server scope table
                        ctx = await this.Provider.EnsureServerScopeAsync(ctx, this.Options.ScopeInfoTableName, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // Get scope 
                        ServerScopeInfo serverScopeInfo;
                        (ctx, serverScopeInfo) = await this.Provider.GetServerScopeAsync(ctx, this.Options.ScopeInfoTableName, this.ScopeName, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        serverScopeInfo.LastCleanupTimestamp = databaseMetadatasCleaned.TimestampLimit;

                        ctx = await this.Provider.WriteServerScopeAsync(ctx, this.Options.ScopeInfoTableName, serverScopeInfo, connection, transaction, cancellationToken, progress);

                        await this.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                        transaction.Commit();
                    }

                    ctx.SyncStage = SyncStage.MetadataCleaned;

                    await this.CloseConnectionAsync(connection, cancellationToken).ConfigureAwait(false);

                    var args = new MetadataCleanedArgs(ctx, databaseMetadatasCleaned, connection);
                    await this.InterceptAsync(args, cancellationToken).ConfigureAwait(false);
                    this.ReportProgress(ctx, progress, args);
                }
                catch (Exception ex)
                {
                    RaiseError(ex);
                }
                finally
                {
                    if (connection != null && connection.State != ConnectionState.Closed)
                        connection.Close();
                }

                return databaseMetadatasCleaned;
            }

        }
    }
}