using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Messages;
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
    public class RemoteOrchestrator : IRemoteOrchestrator
    {
        private SyncContext syncContext;

        /// <summary>
        /// Gets or Sets the provider used by this orchestrator
        /// </summary>
        public CoreProvider Provider { get; set; }

        /// <summary>
        /// Gets or Sets the options used by this orchestrator
        /// </summary>
        public virtual SyncOptions Options { get; set; }

        /// <summary>
        /// Gets or Sets the Setup used by this orchestrator
        /// </summary>
        public SyncSetup Setup { get; set; }

        /// <summary>
        /// Gets or Sets the scope name used by this orchestrator
        /// </summary>
        public string ScopeName { get; set; }

        /// <summary>
        /// Gets or Sets the start time for this orchestrator
        /// </summary>
        public DateTime? StartTime { get; set; }

        /// <summary>
        /// Gets or Sets the end time for this orchestrator
        /// </summary>
        public DateTime? CompleteTime { get; set; }


        /// <summary>
        /// Remote orchestrator 
        /// </summary>
        public RemoteOrchestrator(CoreProvider provider, SyncOptions options, SyncSetup setup, string scopeName = SyncOptions.DefaultScopeName)
        {
            this.ScopeName = scopeName ?? throw new ArgumentNullException(nameof(scopeName));
            this.Provider = provider ?? throw new ArgumentNullException(nameof(provider));
            this.Options = options ?? throw new ArgumentNullException(nameof(options));
            this.Setup = setup ?? throw new ArgumentNullException(nameof(setup));
        }

        /// <summary>
        /// Set an interceptor to get info on the current sync process
        /// </summary>
        public void On<T>(Func<T, Task> interceptorFunc) where T : ProgressArgs => this.Provider.On(interceptorFunc);

        /// <summary>
        /// Set an interceptor to get info on the current sync process
        /// </summary>
        public void On<T>(Action<T> interceptorAction) where T : ProgressArgs => this.Provider.On(interceptorAction);

        /// <summary>
        /// Set a collection of interceptors
        /// </summary>
        public void On(Interceptors interceptors) => this.Provider.On(interceptors);

        /// <summary>
        /// Sets the current context
        /// </summary>
        public void SetContext(SyncContext context) => this.syncContext = context;

        /// <summary>
        /// Gets the current context
        /// </summary>
        public SyncContext GetContext()
        {
            if (this.syncContext != null)
                return this.syncContext;

            // Context, used to back and forth data between servers
            var context = new SyncContext(Guid.NewGuid(), this.ScopeName);

            this.SetContext(context);

            return this.syncContext;
        }

        /// <summary>
        /// Get the local configuration, ensures the local scope is created
        /// </summary>
        /// <returns>current context, the local scope info created or get from the database and the configuration from the client if changed </returns>
        public async Task<ServerScopeInfo> EnsureScopesAsync(CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            // Get context or create a new one
            var ctx = this.GetContext();
            ServerScopeInfo serverScopeInfo;

            DbTransaction transaction = null;

            using (var connection = this.Provider.CreateConnection())
            {
                // Encapsulate in a try catch for a better exception handling
                // Especially whe called from web proxy
                try
                {
                    await connection.OpenAsync().ConfigureAwait(false);

                    // Let provider knows a connection is opened
                    this.Provider.OnConnectionOpened(connection);

                    await this.Provider.InterceptAsync(new ConnectionOpenArgs(ctx, connection)).ConfigureAwait(false);

                    // Create a transaction
                    using (transaction = connection.BeginTransaction())
                    {

                        await this.Provider.InterceptAsync(new TransactionOpenArgs(ctx, connection, transaction)).ConfigureAwait(false);

                        // Create scope server
                        ctx = await this.Provider.EnsureServerScopeAsync(
                                            ctx, this.Options.ScopeInfoTableName,
                                            connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // Create scope history
                        ctx = await this.Provider.EnsureServerHistoryScopeAsync(
                                            ctx, this.Options.ScopeInfoTableName,
                                            connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // Get scope if exists
                        (ctx, serverScopeInfo) = await this.Provider.GetServerScopeAsync(ctx, this.Options.ScopeInfoTableName, this.ScopeName,
                                            connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        await this.Provider.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction)).ConfigureAwait(false);
                        transaction.Commit();

                        return serverScopeInfo;
                    }
                }
                catch (Exception ex)
                {

                    var syncException = new SyncException(ex, SyncStage.ScopeLoading);

                    // try to let the provider enrich the exception
                    this.Provider.EnsureSyncException(syncException);
                    syncException.Side = SyncExceptionSide.ServerSide;
                    throw syncException;
                }
                finally
                {
                    if (transaction != null)
                        transaction.Dispose();

                    if (connection != null && connection.State == ConnectionState.Open)
                        connection.Close();

                    await this.Provider.InterceptAsync(new ConnectionCloseArgs(ctx, connection, transaction)).ConfigureAwait(false);

                    // Let provider knows a connection is closed
                    this.Provider.OnConnectionClosed(connection);
                }
            }
        }



        /// <summary>
        /// Get the scope infos from remote
        /// </summary>
        public async Task<(SyncSet Schema, string Version)> EnsureSchemaAsync(CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            // Get context or create a new one
            var ctx = this.GetContext();

            ServerScopeInfo serverScopeInfo;
            DbTransaction transaction = null;
            SyncSet schema;

            using (var connection = this.Provider.CreateConnection())
            {
                // Encapsulate in a try catch for a better exception handling
                // Especially whe called from web proxy
                try
                {
                    await connection.OpenAsync().ConfigureAwait(false);

                    // Let provider knows a connection is opened
                    this.Provider.OnConnectionOpened(connection);

                    await this.Provider.InterceptAsync(new ConnectionOpenArgs(ctx, connection)).ConfigureAwait(false);

                    // Create a transaction
                    using (transaction = connection.BeginTransaction())
                    {
                        // Open the connection
                        // Interceptors
                        await this.Provider.InterceptAsync(new TransactionOpenArgs(ctx, connection, transaction)).ConfigureAwait(false);


                        // Create scope server
                        ctx = await this.Provider.EnsureServerScopeAsync(
                                            ctx, this.Options.ScopeInfoTableName, 
                                            connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // Create scope history
                        ctx = await this.Provider.EnsureServerHistoryScopeAsync(
                                            ctx, this.Options.ScopeInfoTableName, 
                                            connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // Get scope if exists
                        (ctx, serverScopeInfo) = await this.Provider.GetServerScopeAsync(ctx, this.Options.ScopeInfoTableName, this.ScopeName,
                                            connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // Let's compare this serverScopeInfo with the current Setup
                        // If we don't have any version 
                        // OrElse version are different
                        if (string.IsNullOrEmpty(serverScopeInfo.Schema))
                        {
                            // So far, we don't have already a database provisionned


                            // 1) Get Schema from remote provider
                            (ctx, schema) = await this.Provider.ReadSchemaAsync(ctx, this.Setup, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                            // 2) Ensure databases are ready
                            var provision = SyncProvision.Table | SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;
                            ctx = await this.Provider.ProvisionAsync(ctx, schema, provision, this.Options.ScopeInfoTableName, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                            serverScopeInfo.LastCleanupTimestamp = 0;
                            serverScopeInfo.Schema = JsonConvert.SerializeObject(schema);
                            serverScopeInfo.Version = "1";

                            // 3) Update server scope
                            ctx = await this.Provider.WriteServerScopeAsync(
                                  ctx, this.Options.ScopeInfoTableName, serverScopeInfo,
                                  connection, transaction, cancellationToken, progress).ConfigureAwait(false);
                        }
                        else
                        {
                            // Get the schema saved on server
                            schema = JsonConvert.DeserializeObject<SyncSet>(serverScopeInfo.Schema);
                        }



                        //// If we have a stored schema, make a comparison with the one extract from setup
                        //if (!string.IsNullOrEmpty(serverScopeInfo.Schema))
                        //{
                        //    var currentSchema = JsonConvert.DeserializeObject<SyncSet>(serverScopeInfo.Schema);

                        //    //var migrationTools = new DbMigrationTools();

                        //    //// Check if current schema has exact same properties, relations, tables and columns..
                        //    //var isIdentical = currentSchema == schema;

                        //    //if (!isIdentical)
                        //    //{
                        //    //    // Create a migration args where use can apply or not the migration
                        //    //    var migrationArgs = new MigrationArgs(ctx, migrationTools, currentSchema, schema, connection, transaction);

                        //    //    // Intercept and let user decides and apply migration
                        //    //    await this.Provider.InterceptAsync(migrationArgs).ConfigureAwait(false);
                        //    //}
                        //}
                        //else
                        //{
                        //}


                        if (cancellationToken.IsCancellationRequested)
                            cancellationToken.ThrowIfCancellationRequested();

                        await this.Provider.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction)).ConfigureAwait(false);
                        transaction.Commit();

                        return (schema, serverScopeInfo.Version);

                    }
                }
                catch (Exception ex)
                {
                    var syncException = new SyncException(ex, SyncStage.SchemaProvisioning);
                    // try to let the provider enrich the exception
                    this.Provider.EnsureSyncException(syncException);
                    syncException.Side = SyncExceptionSide.ServerSide;
                    throw syncException;
                }
                finally
                {
                    if (transaction != null)
                        transaction.Dispose();

                    if (connection != null && connection.State == ConnectionState.Open)
                        connection.Close();

                    await this.Provider.InterceptAsync(new ConnectionCloseArgs(ctx, connection, transaction)).ConfigureAwait(false);

                    // Let provider knows a connection is closed
                    this.Provider.OnConnectionClosed(connection);
                }
            }
        }

        /// <summary>
        /// Read server schema.
        /// </summary>
        /// <returns></returns>
        public async Task<SyncSet> ReadSchemaAsync(CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            // Get context or create a new one
            var ctx = this.GetContext();

            SyncSet schema = null;

            DbTransaction transaction = null;

            using (var connection = this.Provider.CreateConnection())
            {
                // Encapsulate in a try catch for a better exception handling
                // Especially whe called from web proxy
                try
                {
                    await connection.OpenAsync().ConfigureAwait(false);

                    // Let provider knows a connection is opened
                    this.Provider.OnConnectionOpened(connection);

                    await this.Provider.InterceptAsync(new ConnectionOpenArgs(ctx, connection)).ConfigureAwait(false);

                    // Create a transaction
                    using (transaction = connection.BeginTransaction())
                    {

                        await this.Provider.InterceptAsync(new TransactionOpenArgs(ctx, connection, transaction)).ConfigureAwait(false);

                        (ctx, schema) = await this.Provider.ReadSchemaAsync(ctx, this.Setup, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        await this.Provider.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction)).ConfigureAwait(false);
                        transaction.Commit();
                    }
                }
                catch (Exception ex)
                {
                    var syncException = new SyncException(ex, SyncStage.SchemaReading);

                    // try to let the provider enrich the exception
                    this.Provider.EnsureSyncException(syncException);
                    syncException.Side = SyncExceptionSide.ServerSide;
                    throw syncException;
                }
                finally
                {
                    if (transaction != null)
                        transaction.Dispose();

                    if (connection != null && connection.State == ConnectionState.Open)
                        connection.Close();

                    await this.Provider.InterceptAsync(new ConnectionCloseArgs(ctx, connection, transaction)).ConfigureAwait(false);

                    // Let provider knows a connection is closed
                    this.Provider.OnConnectionClosed(connection);
                }
            }

            return schema;
        }


        /// <summary>
        /// Provision schema on remote provider.
        /// </summary>
        /// <param name="schema">Schema used to provision server database</param>
        /// <param name="provision">all provision flags</param>
        /// <param name="cancellationToken">cancellation token</param>
        /// <param name="progress">progress</param>
        public async Task<SyncSet> ProvisionAsync(SyncSet schema, SyncProvision provision, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            // Get context or create a new one
            var ctx = this.GetContext();

            DbTransaction transaction = null;

            using (var connection = this.Provider.CreateConnection())
            {
                // Encapsulate in a try catch for a better exception handling
                // Especially whe called from web proxy
                try
                {
                    await connection.OpenAsync().ConfigureAwait(false);

                    // Let provider knows a connection is opened
                    this.Provider.OnConnectionOpened(connection);

                    await this.Provider.InterceptAsync(new ConnectionOpenArgs(ctx, connection)).ConfigureAwait(false);

                    // Create a transaction
                    using (transaction = connection.BeginTransaction())
                    {

                        await this.Provider.InterceptAsync(new TransactionOpenArgs(ctx, connection, transaction)).ConfigureAwait(false);

                        await this.Provider.ProvisionAsync(ctx, schema, provision, this.Options.ScopeInfoTableName, connection, transaction, cancellationToken, progress);

                        await this.Provider.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction)).ConfigureAwait(false);
                        transaction.Commit();
                    }
                }
                catch (Exception ex)
                {
                    var syncException = new SyncException(ex, SyncStage.SchemaReading);

                    // try to let the provider enrich the exception
                    this.Provider.EnsureSyncException(syncException);
                    syncException.Side = SyncExceptionSide.ServerSide;
                    throw syncException;
                }
                finally
                {
                    if (transaction != null)
                        transaction.Dispose();

                    if (connection != null && connection.State == ConnectionState.Open)
                        connection.Close();

                    await this.Provider.InterceptAsync(new ConnectionCloseArgs(ctx, connection, transaction)).ConfigureAwait(false);

                    // Let provider knows a connection is closed
                    this.Provider.OnConnectionClosed(connection);
                }
            }

            return schema;
        }

        /// <summary>
        /// Deprovision schema on remote provider.
        /// </summary>
        /// <param name="schema">Schema used to deprovision server database</param>
        /// <param name="provision">all deprovision flags</param>
        /// <param name="cancellationToken">cancellation token</param>
        /// <param name="progress">progress</param>
        public async Task<SyncSet> DeprovisionAsync(SyncSet schema, SyncProvision provision, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            // Get context or create a new one
            var ctx = this.GetContext();

            DbTransaction transaction = null;

            using (var connection = this.Provider.CreateConnection())
            {
                // Encapsulate in a try catch for a better exception handling
                // Especially when called from web proxy
                try
                {
                    await connection.OpenAsync().ConfigureAwait(false);

                    // Let provider knows a connection is opened
                    this.Provider.OnConnectionOpened(connection);

                    await this.Provider.InterceptAsync(new ConnectionOpenArgs(ctx, connection)).ConfigureAwait(false);

                    // Create a transaction
                    using (transaction = connection.BeginTransaction())
                    {

                        await this.Provider.InterceptAsync(new TransactionOpenArgs(ctx, connection, transaction)).ConfigureAwait(false);

                        await this.Provider.DeprovisionAsync(ctx, schema, provision, this.Options.ScopeInfoTableName,
                            this.Options.DisableConstraintsOnApplyChanges, connection, transaction, cancellationToken, progress);

                        await this.Provider.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction)).ConfigureAwait(false);
                        transaction.Commit();
                    }
                }
                catch (Exception ex)
                {
                    var syncException = new SyncException(ex, SyncStage.SchemaReading);

                    // try to let the provider enrich the exception
                    this.Provider.EnsureSyncException(syncException);
                    syncException.Side = SyncExceptionSide.ServerSide;
                    throw syncException;
                }
                finally
                {
                    if (transaction != null)
                        transaction.Dispose();

                    if (connection != null && connection.State == ConnectionState.Open)
                        connection.Close();

                    await this.Provider.InterceptAsync(new ConnectionCloseArgs(ctx, connection, transaction)).ConfigureAwait(false);

                    // Let provider knows a connection is closed
                    this.Provider.OnConnectionClosed(connection);
                }
            }

            return schema;
        }



        /// <summary>
        /// Apply changes on remote provider
        /// </summary>
        /// <param name="clientScope">client scope</param>
        /// <param name="schema">schema used to apply and then get changes</param>
        /// <param name="clientBatchInfo">changes to apply</param>
        /// <param name="cancellationToken">cancellation token</param>
        /// <param name="progress">progress</param>
        /// <returns></returns>
        public async Task<(long remoteClientTimestamp,
              BatchInfo serverBatchInfo,
              ConflictResolutionPolicy serverPolicy,
              DatabaseChangesApplied clientChangesApplied,
              DatabaseChangesSelected serverChangesSelected)>
            ApplyThenGetChangesAsync(ScopeInfo clientScope, BatchInfo clientBatchInfo,
                                     CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            // Get context or create a new one
            var ctx = this.GetContext();

            long remoteClientTimestamp;
            DatabaseChangesSelected serverChangesSelected;
            DatabaseChangesApplied clientChangesApplied;
            BatchInfo serverBatchInfo;
            DbTransaction transaction = null;
            SyncSet schema;

            using (var connection = this.Provider.CreateConnection())
            {
                try
                {
                    await connection.OpenAsync().ConfigureAwait(false);

                    // Let provider knows a connection is opened
                    this.Provider.OnConnectionOpened(connection);

                    await this.Provider.InterceptAsync(new ConnectionOpenArgs(ctx, connection)).ConfigureAwait(false);

                    // Create two transactions
                    // First one to commit changes
                    // Second one to get changes now that everything is commited
                    using (transaction = connection.BeginTransaction())
                    {
                        await this.Provider.InterceptAsync(new TransactionOpenArgs(ctx, connection, transaction)).ConfigureAwait(false);


                        ServerScopeInfo serverScopeInfo;
                        // Maybe here, get the schema from server, issue from client scope name
                        // Maybe then compare the schema version from client scope with schema version issued from server
                        // Maybe if different, raise an error ?
                        // Get scope if exists

                        // Getting server scope assumes we have already created the schema on server
                        (ctx, serverScopeInfo) = await this.Provider.GetServerScopeAsync(ctx, this.Options.ScopeInfoTableName, clientScope.Name,
                                            connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        // Should we ?
                        if (string.IsNullOrEmpty(serverScopeInfo.Schema))
                            throw new NullReferenceException("Server schema does not exist");

                        // Check if we have a version to control
                        if (!serverScopeInfo.Version.Equals(clientScope.Version, SyncGlobalization.DataSourceStringComparison))
                            throw new ArgumentException("Server schema version does not match client schema version");

                        // deserialiaze schema
                        schema = JsonConvert.DeserializeObject<SyncSet>(serverScopeInfo.Schema);

                        (ctx, clientChangesApplied) =
                            await this.Provider.ApplyChangesAsync(ctx,
                             new MessageApplyChanges(Guid.Empty, clientScope.Id, false, clientScope.LastServerSyncTimestamp, schema, this.Options.ConflictResolutionPolicy,
                                        this.Options.DisableConstraintsOnApplyChanges, this.Options.UseBulkOperations, this.Options.CleanMetadatas, this.Options.CleanFolder, clientBatchInfo),
                             connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        if (cancellationToken.IsCancellationRequested)
                            cancellationToken.ThrowIfCancellationRequested();
                        await this.Provider.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction)).ConfigureAwait(false);

                        // commit first transaction
                        transaction.Commit();
                    }

                    using (transaction = connection.BeginTransaction())
                    {
                        //Direction set to Download
                        ctx.SyncWay = SyncWay.Download;

                        // JUST Before get changes, get the timestamp, to be sure to 
                        // get rows inserted / updated elsewhere since the sync is not over
                        remoteClientTimestamp = this.Provider.GetLocalTimestampAsync(ctx, connection, transaction, cancellationToken, progress);

                        if (cancellationToken.IsCancellationRequested)
                            cancellationToken.ThrowIfCancellationRequested();

                        // Get if we need to get all rows from the datasource
                        var fromScratch = clientScope.IsNewScope || ctx.SyncType == SyncType.Reinitialize || ctx.SyncType == SyncType.ReinitializeWithUpload;

                        // When we get the chnages from server, we create the batches if it's requested by the client
                        // the batch decision comes from batchsize from client
                        (ctx, serverBatchInfo, serverChangesSelected) =
                            await this.Provider.GetChangeBatchAsync(ctx,
                                new MessageGetChangesBatch(clientScope.Id, Guid.Empty, fromScratch, clientScope.LastServerSyncTimestamp,
                                    schema, this.Options.BatchSize, this.Options.BatchDirectory),
                                    connection, transaction, cancellationToken, progress).ConfigureAwait(false);

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
                        await this.Provider.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction)).ConfigureAwait(false);
                        transaction.Commit();
                    }
                }
                catch (Exception ex)
                {
                    var syncException = new SyncException(ex, SyncStage.DatabaseChangesApplying);
                    // try to let the provider enrich the exception
                    this.Provider.EnsureSyncException(syncException);
                    syncException.Side = SyncExceptionSide.ServerSide;
                    throw syncException;
                }
                finally
                {
                    if (transaction != null)
                        transaction.Dispose();

                    if (connection != null && connection.State != ConnectionState.Closed)
                        connection.Close();

                    await this.Provider.InterceptAsync(new ConnectionCloseArgs(ctx, connection, transaction)).ConfigureAwait(false);

                    // Let provider knows a connection is closed
                    this.Provider.OnConnectionClosed(connection);
                }
            }

            return (remoteClientTimestamp, serverBatchInfo, this.Options.ConflictResolutionPolicy, clientChangesApplied, serverChangesSelected);
        }


        public async Task DeleteMetadatasAsync(long timeStampStart, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            // Get context or create a new one
            var ctx = this.GetContext();

            DbTransaction transaction = null;

            using (var connection = this.Provider.CreateConnection())
            {
                try
                {
                    await connection.OpenAsync().ConfigureAwait(false);

                    // Let provider knows a connection is opened
                    this.Provider.OnConnectionOpened(connection);

                    await this.Provider.InterceptAsync(new ConnectionOpenArgs(ctx, connection)).ConfigureAwait(false);

                    // Create a transaction
                    using (transaction = connection.BeginTransaction())
                    {
                        await this.Provider.InterceptAsync(new TransactionOpenArgs(ctx, connection, transaction)).ConfigureAwait(false);

                        SyncSet schema;
                        (ctx, schema) = await this.Provider.ReadSchemaAsync(ctx, this.Setup, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        ctx = await this.Provider.DeleteMetadatasAsync(ctx, schema, timeStampStart, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        await this.Provider.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction)).ConfigureAwait(false);
                        transaction.Commit();
                    }
                }
                catch (Exception ex)
                {
                    var syncException = new SyncException(ex, SyncStage.CleanupMetadata);
                    // try to let the provider enrich the exception
                    this.Provider.EnsureSyncException(syncException);
                    syncException.Side = SyncExceptionSide.ServerSide;
                    throw syncException;
                }
                finally
                {
                    if (transaction != null)
                        transaction.Dispose();

                    if (connection != null && connection.State != ConnectionState.Closed)
                        connection.Close();

                    await this.Provider.InterceptAsync(new ConnectionCloseArgs(ctx, connection, transaction)).ConfigureAwait(false);

                    // Let provider knows a connection is closed
                    this.Provider.OnConnectionClosed(connection);
                }
            }

        }


        public async Task CreateSnapshotAsync(SyncParameters syncParameters = null, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            if (string.IsNullOrEmpty(this.Options.SnapshotsDirectory) || this.Options.BatchSize <= 0)
                throw new SnapshotMissingMandatariesOptionsException();

            // Get context or create a new one
            var ctx = this.GetContext();

            // check parameters
            // If context has no parameters specified, and user specifies a parameter collection we switch them
            if ((ctx.Parameters == null || ctx.Parameters.Count <= 0) && syncParameters != null && syncParameters.Count > 0)
                ctx.Parameters = syncParameters;

            DbTransaction transaction = null;
            long remoteClientTimestamp;

            // Get Schema from remote provider
            var (schema, version) = await this.EnsureSchemaAsync(cancellationToken, progress).ConfigureAwait(false);

            using (var connection = this.Provider.CreateConnection())
            {
                try
                {
                    await connection.OpenAsync().ConfigureAwait(false);

                    // Let provider knows a connection is opened
                    this.Provider.OnConnectionOpened(connection);

                    await this.Provider.InterceptAsync(new ConnectionOpenArgs(ctx, connection)).ConfigureAwait(false);

                    // Create a transaction
                    using (transaction = connection.BeginTransaction())
                    {
                        await this.Provider.InterceptAsync(new TransactionOpenArgs(ctx, connection, transaction)).ConfigureAwait(false);

                        // JUST Before get changes, get the timestamp, to be sure to 
                        // get rows inserted / updated elsewhere since the sync is not over
                        remoteClientTimestamp = this.Provider.GetLocalTimestampAsync(ctx, connection, transaction, cancellationToken, progress);

                        // Create snapshot
                        await this.Provider.CreateSnapshotAsync(ctx, schema, connection, transaction, this.Options.SnapshotsDirectory,
                                this.Options.BatchSize, remoteClientTimestamp, cancellationToken, progress).ConfigureAwait(false);

                        await this.Provider.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction)).ConfigureAwait(false);
                        transaction.Commit();
                    }
                }
                catch (Exception ex)
                {
                    var syncException = new SyncException(ex, SyncStage.SnapshotApplying);
                    // try to let the provider enrich the exception
                    this.Provider.EnsureSyncException(syncException);
                    syncException.Side = SyncExceptionSide.ServerSide;
                    throw syncException;
                }
                finally
                {
                    if (transaction != null)
                        transaction.Dispose();

                    if (connection != null && connection.State != ConnectionState.Closed)
                        connection.Close();

                    await this.Provider.InterceptAsync(new ConnectionCloseArgs(ctx, connection, transaction)).ConfigureAwait(false);

                    // Let provider knows a connection is closed
                    this.Provider.OnConnectionClosed(connection);
                }
            }
        }


        /// <summary>
        /// Get a snapshot
        /// </summary>
        public async Task<(long remoteClientTimestamp, BatchInfo serverBatchInfo)>
                GetSnapshotAsync(CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {

            // TODO: Get snapshot based on version and scopename

            // Get context or create a new one
            var ctx = this.GetContext();

            BatchInfo serverBatchInfo;
            try
            {
                if (string.IsNullOrEmpty(this.Options.SnapshotsDirectory))
                    return (0, null);

                //Direction set to Download
                ctx.SyncWay = SyncWay.Download;

                if (cancellationToken.IsCancellationRequested)
                    cancellationToken.ThrowIfCancellationRequested();

                // Get Schema from remote provider
                var (schema, version) = await this.EnsureSchemaAsync(cancellationToken, progress).ConfigureAwait(false);


                // When we get the chnages from server, we create the batches if it's requested by the client
                // the batch decision comes from batchsize from client
                (ctx, serverBatchInfo) =
                    await this.Provider.GetSnapshotAsync(ctx, schema, this.Options.SnapshotsDirectory, cancellationToken, progress);

            }
            catch (Exception ex)
            {
                var syncException = new SyncException(ex, SyncStage.SnapshotApplying);
                // try to let the provider enrich the exception
                this.Provider.EnsureSyncException(syncException);
                syncException.Side = SyncExceptionSide.ServerSide;
                throw syncException;
            }

            if (serverBatchInfo == null)
                return (0, null);

            return (serverBatchInfo.Timestamp, serverBatchInfo);
        }
    }
}