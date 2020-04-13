using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
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
    public abstract class BaseOrchestrator
    {
        // Collection of Interceptors
        private Interceptors interceptors = new Interceptors();
        internal SyncContext syncContext;

        /// <summary>
        /// Gets or Sets orchestrator side
        /// </summary>
        public abstract SyncSide Side { get; }

        /// <summary>
        /// Gets or Sets the provider used by this local orchestrator
        /// </summary>
        public virtual CoreProvider Provider { get; set; }

        /// <summary>
        /// Gets the options used by this local orchestrator
        /// </summary>
        public virtual SyncOptions Options { get; set; }

        /// <summary>
        /// Gets the Setup used by this local orchestrator
        /// </summary>
        public virtual SyncSetup Setup { get; set; }

        /// <summary>
        /// Gets the scope name used by this local orchestrator
        /// </summary>
        public virtual string ScopeName { get; set; }

        /// <summary>
        /// Gets or Sets the start time for this orchestrator
        /// </summary>
        public virtual DateTime? StartTime { get; set; }

        /// <summary>
        /// Gets or Sets the end time for this orchestrator
        /// </summary>
        public virtual DateTime? CompleteTime { get; set; }


        /// <summary>
        /// Create a local orchestrator, used to orchestrates the whole sync on the client side
        /// </summary>
        public BaseOrchestrator(CoreProvider provider, SyncOptions options, SyncSetup setup, string scopeName = SyncOptions.DefaultScopeName)
        {
            this.ScopeName = scopeName ?? throw new ArgumentNullException(nameof(scopeName));
            this.Provider = provider ?? throw new ArgumentNullException(nameof(provider));
            this.Options = options ?? throw new ArgumentNullException(nameof(options));
            this.Setup = setup ?? throw new ArgumentNullException(nameof(setup));

            this.Provider.Orchestrator = this;
            this.Provider.Options = options;
        }

        /// <summary>
        /// Set an interceptor to get info on the current sync process
        /// </summary>
        internal void On<T>(Func<T, Task> interceptorFunc) where T : ProgressArgs =>
            this.interceptors.GetInterceptor<T>().Set(interceptorFunc);

        /// <summary>
        /// Set an interceptor to get info on the current sync process
        /// </summary>
        internal void On<T>(Action<T> interceptorAction) where T : ProgressArgs =>
            this.interceptors.GetInterceptor<T>().Set(interceptorAction);

        /// <summary>
        /// Set a collection of interceptors
        /// </summary>
        internal void On(Interceptors interceptors) => this.interceptors = interceptors;

        /// <summary>
        /// Returns the Task associated with given type of BaseArgs 
        /// Because we are not doing anything else than just returning a task, no need to use async / await. Just return the Task itself
        /// </summary>
        internal Task InterceptAsync<T>(T args, CancellationToken cancellationToken) where T : ProgressArgs
        {
            if (this.interceptors == null)
                return Task.CompletedTask;

            var interceptor = this.interceptors.GetInterceptor<T>();

            return interceptor.RunAsync(args, cancellationToken);
        }

        /// <summary>
        /// Gets a boolean returning true if an interceptor of type T, exists
        /// </summary>
        internal bool ContainsInterceptor<T>() where T : ProgressArgs => this.interceptors.Contains<T>();


        /// <summary>
        /// Try to report progress
        /// </summary>
        internal void ReportProgress(SyncContext context, IProgress<ProgressArgs> progress, ProgressArgs args, DbConnection connection = null, DbTransaction transaction = null)
        {
            if (progress == null)
                return;

            if (connection == null && args.Connection != null)
                connection = args.Connection;

            if (transaction == null && args.Transaction != null)
                transaction = args.Transaction;

            var dt = DateTime.Now;
            var message = $"{dt.ToLongTimeString()}.{dt.Millisecond}\t {args.Message}";
            var progressArgs = new ProgressArgs(context, message, connection, transaction);

            progress.Report(progressArgs);
        }

        
    
        /// <summary>
        /// Open a connection
        /// </summary>
        internal async Task OpenConnectionAsync(DbConnection connection, CancellationToken cancellationToken)
        {
            //// Defining my retry policy
            //var policy = SyncPolicy.WaitAndRetry(
            //                    3,
            //                    retryAttempt => TimeSpan.FromMilliseconds(Math.Pow(300, retryAttempt)),
            //                    ex => this.Provider.ShouldRetryOn(ex));

            //// Execute my OpenAsync in my policy context
            //await policy.ExecuteAsync(ct => connection.OpenAsync(ct), cancellationToken);


            await connection.OpenAsync();
            // Let provider knows a connection is opened
            this.Provider.OnConnectionOpened(connection);

            await this.InterceptAsync(new ConnectionOpenedArgs(this.GetContext(), connection), cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Close a connection
        /// </summary>
        internal async Task CloseConnectionAsync(DbConnection connection, CancellationToken cancellationToken)
        {
            connection.Close();

            await this.InterceptAsync(new ConnectionClosedArgs(this.GetContext(), connection), cancellationToken).ConfigureAwait(false);

            // Let provider knows a connection is closed
            this.Provider.OnConnectionClosed(connection);
        }


        /// <summary>
        /// Encapsulates an error in a SyncException, let provider enrich the error if needed, then throw again
        /// </summary>
        internal void RaiseError(Exception exception)
        {
            var syncException = new SyncException(exception, this.GetContext().SyncStage);

            // try to let the provider enrich the exception
            this.Provider.EnsureSyncException(syncException);
            syncException.Side = this.Side;

            throw syncException;
        }


        /// <summary>
        /// Sets the current context
        /// </summary>
        internal virtual void SetContext(SyncContext context)
        {
            this.syncContext = context;
        }

        /// <summary>
        /// Gets the current context
        /// </summary>
        public virtual SyncContext GetContext()
        {
            if (this.syncContext != null)
                return this.syncContext;

            this.syncContext = new SyncContext(Guid.NewGuid(), this.ScopeName); ;

            return this.syncContext;
        }

        /// <summary>
        /// Provision the orchestrator database based on the orchestrator Setup, and the provision enumeration
        /// </summary>
        /// <param name="provision">Provision enumeration to determine which components to apply</param>
        public virtual Task<SyncSet> ProvisionAsync(SyncProvision provision, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
                   => this.ProvisionAsync(new SyncSet(this.Setup), provision, cancellationToken, progress);

        /// <summary>
        /// Provision the orchestrator database based on the schema argument, and the provision enumeration
        /// </summary>
        /// <param name="schema">Schema to be applied to the database managed by the orchestrator, through the provider.</param>
        /// <param name="provision">Provision enumeration to determine which components to apply</param>
        /// <returns>Full schema with table and columns properties</returns>
        public virtual async Task<SyncSet> ProvisionAsync(SyncSet schema, SyncProvision provision, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            // Get context or create a new one
            var ctx = this.GetContext();

            using (var connection = this.Provider.CreateConnection())
            {
                try
                {
                    ctx.SyncStage = SyncStage.Provisioning;

                    // If schema does not have any table, just return
                    if (schema == null || schema.Tables == null || !schema.HasTables)
                        throw new MissingTablesException();

                    if (this is LocalOrchestrator && (provision.HasFlag(SyncProvision.ServerHistoryScope) || provision.HasFlag(SyncProvision.ServerScope)))
                        throw new InvalidProvisionForLocalOrchestratorException();
                    else if (!(this is LocalOrchestrator) && provision.HasFlag(SyncProvision.ClientScope))
                        throw new InvalidProvisionForRemoteOrchestratorException();

                    // Open connection
                    await this.OpenConnectionAsync(connection, cancellationToken).ConfigureAwait(false);

                    // Create a transaction
                    using (var transaction = connection.BeginTransaction())
                    {
                        await this.InterceptAsync(new TransactionOpenedArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                        // Check if we have tables AND columns
                        // If we don't have any columns it's most probably because user called method with the Setup only
                        // So far we have only tables names, it's enough to get the schema
                        if (schema.HasTables && !schema.HasColumns)
                            (ctx, schema) = await this.Provider.GetSchemaAsync(ctx, this.Setup, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        if (!schema.HasTables)
                            throw new MissingTablesException();

                        if (!schema.HasColumns)
                            throw new MissingColumnsException();

                        await this.InterceptAsync(new DatabaseProvisioningArgs(ctx, provision, schema, connection, transaction), cancellationToken).ConfigureAwait(false);

                        await this.Provider.ProvisionAsync(ctx, schema, this.Setup, provision, this.Options.ScopeInfoTableName, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        await this.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                        transaction.Commit();
                    }

                    ctx.SyncStage = SyncStage.Provisioned;

                    await this.CloseConnectionAsync(connection, cancellationToken).ConfigureAwait(false);

                    var args = new DatabaseProvisionedArgs(ctx, provision, schema, connection);
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

                return schema;
            }
        }

        /// <summary>
        /// Deprovision the orchestrator database based on the Setup table argument, and the provision enumeration
        /// </summary>
        /// <param name="provision">Provision enumeration to determine which components to deprovision</param>
        public virtual Task DeprovisionAsync(SetupTable table, SyncProvision provision, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            var setup = new SyncSetup();
            setup.Tables.Add(table);

            return this.DeprovisionAsync(new SyncSet(setup), provision, cancellationToken, progress);
        }




        /// <summary>
        /// Deprovision the orchestrator database based on the orchestrator Setup instance, provided on constructor, and the provision enumeration
        /// </summary>
        /// <param name="provision">Provision enumeration to determine which components to deprovision</param>
        public virtual Task DeprovisionAsync(SyncProvision provision, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => this.DeprovisionAsync(new SyncSet(this.Setup), provision, cancellationToken, progress);

        /// <summary>
        /// Deprovision the orchestrator database based on the schema argument, and the provision enumeration
        /// </summary>
        /// <param name="schema">Schema to be deprovisioned from the database managed by the orchestrator, through the provider.</param>
        /// <param name="provision">Provision enumeration to determine which components to deprovision</param>
        public virtual async Task DeprovisionAsync(SyncSet schema, SyncProvision provision, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            // Get context or create a new one
            var ctx = this.GetContext();
            using (var connection = this.Provider.CreateConnection())
            {
                // Encapsulate in a try catch for a better exception handling
                // Especially when called from web proxy
                try
                {
                    ctx.SyncStage = SyncStage.Deprovisioning;

                    // If schema does not have any table, just return
                    if (schema == null || schema.Tables == null || !schema.HasTables)
                        throw new MissingTablesException();

                    // Open connection
                    await this.OpenConnectionAsync(connection, cancellationToken).ConfigureAwait(false);

                    // Create a transaction
                    using (var transaction = connection.BeginTransaction())
                    {
                        await this.InterceptAsync(new TransactionOpenedArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                        await this.InterceptAsync(new DatabaseDeprovisioningArgs(ctx, provision, schema, connection, transaction), cancellationToken).ConfigureAwait(false);

                        await this.Provider.DeprovisionAsync(ctx, schema, this.Setup, provision, this.Options.ScopeInfoTableName, this.Options.DisableConstraintsOnApplyChanges, connection, transaction, cancellationToken, progress);

                        await this.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                        transaction.Commit();
                    }

                    ctx.SyncStage = SyncStage.Deprovisioned;

                    await this.CloseConnectionAsync(connection, cancellationToken).ConfigureAwait(false);

                    var args = new DatabaseDeprovisionedArgs(ctx, provision, schema, connection);
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
        /// Read the schema stored from the orchestrator database, through the provider.
        /// </summary>
        /// <returns>Schema containing tables, columns, relations, primary keys</returns>
        public virtual async Task<SyncSet> GetSchemaAsync(CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            // Get context or create a new one
            var ctx = this.GetContext();

            SyncSet schema = null;
            using (var connection = this.Provider.CreateConnection())
            {
                // Encapsulate in a try catch for a better exception handling
                // Especially whew called from web proxy
                try
                {
                    ctx.SyncStage = SyncStage.SchemaReading;

                    if (this.Setup.Tables.Count <= 0)
                        throw new MissingTablesException();

                    // Open connection
                    await this.OpenConnectionAsync(connection, cancellationToken).ConfigureAwait(false);

                    // Create a transaction
                    using (var transaction = connection.BeginTransaction())
                    {
                        await this.InterceptAsync(new TransactionOpenedArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);

                        (ctx, schema) = await this.Provider.GetSchemaAsync(ctx, this.Setup, connection, transaction, cancellationToken, progress).ConfigureAwait(false);

                        await this.InterceptAsync(new TransactionCommitArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);
                        transaction.Commit();
                    }

                    ctx.SyncStage = SyncStage.SchemaRead;

                    await this.CloseConnectionAsync(connection, cancellationToken).ConfigureAwait(false);

                    var schemaArgs = new SchemaArgs(ctx, schema, connection);
                    await this.InterceptAsync(schemaArgs, cancellationToken).ConfigureAwait(false);
                    this.ReportProgress(ctx, progress, schemaArgs);
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

                return schema;
            }
        }




        /// <summary>
        /// Delete metadatas items from tracking tables
        /// </summary>
        /// <param name="timeStampStart">Timestamp start. Used to limit the delete metadatas rows from now to this timestamp</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <param name="progress">Progress args</param>
        public virtual async Task<DatabaseMetadatasCleaned> DeleteMetadatasAsync(long timeStampStart, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
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


        /// <summary>
        /// Check if the orchestrator database is outdated
        /// </summary>
        /// <param name="timeStampStart">Timestamp start. Used to limit the delete metadatas rows from now to this timestamp</param>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <param name="progress">Progress args</param>
        public virtual async Task<bool> IsOutDated(ScopeInfo clientScopeInfo, ServerScopeInfo serverScopeInfo, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            if (!this.StartTime.HasValue)
                this.StartTime = DateTime.UtcNow;

            bool isOutdated = false;

            // Get context or create a new one
            var ctx = this.GetContext();

            // Check if the provider is not outdated
            // We can have negative value where we want to compare anyway
            if (clientScopeInfo.LastServerSyncTimestamp != 0 || serverScopeInfo.LastCleanupTimestamp != 0)
                isOutdated = clientScopeInfo.LastServerSyncTimestamp < serverScopeInfo.LastCleanupTimestamp;

            // Get a chance to make the sync even if it's outdated
            if (isOutdated)
            {
                var outdatedArgs = new OutdatedArgs(ctx, clientScopeInfo, serverScopeInfo);

                // Interceptor
                await this.InterceptAsync(outdatedArgs, cancellationToken).ConfigureAwait(false);

                if (outdatedArgs.Action != OutdatedAction.Rollback)
                    ctx.SyncType = outdatedArgs.Action == OutdatedAction.Reinitialize ? SyncType.Reinitialize : SyncType.ReinitializeWithUpload;

                if (outdatedArgs.Action == OutdatedAction.Rollback)
                    throw new OutOfDateException(clientScopeInfo.LastServerSyncTimestamp, serverScopeInfo.LastCleanupTimestamp);
            }

            return isOutdated;
        }
    }
}
