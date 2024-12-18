using Dotmim.Sync.Enumerations;
using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Extensions for the DbConnectionRunner. This class is used to encapsulate a connection and a transaction.
    /// </summary>
    public static class DbConnectionRunnerExtensions
    {
        /// <summary>
        /// Create a connection and transaction, encapsulated in a <see cref="DbConnectionRunner"/> instance that is disposable.
        /// </summary>
        public static async Task<DbConnectionRunner> GetConnectionAsync(
                                this BaseOrchestrator orchestrator,
                                SyncContext context,
                                SyncMode syncMode = SyncMode.WithTransaction,
                                SyncStage syncStage = SyncStage.None,
                                DbConnection connection = default,
                                DbTransaction transaction = default,
                                IProgress<ProgressArgs> progress = default,
                                CancellationToken cancellationToken = default)
        {
            try
            {
                Guard.ThrowIfNull(orchestrator);
                Guard.ThrowIfNull(context);

                // Get context or create a new one
                context.SyncStage = syncStage;

                if (orchestrator.Provider == null)
                    return new DbConnectionRunner(null, context, null, null, true, true, progress, cancellationToken);

                connection ??= orchestrator.Provider.CreateConnection();

                // can happens sometimes when transient errors occurs.
                if (string.IsNullOrEmpty(connection.ConnectionString))
                    connection.ConnectionString = orchestrator.Provider.ConnectionString;

                var alreadyOpened = connection.State == ConnectionState.Open;
                var alreadyInTransaction = transaction != null && transaction.Connection == connection;

                // Open connection
                if (!alreadyOpened)
                    await orchestrator.OpenConnectionAsync(context, connection, progress, cancellationToken).ConfigureAwait(false);

                // Create a transaction
                if (!alreadyInTransaction && syncMode == SyncMode.WithTransaction)
                {
#if NET6_0_OR_GREATER
                    transaction = await connection.BeginTransactionAsync(orchestrator.Provider.IsolationLevel, cancellationToken).ConfigureAwait(false);
#else
                    transaction = connection.BeginTransaction(orchestrator.Provider.IsolationLevel);
#endif
                    await orchestrator.InterceptAsync(new TransactionOpenedArgs(context, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
                }

                return new DbConnectionRunner(orchestrator, context, connection, transaction, alreadyOpened, alreadyInTransaction, progress, cancellationToken);
            }
            catch (Exception ex)
            {
                if (orchestrator != null)
                    throw orchestrator.GetSyncError(context, ex);
                else
                    throw;
            }
        }
    }

    /// <summary>
    /// Disposable runner to encapsulate a connection and a transaction.
    /// </summary>
    public sealed class DbConnectionRunner : IDisposable, IAsyncDisposable
    {
        private bool disposedValue;

        /// <summary>
        /// Initializes a new instance of the <see cref="DbConnectionRunner"/> class.
        /// </summary>
        public DbConnectionRunner(BaseOrchestrator orchestrator, SyncContext context, DbConnection connection, DbTransaction transaction,
            bool alreadyOpened, bool alreadyInTransaction,
            IProgress<ProgressArgs> progress = default, CancellationToken cancellationToken = default)
        {
            this.Orchestrator = orchestrator;
            this.Context = context;
            this.Connection = connection;
            this.Transaction = transaction;
            this.AlreadyOpened = alreadyOpened;
            this.AlreadyInTransaction = alreadyInTransaction;
            this.CancellationToken = cancellationToken;
            this.Progress = progress;
        }

        /// <summary>
        /// Gets or sets the orchestrator.
        /// </summary>
        public BaseOrchestrator Orchestrator { get; set; }

        /// <summary>
        /// Gets the sync context.
        /// </summary>
        public SyncContext Context { get; }

        /// <summary>
        /// Gets or sets the connection.
        /// </summary>
        public DbConnection Connection { get; set; }

        /// <summary>
        /// Gets or sets the transaction.
        /// </summary>
        public DbTransaction Transaction { get; set; }

        /// <summary>
        /// Gets a value indicating whether the connection is already opened.
        /// </summary>
        public bool AlreadyOpened { get; }

        /// <summary>
        /// Gets a value indicating whether the transaction is already opened.
        /// </summary>
        public bool AlreadyInTransaction { get; }

        /// <summary>
        /// Gets the cancellation token.
        /// </summary>
        public CancellationToken CancellationToken { get; }

        /// <summary>
        /// Gets the progress.
        /// </summary>
        public IProgress<ProgressArgs> Progress { get; }

        /// <summary>
        /// Commit the transaction and call an interceptor.
        /// </summary>
        public async Task CommitAsync(bool autoClose = true)
        {
            if (this.Orchestrator == null)
                return;

            if (!this.AlreadyInTransaction && this.Transaction != null)
            {
                await this.Orchestrator.InterceptAsync(
                    new TransactionCommitArgs(this.Context, this.Connection, this.Transaction), this.Progress, this.CancellationToken).ConfigureAwait(false);

                // we can have a zombie connection here, due to timeout, check again connection exists and is open
                if (this.Connection != null && this.Connection.State == ConnectionState.Open)
                    this.Transaction.Commit();
            }

            if (autoClose)
                await this.CloseAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Commit the transaction and call an interceptor.
        /// </summary>
        public async Task CloseAsync()
        {
            if (this.Orchestrator == null)
                return;

            if (!this.AlreadyOpened && this.Connection != null)
                await this.Orchestrator.CloseConnectionAsync(this.Context, this.Connection, this.Progress, this.CancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Rollback a transaction.
        /// </summary>
        public Task RollbackAsync(string reason) => Task.Run(() =>
        {
            if (this.Orchestrator == null || this.Transaction == null || this.AlreadyInTransaction)
                return;

            try
            {
                // we can have a zombie connection here, due to timeout, check again connection exists and is open
                if (this.Connection != null && this.Connection.State == ConnectionState.Open)
                    this.Transaction.Rollback();

                return;
            }
            catch (Exception)
            {
            }
        });

        /// <summary>
        /// This code added to correctly implement the disposable pattern.
        /// </summary>
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Dispose the current transaction and connection.
        /// </summary>
        public void Dispose(bool disposing)
        {
            if (!this.disposedValue)
            {
                if (disposing)
                {
                    if (this.Orchestrator != null)
                    {
                        if (!this.AlreadyInTransaction && this.Transaction != null)
                        {
                            this.Transaction.Dispose();
                            this.Transaction = null;
                        }

                        if (!this.AlreadyOpened && this.Connection != null)
                        {
                            if (this.Connection.State == ConnectionState.Open)
                                this.Connection.Close();

                            this.Connection.Dispose();
                            this.Connection = null;
                        }
                    }
                }

                this.disposedValue = true;
            }
        }

        /// <summary>
        /// Async dispose, when using "await using var runner = await this.GetConnectionAsync()".
        /// </summary>
        public async ValueTask DisposeAsync()
        {
            if (this.Orchestrator != null)
            {
                if (!this.AlreadyInTransaction && this.Transaction != null)
                {
#if NET6_0_OR_GREATER
                    await this.Transaction.DisposeAsync().ConfigureAwait(false);
#else
                    this.Transaction.Dispose();
#endif
                    this.Transaction = null;
                }

                if (!this.AlreadyOpened && this.Connection != null)
                {
                    await this.Orchestrator.CloseConnectionAsync(this.Context, this.Connection, this.Progress, this.CancellationToken).ConfigureAwait(false);
#if NET6_0_OR_GREATER
                    await this.Connection.DisposeAsync().ConfigureAwait(false);
#else
                    this.Connection.Dispose();
#endif
                    this.Connection = null;
                }
            }

            this.Dispose(false);
            GC.SuppressFinalize(this);
        }
    }
}