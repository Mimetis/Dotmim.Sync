using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    //Make an extension method to allow calling the static method as in BaseOrchestrator

    public static class DbConnectionRunnerExtensions
    {
        public static async Task<DbConnectionRunner> GetConnectionAsync(this BaseOrchestrator orchestrator,
                                string scopeName,
                                SyncMode syncMode = SyncMode.Writing,
                                SyncStage syncStage = SyncStage.None,
                                DbConnection connection = default,
                                DbTransaction transaction = default,
                                CancellationToken cancellationToken = default,
                                IProgress<ProgressArgs> progress = default)
        {
            if (connection == null)
                connection = orchestrator.Provider.CreateConnection();

            var alreadyOpened = connection.State == ConnectionState.Open;
            var alreadyInTransaction = transaction != null && transaction.Connection == connection;

            // Get context or create a new one
            var ctx = orchestrator.GetContext(scopeName);
            ctx.SyncStage = syncStage;

            // Open connection
            if (!alreadyOpened)
                await orchestrator.OpenConnectionAsync(scopeName, connection, cancellationToken, progress).ConfigureAwait(false);

            // Create a transaction
            if (!alreadyInTransaction && syncMode == SyncMode.Writing)
            {
                transaction = connection.BeginTransaction(orchestrator.Provider.IsolationLevel);
                await orchestrator.InterceptAsync(new TransactionOpenedArgs(ctx, connection, transaction), progress, cancellationToken).ConfigureAwait(false);
            }

            return new DbConnectionRunner(orchestrator, scopeName, connection, transaction, alreadyOpened, alreadyInTransaction, cancellationToken, progress);
        }
    }

    public sealed class DbConnectionRunner : IDisposable, IAsyncDisposable
    {
        public DbConnectionRunner(BaseOrchestrator orchestrator, string scopeName, DbConnection connection, DbTransaction transaction,
            bool alreadyOpened, bool alreadyInTransaction,
            CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = default)
        {
            this.Orchestrator = orchestrator;
            this.ScopeName = scopeName;
            this.Connection = connection;
            this.Transaction = transaction;
            this.AlreadyOpened = alreadyOpened;
            this.AlreadyInTransaction = alreadyInTransaction;
            this.CancellationToken = cancellationToken;
            this.Progress = progress;
        }

        public BaseOrchestrator Orchestrator { get; set; }
        public string ScopeName { get; }
        public DbConnection Connection { get; set; }
        public DbTransaction Transaction { get; set; }
        public bool AlreadyOpened { get; }
        public bool AlreadyInTransaction { get; }
        public CancellationToken CancellationToken { get; }
        public IProgress<ProgressArgs> Progress { get; }

        /// <summary>
        /// Commit the transaction and call an interceptor
        /// </summary>
        public async Task CommitAsync(bool autoClose = true)
        {
            if (!this.AlreadyInTransaction && this.Transaction != null)
            {
                await this.Orchestrator.InterceptAsync(
                    new TransactionCommitArgs(this.Orchestrator.GetContext(ScopeName), this.Connection, this.Transaction), this.Progress, this.CancellationToken).ConfigureAwait(false);

                this.Transaction.Commit();
            }

            if (autoClose)
                await CloseAsync();
        }

        /// <summary>
        /// Commit the transaction and call an interceptor
        /// </summary>
        public async Task CloseAsync()
        {
            if (!this.AlreadyOpened && this.Connection != null)
                await this.Orchestrator.CloseConnectionAsync(ScopeName, this.Connection, this.CancellationToken, this.Progress).ConfigureAwait(false);
        }

        public Task RollbackAsync() => Task.Run(() => this.Transaction.Rollback());


        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        private bool disposedValue = false;

        public void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
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
                disposedValue = true;
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (!this.AlreadyInTransaction && this.Transaction != null)
            {
                this.Transaction.Dispose();
                this.Transaction = null;
            }

            if (!this.AlreadyOpened && this.Connection != null)
            {
                await this.Orchestrator.CloseConnectionAsync(ScopeName, this.Connection, this.CancellationToken, this.Progress).ConfigureAwait(false);
                this.Connection.Dispose();
                this.Connection = null;
            }

            this.Dispose(false);
            GC.SuppressFinalize(this);
        }
    }
}
