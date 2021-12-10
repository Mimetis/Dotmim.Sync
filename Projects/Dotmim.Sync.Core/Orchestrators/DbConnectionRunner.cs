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
                                SyncStage syncStage = SyncStage.None,
                                DbConnection connection = default,
                                DbTransaction transaction = default,
                                CancellationToken cancellationToken = default)
        {
            if (connection == null)
                connection = orchestrator.Provider.CreateConnection();

            var alreadyOpened = connection.State == ConnectionState.Open;
            var alreadyInTransaction = transaction != null && transaction.Connection == connection;

            // Get context or create a new one
            var ctx = orchestrator.GetContext();
            ctx.SyncStage = syncStage;

            // Open connection
            if (!alreadyOpened)
                await orchestrator.OpenConnectionAsync(connection, cancellationToken).ConfigureAwait(false);

            // Create a transaction
            if (!alreadyInTransaction)
            {
                transaction = connection.BeginTransaction(orchestrator.Provider.IsolationLevel);
                await orchestrator.InterceptAsync(new TransactionOpenedArgs(ctx, connection, transaction), cancellationToken).ConfigureAwait(false);
            }

            return new DbConnectionRunner(orchestrator, connection, transaction, alreadyOpened, alreadyInTransaction, cancellationToken); ;
        }
    }

    public sealed class DbConnectionRunner : IDisposable, IAsyncDisposable
    {
        public DbConnectionRunner(BaseOrchestrator orchestrator, DbConnection connection, DbTransaction transaction,
            bool alreadyOpened, bool alreadyInTransaction,
            CancellationToken cancellationToken = default)
        {
            this.Orchestrator = orchestrator;
            this.Connection = connection;
            this.Transaction = transaction;
            this.AlreadyOpened = alreadyOpened;
            this.AlreadyInTransaction = alreadyInTransaction;
            this.CancellationToken = cancellationToken;
        }

        public BaseOrchestrator Orchestrator { get; set; }
        public DbConnection Connection { get; set; }
        public DbTransaction Transaction { get; set; }
        public bool AlreadyOpened { get; }
        public bool AlreadyInTransaction { get; }
        public CancellationToken CancellationToken { get; }

        /// <summary>
        /// Commit the transaction and call an interceptor
        /// </summary>
        public async Task CommitAsync(bool autoClose = true)
        {
            await this.Orchestrator.InterceptAsync(
                new TransactionCommitArgs(this.Orchestrator.GetContext(), this.Connection, this.Transaction), this.CancellationToken).ConfigureAwait(false);

            if (!this.AlreadyInTransaction && this.Transaction != null)
                this.Transaction.Commit();

            if (autoClose)
                await CloseAsync();
        }

        /// <summary>
        /// Commit the transaction and call an interceptor
        /// </summary>
        public async Task CloseAsync()
        {
            if (!this.AlreadyOpened && this.Connection != null)
                await this.Orchestrator.CloseConnectionAsync(this.Connection, this.CancellationToken).ConfigureAwait(false);
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
                await this.Orchestrator.CloseConnectionAsync(this.Connection, this.CancellationToken).ConfigureAwait(false);
                this.Connection.Dispose();
                this.Connection = null;
            }

            this.Dispose(false);
            GC.SuppressFinalize(this);
        }
    }
}
