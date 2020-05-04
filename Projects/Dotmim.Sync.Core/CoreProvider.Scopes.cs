using Dotmim.Sync.Enumerations;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public abstract partial class CoreProvider
    {

        /// <summary>
        /// Ensure the scope is created on the local provider.
        /// The scope contains all about last sync, schema and scope and local / remote timestamp 
        /// </summary>
        public virtual async Task<SyncContext> EnsureClientScopeAsync(SyncContext context, string scopeInfoTableName,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {

            var scopeBuilder = this.GetScopeBuilder();
            var scopeInfoBuilder = scopeBuilder.CreateScopeInfoBuilder(scopeInfoTableName, connection, transaction);

            var needToCreateScopeInfoTable = await scopeInfoBuilder.NeedToCreateClientScopeInfoTableAsync().ConfigureAwait(false);

            this.Orchestrator.logger.LogDebug(SyncEventsId.CreateTable, new { TableName = scopeInfoTableName, TableExists = !needToCreateScopeInfoTable });

            // create the scope info table if needed
            if (needToCreateScopeInfoTable)
                await scopeInfoBuilder.CreateClientScopeInfoTableAsync().ConfigureAwait(false);

            return context;
        }

        /// <summary>
        /// Gets a boolean indicating if client scope table exists
        /// </summary>
        public virtual async Task<(SyncContext, bool)> ClientScopeExistsAsync(SyncContext context, string scopeInfoTableName,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {

            var scopeBuilder = this.GetScopeBuilder();
            var scopeInfoBuilder = scopeBuilder.CreateScopeInfoBuilder(scopeInfoTableName, connection, transaction);

            var exist = !await scopeInfoBuilder.NeedToCreateClientScopeInfoTableAsync().ConfigureAwait(false);

            return (context, exist);
        }

        /// <summary>
        /// Drop client scope
        /// </summary>
        public virtual async Task<SyncContext> DropClientScopeAsync(SyncContext context, string scopeInfoTableName,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {

            var scopeBuilder = this.GetScopeBuilder();
            var scopeInfoBuilder = scopeBuilder.CreateScopeInfoBuilder(scopeInfoTableName, connection, transaction);

            var needToCreateScopeInfoTable = await scopeInfoBuilder.NeedToCreateClientScopeInfoTableAsync().ConfigureAwait(false);

            this.Orchestrator.logger.LogDebug(SyncEventsId.DropTable, new { TableName = scopeInfoTableName, TableExists = !needToCreateScopeInfoTable });

            if (!needToCreateScopeInfoTable)
                await scopeInfoBuilder.DropClientScopeInfoTableAsync().ConfigureAwait(false);

            return context;
        }

        /// <summary>
        /// Ensure the scope history is created on the remote provider.
        /// Contains all history for each client scope
        /// </summary>
        public virtual async Task<SyncContext> EnsureServerHistoryScopeAsync(SyncContext context, string scopeInfoTableName,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {

            var scopeBuilder = this.GetScopeBuilder();
            var scopeInfoBuilder = scopeBuilder.CreateScopeInfoBuilder(scopeInfoTableName, connection, transaction);

            var need = await scopeInfoBuilder.NeedToCreateServerHistoryScopeInfoTableAsync().ConfigureAwait(false);

            this.Orchestrator.logger.LogDebug(SyncEventsId.CreateTable, new { TableName = scopeInfoTableName, TableExists = !need });

            // create the scope info table if needed
            if (need)
                await scopeInfoBuilder.CreateServerHistoryScopeInfoTableAsync().ConfigureAwait(false);

            return context;
        }

        /// <summary>
        /// Drop the scope history
        /// </summary>
        public virtual async Task<SyncContext> DropServerHistoryScopeAsync(SyncContext context, string scopeInfoTableName,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {

            var scopeBuilder = this.GetScopeBuilder();
            var scopeInfoBuilder = scopeBuilder.CreateScopeInfoBuilder(scopeInfoTableName, connection, transaction);

            var need = await scopeInfoBuilder.NeedToCreateServerHistoryScopeInfoTableAsync().ConfigureAwait(false);

            this.Orchestrator.logger.LogDebug(SyncEventsId.DropTable, new { TableName = scopeInfoTableName, TableExists = !need });

            // create the scope info table if needed
            if (!need)
                await scopeInfoBuilder.DropServerHistoryScopeInfoTableAsync().ConfigureAwait(false);

            return context;
        }

        /// <summary>
        /// Ensure the scope is created on the remote provider.
        /// The scope contains schema and last clean metadatas
        /// </summary>
        public virtual async Task<SyncContext> EnsureServerScopeAsync(SyncContext context, string scopeInfoTableName,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {

            var scopeBuilder = this.GetScopeBuilder();
            var scopeInfoBuilder = scopeBuilder.CreateScopeInfoBuilder(scopeInfoTableName, connection, transaction);

            var needToCreateScopeInfoTable = await scopeInfoBuilder.NeedToCreateServerScopeInfoTableAsync().ConfigureAwait(false);

            this.Orchestrator.logger.LogDebug(SyncEventsId.CreateTable, new { TableName = scopeInfoTableName, TableExists = !needToCreateScopeInfoTable });

            // create the scope info table if needed
            if (needToCreateScopeInfoTable)
                await scopeInfoBuilder.CreateServerScopeInfoTableAsync().ConfigureAwait(false);

            return context;
        }

        /// <summary>
        /// Drop the scope created on the remote provider.
        /// </summary>
        public virtual async Task<SyncContext> DropServerScopeAsync(SyncContext context, string scopeInfoTableName,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {

            var scopeBuilder = this.GetScopeBuilder();
            var scopeInfoBuilder = scopeBuilder.CreateScopeInfoBuilder(scopeInfoTableName, connection, transaction);

            var needToCreateScopeInfoTable = await scopeInfoBuilder.NeedToCreateServerScopeInfoTableAsync().ConfigureAwait(false);

            this.Orchestrator.logger.LogDebug(SyncEventsId.DropTable, new { TableName = scopeInfoTableName, TableExists = !needToCreateScopeInfoTable });

            // create the scope info table if needed
            if (!needToCreateScopeInfoTable)
                await scopeInfoBuilder.DropServerScopeInfoTableAsync().ConfigureAwait(false);

            return context;
        }


        /// <summary>
        /// Get Client scope information
        /// </summary>
        public virtual async Task<(SyncContext, ScopeInfo)> GetClientScopeAsync(SyncContext context, string scopeInfoTableName, string scopeName,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {

            var scopes = new List<ScopeInfo>();

            var scopeBuilder = this.GetScopeBuilder();
            var scopeInfoBuilder = scopeBuilder.CreateScopeInfoBuilder(scopeInfoTableName, connection, transaction);

            // get all scopes shared by all (identified by scopeName)
            scopes = await scopeInfoBuilder.GetAllClientScopesAsync(scopeName).ConfigureAwait(false);

            this.Orchestrator.logger.LogDebug(SyncEventsId.GetClientScope, new { ScopeName = scopeName, ScopeInfoTableName = scopeInfoTableName });

            // If no scope found, create it on the local provider
            if (scopes == null || scopes.Count <= 0)
            {
                scopes = new List<ScopeInfo>();

                // create a new scope id for the current owner (could be server or client as well)
                var scope = new ScopeInfo
                {
                    Id = Guid.NewGuid(),
                    Name = scopeName,
                    IsNewScope = true,
                    LastSync = null,
                };

                scope = await scopeInfoBuilder.InsertOrUpdateClientScopeInfoAsync(scope).ConfigureAwait(false);
                scopes.Add(scope);
            }
            else
            {
                //check if we have alread a good last sync. if no, treat it as new
                scopes.ForEach(sc => sc.IsNewScope = sc.LastSync == null);
            }

            // get first scope
            var localScope = scopes.FirstOrDefault();

            if (localScope.Schema != null)
                localScope.Schema.EnsureSchema();

            this.Orchestrator.logger.LogDebug(SyncEventsId.GetClientScope, localScope);

            return (context, localScope);
        }



        /// <summary>
        /// Get Client scope information
        /// </summary>
        public virtual async Task<(SyncContext, ServerScopeInfo)> GetServerScopeAsync(SyncContext context, string scopeInfoTableName, string scopeName,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {

            var scopeBuilder = this.GetScopeBuilder();
            var scopeInfoBuilder = scopeBuilder.CreateScopeInfoBuilder(scopeInfoTableName, connection, transaction);

            this.Orchestrator.logger.LogDebug(SyncEventsId.GetClientScope, new { ScopeName = scopeName, ScopeInfoTableName = scopeInfoTableName });

            // get all scopes shared by all (identified by scopeName)
            var serverScopes = await scopeInfoBuilder.GetAllServerScopesAsync(scopeName).ConfigureAwait(false);

            this.Orchestrator.logger.LogDebug(SyncEventsId.GetServerScope, new { ScopeCountFound = serverScopes.Count });

            // If no scope found, create it on the local provider
            if (serverScopes == null || serverScopes.Count <= 0)
            {
                serverScopes = new List<ServerScopeInfo>();

                // create a new scope id for the current owner (could be server or client as well)
                var scope = new ServerScopeInfo
                {
                    Name = scopeName,
                    LastCleanupTimestamp = 0,
                    Version = "1"
                };

                scope = await scopeInfoBuilder.InsertOrUpdateServerScopeInfoAsync(scope).ConfigureAwait(false);
                serverScopes.Add(scope);
            }

            // get first scope
            var localScope = serverScopes.FirstOrDefault();

            if (localScope.Schema != null)
                localScope.Schema.EnsureSchema();

            this.Orchestrator.logger.LogDebug(SyncEventsId.GetServerScope, localScope);

            return (context, localScope);
        }


        /// <summary>
        /// Get Client scope information
        /// </summary>
        public virtual async Task<(SyncContext, List<ServerHistoryScopeInfo>)> GetServerHistoryScopesAsync(SyncContext context, string scopeInfoTableName, string scopeName,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {

            var scopeBuilder = this.GetScopeBuilder();
            var scopeInfoBuilder = scopeBuilder.CreateScopeInfoBuilder(scopeInfoTableName, connection, transaction);

            this.Orchestrator.logger.LogDebug(SyncEventsId.GetServerScopeHistory, new { ScopeName = scopeName, ScopeInfoTableName = scopeInfoTableName });

            // get all scopes shared by all (identified by scopeName)
            var serverHistoryScopes = await scopeInfoBuilder.GetAllServerHistoryScopesAsync(scopeName).ConfigureAwait(false);

            foreach (var scope in serverHistoryScopes)
                this.Orchestrator.logger.LogDebug(SyncEventsId.GetServerScopeHistory, scope);

            return (context, serverHistoryScopes);
        }

        /// <summary>
        /// Write scope in the local data source
        /// </summary>
        public virtual async Task<SyncContext> WriteClientScopeAsync(SyncContext context, string scopeInfoTableName, ScopeInfo scope,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {

            var scopeBuilder = this.GetScopeBuilder();
            var scopeInfoBuilder = scopeBuilder.CreateScopeInfoBuilder(scopeInfoTableName, connection, transaction);

            this.Orchestrator.logger.LogDebug(SyncEventsId.WriteClientScope, scope);

            await scopeInfoBuilder.InsertOrUpdateClientScopeInfoAsync(scope).ConfigureAwait(false);

            return context;
        }

        /// <summary>
        /// Write scope in the remote data source
        /// </summary>
        public virtual async Task<SyncContext> WriteServerScopeAsync(SyncContext context, string scopeInfoTableName, ServerScopeInfo scope,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {

            var scopeBuilder = this.GetScopeBuilder();
            var scopeInfoBuilder = scopeBuilder.CreateScopeInfoBuilder(scopeInfoTableName, connection, transaction);

            this.Orchestrator.logger.LogDebug(SyncEventsId.WriteServerScope, scope);

            await scopeInfoBuilder.InsertOrUpdateServerScopeInfoAsync(scope).ConfigureAwait(false);

            return context;
        }

        /// <summary>
        /// Write scope history in the remote data source
        /// </summary>
        public virtual async Task<SyncContext> WriteServerHistoryScopeAsync(SyncContext context, string scopeInfoTableName, ServerHistoryScopeInfo scope,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {

            var scopeBuilder = this.GetScopeBuilder();
            var scopeInfoBuilder = scopeBuilder.CreateScopeInfoBuilder(scopeInfoTableName, connection, transaction);

            this.Orchestrator.logger.LogDebug(SyncEventsId.WriteServerScopeHistory, scope);

            await scopeInfoBuilder.InsertOrUpdateServerHistoryScopeInfoAsync(scope).ConfigureAwait(false);

            return context;
        }

    }
}
