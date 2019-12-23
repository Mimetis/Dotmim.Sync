using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Messages;
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
        public virtual async Task<(SyncContext, ScopeInfo)> EnsureScopesAsync(SyncContext context, string scopeInfoTableName, string scopeName,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                var scopes = new List<ScopeInfo>();

                var scopeBuilder = this.GetScopeBuilder();
                var scopeInfoBuilder = scopeBuilder.CreateScopeInfoBuilder(scopeInfoTableName, connection, transaction);

                var needToCreateScopeInfoTable = scopeInfoBuilder.NeedToCreateScopeInfoTable();

                // create the scope info table if needed
                if (needToCreateScopeInfoTable)
                    scopeInfoBuilder.CreateScopeInfoTable();

                // not the first time we ensure scopes, so get scopes
                if (!needToCreateScopeInfoTable)
                {
                    // get all scopes shared by all (identified by scopeName)
                    scopes = scopeInfoBuilder.GetAllScopes(scopeName);
                }

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

                    scope = scopeInfoBuilder.InsertOrUpdateScopeInfo(scope);
                    scopes.Add(scope);
                }
                else
                {
                    //check if we have alread a good last sync. if no, treat it as new
                    scopes.ForEach(sc => sc.IsNewScope = sc.LastSync == null);
                }

                // get first scope
                var localScope = scopes.FirstOrDefault();

                // Progress & Interceptor
                context.SyncStage = SyncStage.ScopeLoading;
                var scopeArgs = new ScopeArgs(context, localScope, connection, transaction);
                this.ReportProgress(context, progress, scopeArgs);
                await this.InterceptAsync(scopeArgs).ConfigureAwait(false);

                return (context, localScope);
            }
            catch (Exception ex)
            {
                throw new SyncException(ex, SyncStage.ScopeLoading);
            }
        }

        /// <summary>
        /// Write scope in the local data source
        /// </summary>
        public virtual async Task<SyncContext> WriteScopesAsync(SyncContext context, string scopeInfoTableName, ScopeInfo scope,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                var scopeBuilder = this.GetScopeBuilder();
                var scopeInfoBuilder = scopeBuilder.CreateScopeInfoBuilder(scopeInfoTableName, connection, transaction);

                scopeInfoBuilder.InsertOrUpdateScopeInfo(scope);

                // Progress & Interceptor
                context.SyncStage = SyncStage.ScopeSaved;
                var scopeArgs = new ScopeArgs(context, scope, connection, transaction);
                this.ReportProgress(context, progress, scopeArgs);
                await this.InterceptAsync(scopeArgs).ConfigureAwait(false);

                return context;
            }
            catch (Exception ex)
            {
                throw new SyncException(ex, SyncStage.ScopeSaved);
            }


        }

    }
}
