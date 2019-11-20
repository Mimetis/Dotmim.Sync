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
        /// Called when the sync ensure scopes are created
        /// </summary>
        public virtual async Task<(SyncContext, ScopeInfo)> EnsureScopesAsync(SyncContext context, MessageEnsureScopes message,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                var scopes = new List<ScopeInfo>();

                var scopeBuilder = this.GetScopeBuilder();
                var scopeInfoBuilder = scopeBuilder.CreateScopeInfoBuilder(
                    message.ScopeInfoTableName, connection, transaction);

                var needToCreateScopeInfoTable = scopeInfoBuilder.NeedToCreateScopeInfoTable();

                // create the scope info table if needed
                if (needToCreateScopeInfoTable)
                    scopeInfoBuilder.CreateScopeInfoTable();

                // not the first time we ensure scopes, so get scopes
                if (!needToCreateScopeInfoTable)
                {
                    // get all scopes shared by all (identified by scopeName)
                    scopes = scopeInfoBuilder.GetAllScopes(message.ScopeName);
                }

                // If no scope found, create it on the local provider
                if (scopes == null || scopes.Count <= 0)
                {
                    scopes = new List<ScopeInfo>();

                    // create a new scope id for the current owner (could be server or client as well)
                    var scope = new ScopeInfo
                    {
                        Id = Guid.NewGuid(),
                        Name = message.ScopeName,
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
        /// Write scope in the provider datasource
        /// </summary>
        public virtual async Task<SyncContext> WriteScopesAsync(SyncContext context, MessageWriteScopes message,
                             DbConnection connection, DbTransaction transaction,
                             CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                var scopeBuilder = this.GetScopeBuilder();
                var scopeInfoBuilder = scopeBuilder.CreateScopeInfoBuilder(message.ScopeInfoTableName, connection, transaction);

                scopeInfoBuilder.InsertOrUpdateScopeInfo(message.Scope);

                // Progress & Interceptor
                context.SyncStage = SyncStage.ScopeSaved;
                var scopeArgs = new ScopeArgs(context, message.Scope, connection, transaction);
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
