using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Messages;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public abstract partial class CoreProvider
    {

        /// <summary>
        /// Called when the sync ensure scopes are created
        /// </summary>
        public virtual async Task<(SyncContext, List<ScopeInfo>)> EnsureScopesAsync (SyncContext context, MessageEnsureScopes message)
        {
            DbConnection connection = null;
            try
            {
                var scopes = new List<ScopeInfo>();

                // Open the connection
                using (connection = this.CreateConnection())
                {
                    await connection.OpenAsync();

                    using (var transaction = connection.BeginTransaction())
                    {
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
                            var lstScopes = scopeInfoBuilder.GetAllScopes(message.ScopeName);

                            // try to get the scopes from database
                            // could be two scopes if from server or a single scope if from client
                            scopes = lstScopes.Where(s => (s.IsLocal == true || (message.ClientReferenceId.HasValue && s.Id == message.ClientReferenceId.Value))).ToList();

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
                                IsLocal = true,
                                IsNewScope = true,
                                LastSync = null
                            };

                            scope = scopeInfoBuilder.InsertOrUpdateScopeInfo(scope);

                            scopes.Add(scope);
                        }
                        else
                        {
                            //check if we have alread a good last sync. if no, treat it as new
                            scopes.ForEach(sc => sc.IsNewScope = sc.LastSync == null);
                        }

                        // if we are not on the server, we have to check that we only have one scope
                        if (!message.ClientReferenceId.HasValue && scopes.Count > 1)
                            throw new InvalidOperationException("On Local provider, we should have only one scope info");


                        // if we have a reference in args, we need to get this specific line from database
                        // this happen only on the server side
                        if (message.ClientReferenceId.HasValue)
                        {
                            var refScope = scopes.FirstOrDefault(s => s.Id == message.ClientReferenceId);

                            if (refScope == null)
                            {
                                refScope = new ScopeInfo
                                {
                                    Id = message.ClientReferenceId.Value,
                                    Name = message.ScopeName,
                                    IsLocal = false,
                                    IsNewScope = true,
                                    LastSync = null
                                };

                                refScope = scopeInfoBuilder.InsertOrUpdateScopeInfo(refScope);

                                scopes.Add(refScope);
                            }
                            else
                            {
                                refScope.IsNewScope = refScope.LastSync == null;
                            }
                        }

                        context.SyncStage = SyncStage.ScopeLoading;
                        this.ReportProgress(context, connection, transaction);

                        await this.InterceptAsync(new ScopeArgs(context, scopes.FirstOrDefault(s => s.IsLocal), connection, transaction));

                        transaction.Commit();
                    }

                    connection.Close();
                }

                return (context, scopes);
            }
            catch (Exception ex)
            {
                throw new SyncException(ex, SyncStage.ScopeLoading);
            }
            finally
            {
                if (connection != null && connection.State != ConnectionState.Closed)
                    connection.Close();
            }

        }

        /// <summary>
        /// Write scope in the provider datasource
        /// </summary>
        public virtual async Task<SyncContext> WriteScopesAsync(SyncContext context,
            MessageWriteScopes message)
        {
            DbConnection connection = null;

            try
            {
                // Open the connection
                using (connection = this.CreateConnection())
                {
                    await connection.OpenAsync();

                    using (var transaction = connection.BeginTransaction())
                    {

                        var scopeBuilder = this.GetScopeBuilder();
                        var scopeInfoBuilder = scopeBuilder.CreateScopeInfoBuilder(message.ScopeInfoTableName, connection, transaction);

                        var lstScopes = new List<ScopeInfo>();

                        foreach (var scope in message.Scopes)
                            lstScopes.Add(scopeInfoBuilder.InsertOrUpdateScopeInfo(scope));

                        context.SyncStage = SyncStage.ScopeSaved;

                        this.ReportProgress(context, connection, transaction);

                        await this.InterceptAsync(new ScopeArgs(context, lstScopes.FirstOrDefault(s => s.IsLocal), connection, transaction));

                        this.ReportProgress(context, connection, transaction);

                        transaction.Commit();
                    }

                    connection.Close();
                }
            }
            catch (Exception ex)
            {
                throw new SyncException(ex, SyncStage.ScopeSaved);
            }
            finally
            {
                if (connection != null && connection.State != ConnectionState.Closed)
                    connection.Close();
            }
            return context;

        }

    }
}
