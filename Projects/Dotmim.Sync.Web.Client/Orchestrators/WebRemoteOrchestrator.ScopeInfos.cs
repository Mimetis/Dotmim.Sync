using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;

namespace Dotmim.Sync.Web.Client
{
    public partial class WebRemoteOrchestrator : RemoteOrchestrator
    {

        internal override async Task<(SyncContext context, ScopeInfo scopeInfo)> InternalLoadScopeInfoAsync(SyncContext context, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {
            ScopeInfo scopeInfo;
            (context, scopeInfo, _) = await this.InternalEnsureScopeInfoAsync(context, default, false, connection, transaction, cancellationToken, progress);
            return (context, scopeInfo);
        }

        internal override async Task<(SyncContext context, ScopeInfo serverScopeInfo, bool shouldProvision)>
            InternalEnsureScopeInfoAsync(
            SyncContext context, SyncSetup setup, bool overwrite, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            try
            {

                // Create the message to be sent
                var httpMessage = new HttpMessageEnsureScopesRequest(context);

                // Raise progress for sending request and waiting server response
                await this.InterceptAsync(new HttpGettingScopeRequestArgs(context, this.GetServiceHost()), progress, cancellationToken).ConfigureAwait(false);

                // No batch size submitted here, because the schema will be generated in memory and send back to the user.
                var ensureScopesResponse = await this.ProcessRequestAsync<HttpMessageEnsureScopesResponse>
                    (context, httpMessage, HttpStep.EnsureScopes, 0, cancellationToken, progress).ConfigureAwait(false);

                if (ensureScopesResponse == null)
                    throw new ArgumentException("Http Message content for Ensure scope can't be null");

                if (ensureScopesResponse.ServerScopeInfo == null)
                    throw new ArgumentException("Server scope from EnsureScopesAsync can't be null and may contains a server scope");

                // Re build schema relationships with all tables
                ensureScopesResponse.ServerScopeInfo.Schema?.EnsureSchema();

                // Report Progress
                await this.InterceptAsync(new HttpGettingScopeResponseArgs(ensureScopesResponse.ServerScopeInfo, ensureScopesResponse.SyncContext, this.GetServiceHost()), progress, cancellationToken).ConfigureAwait(false);

                // Return scopes and new shema
                return (context, ensureScopesResponse.ServerScopeInfo, false);
            }
            catch (HttpSyncWebException) { throw; } // throw server error
            catch (Exception ex) { throw GetSyncError(context, ex); } // throw client error

        }

        public override Task<ScopeInfo> SaveScopeInfoAsync(ScopeInfo scopeInfo, DbConnection connection = null, DbTransaction transaction = null)
            => throw new NotImplementedException();

        public override Task<bool> DeleteScopeInfoAsync(ScopeInfo scopeInfo, DbConnection connection = null, DbTransaction transaction = null)
            => throw new NotImplementedException();


    }
}
