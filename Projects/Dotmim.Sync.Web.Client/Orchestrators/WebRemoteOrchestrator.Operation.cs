using Dotmim.Sync.Enumerations;
using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync.Web.Client
{
    /// <summary>
    /// Contains the logic to get the hypothetical override operation from the server.
    /// </summary>
    public partial class WebRemoteOrchestrator : RemoteOrchestrator
    {

        /// <summary>
        /// Get server scope from server, by sending an http request to the server.
        /// </summary>
        internal override async Task<(SyncContext Context, SyncOperation Operation)>
            InternalGetOperationAsync(ScopeInfo serverScopeInfo, ScopeInfo cScopeInfo, ScopeInfoClient cScopeInfoClient, SyncContext context,
            DbConnection connection = null, DbTransaction transaction = null, IProgress<ProgressArgs> progress = null, CancellationToken cancellationToken = default)
        {
            try
            {
                // Create the message to be sent
                var httpMessage = new HttpMessageOperationRequest(context, cScopeInfo, cScopeInfoClient);

                // No batch size submitted here, because the schema will be generated in memory and send back to the user.
                var operationResponse = await this.ProcessRequestAsync<HttpMessageOperationResponse>(
                    context, httpMessage, HttpStep.GetOperation, 0, progress, cancellationToken).ConfigureAwait(false);

                if (operationResponse == null)
                    throw new ArgumentException("Http Message content for Get Operation scope can't be null");

                // Return scopes and new shema
                return (context, operationResponse.SyncOperation);
            }
            catch (HttpSyncWebException)
            {
                throw;
            } // throw server error
            catch (Exception ex)
            {
                throw this.GetSyncError(context, ex);
            } // throw client error
        }
    }
}