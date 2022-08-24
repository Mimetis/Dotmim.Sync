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

        /// <summary>
        /// Get server scope from server, by sending an http request to the server 
        /// </summary>
        internal override async Task<(SyncContext context, SyncOperation operation)>
                InternalGetOperationAsync(ServerScopeInfo serverScopeInfo, ScopeInfo clientScopeInfo, SyncContext context, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            // Create the message to be sent
            var httpMessage = new HttpMessageOperationRequest(context, clientScopeInfo);

            // serialize message
            var serializer = this.SerializerFactory.GetSerializer<HttpMessageOperationRequest>();
            var binaryData = await serializer.SerializeAsync(httpMessage);

            // No batch size submitted here, because the schema will be generated in memory and send back to the user.
            var response = await this.httpRequestHandler.ProcessRequestAsync
                (this.HttpClient,context, this.ServiceUri, binaryData, HttpStep.GetOperation, 
                 this.SerializerFactory, this.Converter, 0, this.SyncPolicy, cancellationToken, progress).ConfigureAwait(false);

            HttpMessageOperationResponse operationResponse = null;

            using (var streamResponse = await response.Content.ReadAsStreamAsync().ConfigureAwait(false))
            {
                if (streamResponse.CanRead)
                    operationResponse = await this.SerializerFactory.GetSerializer<HttpMessageOperationResponse>().DeserializeAsync(streamResponse);
            }

            if (operationResponse == null)
                throw new ArgumentException("Http Message content for Get Operation scope can't be null");

            // Return scopes and new shema
            return (context, operationResponse.SyncOperation);
        }

    }
}
