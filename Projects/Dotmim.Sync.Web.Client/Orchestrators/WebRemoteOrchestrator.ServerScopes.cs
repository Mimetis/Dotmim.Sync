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
        internal override async Task<(SyncContext context, ServerScopeInfo serverScopeInfo)>
                InternalGetServerScopeInfoAsync(SyncContext context, SyncSetup setup, DbConnection connection, DbTransaction transaction, CancellationToken cancellationToken, IProgress<ProgressArgs> progress)
        {

            // Create the message to be sent
            var httpMessage = new HttpMessageEnsureScopesRequest(context);

            // serialize message
            var serializer = this.SerializerFactory.GetSerializer<HttpMessageEnsureScopesRequest>();
            var binaryData = await serializer.SerializeAsync(httpMessage);

            // Raise progress for sending request and waiting server response
            await this.InterceptAsync(new HttpGettingScopeRequestArgs(context, this.GetServiceHost()), progress, cancellationToken).ConfigureAwait(false);

            // No batch size submitted here, because the schema will be generated in memory and send back to the user.
            var response = await this.httpRequestHandler.ProcessRequestAsync
                (this.HttpClient,context, this.ServiceUri, binaryData, HttpStep.EnsureScopes, 
                 this.SerializerFactory, this.Converter, 0, this.SyncPolicy, cancellationToken, progress).ConfigureAwait(false);

            HttpMessageEnsureScopesResponse ensureScopesResponse = null;

            using (var streamResponse = await response.Content.ReadAsStreamAsync().ConfigureAwait(false))
            {
                if (streamResponse.CanRead)
                    ensureScopesResponse = await this.SerializerFactory.GetSerializer<HttpMessageEnsureScopesResponse>().DeserializeAsync(streamResponse);
            }

            if (ensureScopesResponse == null)
                throw new ArgumentException("Http Message content for Ensure scope can't be null");

            if (ensureScopesResponse.ServerScopeInfo == null)
                throw new ArgumentException("Server scope from EnsureScopesAsync can't be null and may contains a server scope");

            // Re build schema relationships with all tables
            ensureScopesResponse.ServerScopeInfo.Schema?.EnsureSchema();

            // Report Progress
            await this.InterceptAsync(new HttpGettingScopeResponseArgs(ensureScopesResponse.ServerScopeInfo, ensureScopesResponse.SyncContext, this.GetServiceHost()), progress, cancellationToken).ConfigureAwait(false);

            // Return scopes and new shema
            return (context, ensureScopesResponse.ServerScopeInfo);
        }

        public override Task<ServerScopeInfo> SaveServerScopeInfoAsync(ServerScopeInfo serverScopeInfo, DbConnection connection = null, DbTransaction transaction = null, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => throw new NotImplementedException();

    }
}
