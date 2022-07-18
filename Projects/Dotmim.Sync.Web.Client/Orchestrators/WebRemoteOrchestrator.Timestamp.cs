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

        internal override async Task<(SyncContext context, long timestamp)> InternalGetLocalTimestampAsync(SyncContext context,
                     DbConnection connection, DbTransaction transaction,
                     CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)

        {
            // Create the message to be sent
            var httpMessage = new HttpMessageRemoteTimestampRequest(context);

            // serialize message
            var serializer = this.SerializerFactory.GetSerializer<HttpMessageRemoteTimestampRequest>();
            var binaryData = await serializer.SerializeAsync(httpMessage);

            // No batch size submitted here, because the schema will be generated in memory and send back to the user.
            var response = await this.httpRequestHandler.ProcessRequestAsync
                (this.HttpClient, context, this.ServiceUri, binaryData, HttpStep.GetRemoteClientTimestamp, 
                 this.SerializerFactory, this.Converter, 0, this.SyncPolicy, cancellationToken, progress).ConfigureAwait(false);

            HttpMessageRemoteTimestampResponse responseTimestamp = null;

            using (var streamResponse = await response.Content.ReadAsStreamAsync().ConfigureAwait(false))
            {
                if (streamResponse.CanRead)
                    responseTimestamp = await this.SerializerFactory.GetSerializer<HttpMessageRemoteTimestampResponse>().DeserializeAsync(streamResponse);
            }

            if (responseTimestamp == null)
                throw new ArgumentException("Http Message content for Get Client Remote Timestamp can't be null");

            // Return scopes and new shema
            return (responseTimestamp.SyncContext, responseTimestamp.RemoteClientTimestamp);
        }



    }
}
