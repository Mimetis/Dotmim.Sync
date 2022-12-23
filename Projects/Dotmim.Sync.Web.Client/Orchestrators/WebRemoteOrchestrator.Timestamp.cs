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
            try
            {
                // Create the message to be sent
                var httpMessage = new HttpMessageRemoteTimestampRequest(context);

                // No batch size submitted here, because the schema will be generated in memory and send back to the user.
                var responseTimestamp = await this.ProcessRequestAsync<HttpMessageRemoteTimestampResponse>
                    (context, httpMessage, HttpStep.GetRemoteClientTimestamp, 0, cancellationToken, progress).ConfigureAwait(false);

                if (responseTimestamp == null)
                    throw new ArgumentException("Http Message content for Get Client Remote Timestamp can't be null");

                // Return scopes and new shema
                return (context, responseTimestamp.RemoteClientTimestamp);
            }
            catch (HttpSyncWebException) { throw; } // throw server error
            catch (Exception ex) { throw GetSyncError(context, ex); } // throw client error

        }



    }
}
