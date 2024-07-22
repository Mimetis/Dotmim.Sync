using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync.Web.Client
{
    public partial class WebRemoteOrchestrator : RemoteOrchestrator
    {

        internal override async Task<(SyncContext Context, long Timestamp)> InternalGetLocalTimestampAsync(
            SyncContext context,
            DbConnection connection, DbTransaction transaction,
            IProgress<ProgressArgs> progress = default, CancellationToken cancellationToken = default)
        {
            try
            {
                // Create the message to be sent
                var httpMessage = new HttpMessageRemoteTimestampRequest(context);

                // No batch size submitted here, because the schema will be generated in memory and send back to the user.
                var responseTimestamp = await this.ProcessRequestAsync<HttpMessageRemoteTimestampResponse>(
                    context, httpMessage, HttpStep.GetRemoteClientTimestamp, 0, progress, cancellationToken).ConfigureAwait(false);

                if (responseTimestamp == null)
                    throw new ArgumentException("Http Message content for Get Client Remote Timestamp can't be null");

                // Return scopes and new shema
                return (context, responseTimestamp.RemoteClientTimestamp);
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