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

        internal override async Task<SyncContext> InternalBeginSessionAsync(SyncContext context, CancellationToken cancellationToken, IProgress<ProgressArgs> progress = null)
        {
            // Progress & interceptor
            var sessionBegin = new SessionBeginArgs(context, null) { Source = this.GetServiceHost() };
            
            await this.InterceptAsync(sessionBegin, progress, cancellationToken).ConfigureAwait(false);

            return context;

        }

        internal override async Task<SyncContext> InternalEndSessionAsync(SyncContext context, SyncResult result, ServerSyncChanges serverSyncChanges, SyncException syncException = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
        {
            try
            {
                // Create the message to be sent
                var httpMessage = new HttpMessageEndSessionRequest(context)
                {
                    ChangesAppliedOnClient = result.ChangesAppliedOnClient,
                    ClientChangesSelected = result.ClientChangesSelected,
                    ChangesAppliedOnServer = result.ChangesAppliedOnServer,
                    CompleteTime = result.CompleteTime,
                    ServerChangesSelected = result.ServerChangesSelected,
                    SnapshotChangesAppliedOnClient = result.SnapshotChangesAppliedOnClient,
                    StartTime = result.StartTime,
                    SyncExceptionMessage = syncException?.Message,
                };

                // serialize message
                var serializer = this.SerializerFactory.GetSerializer<HttpMessageEndSessionRequest>();
                var binaryData = await serializer.SerializeAsync(httpMessage);

                // No batch size submitted here, because the schema will be generated in memory and send back to the user.
                await this.httpRequestHandler.ProcessRequestAsync
                    (this.HttpClient, context, this.ServiceUri, binaryData, HttpStep.EndSession,
                     this.SerializerFactory, this.Converter, 0, this.SyncPolicy, cancellationToken, progress).ConfigureAwait(false);

                // Progress & interceptor
                var sessionEnd = new SessionEndArgs(context, result, syncException, null) { Source = this.GetServiceHost() };

                await this.InterceptAsync(sessionEnd, progress, cancellationToken).ConfigureAwait(false);

                // Return scopes and new shema
                return context;
            }
            catch (HttpSyncWebException) { throw; } // throw server error
            catch (Exception ex) { throw GetSyncError(context, ex); } // throw client error

        }
    }
}
