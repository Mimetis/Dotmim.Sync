using System;
using System.Collections.Generic;
using System.Text;
using Dotmim.Sync.Core.Builders;
using Dotmim.Sync.Core.Scope;
using Dotmim.Sync.Core.Batch;
using DmBinaryFormatter;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Dotmim.Sync.Core.Proxy.Client;
using System.IO;
using System.Linq;

namespace Dotmim.Sync.Core.Proxy
{

    /// <summary>
    /// Class used when you have to deal with a Web Server
    /// </summary>
    public class WebProxyProvider : IResponseHandler
    {
        bool isRemote;
        private CoreProvider localProvider;
        private Uri serviceUri;
        private HttpRequest httpRequest;
     


        /// <summary>
        /// Use this Constructor if you are on the Client Side, only
        /// </summary>
        /// <param name="serviceUri"></param>
        public WebProxyProvider(Uri serviceUri)
        {
            this.serviceUri = serviceUri;
        }

        /// <summary>
        /// Use this constructor when you are on the Remote Side, only
        /// </summary>
        public WebProxyProvider(CoreProvider localProvider, ServiceConfiguration configuration)
        {
            this.localProvider = localProvider;
            this.localProvider.Configuration = configuration;
        }

        public event EventHandler<ScopeProgressEventArgs> SyncProgress;

        public void HandleRequest(HttpRequest httpRequest)
        {
            if (!this.isRemote)
                throw new Exception("Use this method only when you are on the server side");

            this.httpRequest = httpRequest;

            var streamArray = this.httpRequest.Body;

            DmSerializer serializer = new DmSerializer();

            var changeSetRequest = serializer.Deserialize<ChangeSetRequest>(streamArray);

            switch (changeSetRequest.Step)
            {
                case HttpStep.BeginSession:
                    this.BeginSession();
                    break;
                case HttpStep.ApplyConfiguration:
                    this.ApplyConfiguration();
                    break;
                case HttpStep.EnsureDatabase:
                    this.EnsureDatabase(DbBuilderOption.UseExistingSchema | DbBuilderOption.UseExistingTrackingTables);
                    break;
                case HttpStep.EnsureScopes:
                    this.EnsureScopes(changeSetRequest.ServerScopeName, changeSetRequest.ClientScopeName);
                    break;
                case HttpStep.GetChangeBatch:
                    this.GetChangeBatch();
                    break;
                case HttpStep.ApplyChanges:

                    this.InternallyApplyChanges(changeSetRequest);
                    break;
                case HttpStep.GetLocalTimestamp:
                    break;
                case HttpStep.WriteScopes:
                    this.WriteScopes();
                    break;
                case HttpStep.EndSession:
                    this.EndSession();
                    break;
            }



        }

        /// <summary>
        /// Have to internally apply changes
        /// </summary>
        /// <param name="changeSetRequest"></param>
        private void InternallyApplyChanges(ChangeSetRequest changeSetRequest)
        {
            if (!this.isRemote)
                throw new Exception("Use this method only when you are on the server side");

            // It's the last batch
            // So take all batches on the local machine
            // and reconstruct a BatchInfo to be applied
            if (changeSetRequest.IsLastBatch)
            {
                //// TODO Write file even if it's the last one !!

                //var filesDirectory = new DirectoryInfo(Path.Combine(
                //    this.localProvider.BatchingDirectory, 
                //    changeSetRequest.Id));

                //// Todo : Ordering files by name ==> name == index ?
                //var filesOrderd = filesDirectory.EnumerateFiles().OrderBy(fi => fi.Name);

                //BatchInfo bi = new BatchInfo();
                //bi.Id = changeSetRequest.Id;
                //bi.PathName = filesDirectory.FullName;
            
                //ScopeInfo fromScope = new ScopeInfo();
                //fromScope.LastTimestamp = changeSetRequest.LastTimestamp;
                //// we are on the remote, so it's came from client
                //fromScope.Name = changeSetRequest.ClientScopeName;

                //this.localProvider.ApplyChanges(fromScope, bi);

            }
            else
            {

            }
        }

        public async void ApplyChanges(ScopeInfo fromScope, BatchInfo changes)
        {
            if (!isRemote)
                await SendApplyChangesFromClientAsync(fromScope, changes);
            //else


        }

        /// <summary>
        /// Apply Changes from the local syncAgent, on the client side
        /// </summary>
        private async Task SendApplyChangesFromClientAsync(ScopeInfo fromScope, BatchInfo changes)
        {

            DmSerializer serializer = new DmSerializer();
            var client = new HttpClient();

            // Send all the parts
            foreach (var part in changes.BatchPartsInfo)
            {
                var changeSet = new ChangeSetRequest();
                //changeSet.ScopeName = fromScope.Name;
                changeSet.LastTimestamp = fromScope.LastTimestamp;
                changeSet.DmSetSurrogate = part.GetBatch().DmSetSurrogate;
                changeSet.Index = part.Index;
                //changeSet.Id = part.Id;
                changeSet.Index = part.Index;
                changeSet.IsLastBatch = part.IsLastBatch;

                var binaryData = serializer.Serialize(changeSet);

                // Don't need the Set surrogate anymore, so delete it.
                part.GetBatch().Clear();

                ByteArrayContent arrayContent = new ByteArrayContent(binaryData);
                var response = await client.PostAsync(this.serviceUri, arrayContent);

                if (!response.IsSuccessStatusCode)
                    throw new Exception("Can't send all changes part");

            }


        }


        public void BeginSession()
        {
            throw new NotImplementedException();
        }

     
        public void EndSession()
        {
            throw new NotImplementedException();
        }

        public void EnsureDatabase(DbBuilderOption options)
        {
            throw new NotImplementedException();
        }

        public (ScopeInfo serverScope, ScopeInfo clientScope) EnsureScopes(string serverScopeName, string clientScopeName = null)
        {
            throw new NotImplementedException();
        }

        public BatchInfo GetChangeBatch()
        {
            throw new NotImplementedException();
        }

        public long GetLocalTimestamp()
        {
            throw new NotImplementedException();
        }

        public void WriteScopes()
        {
            throw new NotImplementedException();
        }

        public ServiceConfiguration GetConfiguration()
        {
            throw new NotImplementedException();
        }

        public void ApplyConfiguration(ServiceConfiguration configuration = null)
        {
            throw new NotImplementedException();
        }
    }
}
