using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using Dotmim.Sync.Core.Batch;
using Dotmim.Sync.Core.Builders;
using Dotmim.Sync.Core.Manager;
using Dotmim.Sync.Core.Scope;
using Dotmim.Sync.Data;

namespace Dotmim.Sync.Core.Proxy.Client
{
    public class RemoteClientProxyProvider : IResponseHandler
    {
        /// <summary>
        /// Remote server URI
        /// </summary>
        public Uri ServiceUri { get; private set; }

        public event EventHandler<ScopeProgressEventArgs> SyncProgress;


        public RemoteClientProxyProvider(Uri serviceUri)
        {
            this.ServiceUri = serviceUri;
        }


        public void BeginSession()
        {
            // Send a beginSession to the remote
        }

       
        public void EndSession()
        {
            // Send a endsession to the remote
        }

        public void EnsureDatabase(DbBuilderOption options)
        {
           // Send a ensuredatabase
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

        public void ApplyChanges(ScopeInfo fromScope, BatchInfo changes)
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
