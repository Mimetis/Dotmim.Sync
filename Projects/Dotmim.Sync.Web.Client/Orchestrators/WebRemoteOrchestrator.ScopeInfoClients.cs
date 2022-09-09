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
        public override Task<List<ScopeInfoClient>> GetAllScopeInfoClientsAsync(string scopeName = "DefaultScope", DbConnection connection = null, DbTransaction transaction = null)
              => throw new NotImplementedException();

        public override Task<ScopeInfoClient> GetScopeInfoClientAsync(Guid clientId, string scopeName = "DefaultScope", SyncParameters syncParameters = null, DbConnection connection = null, DbTransaction transaction = null)
              => throw new NotImplementedException();

        public override Task<ScopeInfoClient> SaveScopeInfoClientAsync(ScopeInfoClient scopeInfoClient, DbConnection connection = null, DbTransaction transaction = null)
              => throw new NotImplementedException();
    }
}
