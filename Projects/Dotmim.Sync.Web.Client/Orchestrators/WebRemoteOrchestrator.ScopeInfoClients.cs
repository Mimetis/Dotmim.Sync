using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync.Web.Client
{
    /// <summary>
    /// Contains the forbidden logic to handle scope info clients on the server side.
    /// </summary>
    public partial class WebRemoteOrchestrator : RemoteOrchestrator
    {

        /// <summary>
        /// Method not allowed from WebRemoteOrchestrator.
        /// </summary>
        public override Task<List<ScopeInfoClient>> GetAllScopeInfoClientsAsync(DbConnection connection = null, DbTransaction transaction = null)
              => throw new NotImplementedException();

        /// <summary>
        /// Method not allowed from WebRemoteOrchestrator.
        /// </summary>
        public override Task<ScopeInfoClient> GetScopeInfoClientAsync(Guid clientId, string scopeName = "DefaultScope", SyncParameters parameters = null, DbConnection connection = null, DbTransaction transaction = null)
              => throw new NotImplementedException();

        /// <summary>
        /// Method not allowed from WebRemoteOrchestrator.
        /// </summary>
        public override Task<ScopeInfoClient> SaveScopeInfoClientAsync(ScopeInfoClient scopeInfoClient, DbConnection connection = null, DbTransaction transaction = null)
              => throw new NotImplementedException();
    }
}