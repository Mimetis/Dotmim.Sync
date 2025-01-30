using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync.Web.Client
{
    /// <summary>
    /// Contains the forbidden logic to handle schema on the server side.
    /// </summary>
    public partial class WebRemoteOrchestrator : RemoteOrchestrator
    {

        /// <summary>
        /// Method not allowed from WebRemoteOrchestrator.
        /// </summary>
        public override Task<SyncSet> GetSchemaAsync(SyncSetup setup, DbConnection connection = null, DbTransaction transaction = null)
            => throw new NotImplementedException();

        /// <summary>
        /// Method not allowed from WebRemoteOrchestrator.
        /// </summary>
        public override Task<SyncSet> GetSchemaAsync(string scopeName, SyncSetup setup, DbConnection connection = null, DbTransaction transaction = null)
            => throw new NotImplementedException();
    }
}