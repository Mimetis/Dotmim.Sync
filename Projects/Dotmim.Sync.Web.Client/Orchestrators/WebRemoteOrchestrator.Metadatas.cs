using System;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync.Web.Client
{
    /// <summary>
    /// Contains the forbidden logic to handle metadata on the server side.
    /// </summary>
    public partial class WebRemoteOrchestrator : RemoteOrchestrator
    {
        /// <summary>
        /// Http Client is not authorized to ask metadatas deletion on the server.
        /// </summary>
        public override Task<DatabaseMetadatasCleaned> DeleteMetadatasAsync(DbConnection connection = null, DbTransaction transaction = null)
            => throw new NotImplementedException();

        /// <summary>
        /// Http Client is not authorized to ask metadatas deletion on the server.
        /// </summary>
        public override Task<DatabaseMetadatasCleaned> DeleteMetadatasAsync(long timeStampStart, DbConnection connection = null, DbTransaction transaction = null)
            => throw new NotImplementedException();
    }
}