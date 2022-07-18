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
        /// <summary>
        /// Http Client is not authorized to get server scope histories from the server
        /// </summary>
        public override Task<ServerHistoryScopeInfo> GetServerHistoryScopeInfoAsync(string scopeId, string scopeName = SyncOptions.DefaultScopeName, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
            => throw new NotImplementedException();

        /// <summary>
        /// Http Client is not authorized to save server scope history on the server
        /// </summary>
        public override Task<ServerHistoryScopeInfo> SaveServerHistoryScopeInfoAsync(ServerHistoryScopeInfo serverHistoryScopeInfo, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
              => throw new NotImplementedException();
    }
}
