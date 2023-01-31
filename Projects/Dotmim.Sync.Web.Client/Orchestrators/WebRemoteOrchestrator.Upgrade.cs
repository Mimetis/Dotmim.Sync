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
        /// Not Allowed from WebRemoteOrchestrator
        /// </summary>
        internal override Task<(List<ScopeInfo> scopeInfos, List<ScopeInfoClient> scopeInfoClients)> InternalUpgradeAsync(
            SyncContext context, DbConnection connection = default, DbTransaction transaction = default,
                        CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = default)
            => throw new NotImplementedException();


        /// <summary>
        /// Not Allowed from WebRemoteOrchestrator
        /// </summary>
        public override Task<bool> NeedsToUpgradeAsync(SyncContext context) => Task.FromResult(false);

    }
}
