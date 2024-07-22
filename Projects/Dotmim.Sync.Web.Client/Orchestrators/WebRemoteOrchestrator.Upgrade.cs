using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
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

namespace Dotmim.Sync.Web.Client
{
    public partial class WebRemoteOrchestrator : RemoteOrchestrator
    {
        /// <summary>
        /// Not Allowed from WebRemoteOrchestrator.
        /// </summary>
        internal override Task<bool> InternalUpgradeAsync(
            SyncContext context, DbConnection connection = default, DbTransaction transaction = default,
            IProgress<ProgressArgs> progress = default, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();

        /// <summary>
        /// Not Allowed from WebRemoteOrchestrator.
        /// </summary>
        public override Task<bool> NeedsToUpgradeAsync(SyncContext context) => Task.FromResult(false);
    }
}