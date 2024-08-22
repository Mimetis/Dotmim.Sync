using System;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync.Web.Client
{
    /// <summary>
    /// Contains the forbidden logic to handle upgrade on the server side.
    /// </summary>
    public partial class WebRemoteOrchestrator : RemoteOrchestrator
    {

        /// <summary>
        /// Not Allowed from WebRemoteOrchestrator.
        /// </summary>
        public override Task<bool> NeedsToUpgradeAsync(SyncContext context) => Task.FromResult(false);

        /// <summary>
        /// Not Allowed from WebRemoteOrchestrator.
        /// </summary>
        internal override Task<bool> InternalUpgradeAsync(
            SyncContext context, DbConnection connection = default, DbTransaction transaction = default,
            IProgress<ProgressArgs> progress = default, CancellationToken cancellationToken = default)
            => throw new NotImplementedException();
    }
}