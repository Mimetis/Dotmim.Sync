using Dotmim.Sync.Batch;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public partial class RemoteOrchestrator : BaseOrchestrator
    {

        /// <summary>
        /// Gets the sync side of this Orchestrator. RemoteOrchestrator is always used on server side
        /// </summary>
        public override SyncSide Side => SyncSide.ServerSide;


        /// <summary>
        /// Create a remote orchestrator, used to orchestrates the whole sync on the server side
        /// </summary>
        public RemoteOrchestrator(CoreProvider provider, SyncOptions options) : base(provider, options)
        {
            if (this.Provider != null && !this.Provider.CanBeServerProvider)
                throw GetSyncError(null, new UnsupportedServerProviderException(this.Provider.GetProviderTypeName()));
        }

        /// <summary>
        /// Create a remote orchestrator, used to orchestrates the whole sync on the server side
        /// </summary>
        public RemoteOrchestrator(CoreProvider provider) : base(provider, new SyncOptions())
        {
            if (this.Provider != null && !this.Provider.CanBeServerProvider)
                throw GetSyncError(null, new UnsupportedServerProviderException(this.Provider.GetProviderTypeName()));
        }
    }
}