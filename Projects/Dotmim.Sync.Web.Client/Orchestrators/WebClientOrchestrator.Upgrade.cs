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
    public partial class WebClientOrchestrator : RemoteOrchestrator
    {
        /// <summary>
        /// Not Allowed from WebClientOrchestrator
        /// </summary>
        public override Task<bool> UpgradeAsync(DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
          => throw new NotImplementedException();


        /// <summary>
        /// Not Allowed from WebClientOrchestrator
        /// </summary>
        public override Task<bool> NeedsToUpgradeAsync(string scopeName, DbConnection connection = default, DbTransaction transaction = default, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
                        => throw new NotImplementedException();

    }
}
