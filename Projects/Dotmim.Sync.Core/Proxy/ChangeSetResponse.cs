using Dotmim.Sync.Core.Batch;
using Dotmim.Sync.Core.Proxy.Client;
using Dotmim.Sync.Core.Scope;
using Dotmim.Sync.Data;
using Dotmim.Sync.Data.Surrogate;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Core.Proxy
{
    /// <summary>
    /// Denotes a list of changes that is either to be uploaded or downloaded.
    /// </summary>
    public class ChangeSetResponse
    {
        /// <summary>
        /// Get the current step
        /// </summary>
        public HttpStep Step { get; set; }

        /// <summary>
        /// Scope name
        /// </summary>
        public string ServerScopeName { get; set; }

        /// <summary>
        /// Scope name
        /// </summary>
        public string ClientScopeName { get; set; }

        /// <summary>
        /// Last time the remote has done a good sync
        /// IF it's a new scope force to Zero to be sure, the first sync will get all datas
        /// </summary>
        public long LastTimestamp { get; set; }

        /// <summary>
        /// Gets or Sets the tables used for sync
        /// </summary>
        public string[] Tables { get; set; }

        /// <summary>Gets or sets a DmSet object, 
        /// which contains the in-memory data set that represents the batch.
        /// </summary>
        public DmSetSurrogate DmSetSurrogate { get; set; }

        /// <summary>
        /// Gets or sets an ID that uniquely identifies the batch.
        /// Should be the same as BathInfo
        /// </summary>
        public string Id { get; set; }

        /// <summary>
        /// Gets or sets whether the current batch is the last batch of the synchronization session.
        /// </summary>
        public bool IsLastBatch { get; set; }

        /// <summary>
        /// Gets or sets the sequence number of the batch at the source provider so that 
        /// the destination provider processes batches in the correct order.
        /// </summary>
        public int Index { get; set; }

    }
}
