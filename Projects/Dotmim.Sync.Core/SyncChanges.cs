using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync
{
    /// <summary>
    /// Represents a set of changes 
    /// </summary>
    public class ClientSyncChanges
    {
        public ClientSyncChanges(long clientTimestamp, BatchInfo clientBatchInfo, DatabaseChangesSelected clientChangesSelected)
        {
            this.ClientTimestamp = clientTimestamp;
            this.ClientBatchInfo = clientBatchInfo;
            this.ClientChangesSelected = clientChangesSelected;
        }


        /// <summary>
        /// Gets the timestamp limit used to get the changes
        /// </summary>
        public long ClientTimestamp { get; }

        /// <summary>
        /// Gets the batches serialized locally with all changes. Is Null if called from GetEstimatedChanges
        /// </summary>
        public BatchInfo ClientBatchInfo { get; }

        /// <summary>
        /// Gets statistics about changes selected
        /// </summary>
        public DatabaseChangesSelected ClientChangesSelected { get; }
    }

    /// <summary>
    /// Represents a set of changes 
    /// </summary>
    public class ServerSyncChanges
    {
        public ServerSyncChanges(long remoteClientTimestamp, BatchInfo serverBatchInfo,
            DatabaseChangesSelected serverChangesSelected)
        {
            this.RemoteClientTimestamp = remoteClientTimestamp;
            this.ServerBatchInfo = serverBatchInfo;
            this.ServerChangesSelected = serverChangesSelected;
        }


        /// <summary>
        /// Gets the timestamp limit used to get the changes
        /// </summary>
        public long RemoteClientTimestamp { get; }

        /// <summary>
        /// Gets the batches serialized locally with all changes
        /// </summary>
        public BatchInfo ServerBatchInfo { get; }

        /// <summary>
        /// Gets statistics about changes selected
        /// </summary>
        public DatabaseChangesSelected ServerChangesSelected { get; }
    }
   
}
