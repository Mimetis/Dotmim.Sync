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

        public ClientSyncChanges(long timestamp, BatchInfo batchInfo, DatabaseChangesSelected clientChangesSelected)
        {
            this.Timestamp = timestamp;
            this.BatchInfo = batchInfo;
            this.ClientChangesSelected = clientChangesSelected;
        }


        /// <summary>
        /// Gets the timestamp limit used to get the changes
        /// </summary>
        public long Timestamp { get; }

        /// <summary>
        /// Gets the batches serialized locally with all changes
        /// </summary>
        public BatchInfo BatchInfo { get; }

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

        public ServerSyncChanges(long remoteClientTimestamp, BatchInfo batchInfo, 
            DatabaseChangesSelected serverChangesSelected, DatabaseChangesApplied clientChangesApplied,
            ConflictResolutionPolicy serverResolutionPolicy)
        {
            this.RemoteClientTimestamp = remoteClientTimestamp;
            this.BatchInfo = batchInfo;
            this.ClientChangesApplied = clientChangesApplied;
            this.ServerResolutionPolicy = serverResolutionPolicy;
            this.ServerChangesSelected = serverChangesSelected;
        }


        /// <summary>
        /// Gets the timestamp limit used to get the changes
        /// </summary>
        public long RemoteClientTimestamp { get; }

        /// <summary>
        /// Gets the batches serialized locally with all changes
        /// </summary>
        public BatchInfo BatchInfo { get; }
        public DatabaseChangesApplied ClientChangesApplied { get; }
        public ConflictResolutionPolicy ServerResolutionPolicy { get; }

        /// <summary>
        /// Gets statistics about changes selected
        /// </summary>
        public DatabaseChangesSelected ServerChangesSelected { get; }
    }

}
