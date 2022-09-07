using Dotmim.Sync.Batch;
using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync
{
    /// <summary>
    /// Client changes selected and server changes applied on client + stats
    /// </summary>
    public class ClientSyncChanges
    {
        /// <summary>
        /// Client changes selected and server changes applied on client + stats
        /// </summary>
        public ClientSyncChanges(long clientTimestamp, BatchInfo clientBatchInfo, DatabaseChangesSelected clientChangesSelected, DatabaseChangesApplied clientChangesApplied)
        {
            this.ClientTimestamp = clientTimestamp;
            this.ClientBatchInfo = clientBatchInfo;
            this.ClientChangesSelected = clientChangesSelected;
            this.ClientChangesApplied = clientChangesApplied;
            //this.ErrorsBatchInfo = errorsBatchInfo;
        }

        /// <summary>
        /// Gets the timestamp limit used to get the changes
        /// </summary>
        public long ClientTimestamp { get; set; }

        /// <summary>
        /// Gets the batches serialized locally with all changes. Is Null if called from GetEstimatedChanges
        /// </summary>
        public BatchInfo ClientBatchInfo { get; set; }

        /// <summary>
        /// Gets statistics about changes selected
        /// </summary>
        public DatabaseChangesSelected ClientChangesSelected { get; set; }

        /// <summary>
        /// Gets the changes applied on client, plus failed rows
        /// </summary>
        public DatabaseChangesApplied ClientChangesApplied { get; set; }

        ///// <summary>
        ///// Gets the batches serialized locally with all failed rows applied on server
        ///// </summary>
        //public BatchInfo ErrorsBatchInfo { get; set; }
    }

    /// <summary>
    /// Server changes selected and client changes applied on server + stats
    /// </summary>
    public class ServerSyncChanges
    {
        /// <summary>
        /// Server changes selected and client changes applied on server + stats
        /// </summary>
        public ServerSyncChanges(long remoteClientTimestamp, BatchInfo serverBatchInfo, DatabaseChangesSelected serverChangesSelected, DatabaseChangesApplied serverChangesApplied)
        {
            this.RemoteClientTimestamp = remoteClientTimestamp;
            this.ServerBatchInfo = serverBatchInfo;
            this.ServerChangesSelected = serverChangesSelected;
            this.ServerChangesApplied = serverChangesApplied;
            //this.ErrorsBatchInfo = errorsBatchInfo;
        }


        /// <summary>
        /// Gets the timestamp limit used to get the changes
        /// </summary>
        public long RemoteClientTimestamp { get; set; }

        /// <summary>
        /// Gets the batches serialized locally with all changes
        /// </summary>
        public BatchInfo ServerBatchInfo { get; set; }

        /// <summary>
        /// Gets statistics about changes selected
        /// </summary>
        public DatabaseChangesSelected ServerChangesSelected { get; set; }

        /// <summary>
        /// Gets server changes applied + failed applied rows
        /// </summary>
        public DatabaseChangesApplied ServerChangesApplied { get; set; }
    
        ///// <summary>
        ///// 
        ///// Gets the batches serialized locally with all failed rows applied on server
        ///// </summary>
        //public BatchInfo ErrorsBatchInfo { get; set; }

    }
   
}
