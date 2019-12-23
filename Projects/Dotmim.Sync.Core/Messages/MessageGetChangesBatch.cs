using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Filter;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Messages
{
    /// <summary>
    /// Message exchanged during the Get Changes Batch sync stage
    /// </summary>
    public class MessageGetChangesBatch
    {
        public MessageGetChangesBatch(Guid excludingScopeId, bool isNew, long lastTimestamp,  SyncSet schema, 
                                      int batchSize, string batchDirectory)
        {
            this.Schema = schema ?? throw new ArgumentNullException(nameof(schema));
            this.BatchDirectory = batchDirectory ?? throw new ArgumentNullException(nameof(batchDirectory));
            this.ExcludingScopeId = excludingScopeId;
            this.IsNew = isNew;
            this.LastTimestamp = lastTimestamp;
            this.BatchSize = batchSize;
        }

        /// <summary>
        /// Gets or Sets the Scope Id that should be excluded when we get lines from the local store
        /// IE : When we get lines on the client, we don't want all lines where last updates have been made by the server.
        /// </summary>
        public Guid ExcludingScopeId { get; set; }


        /// <summary>
        /// Gets or Sets if the sync is a first sync. In this case, the last sync timestamp is ignored
        /// </summary>
        public bool IsNew { get; set; }


        /// <summary>
        /// Gets or Sets the last date timestamp from where we want rows
        /// </summary>
        public long LastTimestamp { get; set; }

        /// <summary>
        /// Gets or Sets the schema used for this sync
        /// </summary>
        public SyncSet Schema { get; set; }

        /// <summary>
        /// Gets or Sets the download batch size, if needed
        /// </summary>
        public int BatchSize { get; set; }

        /// <summary>
        /// Gets or Sets the batch directory used to serialize the datas
        /// </summary>
        public string BatchDirectory { get; set; }

        ///// <summary>
        ///// Gets or Sets the current Conflict resolution policy
        ///// </summary>
        //public ConflictResolutionPolicy Policy { get; set; }
    }
}
