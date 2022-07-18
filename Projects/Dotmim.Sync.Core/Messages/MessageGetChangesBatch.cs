using Dotmim.Sync.Serialization;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync
{
    /// <summary>
    /// Message exchanged during the Get Changes Batch sync stage
    /// </summary>
    public class MessageGetChangesBatch
    {
        public MessageGetChangesBatch(Guid? excludingScopeId, Guid localScopeId, bool isNew, SyncSet schema,
                                      int batchSize, string batchDirectory, string batchDirectoryName, bool supportsMultiActiveResultSets)
        {
            this.Schema = schema ?? throw new ArgumentNullException(nameof(schema));
            this.BatchDirectory = batchDirectory ?? throw new ArgumentNullException(nameof(batchDirectory));
            this.BatchDirectoryName = batchDirectoryName;
            this.SupportsMultiActiveResultSets = supportsMultiActiveResultSets;
            this.ExcludingScopeId = excludingScopeId;
            this.LocalScopeId = localScopeId;
            this.IsNew = isNew;
            this.BatchSize = batchSize;
        }

        /// <summary>
        /// Gets or Sets the Scope Id that should be excluded when we get lines from the local store
        /// Usable only from Server side
        /// </summary>
        public Guid? ExcludingScopeId { get; set; }

        /// <summary>
        /// Gets or Sets the local Scope Id that will replace <NULL> values when creating the row
        /// </summary>
        public Guid LocalScopeId { get; set; }

        /// <summary>
        /// Gets or Sets if the sync is a first sync. In this case, the last sync timestamp is ignored
        /// </summary>
        public bool IsNew { get; set; }

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

        /// <summary>
        /// Gets or Sets the batch directory name to concat (optional)
        /// </summary>
        public string BatchDirectoryName { get; set; }

        public bool SupportsMultiActiveResultSets { get; }
    }
}
