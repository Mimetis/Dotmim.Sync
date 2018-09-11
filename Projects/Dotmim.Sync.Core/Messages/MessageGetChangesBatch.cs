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
    [Serializable]
    public class MessageGetChangesBatch
    {
        [NonSerialized]
        private DmSet _schema;

        /// <summary>
        /// Gets or Sets the scope info for the current sync
        /// </summary>
        public ScopeInfo ScopeInfo { get; set; }

        /// <summary>
        /// Gets or Sets the schema used for this sync
        /// </summary>
        [JsonIgnore]
        public DmSet Schema { get => _schema; set => _schema = value; }

        /// <summary>
        /// Gets or Sets the download batch size, if needed
        /// </summary>
        public int DownloadBatchSizeInKB { get; set; }

        /// <summary>
        /// Gets or Sets the batch directory used to serialize the datas
        /// </summary>
        public string BatchDirectory { get; set; }

        /// <summary>
        /// Gets or Sets the current Conflict resolution policy
        /// </summary>
        public ConflictResolutionPolicy Policy { get; set; }

        /// <summary>
        /// Gets or Sets the Batch Info used for this sync session
        /// </summary>
        public List<FilterClause2> Filters { get; set; }

        /// <summary>
        /// Gets or Sets the Serialization format used during the sync
        /// </summary>
        public SerializationFormat SerializationFormat { get; set; }
    }
}
