﻿using Dotmim.Sync.Data;
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

        public MessageGetChangesBatch(ScopeInfo scopeInfo, DmSet schema, int batchSize, string batchDirectory, ConflictResolutionPolicy policy, ICollection<FilterClause> filters, SerializationFormat serializationFormat)
        {
            this.ScopeInfo = scopeInfo ?? throw new ArgumentNullException(nameof(scopeInfo));
            this.Schema = schema ?? throw new ArgumentNullException(nameof(schema));
            this.BatchSize = batchSize;
            this.BatchDirectory = batchDirectory ?? throw new ArgumentNullException(nameof(batchDirectory));
            this.Policy = policy;
            this.SerializationFormat = serializationFormat;
        }

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
        public int BatchSize { get; set; }

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
        public ICollection<FilterClause> Filters { get; set; }

        /// <summary>
        /// Gets or Sets the Serialization format used during the sync
        /// </summary>
        public SerializationFormat SerializationFormat { get; set; }
    }
}
