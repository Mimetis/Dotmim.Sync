using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Messages
{
    /// <summary>
    /// Message exchanged during the Ensure Schema sync stage
    /// </summary>
    [Serializable]
    public class MessageEnsureSchema
    {
        [NonSerialized]
        private DmSet _schema;

        /// <summary>
        /// Gets or Sets the database schema
        /// </summary>
        [JsonIgnore]
        public DmSet Schema { get => _schema; set => _schema = value; }

        /// <summary>
        /// Gets or Sets the Serialization format used during the sync
        /// </summary>
        public SerializationFormat SerializationFormat { get; set; }
    }
}
