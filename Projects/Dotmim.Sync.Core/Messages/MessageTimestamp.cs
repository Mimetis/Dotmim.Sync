using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Messages
{
    /// <summary>
    /// Message exchanged during the Get Timestamp sync stage
    /// </summary>
    public class MessageTimestamp
    {
        public MessageTimestamp(string scopeInfoTableName, SerializationFormat serializationFormat)
        {
            this.ScopeInfoTableName = scopeInfoTableName ?? throw new ArgumentNullException(nameof(scopeInfoTableName));
            this.SerializationFormat = serializationFormat;
        }

        /// <summary>
        /// Gets or Sets the Scope info table name, used to get the timestamp
        /// </summary>
        public String ScopeInfoTableName { get; set; }

        /// <summary>
        /// Gets or Sets the Serialization format used during the sync
        /// </summary>
        public SerializationFormat SerializationFormat { get; set; }
    }
}
