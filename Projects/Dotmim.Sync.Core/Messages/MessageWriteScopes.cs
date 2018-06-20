using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Messages
{
    /// <summary>
    /// Message exchanged during the Write Scopes sync stage
    /// </summary>
    [Serializable]
    public class MessageWriteScopes
    {
        /// <summary>
        /// Gets or Sets the scope info table used to set the scopes
        /// </summary>
        public String ScopeInfoTableName { get; set; }

        /// <summary>
        /// Gets or Sets the Scopes to write in the database
        /// </summary>
        public List<ScopeInfo> Scopes { get; set; }

        /// <summary>
        /// Gets or Sets the Serialization format used during the sync
        /// </summary>
        public SerializationFormat SerializationFormat { get; set; }
    }
}
