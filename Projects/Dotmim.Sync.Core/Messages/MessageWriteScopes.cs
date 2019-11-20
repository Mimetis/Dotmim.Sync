using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Messages
{
    /// <summary>
    /// Message exchanged during the Write Scopes sync stage
    /// </summary>
    public class MessageWriteScopes
    {
        public MessageWriteScopes(string scopeInfoTableName, ScopeInfo scope, SerializationFormat serializationFormat)
        {
            this.ScopeInfoTableName = scopeInfoTableName ?? throw new ArgumentNullException(nameof(scopeInfoTableName));
            this.Scope = scope ?? throw new ArgumentNullException(nameof(scope));
            this.SerializationFormat = serializationFormat;
        }

        /// <summary>
        /// Gets or Sets the scope info table used to set the scopes
        /// </summary>
        public String ScopeInfoTableName { get; set; }

        /// <summary>
        /// Gets or Sets the Scope to write in the database
        /// </summary>
        public ScopeInfo Scope { get; set; }

        /// <summary>
        /// Gets or Sets the Serialization format used during the sync
        /// </summary>
        public SerializationFormat SerializationFormat { get; set; }
    }
}
