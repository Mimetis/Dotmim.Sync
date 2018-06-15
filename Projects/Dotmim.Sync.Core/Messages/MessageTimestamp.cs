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
        /// <summary>
        /// Gets or Sets the Scope info table name, used to get the timestamp
        /// </summary>
        public String ScopeInfoTableName { get; set; }

    }
}
