using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Messages
{
    /// <summary>
    /// Message exchanged during the Ensure scopes sync stage
    /// </summary>
    public class MessageEnsureScopes
    {
        /// <summary>
        /// Gets or Sets the client id. If null, the ensure scope step is occuring on the client. If not null, we are on the server
        /// </summary>
        public Guid? ClientReferenceId { get; set; }

        /// <summary>
        /// Gets or Sets the scope info table name used for ensuring scopes
        /// </summary>
        public String ScopeInfoTableName { get; set; }

        /// <summary>
        /// Gets or Sets the scope name
        /// </summary>
        public String ScopeName { get; set; }

    }
}
