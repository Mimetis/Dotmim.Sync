using Dotmim.Sync.Enumerations;
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
        public MessageEnsureScopes(string scopeInfoTableName, string scopeName)
        {
            this.ScopeInfoTableName = scopeInfoTableName ?? throw new ArgumentNullException(nameof(scopeInfoTableName));
            this.ScopeName = scopeName ?? throw new ArgumentNullException(nameof(scopeName));
        }

        /// <summary>
        /// Gets or Sets the scope info table name used for ensuring scopes
        /// </summary>
        public string ScopeInfoTableName { get; private set; }

        /// <summary>
        /// Gets or Sets the scope name
        /// </summary>
        public string ScopeName { get; private set; }

    }
}
