using Dotmim.Sync.Data;
using Dotmim.Sync.Filter;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Messages
{
    /// <summary>
    /// Message exchanged during the Ensure Database sync stage
    /// </summary>
    public class MessageEnsureDatabase
    {
        /// <summary>
        /// Gets or Sets he scope info used during the ensure database sync stage
        /// </summary>
        public ScopeInfo ScopeInfo { get; set; }

        /// <summary>
        /// Gets or Sets the database schema
        /// </summary>
        public DmSet Schema { get; set; }

        /// <summary>
        /// Gets or Sets the filters used during the sync, to be applied on the database
        /// </summary>
        public ICollection<FilterClause> Filters { get; set; }

    }
}
