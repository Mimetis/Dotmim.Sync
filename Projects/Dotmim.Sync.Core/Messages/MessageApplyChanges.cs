using Dotmim.Sync.Batch;
using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Messages
{
    /// <summary>
    /// Message exchanged during the Begin session sync stage
    /// </summary>
    public class MessageApplyChanges
    {
        /// <summary>
        /// Gets or Sets the scope info for the current sync
        /// </summary>
        public ScopeInfo FromScope { get; set; }

        /// <summary>
        /// Gets or Sets the schema used for this sync
        /// </summary>
        public DmSet Schema { get; set; }

        /// <summary>
        /// Gets or Sets the current Conflict resolution policy
        /// </summary>
        public ConflictResolutionPolicy Policy { get; set; }

        /// <summary>
        /// Gets or sets the boolean indicating if we can use bulk operations
        /// </summary>
        public Boolean UseBulkOperations { get; set; }

        /// <summary>
        /// Gets ors Sets the Scope info table name used for the sync
        /// </summary>
        public String ScopeInfoTableName { get; set; }

        /// <summary>
        /// Gets or Sets the Batch Info used for this sync session
        /// </summary>
        public BatchInfo Changes { get; set; }

    }
}
