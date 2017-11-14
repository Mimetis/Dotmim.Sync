using Dotmim.Sync.Enumerations;
using System;

namespace Dotmim.Sync
{
    /// <summary>
    /// Mapping sur la table ScopeInfo
    /// </summary>
    [Serializable]
    public class ScopeInfo
    {
        /// <summary>
        /// Scope name. Shared by all clients and the server
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Id of the scope owner
        /// </summary>
        public Guid Id { get; set; }

        /// <summary>
        /// Gets or Sets a boolean indicating if the scope info is local to the provider (or remote)
        /// </summary>
        public Boolean IsLocal { get; set; }

        /// <summary>
        /// Last time the remote has done a good sync
        /// IF it's a new scope force to Zero to be sure, the first sync will get all datas
        /// </summary>
        public long LastTimestamp { get; set; }


        /// <summary>
        /// Gets or Sets if the current provider is newly created one in database.
        /// If new, we will override timestamp for first synchronisation to be sure to get all datas from server
        /// </summary>
        public Boolean IsNewScope { get; set; }


        /// <summary>
        /// Gets or Sets the last datetime when a sync has successfully ended.
        /// </summary>
        public DateTime? LastSync { get; set; }

        /// <summary>
        /// Gets or sets the last time we apply a clean up on metadata
        /// </summary>
        public long LastCleanupTimestamp { get; set; }

        /// <summary>
        /// Gets or sets the last sync state.
        /// </summary>
        public SyncState SyncState { get; set; }

    }
}
