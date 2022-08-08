using Dotmim.Sync.Enumerations;
using System;
using System.Runtime.Serialization;

namespace Dotmim.Sync
{
    /// <summary>
    /// Mapping sur la table ScopeInfo
    /// </summary>
    public class ServerHistoryScopeInfo : IScopeInfo
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
        /// Gets or Sets the last timestamp a sync has occured. This timestamp is set just 'before' sync start.
        /// </summary>
        public long LastSyncTimestamp { get; set; }

        /// <summary>
        /// Gets or Sets the last datetime when a sync has successfully ended.
        /// </summary>
        public DateTime? LastSync { get; set; }

        /// <summary>
        /// Gets or Sets the last duration a sync has occured. 
        /// </summary>
        public long LastSyncDuration { get; set; }


        /// <summary>
        /// Gets a readable version of LastSyncDuration
        /// </summary>
        /// <returns></returns>
        public string LastSyncDurationString
        {
            get
            {
                var durationTs = new TimeSpan(this.LastSyncDuration);
                return $"{durationTs.Hours}:{durationTs.Minutes}:{durationTs.Seconds}.{durationTs.Milliseconds}";
            }
        }

        public SyncSet Schema { get; set; }
        public SyncSetup Setup { get; set; }
        public string Version { get; set; }

        public string Properties { get; set; }
    }
}
