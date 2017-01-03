using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Core.Scope
{
    /// <summary>
    /// Mapping sur la table ScopeInfo
    /// </summary>
    public class ScopeInfo
    {
        private long lastTimestamp;

        /// <summary>
        /// Scope name. Primary key
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Last time the remote has done a good sync
        /// IF it's a new scope force to Zero to be sure, the first sync will get all datas
        /// </summary>
        public long LastTimestamp
        {
            get
            {
                return this.IsNewScope ? 0 : lastTimestamp;
            }
            set
            {
                this.lastTimestamp = value;
            }
        }

        /// <summary>
        /// Gets or Sets if the current provider is newly created one in database.
        /// If new, we will override timestamp for first synchronisation to be sure to get all datas from server
        /// </summary>

        public Boolean IsNewScope { get; set; }
    }
}
