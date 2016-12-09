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
        /// <summary>
        /// Scope name. Primary key
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Last time the remote has done a good sync
        /// </summary>
        public long LastTimestamp { get; set; }

        /// <summary>
        /// Scope config id
        /// </summary>
        public Guid ConfigId { get; set; }

        /// <summary>
        /// Comment
        /// </summary>
        public string UserComment { get; set; }

     
    }
}
