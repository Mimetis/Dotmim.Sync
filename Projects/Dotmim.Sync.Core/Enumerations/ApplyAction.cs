using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Enumerations
{
    /// <summary>
    /// Specifies the options for processing a row when the row cannot be applied during synchronization.
    /// </summary>
    public enum ApplyAction
    {
        /// <summary>
        /// Continue processing (ie server wins)
        /// This is the default behavior.
        /// </summary>
        Continue,

        /// <summary>
        /// Force the row to be applied by using logic that is included in synchronization adapter commands.
        /// </summary>
        RetryWithForceWrite,

        /// <summary>
        /// Force the finale row to be applied locally
        /// </summary>
        Merge,

        /// <summary>
        /// Force to rollback all the sync processus
        /// </summary>
        Throw,
    }
}
