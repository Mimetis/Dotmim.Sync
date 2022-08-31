using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Enumerations
{

    /// <summary>
    /// Determines what kind of action should be taken when an error is raised from the datasource
    /// during an insert / update or delete command
    /// </summary>
    public enum ErrorResolution
    {
        /// <summary>
        /// Ignore the error and continue to sync
        /// </summary>
        ContinueOnError,

        /// <summary>
        /// Will try one more time once after all the others rows in the table
        /// </summary>
        RetryOneMoreTime,

        /// <summary>
        /// Considers the row as applied
        /// </summary>
        Resolved,

        /// <summary>
        /// Throw the error. Default value
        /// </summary>
        Throw
    }
}
