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
        /// Ignore the error and continue to sync. Error will be stored 
        /// locally in a separate batch info file
        /// <para>
        /// Row is stored locally with a state of <see cref="SyncRowState.ApplyDeletedFailed"/> 
        /// or <see cref="SyncRowState.ApplyModifiedFailed"/> depending on the row state.
        /// </para>
        /// </summary>
        ContinueOnError,

        /// <summary>
        /// Will try one more time once after all the others rows in the table. 
        /// <para>
        /// If the error is raised again, it will be stored locally in a 
        /// separate batch info file with a state of <see cref="SyncRowState.ApplyDeletedFailed"/> 
        /// or <see cref="SyncRowState.ApplyModifiedFailed"/> depending on the row state.
        /// </para>
        /// </summary>
        RetryOneMoreTime,

        /// <summary>
        /// Row is stored locally and will be applied again on next sync.
        /// Row is stored locally with a state of <see cref="SyncRowState.RetryDeletedOnNextSync"/> 
        /// or <see cref="SyncRowState.RetryModifiedOnNextSync"/> depending on the row state.
        /// </summary>
        RetryOnNextSync,

        /// <summary>
        /// Considers the row as applied.
        /// </summary>
        Resolved,

        /// <summary>
        /// Throw the error. Default value.
        /// </summary>
        Throw
    }
}
