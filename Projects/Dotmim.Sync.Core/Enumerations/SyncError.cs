using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Enumerations
{

    /// <summary>
    /// This enum is containing the last sync result situation
    /// TODO : Implentation needed ?
    /// </summary>
    public enum SyncState
    {
        UnknownError,
        Successful,
        RollbackBeforeEnsuringScopes,
        RollbackAfterEnsuringScopes,
        RollbackBeforeEnsuringConfiguration,
        RollbackAfterEnsuringConfiguration,
        RollbackBeforeEnsuringDatabase,
        RollbackAfterEnsuringDatabase,
        RollbackBeforeelectingChanges,
        RollbackAfterSelectedChanges,
        RollbackBeforeApplyingChanges,
        RollbackAfterAppliedChanges,
    }
}
