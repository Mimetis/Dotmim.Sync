﻿using System;
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
        Successful,
        RollbackBeforeEnsuringScopes,
        RollbackAfterEnsuringScopes,
        RollbackBeforeEnsuringConfiguration,
        RollbackAfterEnsuringConfiguration,
        RollbackBeforeEnsuringDatabase,
        RollbackAfterEnsuringDatabase,
        RollbackBeforeSelectingChanges,
        RollbackAfterSelectedChanges,
        RollbackBeforeApplyingChanges,
        RollbackAfterAppliedChanges,
        UnknownError,
    }
}
