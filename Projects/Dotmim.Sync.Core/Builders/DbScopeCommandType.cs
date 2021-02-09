
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Builders
{
    public enum DbScopeCommandType
    {
        ExistsScopeTable,
        CreateScopeTable,
        DropScopeTable,
        GetScopes,
        InsertScope,
        UpdateScope,
        ExistScope,
        GetLocalTimestamp,
    }
}
