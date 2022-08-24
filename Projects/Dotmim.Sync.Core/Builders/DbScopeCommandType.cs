
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Builders
{
    public enum DbScopeCommandType
    {
        ExistsScopeInfoTable,
        ExistsScopeInfoClientTable,

        CreateScopeInfoTable,
        CreateScopeInfoClientTable,

        DropScopeInfoTable,
        DropScopeInfoClientTable,

        GetAllScopeInfos,
        GetAllScopeInfoClients,

        GetScopeInfo, 
        GetScopeInfoClient,

        InsertScopeInfo,
        InsertScopeInfoClient,

        UpdateScopeInfo,
        UpdateScopeInfoClient,

        DeleteScopeInfo,
        DeleteScopeInfoClient,

        ExistScopeInfo,
        ExistScopeInfoClient,

        GetLocalTimestamp,
    }
}
