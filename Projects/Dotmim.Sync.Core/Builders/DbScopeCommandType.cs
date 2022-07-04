
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Builders
{
    public enum DbScopeCommandType
    {
        ExistsClientScopeInfoTable,
        ExistsServerScopeInfoTable,
        ExistsServerHistoryScopeInfoTable,

        CreateClientScopeInfoTable,
        CreateServerScopeInfoTable,
        CreateServerHistoryScopeInfoTable,

        DropClientScopeInfoTable,
        DropServerScopeInfoTable,
        DropServerHistoryScopeInfoTable,

        GetAllClientScopesInfo,
        GetAllServerScopesInfo,
        GetAllServerHistoryScopesInfo,

        GetClientScopeInfo,
        GetServerScopeInfo,
        GetServerHistoryScopeInfo,

        InsertClientScopeInfo,
        InsertServerScopeInfo,
        InsertServerHistoryScopeInfo,

        UpdateClientScopeInfo,
        UpdateServerScopeInfo,
        UpdateServerHistoryScopeInfo,

        DeleteClientScopeInfo,
        DeleteServerScopeInfo,
        DeleteServerHistoryScopeInfo,

        ExistClientScopeInfo,
        ExistServerScopeInfo,
        ExistServerHistoryScopeInfo,

        GetLocalTimestamp,
    }
}
