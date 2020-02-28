using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync.Builders
{
    public interface IDbScopeInfoBuilder
    {
        bool NeedToCreateClientScopeInfoTable();
        bool NeedToCreateServerScopeInfoTable();
        bool NeedToCreateServerHistoryScopeInfoTable();

        void CreateClientScopeInfoTable();
        void CreateServerScopeInfoTable();
        void CreateServerHistoryScopeInfoTable();

        List<ScopeInfo> GetAllClientScopes(string scopeName);
        List<ServerScopeInfo> GetAllServerScopes(string scopeName);

        ScopeInfo InsertOrUpdateClientScopeInfo(ScopeInfo scopeInfo);
        ServerScopeInfo InsertOrUpdateServerScopeInfo(ServerScopeInfo serverScopeInfo);
        ServerHistoryScopeInfo InsertOrUpdateServerHistoryScopeInfo(ServerHistoryScopeInfo serverHistoryScopeInfo);

        long GetLocalTimestamp();

        void DropClientScopeInfoTable();
        void DropServerScopeInfoTable();
        void DropServerHistoryScopeInfoTable();

    }
}
