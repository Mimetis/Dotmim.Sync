using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync.Builders
{
    public interface IDbScopeInfoBuilder
    {
        bool NeedToCreateScopeInfoTable();
        void CreateScopeInfoTable();
        List<ScopeInfo> GetAllScopes(string scopeName);
        ScopeInfo InsertOrUpdateScopeInfo(ScopeInfo scopeInfo);
        long GetLocalTimestamp();
    }
}
