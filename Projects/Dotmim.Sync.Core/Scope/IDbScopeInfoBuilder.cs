using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync.Core.Scope
{
    public interface IDbScopeInfoBuilder
    {
        bool NeedToCreateScopeInfoTable();
        void CreateScopeInfoTable();
        ScopeInfo ReadFirstScopeInfo();
        ScopeInfo ReadScopeInfo(string scopeName);
        List<ScopeInfo> GetAllScopes();
        ScopeInfo InsertOrUpdateScopeInfo(string scopeName, bool? isDatabaseCreated = null);
        long GetLocalTimestamp();
    }
}
