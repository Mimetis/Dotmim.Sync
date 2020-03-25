using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync.Tests.UnitTests
{
    public class MockScopeBuilder : DbScopeBuilder
    {
        public override IDbScopeInfoBuilder CreateScopeInfoBuilder(string scopeTableName, DbConnection connection, DbTransaction transaction = null) 
            => new MockScopeInfoBuilder();
    }

    public class MockScopeInfoBuilder : IDbScopeInfoBuilder
    {
        public void CreateClientScopeInfoTable()
        {
            
        }

        public void CreateServerHistoryScopeInfoTable()
        {
        }

        public void CreateServerScopeInfoTable()
        {
        }

        public void DropClientScopeInfoTable()
        {
        }

        public void DropServerHistoryScopeInfoTable()
        {
        }

        public void DropServerScopeInfoTable()
        {
        }

        public List<ScopeInfo> GetAllClientScopes(string scopeName)
        {
            throw new NotImplementedException();
        }

        public List<ServerScopeInfo> GetAllServerScopes(string scopeName)
        {
            throw new NotImplementedException();
        }

        public long GetLocalTimestamp() => 1000;

        public ScopeInfo InsertOrUpdateClientScopeInfo(ScopeInfo scopeInfo) => scopeInfo;


        public ServerHistoryScopeInfo InsertOrUpdateServerHistoryScopeInfo(ServerHistoryScopeInfo serverHistoryScopeInfo) 
            => serverHistoryScopeInfo;

        public ServerScopeInfo InsertOrUpdateServerScopeInfo(ServerScopeInfo serverScopeInfo)
            => serverScopeInfo;

        public bool NeedToCreateClientScopeInfoTable() => true;

        public bool NeedToCreateServerHistoryScopeInfoTable() => true;

        public bool NeedToCreateServerScopeInfoTable() => true;
    }
}
