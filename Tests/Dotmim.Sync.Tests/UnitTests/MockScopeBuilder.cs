using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Tests.UnitTests
{
    public class MockScopeBuilder : DbScopeBuilder
    {
        public override IDbScopeInfoBuilder CreateScopeInfoBuilder(string scopeTableName, DbConnection connection, DbTransaction transaction = null) 
            => new MockScopeInfoBuilder();
    }

    public class MockScopeInfoBuilder : IDbScopeInfoBuilder
    {
        public Task CreateClientScopeInfoTableAsync() => Task.CompletedTask;

        public Task CreateServerHistoryScopeInfoTableAsync() => Task.CompletedTask;

        public Task CreateServerScopeInfoTableAsync() => Task.CompletedTask;

        public Task DropClientScopeInfoTableAsync() => Task.CompletedTask;

        public Task DropServerHistoryScopeInfoTableAsync() => Task.CompletedTask;

        public Task DropServerScopeInfoTableAsync() => Task.CompletedTask;

        public Task<List<ScopeInfo>> GetAllClientScopesAsync(string scopeName) => Task.FromResult(new List<ScopeInfo>());

        public Task<List<ServerScopeInfo>> GetAllServerScopesAsync(string scopeName) => Task.FromResult(new List<ServerScopeInfo>());


        public Task<long> GetLocalTimestampAsync() => Task.FromResult(1000L);


        public Task<ScopeInfo> InsertOrUpdateClientScopeInfoAsync(ScopeInfo scopeInfo) => Task.FromResult(scopeInfo);


        public Task<ServerHistoryScopeInfo> InsertOrUpdateServerHistoryScopeInfoAsync(ServerHistoryScopeInfo serverHistoryScopeInfo) 
            => Task.FromResult(serverHistoryScopeInfo);


        public Task<ServerScopeInfo> InsertOrUpdateServerScopeInfoAsync(ServerScopeInfo serverScopeInfo) => Task.FromResult(serverScopeInfo);


        public Task<bool> NeedToCreateClientScopeInfoTableAsync() => Task.FromResult(true);


        public Task<bool> NeedToCreateServerHistoryScopeInfoTableAsync() => Task.FromResult(true);


        public Task<bool> NeedToCreateServerScopeInfoTableAsync() => Task.FromResult(true);

    }
}
