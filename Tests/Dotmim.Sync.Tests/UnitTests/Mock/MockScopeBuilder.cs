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
        public override IDbScopeInfoBuilder CreateScopeInfoBuilder(string scopeTableName) 
            => new MockScopeInfoBuilder();
    }

    public class MockScopeInfoBuilder : IDbScopeInfoBuilder
    {
        public Task CreateClientScopeInfoTableAsync(DbConnection connection, DbTransaction transaction) => Task.CompletedTask;

        public Task CreateServerHistoryScopeInfoTableAsync(DbConnection connection, DbTransaction transaction) => Task.CompletedTask;

        public Task CreateServerScopeInfoTableAsync(DbConnection connection, DbTransaction transaction) => Task.CompletedTask;

        public Task DropClientScopeInfoTableAsync(DbConnection connection, DbTransaction transaction) => Task.CompletedTask;

        public Task DropServerHistoryScopeInfoTableAsync(DbConnection connection, DbTransaction transaction) => Task.CompletedTask;

        public Task DropServerScopeInfoTableAsync(DbConnection connection, DbTransaction transaction) => Task.CompletedTask;

        public Task<List<ScopeInfo>> GetAllClientScopesAsync(string scopeName, DbConnection connection, DbTransaction transaction) => Task.FromResult(new List<ScopeInfo>());

        public Task<List<ServerHistoryScopeInfo>> GetAllServerHistoryScopesAsync(string scopeName, DbConnection connection, DbTransaction transaction) => throw new NotImplementedException();

        public Task<List<ServerScopeInfo>> GetAllServerScopesAsync(string scopeName, DbConnection connection, DbTransaction transaction) => Task.FromResult(new List<ServerScopeInfo>());


        public Task<long> GetLocalTimestampAsync(DbConnection connection, DbTransaction transaction) => Task.FromResult(1000L);


        public Task<ScopeInfo> InsertOrUpdateClientScopeInfoAsync(ScopeInfo scopeInfo, DbConnection connection, DbTransaction transaction) => Task.FromResult(scopeInfo);


        public Task<ServerHistoryScopeInfo> InsertOrUpdateServerHistoryScopeInfoAsync(ServerHistoryScopeInfo serverHistoryScopeInfo, DbConnection connection, DbTransaction transaction) 
            => Task.FromResult(serverHistoryScopeInfo);


        public Task<ServerScopeInfo> InsertOrUpdateServerScopeInfoAsync(ServerScopeInfo serverScopeInfo, DbConnection connection, DbTransaction transaction) => Task.FromResult(serverScopeInfo);


        public Task<bool> NeedToCreateClientScopeInfoTableAsync(DbConnection connection, DbTransaction transaction) => Task.FromResult(true);


        public Task<bool> NeedToCreateServerHistoryScopeInfoTableAsync(DbConnection connection, DbTransaction transaction) => Task.FromResult(true);


        public Task<bool> NeedToCreateServerScopeInfoTableAsync(DbConnection connection, DbTransaction transaction) => Task.FromResult(true);

    }
}
