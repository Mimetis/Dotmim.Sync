using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Builders
{
    public interface IDbScopeInfoBuilder
    {
        Task<bool> NeedToCreateClientScopeInfoTableAsync(DbConnection connection, DbTransaction transaction);
        Task<bool> NeedToCreateServerScopeInfoTableAsync(DbConnection connection, DbTransaction transaction);
        Task<bool> NeedToCreateServerHistoryScopeInfoTableAsync(DbConnection connection, DbTransaction transaction);

        Task CreateClientScopeInfoTableAsync(DbConnection connection, DbTransaction transaction);
        Task CreateServerScopeInfoTableAsync(DbConnection connection, DbTransaction transaction);
        Task CreateServerHistoryScopeInfoTableAsync(DbConnection connection, DbTransaction transaction);

        Task<List<ScopeInfo>> GetAllClientScopesAsync(string scopeName, DbConnection connection, DbTransaction transaction);
        Task<List<ServerScopeInfo>> GetAllServerScopesAsync(string scopeName, DbConnection connection, DbTransaction transaction);
        Task<List<ServerHistoryScopeInfo>> GetAllServerHistoryScopesAsync(string scopeName, DbConnection connection, DbTransaction transaction);

        Task<ScopeInfo> InsertOrUpdateClientScopeInfoAsync(ScopeInfo scopeInfo, DbConnection connection, DbTransaction transaction);
        Task<ServerScopeInfo> InsertOrUpdateServerScopeInfoAsync(ServerScopeInfo serverScopeInfo, DbConnection connection, DbTransaction transaction);
        Task<ServerHistoryScopeInfo> InsertOrUpdateServerHistoryScopeInfoAsync(ServerHistoryScopeInfo serverHistoryScopeInfo, DbConnection connection, DbTransaction transaction);

        Task<long> GetLocalTimestampAsync(DbConnection connection, DbTransaction transaction);

        Task DropClientScopeInfoTableAsync(DbConnection connection, DbTransaction transaction);
        Task DropServerScopeInfoTableAsync(DbConnection connection, DbTransaction transaction);
        Task DropServerHistoryScopeInfoTableAsync(DbConnection connection, DbTransaction transaction);

    }
}
