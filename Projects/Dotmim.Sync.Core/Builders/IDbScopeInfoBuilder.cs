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
        Task<bool> NeedToCreateClientScopeInfoTableAsync();
        Task<bool> NeedToCreateServerScopeInfoTableAsync();
        Task<bool> NeedToCreateServerHistoryScopeInfoTableAsync();

        Task CreateClientScopeInfoTableAsync();
        Task CreateServerScopeInfoTableAsync();
        Task CreateServerHistoryScopeInfoTableAsync();

        Task<List<ScopeInfo>> GetAllClientScopesAsync(string scopeName);
        Task<List<ServerScopeInfo>> GetAllServerScopesAsync(string scopeName);
        Task<List<ServerHistoryScopeInfo>> GetAllServerHistoryScopesAsync(string scopeName);

        Task<ScopeInfo> InsertOrUpdateClientScopeInfoAsync(ScopeInfo scopeInfo);
        Task<ServerScopeInfo> InsertOrUpdateServerScopeInfoAsync(ServerScopeInfo serverScopeInfo);
        Task<ServerHistoryScopeInfo> InsertOrUpdateServerHistoryScopeInfoAsync(ServerHistoryScopeInfo serverHistoryScopeInfo);

        Task<long> GetLocalTimestampAsync();

        Task DropClientScopeInfoTableAsync();
        Task DropServerScopeInfoTableAsync();
        Task DropServerHistoryScopeInfoTableAsync();

    }
}
