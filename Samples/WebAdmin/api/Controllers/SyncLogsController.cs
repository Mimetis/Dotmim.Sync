using Api.Context;
using Dotmim.Sync;
using Dotmim.Sync.SqlServer;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Newtonsoft.Json.Linq;

namespace Api.Controllers;

[ApiController]
[Route("api/[controller]")]
public class SyncLogsController : ControllerBase
{
    private IDBHelper dbHelper;
    private readonly SyncLogsContext context;
    private readonly IConfiguration configuration;

    public SyncLogsController(IDBHelper dbHelper, SyncLogsContext context, IConfiguration configuration)
    {
        this.dbHelper = dbHelper;
        this.context = context;
        this.configuration = configuration;
    }

    /// <summary>
    /// Get all sync logs
    /// </summary>
    [HttpGet]
    public async Task<IList<SyncLog>> GetSyncLogsAsync()
    {
        var logs = await context.SyncLog.OrderByDescending(sl => sl.StartTime).Include(l => l.Details).ToListAsync();

        return logs;
    }


    [HttpGet("/api/scopes")]
    public async Task<JArray> GetScopes()
    {
        var sqlProvider = new SqlSyncProvider(this.configuration.GetConnectionString("SqlConnection"));
        var remoteOrchestrator = new RemoteOrchestrator(sqlProvider);

        var scopes = await remoteOrchestrator.GetAllScopeInfosAsync().ConfigureAwait(false);

        var jArray = new JArray();

        foreach (var scope in scopes)
        {
            jArray.Add(new JObject{
                { "name", scope.Name },
                { "setup", JObject.FromObject(scope.Setup) },
                { "lastCleanup", scope.LastCleanupTimestamp },
                { "version", scope.Version },
            });
        }

        return jArray;
    }

    [HttpGet("/api/clientsScopes")]
    public async Task<JArray> GetClientScopes()
    {
        var sqlProvider = new SqlSyncProvider(this.configuration.GetConnectionString("SqlConnection"));
        var remoteOrchestrator = new RemoteOrchestrator(sqlProvider);

        var sScopeInfosClients = await remoteOrchestrator.GetAllScopeInfoClientsAsync().ConfigureAwait(false);

        var jArray = new JArray();

        foreach (var scopeInfoClient in sScopeInfosClients)
        {
            jArray.Add(new JObject{
                { "id", scopeInfoClient.Id },
                { "scopeName", scopeInfoClient.Name },
                { "lastSync", scopeInfoClient.LastSync },
                { "lastSyncDuration", scopeInfoClient.LastSyncDuration },
                { "lastSyncTimestamp", scopeInfoClient.LastSyncTimestamp },
                { "properties", scopeInfoClient.Properties },
                { "parameters", scopeInfoClient.Parameters != null ? JArray.FromObject(scopeInfoClient.Parameters) :null},
                { "errors", scopeInfoClient.Errors}
            });
        }

        return jArray;
    }

    [HttpGet("/api/clientsScopesById")]
    public async Task<JArray> GetClientScopesById(Guid id)
    {
        var sqlProvider = new SqlSyncProvider(this.configuration.GetConnectionString("SqlConnection"));
        var remoteOrchestrator = new RemoteOrchestrator(sqlProvider);

        var sScopeInfosClients = await remoteOrchestrator.GetAllScopeInfoClientsAsync().ConfigureAwait(false);

        if (sScopeInfosClients == null)
            return null;

        sScopeInfosClients = sScopeInfosClients.Where(s => s.Id == id).ToList();

        var jArray = new JArray();

        foreach (var scopeInfoClient in sScopeInfosClients)
        {
            jArray.Add(new JObject{
                { "id", scopeInfoClient.Id },
                { "scopeName", scopeInfoClient.Name },
                { "lastSync", scopeInfoClient.LastSync },
                { "lastSyncDuration", scopeInfoClient.LastSyncDuration },
                { "lastSyncTimestamp", scopeInfoClient.LastSyncTimestamp },
                { "properties", scopeInfoClient.Properties },
                { "parameters", JArray.FromObject(scopeInfoClient.Parameters) },
                { "errors", scopeInfoClient.Errors}

            });
        }

        return jArray;
    }
}