using Dotmim.Sync;
using System.Net.Http;

namespace MauiAppClient.Services
{
    public interface ISyncServices
    {
        SyncAgent GetSyncAgent();
        HttpClient GetHttpClient();
    }
}