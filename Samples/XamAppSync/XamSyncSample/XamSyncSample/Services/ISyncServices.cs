using Dotmim.Sync;
using System.Net.Http;

namespace XamSyncSample.Services
{
    public interface ISyncServices
    {
        SyncAgent GetSyncAgent();
        HttpClient GetHttpClient();
    }
}