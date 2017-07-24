using Dotmim.Sync.SqlServer;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Core
{
    public static class SqlAgentExtensionsMethods
    {
        public static SyncAgent UseSqlServer(this SyncAgent syncAgent, string connectionString, SyncProviderType providerType)
        {
            if (providerType == SyncProviderType.IsLocal)
                syncAgent.LocalProvider = new SqlSyncProvider(connectionString);
            else
                syncAgent.RemoteProvider = new SqlSyncProvider(connectionString);

            return syncAgent;
        }

    }
}
