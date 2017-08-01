using Dotmim.Sync.SQLite;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Core
{
    public static class SQLiteAgentExtensionsMethods
    {
        public static SyncAgent UseSQLite(this SyncAgent syncAgent, string connectionString, SyncProviderType providerType)
        {
            if (providerType == SyncProviderType.IsLocal)
                syncAgent.LocalProvider = new SQLiteSyncProvider(connectionString);
            else
                syncAgent.RemoteProvider = new SQLiteSyncProvider(connectionString);

            return syncAgent;
        }

    }
}
