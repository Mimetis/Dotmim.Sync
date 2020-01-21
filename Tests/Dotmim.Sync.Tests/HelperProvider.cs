using Dotmim.Sync.MySql;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Models;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using Microsoft.Data.Sqlite;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Tests
{
    public class HelperProvider : IDisposable
    {

        public HelperProvider()
        {
        }

        /// <summary>
        /// Create a server database, and get the server provider associated
        /// </summary>
        public T CreateOrchestrator<T>(ProviderType providerType, string dbName, bool useChangeTracking = false) where T : IOrchestrator
        {
            // Get connection string
            var cs = HelperDatabase.GetConnectionString(providerType, dbName);

            IOrchestrator orchestrator = null;

            if (typeof(T) == typeof(RemoteOrchestrator))
                orchestrator = new RemoteOrchestrator();
            else if (typeof(T) == typeof(LocalOrchestrator))
                orchestrator = new LocalOrchestrator();
            else if (typeof(T) == typeof(WebServerOrchestrator))
                orchestrator = new WebServerOrchestrator();

            if (orchestrator == null)
                throw new Exception("Orchestrator does not exists");

            switch (providerType)
            {
                case ProviderType.Sql:
                    orchestrator.Provider = useChangeTracking ? new SqlSyncChangeTrackingProvider(cs) : new SqlSyncProvider(cs);
                    break;
                case ProviderType.MySql:
                    orchestrator.Provider = new MySqlSyncProvider(cs);
                    break;
                case ProviderType.Sqlite:
                    orchestrator.Provider = new SqliteSyncProvider(cs);
                    break;
            }
            return (T)orchestrator;
        }

 
        public void Dispose()
        {
        }


    }

}
