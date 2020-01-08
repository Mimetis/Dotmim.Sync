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

namespace Dotmim.Sync.Tests.V2
{
    public class HelperProvider : IDisposable
    {

        private readonly List<Orchestrators> availablesOrchestrators;

        public HelperProvider()
        {
            this.availablesOrchestrators = new List<Orchestrators>();
        }

        /// <summary>
        /// Create a server database, and get the server provider associated
        /// </summary>
        public T CreateOrchestrator<T>(ProviderType providerType, string dbName) where T : IOrchestrator
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
                    orchestrator.Provider = new SqlSyncProvider(cs);
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

        /// <summary>
        /// Create schema and seed the database
        /// </summary>
        internal void EnsureDatabaseSchemaAndSeed((string DatabaseName, ProviderType ProviderType, IOrchestrator Orchestrator) t, bool fallbackUseSchema = false, bool useSeeding = false)
        {
            using (var ctx = new AdventureWorksContext(t, fallbackUseSchema, useSeeding))
                ctx.Database.EnsureCreated();
        }

        public void DropDatabase(IOrchestrator orchrestrator)
        {
            if (orchrestrator.Provider.ProviderTypeName.Contains("SqlServer.SqlSyncProvider"))
            {
                var cBuilder = new SqlConnectionStringBuilder(orchrestrator.Provider.ConnectionString);
                HelperDatabase.DropDatabase(ProviderType.Sql, cBuilder.InitialCatalog);
            }
            else if (orchrestrator.Provider.ProviderTypeName.Contains("MySql.MySqlSyncProvider"))
            {
                var cBuilder = new MySqlConnectionStringBuilder(orchrestrator.Provider.ConnectionString);
                HelperDatabase.DropDatabase(ProviderType.MySql, cBuilder.Database);
            }
            else if (orchrestrator.Provider.ProviderTypeName.Contains("Sqlite.SqliteSyncProvider"))
            {
                var cBuilder = new SqliteConnectionStringBuilder(orchrestrator.Provider.ConnectionString);
                HelperDatabase.DropDatabase(ProviderType.Sqlite, cBuilder.DataSource);
            }
        }

        public void DropDatabases(Orchestrators orchestrators)
        {
            foreach (var o in orchestrators.LocalOrchestrators)
                DropDatabase(o);

            DropDatabase(orchestrators.RemoteOrchestrator);

            this.availablesOrchestrators.Remove(orchestrators);
        }



        ///// <summary>
        ///// Create classic remote, local, and web orchestrators
        ///// </summary>
        //public async Task<Orchestrators>
        //    CreateProviders(ProviderType serverType, ProviderType clientsTypes, bool fallbackUseSchema = false, bool useSeeding = false)
        //{
        //    // get the server provider (and db created) without seed
        //    var dbName = HelperDB.GetRandomDatabaseName("sv_");
        //    var remoteOrchestrator = CreateOrchestrator<RemoteOrchestrator>(serverType, dbName);
        //    EnsureServerDatabase(remoteOrchestrator, fallbackUseSchema, useSeeding);

        //    var lstLocalOrchestrators = new List<LocalOrchestrator>();
        //    // Get all clients providers
        //    foreach (ProviderType clientType in clientsTypes.GetFlags())
        //    {
        //        var dbCliName = HelperDB.GetRandomDatabaseName("cli_");
        //        var localOrchestrator = CreateOrchestrator<LocalOrchestrator>(clientType, dbCliName);
        //        await HelperDatabase.CreateDatabaseAsync(clientType, dbCliName, true);

        //        lstLocalOrchestrators.Add(localOrchestrator);
        //    }

        //    // Create web server orchestrator and client web proxy orchestrator
        //    var webServerOrchestrator = new WebServerOrchestrator(remoteOrchestrator.Provider);
        //    var webClientOrchestrator = new WebClientOrchestrator();

        //    var o = new Orchestrators(remoteOrchestrator, lstLocalOrchestrators, webServerOrchestrator, webClientOrchestrator);

        //    this.availablesOrchestrators.Add(o);

        //    return o;
        //}

        public void Dispose()
        {
            foreach (var o in availablesOrchestrators)
                DropDatabases(o);

            availablesOrchestrators.Clear();
        }

        //protected void Dispose(bool cleanup)
        //{
        //    if (cleanup)
        //    {
        //        DeleteDatabases();
        //    }
        //}
    }

    public class Orchestrators
    {
        public Orchestrators(RemoteOrchestrator remoteOrchestrator, List<LocalOrchestrator> localOrchestrators, WebServerOrchestrator webServerOrchestrator, WebClientOrchestrator webClientOrchestrator)
        {
            this.RemoteOrchestrator = remoteOrchestrator;
            this.LocalOrchestrators = localOrchestrators;
            this.WebServerOrchestrator = webServerOrchestrator;
            this.WebClientOrchestrator = webClientOrchestrator;
        }

        public RemoteOrchestrator RemoteOrchestrator { get; set; }
        public List<LocalOrchestrator> LocalOrchestrators { get; set; }
        public WebServerOrchestrator WebServerOrchestrator { get; set; }
        public WebClientOrchestrator WebClientOrchestrator { get; set; }
    }
}
