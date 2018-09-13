
using Dotmim.Sync.MySql;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Models;
using Microsoft.Data.Sqlite;
using MySql.Data.MySqlClient;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace Dotmim.Sync.Tests.Core
{
    /// <summary>
    /// Each new provider, to be tested, shoud inherits from this fixture.
    /// It will be in charge to create / drop your server database, regarding your properties.
    /// In your provider fixture instance, you will be able to select o, which kind of client databases you want to test, thx to all "Enable..." properties
    /// </summary>
    /// <typeparam name="T">The provider we want to test</typeparam>
    public abstract class ProviderFixture<T> : IDisposable where T : CoreProvider
    {

        private bool isConfigured = false;

        // list of client providers we want to create
        private readonly Dictionary<(ProviderType ServerType, NetworkType NetworkType), ProviderType> lstClientsType = new Dictionary<(ProviderType, NetworkType), ProviderType>();

        // internal static list of registered providers
        static Dictionary<ProviderType, ProviderFixture<CoreProvider>> registeredProviders = new Dictionary<ProviderType, ProviderFixture<CoreProvider>>();

        /// <summary>
        /// All clients providers registerd to run. Depends on "Enable..." properities
        /// </summary>
        public List<ProviderRun> ClientRuns { get; } = new List<ProviderRun>();

        /// <summary>
        /// Gets or Sets the sync tables involved in the tests
        /// </summary>
        public String[] Tables { get; set; }

        /// <summary>
        /// Sets the tables used for this server provider
        /// </summary>
        internal void AddTables(ProviderType providerType, string[] tables)
        {
            if (providerType == this.ProviderType)
                this.Tables = tables;
        }

        /// <summary>
        /// Will configure the fixture on first test launch
        /// </summary>
        internal void Configure()
        {
            if (!isConfigured)
            {
                // get database name, tables and clients we want to register
                Setup.OnConfiguring(this);

                // create the server database
                this.ServerDatabaseEnsureCreated();


                var listOfBs = (from assemblyType in typeof(ProviderFixture<T>).Assembly.DefinedTypes
                                where typeof(ProviderFixture<T>).IsAssignableFrom(assemblyType)
                                select assemblyType).ToArray();

                Debug.WriteLine(listOfBs);

                foreach(var t in listOfBs)
                {
                    var c = GetDefaultConstructor(t);
                    ProviderFixture<CoreProvider> instance = c.Invoke(null) as ProviderFixture<CoreProvider>;
                    RegisterProvider(instance.ProviderType, instance);
                }


                // create the clients database
                this.ClientDatabasesEnsureCreated();

                isConfigured = true;
            }
        }

        public static ConstructorInfo GetDefaultConstructor(Type t, bool nonPublic = true)
        {
            BindingFlags bindingFlags = BindingFlags.Instance | BindingFlags.Public;

            if (nonPublic)
                bindingFlags = bindingFlags | BindingFlags.NonPublic;

            var gtcs = t.GetConstructors(bindingFlags);

            return gtcs.SingleOrDefault(c => !c.GetParameters().Any());
        }

        /// <summary>
        /// Gets or Sets if we should delete all the databases. 
        /// Useful for debug purpose. Do not forget to set to false when commit
        /// </summary>
        internal virtual bool DeleteAllDatabasesOnDispose { get; } = true;


        /// <summary>
        /// register all Provider fixture to be able to call them when we need to create a client provider
        /// </summary>
        internal static void RegisterProvider(ProviderType providerType, ProviderFixture<CoreProvider> providerFixture)
        {
            // Register the provider to be able to call it for create some client provider
            if (!registeredProviders.ContainsKey(providerType))
                registeredProviders.Add(providerType, providerFixture);

        }

        public ProviderFixture()
        {
        }

        /// <summary>
        /// Add a run. A run is all the clients you want to test for one server provider on one network type
        /// </summary>
        /// <param name="key">The server provider type and the network (http / tcp) you want to test</param>
        /// <param name="clientsType">a flags enum of all client you want to create</param>
        internal ProviderFixture<T> AddRun((ProviderType ServerType, NetworkType NetworkType) key, ProviderType clientsType)
        {
            if (!lstClientsType.ContainsKey(key))
                lstClientsType.Add(key, clientsType);

            return this;
        }

        /// <summary>
        /// Add the database name used for the server provider
        /// </summary>
        internal void AddDatabaseName(ProviderType providerType, string databaseName)
        {
            if (providerType == this.ProviderType)
                this.DatabaseName = databaseName;
        }


        /// <summary>
        /// On dispose, ensure all databases are cleaned and deleted
        /// </summary>
        public void Dispose()
        {
            if (DeleteAllDatabasesOnDispose)
            {
                this.ClientDatabasesEnsureDeleted();
                this.ServerDatabaseEnsureDeleted();
            }
        }

        /// <summary>
        /// gets or sets the database name used for server database
        /// </summary>
        public string DatabaseName { get; private set; }

        /// <summary>
        /// gets or sets the provider type we are going to test
        /// </summary>
        public abstract ProviderType ProviderType { get; }

        /// <summary>
        /// Gets a new instance of the CoreProvider we want to test
        /// </summary>
        public abstract CoreProvider NewServerProvider(string connectionString);

        /// <summary>
        /// Ensure provider server database is correctly created
        /// </summary>
        internal virtual void ServerDatabaseEnsureCreated()
        {

            try
            {
            using (AdventureWorksContext ctx =
                new AdventureWorksContext(ProviderType, HelperDB.GetConnectionString(ProviderType, DatabaseName)))
            {
                ctx.Database.EnsureDeleted();
                ctx.Database.EnsureCreated();
            }

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                throw ex;
            }
        }

        /// <summary>
        /// Ensure provider server database is correctly droped at the end of tests
        /// </summary>
        internal virtual void ServerDatabaseEnsureDeleted()
        {
            using (AdventureWorksContext ctx =
                new AdventureWorksContext(ProviderType, HelperDB.GetConnectionString(ProviderType, DatabaseName)))
            {
                ctx.Database.EnsureDeleted();
            }
        }

        /// <summary>
        /// Used to generate client databases
        /// </summary>
        internal virtual string GetRandomDatabaseName()
        {
            var str1 = Path.GetRandomFileName().Replace(".", "").ToLowerInvariant();
            return $"st_{str1}";
        }

        internal virtual void ClientDatabasesEnsureDeleted()
        {
            foreach (var tr in ClientRuns)
                HelperDB.DropDatabase(tr.ClientProviderType, tr.DatabaseName);

            ClientRuns.Clear();

        }

        internal virtual void ClientDatabasesEnsureCreated()
        {
            // foreach server provider to test, there is a list of client / network
            foreach(var serverKey in lstClientsType)
            {
                // get the server provider type and the network we want to use
                (ProviderType serverType, NetworkType networkType) = serverKey.Key;

                if (serverType != this.ProviderType)
                    continue;

                // create all the client databases based in the flags we set
                foreach(var cpt in serverKey.Value.GetFlags())
                {
                    // cast (wait for C# 7.3 to be able to set an extension method where a generic T can be marked as Enum compliant :)
                    ProviderType clientProviderType = (ProviderType)cpt;

                    // check if we have the fixture provider to be able to create a client provider
                    if (!registeredProviders.ContainsKey(clientProviderType))
                        continue;


                    // generate a new database name
                    var dbName = GetRandomDatabaseName();

                    Console.WriteLine("Create a database called " + dbName + " for provider " + ProviderType);
                    
                    // get the connection string
                    var connectionString = HelperDB.GetConnectionString(clientProviderType, dbName);

                    // create the database on the client provider
                    HelperDB.CreateDatabase(clientProviderType, dbName);

                    // generate the client provider
                    var clientProvider = registeredProviders[clientProviderType].NewServerProvider(connectionString);


                    // then add the run 
                    ClientRuns.Add(new ProviderRun(dbName, clientProvider, clientProviderType, networkType));
                }


            }


            //// Add filled client tcp providers
            //if (this.EnableSqlServerClientOnTcp && !ClientRuns.Any(tr => !tr.IsHttp && tr.ClientProviderType == ProviderType.Sql))
            //{
            //    var dbName = GetRandomDatabaseName();
            //    var connectionString = HelperDB.GetConnectionString(ProviderType.Sql, dbName);
            //    HelperDB.CreateSqlServerDatabase(dbName);
            //    ClientRuns.Add(new ProviderRun(
            //        dbName, new SqlSyncProvider(connectionString), false, ProviderType.Sql)
            //    );
            //}

            //if (this.enableMySqlClientOnTcp && !ClientRuns.Any(tr => !tr.IsHttp && tr.ClientProviderType == ProviderType.MySql))
            //{
            //    var dbName = GetRandomDatabaseName();
            //    var connectionString = HelperDB.GetConnectionString(ProviderType.MySql, dbName);
            //    HelperDB.CreateMySqlDatabase(dbName);

            //    ClientRuns.Add(new ProviderRun(
            //        dbName, new MySqlSyncProvider(connectionString), false, ProviderType.MySql)
            //    );
            //}

            //if (this.enableSqliteClientOnTcp && !ClientRuns.Any(tr => !tr.IsHttp && tr.ClientProviderType == ProviderType.Sqlite))
            //{
            //    var dbName = GetRandomDatabaseName();
            //    var connectionString = HelperDB.GetSqliteDatabaseConnectionString(dbName);

            //    ClientRuns.Add(new ProviderRun(
            //        dbName, new SqliteSyncProvider(connectionString), false, ProviderType.Sqlite)
            //    );

            //}

            //if (this.enableSqlServerClientOnHttp && !ClientRuns.Any(tr => tr.IsHttp && tr.ClientProviderType == ProviderType.Sql))
            //{
            //    var dbName = GetRandomDatabaseName();
            //    var connectionString = HelperDB.GetConnectionString(ProviderType.Sql, dbName);
            //    HelperDB.CreateSqlServerDatabase(dbName);

            //    ClientRuns.Add(new ProviderRun(
            //        dbName, new SqlSyncProvider(connectionString), true, ProviderType.Sql)
            //    );


            //}

            //if (this.enableMySqlClientOnHttp && !ClientRuns.Any(tr => tr.IsHttp && tr.ClientProviderType == ProviderType.MySql))
            //{
            //    var dbName = GetRandomDatabaseName();
            //    var connectionString = HelperDB.GetConnectionString(ProviderType.MySql, dbName);
            //    HelperDB.CreateMySqlDatabase(dbName);

            //    ClientRuns.Add(new ProviderRun(
            //        dbName, new MySqlSyncProvider(connectionString), true, ProviderType.MySql)
            //    );
            //}

            //if (this.enableSqliteClientOnHttp && !ClientRuns.Any(tr => tr.IsHttp && tr.ClientProviderType == ProviderType.Sqlite))
            //{
            //    var dbName = GetRandomDatabaseName();
            //    var connectionString = HelperDB.GetSqliteDatabaseConnectionString(dbName);

            //    ClientRuns.Add(new ProviderRun(
            //        dbName, new SqliteSyncProvider(connectionString), true, ProviderType.Sqlite)
            //    );

            //}
        }


        internal void CopyConfiguration(SyncConfiguration agentConfiguration, SyncConfiguration conf)
        {
            agentConfiguration.DownloadBatchSizeInKB = conf.DownloadBatchSizeInKB;
            agentConfiguration.UseBulkOperations = conf.UseBulkOperations;
            agentConfiguration.SerializationFormat = conf.SerializationFormat;
            agentConfiguration.Archive = conf.Archive;
            agentConfiguration.BatchDirectory = conf.BatchDirectory;
            agentConfiguration.ConflictResolutionPolicy = conf.ConflictResolutionPolicy;
            agentConfiguration.DownloadBatchSizeInKB = conf.DownloadBatchSizeInKB;
            agentConfiguration.SerializationFormat = conf.SerializationFormat;
            agentConfiguration.StoredProceduresPrefix = conf.StoredProceduresPrefix;
            agentConfiguration.StoredProceduresSuffix = conf.StoredProceduresSuffix;
            agentConfiguration.TrackingTablesPrefix = conf.TrackingTablesPrefix;
            agentConfiguration.TrackingTablesSuffix = conf.TrackingTablesSuffix;
            agentConfiguration.TriggersPrefix = conf.TriggersPrefix;
            agentConfiguration.TriggersSuffix = conf.TriggersSuffix;
            agentConfiguration.UseVerboseErrors = conf.UseVerboseErrors;

        }



    }
}
