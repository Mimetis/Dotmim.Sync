
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
using System.IO;
using System.Linq;
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
        /// <summary>
        /// Gets if the tests should run on a tcp sql server client
        /// </summary>
        public abstract bool EnableSqlServerClientOnTcp { get; }

        /// <summary>
        /// Gets if the tests should run on a htt sql server client
        /// </summary>
        public abstract bool EnableSqlServerClientOnHttp { get; }

        /// <summary>
        /// Gets if the tests should run on a tcp sql server client
        /// </summary>
        public abstract bool EnableOracleClientOnTcp { get; }

        /// <summary>
        /// Gets if the tests should run on a htt sql server client
        /// </summary>
        public abstract bool EnableOracleClientOnHttp { get; }


        /// <summary>
        /// Gets if the tests should run on a tcp mysql client
        /// </summary>
        public abstract bool EnableMySqlClientOnTcp { get; }

        /// <summary>
        /// Gets if the tests should run on a http mysql client
        /// </summary>
        public abstract bool EnableMySqlClientOnHttp { get; }

        /// <summary>
        /// Gets if the tests should run on a tcp sqlite client
        /// </summary>
        public abstract bool EnableSqliteClientOnTcp { get; }

        /// <summary>
        /// Gets if the tests should run on a http sqlite client
        /// </summary>
        public abstract bool EnableSqliteClientOnHttp { get; }

        /// <summary>
        /// All clients providers registerd to run. Depends on "Enable..." properities
        /// </summary>
        public List<ProviderRun> ClientRuns { get; } = new List<ProviderRun>();

        /// <summary>
        /// Gets or Sets the sync tables involved in the tests
        /// </summary>
        public String[] Tables { get; set; }

        /// <summary>
        /// Gets or Sets if we should delete all the databases. 
        /// Useful for debug purpose. Do not forget to set to false when commit
        /// </summary>
        public virtual bool DeleteAllDatabasesOnDispose { get; } = true;

        /// <summary>
        /// On ctor, ensure the server database is ready.
        /// </summary>
        public ProviderFixture()
        {
            this.ServerDatabaseEnsureCreated();
            this.ClientDatabasesEnsureCreated();
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
        public abstract string DatabaseName { get; }

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
        public virtual void ServerDatabaseEnsureCreated()
        {
            using (AdventureWorksContext ctx =
                new AdventureWorksContext(ProviderType, HelperDB.GetConnectionString(ProviderType, DatabaseName)))
            {
                ctx.Database.EnsureDeleted();
                ctx.Database.EnsureCreated();
            }
        }

        /// <summary>
        /// Ensure provider server database is correctly droped at the end of tests
        /// </summary>
        public virtual void ServerDatabaseEnsureDeleted()
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
        public string GetRandomDatabaseName()
        {
            var str1 = Path.GetRandomFileName().Replace(".", "").ToLowerInvariant();
            return $"st_{str1}";
        }
        public void ClientDatabasesEnsureDeleted()
        {
            foreach (var tr in ClientRuns)
                HelperDB.DropDatabase(tr.ProviderType, tr.DatabaseName);

            ClientRuns.Clear();

        }
        public void ClientDatabasesEnsureCreated()
        {
            // Add filled client tcp providers
            if (this.EnableSqlServerClientOnTcp && !ClientRuns.Any(tr => !tr.IsHttp && tr.ProviderType == ProviderType.Sql))
            {
                var dbName = GetRandomDatabaseName();
                var connectionString = HelperDB.GetSqlDatabaseConnectionString(dbName);
                HelperDB.CreateSqlServerDatabase(dbName);
                ClientRuns.Add(new ProviderRun(
                    dbName, new SqlSyncProvider(connectionString), false, ProviderType.Sql)
                );
            }

            if (this.EnableMySqlClientOnTcp && !ClientRuns.Any(tr => !tr.IsHttp && tr.ProviderType == ProviderType.MySql))
            {
                var dbName = GetRandomDatabaseName();
                var connectionString = HelperDB.GetMySqlDatabaseConnectionString(dbName);
                HelperDB.CreateMySqlDatabase(dbName);

                ClientRuns.Add(new ProviderRun(
                    dbName, new MySqlSyncProvider(connectionString), false, ProviderType.MySql)
                );
            }

            if (this.EnableSqliteClientOnTcp && !ClientRuns.Any(tr => !tr.IsHttp && tr.ProviderType == ProviderType.Sqlite))
            {
                var dbName = GetRandomDatabaseName();
                var connectionString = HelperDB.GetSqliteDatabaseConnectionString(dbName);

                ClientRuns.Add(new ProviderRun(
                    dbName, new SqliteSyncProvider(connectionString), false, ProviderType.Sqlite)
                );

            }

            if (this.EnableSqlServerClientOnHttp && !ClientRuns.Any(tr => tr.IsHttp && tr.ProviderType == ProviderType.Sql))
            {
                var dbName = GetRandomDatabaseName();
                var connectionString = HelperDB.GetSqlDatabaseConnectionString(dbName);
                HelperDB.CreateSqlServerDatabase(dbName);

                ClientRuns.Add(new ProviderRun(
                    dbName, new SqlSyncProvider(connectionString), true, ProviderType.Sql)
                );


            }

            if (this.EnableMySqlClientOnHttp && !ClientRuns.Any(tr => tr.IsHttp && tr.ProviderType == ProviderType.MySql))
            {
                var dbName = GetRandomDatabaseName();
                var connectionString = HelperDB.GetMySqlDatabaseConnectionString(dbName);
                HelperDB.CreateMySqlDatabase(dbName);

                ClientRuns.Add(new ProviderRun(
                    dbName, new MySqlSyncProvider(connectionString), true, ProviderType.MySql)
                );
            }

            if (this.EnableSqliteClientOnHttp && !ClientRuns.Any(tr => tr.IsHttp && tr.ProviderType == ProviderType.Sqlite))
            {
                var dbName = GetRandomDatabaseName();
                var connectionString = HelperDB.GetSqliteDatabaseConnectionString(dbName);

                ClientRuns.Add(new ProviderRun(
                    dbName, new SqliteSyncProvider(connectionString), true, ProviderType.Sqlite)
                );

            }
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
