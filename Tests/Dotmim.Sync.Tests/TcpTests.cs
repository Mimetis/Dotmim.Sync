using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.MariaDB;
using Dotmim.Sync.MySql;
using Dotmim.Sync.PostgreSql;
using Dotmim.Sync.Serialization;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.SqlServer.Manager;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Misc;
using Dotmim.Sync.Tests.Models;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Http;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
#if NET5_0 || NET6_0 || NET7_0 || NETCOREAPP3_1
using MySqlConnector;
#elif NETCOREAPP2_1
using MySql.Data.MySqlClient;
#endif
using Newtonsoft.Json;
using Npgsql;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace Dotmim.Sync.Tests
{
    //[TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]
    public abstract partial class TcpTests : IClassFixture<HelperProvider>, IDisposable
    {
        private Stopwatch stopwatch;

        /// <summary>
        /// Gets the sync tables involved in the tests
        /// </summary>
        public abstract string[] Tables { get; }

        /// <summary>
        /// Gets the clients type we want to tests
        /// </summary>
        public abstract List<ProviderType> ClientsType { get; }

        /// <summary>
        /// Gets the server type we want to test
        /// </summary>
        public abstract ProviderType ServerType { get; }

        /// <summary>
        /// Get the server rows count
        /// </summary>
        public abstract int GetServerDatabaseRowsCount((string DatabaseName, ProviderType ProviderType, CoreProvider Provider) t);

        /// <summary>
        /// Create a provider
        /// </summary>
        public CoreProvider CreateProvider(ProviderType providerType, string dbName)
        {
            var cs = HelperDatabase.GetConnectionString(providerType, dbName);
            return providerType switch
            {
                ProviderType.MySql => new MySqlSyncProvider(cs),
                ProviderType.MariaDB => new MariaDBSyncProvider(cs),
                ProviderType.Sqlite => new SqliteSyncProvider(cs),
                ProviderType.Postgres => new NpgsqlSyncProvider(cs),
                _ => new SqlSyncProvider(cs),
            };
        }
        /// <summary>
        /// Create database, seed it, with or without schema
        /// </summary>
        /// <param name="t"></param>
        /// <param name="useSeeding"></param>
        /// <param name="useFallbackSchema"></param>
        public abstract Task EnsureDatabaseSchemaAndSeedAsync((string DatabaseName,
            ProviderType ProviderType, CoreProvider Provider) t, bool useSeeding = false, bool useFallbackSchema = false);

        /// <summary>
        /// Create an empty database
        /// </summary>
        public abstract Task CreateDatabaseAsync(ProviderType providerType, string dbName, bool recreateDb = true);

        // abstract fixture used to run the tests
        protected readonly HelperProvider fixture;

        // Current test running
        private XunitTest test;

        /// <summary>
        /// Gets the remote orchestrator and its database name
        /// </summary>
        public (string DatabaseName, ProviderType ProviderType, CoreProvider Provider) Server { get; private set; }

        /// <summary>
        /// Gets the dictionary of all local orchestrators with database name as key
        /// </summary>
        public List<(string DatabaseName, ProviderType ProviderType, CoreProvider Provider)> Clients { get; set; }

        /// <summary>
        /// Gets a bool indicating if we should generate the schema for tables
        /// </summary>
        public bool UseFallbackSchema => ServerType == ProviderType.Sql;

        public ITestOutputHelper Output { get; }

        /// <summary>
        /// Output to console and debug the current state
        /// </summary>
        public void OutputCurrentState(string subCategory = null)
        {
            var t = string.IsNullOrEmpty(subCategory) ? "" : $" - {subCategory}";
            t = $"{this.test.TestCase.Method.Name}{t}: {this.stopwatch.Elapsed.Minutes}:{this.stopwatch.Elapsed.Seconds}.{this.stopwatch.Elapsed.Milliseconds}";
            Console.WriteLine(t);
            Debug.WriteLine(t);
        }

        /// <summary>
        /// For each test, Create a server database and some clients databases, depending on ProviderType provided in concrete class
        /// </summary>
        public TcpTests(HelperProvider fixture, ITestOutputHelper output)
        {
            // Getting the test running
            this.Output = output;
            var type = output.GetType();
            var testMember = type.GetField("test", BindingFlags.Instance | BindingFlags.NonPublic);
            this.test = (XunitTest)testMember.GetValue(output);
            this.stopwatch = Stopwatch.StartNew();

            this.fixture = fixture;

            // Since we are creating a lot of databases
            // each database will have its own pool
            // Droping database will not clear the pool associated
            // So clear the pools on every start of a new test
            SqlConnection.ClearAllPools();
            MySqlConnection.ClearAllPools();
            NpgsqlConnection.ClearAllPools();


            // get the server provider (and db created) without seed
            var serverDatabaseName = HelperDatabase.GetRandomName("tcp_sv_");

            // create remote orchestrator
            var serverProvider = this.CreateProvider(this.ServerType, serverDatabaseName);

            this.Server = (serverDatabaseName, this.ServerType, serverProvider);

            // Get all clients providers
            Clients = new List<(string DatabaseName, ProviderType ProviderType, CoreProvider provider)>(this.ClientsType.Count);

            // Generate Client database
            foreach (var clientType in this.ClientsType)
            {
                var dbCliName = HelperDatabase.GetRandomName("tcp_cli_");
                var localProvider = this.CreateProvider(clientType, dbCliName);
                this.Clients.Add((dbCliName, clientType, localProvider));
            }
        }

        /// <summary>
        /// Drop all databases used for the tests
        /// </summary>
        public void Dispose()
        {
            try
            {
                // Clear pool to be able to delete the database file
                SqliteConnection.ClearAllPools();
                GC.Collect();
                GC.WaitForPendingFinalizers();
                HelperDatabase.DropDatabase(this.ServerType, Server.DatabaseName);

                foreach (var client in Clients)
                    HelperDatabase.DropDatabase(client.ProviderType, client.DatabaseName);
            }
            catch (Exception) { }
            finally { }

            this.stopwatch.Stop();

            OutputCurrentState();

        }


        /// <summary>
        /// Testing an insert / update on a table where a column is not part of the sync setup, and should stay alive after a sync
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async virtual Task OneColumn_NotInSetup_Row_IsUploaded_ToServer_ButValue_RemainsTheSame(SyncOptions options)
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases with schema
            foreach (var client in this.Clients)
                await this.EnsureDatabaseSchemaAndSeedAsync(client, false, UseFallbackSchema);


            // this Guid will be updated on the client
            var clientGuid = Guid.NewGuid();

            // Get server Guid value, that should not change
            Guid? serverGuid;
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                var address = await serverDbCtx.Address.SingleAsync(a => a.AddressId == 1);
                serverGuid = address.Rowguid;
            }


            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var setup = new SyncSetup(new string[] { "Address" });

                // Add all columns to address except Rowguid and ModifiedDate
                setup.Tables["Address"].Columns.AddRange(new string[] {
                    "AddressId", "AddressLine1", "AddressLine2", "City", "StateProvince",
                    "CountryRegion", "PostalCode" });

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync(setup);

                // Editing Rowguid on client. This column is not part of the setup
                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);

                var cliAddress = await ctx.Address.SingleAsync(a => a.AddressId == 1);

                // Now Update on client this address with a rowGuid
                cliAddress.Rowguid = clientGuid;

                await ctx.SaveChangesAsync();
            }

            // each client (except first one) will downloaded row from previous client sync
            var cliDownload = 0;
            // but it will raise a conflict
            var cliConflict = 0;

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                var s = await agent.SynchronizeAsync();

                Assert.Equal(cliDownload, s.TotalChangesDownloadedFromServer);

                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(cliConflict, s.TotalResolvedConflicts);

                // check row on client should not have been updated 
                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
                var cliAddress = await ctx.Address.AsNoTracking().SingleAsync(a => a.AddressId == 1);

                Assert.Equal(clientGuid, cliAddress.Rowguid);

                cliDownload = 1;
                cliConflict = 1;
            }


            // Check on server guid has not been uploaded
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                var address = await serverDbCtx.Address.SingleAsync(a => a.AddressId == 1);
                Assert.Equal(serverGuid, address.Rowguid);
            }
        }

        /// <summary>
        /// Testing an insert / update on a table where a column is not part of the sync setup, and should stay alive after a sync
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async virtual Task OneColumn_NotInSetup_AfterCleanMetadata_IsTracked_ButNotUpdated(SyncOptions options)
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases with schema
            foreach (var client in this.Clients)
                await this.EnsureDatabaseSchemaAndSeedAsync(client, false, UseFallbackSchema);

            // this Guid will be updated on the client
            var clientGuid = Guid.NewGuid();

            // Get server Guid value, that should not change
            Guid? serverGuid;
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                var address = await serverDbCtx.Address.SingleAsync(a => a.AddressId == 1);
                serverGuid = address.Rowguid;
            }

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var setup = new SyncSetup(new string[] { "Address" });

                // Add all columns to address except Rowguid and ModifiedDate
                setup.Tables["Address"].Columns.AddRange(
                    new string[] { "AddressId", "AddressLine1", "AddressLine2",
                        "City", "StateProvince", "CountryRegion", "PostalCode" });

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync(setup);

                // call CleanMetadata to be sure we don't have row in tracking
                var ts = await agent.LocalOrchestrator.GetLocalTimestampAsync();

                // be sure we are deleting ALL rows from tracking table
                var dc = await agent.LocalOrchestrator.DeleteMetadatasAsync(ts + 1);

                // checking if there is no rows in tracking table for address
                var connection = client.Provider.CreateConnection();
                var command = connection.CreateCommand();

                // As Column names are case-sensitive in postgresql
                if (client.ProviderType == ProviderType.Postgres)
                    command.CommandText = "SELECT COUNT(*) FROM \"Address_tracking\"";
                else
                    command.CommandText = "SELECT COUNT(*) FROM Address_tracking";

                command.Connection = connection;
                await connection.OpenAsync();
                var count = await command.ExecuteScalarAsync();
                var countRows = Convert.ToInt32(count);
                Assert.Equal(0, countRows);
                connection.Close();

                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);

                // Editing Rowguid on client. This column is not part of the setup
                // So far, it should be uploaded to server but without the column value
                var cliAddress = await ctx.Address.SingleAsync(a => a.AddressId == 1);

                // Now Update on client this address with a rowGuid
                cliAddress.Rowguid = clientGuid;

                await ctx.SaveChangesAsync();

                await connection.OpenAsync();
                count = await command.ExecuteScalarAsync();
                countRows = Convert.ToInt32(count);
                Assert.Equal(1, countRows);
                connection.Close();


            }

            // each client (except first one) will downloaded row from previous client sync
            var cliDownload = 0;
            // but it will raise a conflict
            var cliConflict = 0;

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(cliDownload, s.TotalChangesDownloadedFromServer);

                // 1 upload since Rowguid is modified, but the column value is not part of the upload
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(cliConflict, s.TotalResolvedConflicts);

                // check row on client should not have been updated 
                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
                var cliAddress = await ctx.Address.AsNoTracking().SingleAsync(a => a.AddressId == 1);

                Assert.Equal(clientGuid, cliAddress.Rowguid);
                cliConflict = 1;
                cliDownload = 1;
            }


            // Check on server guid has not been uploaded
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                var address = await serverDbCtx.Address.SingleAsync(a => a.AddressId == 1);
                Assert.Equal(serverGuid, address.Rowguid);
            }
        }


        /// <summary>
        /// Testing that an upate from the server does not replace, but just update the local row, so that columns that are not included in the sync are not owervritten/cleared
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async virtual Task OneColumn_NotInSetup_IfServerSendsChanges_UpdatesLocalRow_AndDoesNotClear_OneColumn(SyncOptions options)
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases with schema
            foreach (var client in this.Clients)
                await this.EnsureDatabaseSchemaAndSeedAsync(client, false, UseFallbackSchema);


            // this Guid will be updated on the client
            var clientGuid = Guid.NewGuid();

            // Get server Guid value, that should not change
            Guid? serverGuid;
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                var address = await serverDbCtx.Address.SingleAsync(a => a.AddressId == 1);
                serverGuid = address.Rowguid;
            }


            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var setup = new SyncSetup(new string[] { "Address" });

                // Add all columns to address except Rowguid and ModifiedDate
                setup.Tables["Address"].Columns.AddRange(new string[]
                {   "AddressId", "AddressLine1",
                    "AddressLine2", "City", "StateProvince",
                    "CountryRegion", "PostalCode"
                });

                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync(setup);

                // Editing Rowguid on client. This column is not part of the setup
                // The row will be uploaded to the server but the column will not be overriden
                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);

                var cliAddress = await ctx.Address.SingleAsync(a => a.AddressId == 1);

                // Now Update on client this address with a rowGuid
                cliAddress.Rowguid = clientGuid;

                await ctx.SaveChangesAsync();
            }

            // Act
            // Change row on server and make sure that client rows are just UPDATED and not REPLACED
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                var address = await serverDbCtx.Address.SingleAsync(a => a.AddressId == 1);
                address.City = "Mimecity";
                await serverDbCtx.SaveChangesAsync();
            }

            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                var s = await agent.SynchronizeAsync();

                // "Mimecity" change should be received from server
                Assert.Equal(1, s.TotalChangesDownloadedFromServer);

                // One upload
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                // We have resolved a conflict here
                Assert.Equal(1, s.TotalResolvedConflicts);

                // check row on client should not have been updated 
                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
                var cliAddress = await ctx.Address.AsNoTracking().SingleAsync(a => a.AddressId == 1);

                Assert.Equal(clientGuid, cliAddress.Rowguid);
                Assert.Equal("Mimecity", cliAddress.City);
            }


            // Check on server guid has not been uploaded
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                var address = await serverDbCtx.Address.SingleAsync(a => a.AddressId == 1);
                Assert.Equal(serverGuid, address.Rowguid);
            }
        }


        /// <summary>
        /// </summary>
        [Fact]
        public virtual async Task Using_ExistingClientDatabase_UpdateUntrackedRowsAsync()
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // generate a sync conf to host the schema
            var setup = new SyncSetup(this.Tables);

            // options
            var options = new SyncOptions();

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                // Get count of rows
                var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

                // create a client schema without seeding
                await this.EnsureDatabaseSchemaAndSeedAsync(client, false, UseFallbackSchema);

                var name = HelperDatabase.GetRandomName();
                var productNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

                var product = new Product { ProductId = Guid.NewGuid(), Name = name, ProductNumber = productNumber };

                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
                ctx.Product.Add(product);
                await ctx.SaveChangesAsync();

                // create an agent
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync(setup);

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                // mark local row as tracked
                await agent.LocalOrchestrator.UpdateUntrackedRowsAsync();
                s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                Assert.Equal(this.GetServerDatabaseRowsCount(Server), this.GetServerDatabaseRowsCount(client));
            }
        }


    }
}
