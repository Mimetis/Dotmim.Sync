using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SampleConsole;
using Dotmim.Sync.Serialization;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Misc;
using Dotmim.Sync.Tests.Models;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Http;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Dotmim.Sync.Tests
{
    [TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]
    public abstract class HttpTests : IClassFixture<HelperProvider>, IDisposable
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
        /// Gets if fiddler is in use
        /// </summary>
        public abstract bool UseFiddler { get; }

        /// <summary>
        /// Get the server rows count
        /// </summary>
        public abstract int GetServerDatabaseRowsCount((string DatabaseName, ProviderType ProviderType, IOrchestrator Orchestrator) t);


        // abstract fixture used to run the tests
        protected readonly HelperProvider fixture;

        // Current test running
        private ITest test;
        private KestrellTestServer kestrell;

        /// <summary>
        /// Gets the remote orchestrator and its database name
        /// </summary>
        public (string DatabaseName, ProviderType ProviderType, WebServerOrchestrator WebServerOrchestrator) Server { get; private set; }

        /// <summary>
        /// Gets the dictionary of all local orchestrators with database name as key
        /// </summary>
        public List<(string DatabaseName, ProviderType ProviderType, LocalOrchestrator LocalOrchestrator, WebClientOrchestrator WebClientOrchestrator)> Clients { get; set; }

        /// <summary>
        /// Gets a bool indicating if we should generate the schema for tables
        /// </summary>
        public bool UseFallbackSchema => ServerType == ProviderType.Sql;

        /// <summary>
        /// ctor
        /// </summary>
        public HttpTests(HelperProvider fixture, ITestOutputHelper output)
        {

            // Getting the test running
            var type = output.GetType();
            var testMember = type.GetField("test", BindingFlags.Instance | BindingFlags.NonPublic);
            this.test = (ITest)testMember.GetValue(output);

            this.stopwatch = Stopwatch.StartNew();

            this.fixture = fixture;

            // Since we are creating a lot of databases
            // each database will have its own pool
            // Droping database will not clear the pool associated
            // So clear the pools on every start of a new test
            SqlConnection.ClearAllPools();
            MySqlConnection.ClearAllPools();

            // get the server provider (and db created) without seed
            var serverDatabaseName = HelperDatabase.GetRandomName("http_sv_");

            // create remote orchestrator
            var webServerOrchestrator = this.fixture.CreateOrchestrator<WebServerOrchestrator>(this.ServerType, serverDatabaseName);

            // public property
            this.Server = (serverDatabaseName, this.ServerType, webServerOrchestrator);

            // Create a kestrell server
            this.kestrell = new KestrellTestServer(this.Server, this.UseFiddler);

            // start server and get uri
            var serviceUri = this.kestrell.Run();

            // Get all clients providers
            Clients = new List<(string, ProviderType, LocalOrchestrator, WebClientOrchestrator)>(this.ClientsType.Count);

            // Generate Client database
            foreach (var clientType in this.ClientsType)
            {
                var dbCliName = HelperDatabase.GetRandomName("http_cli_");
                // create local orchestratpr
                var localOrchestrator = this.fixture.CreateOrchestrator<LocalOrchestrator>(clientType, dbCliName);

                // create local proxy client
                var webclientOrchestrator = new WebClientOrchestrator(serviceUri);

                // call a synchronous database creation
                HelperDatabase.CreateDatabaseAsync(clientType, dbCliName, true).GetAwaiter().GetResult();

                this.Clients.Add((dbCliName, clientType, localOrchestrator, webclientOrchestrator));

                // wait for 1 sec to be sure database are created correctly
                System.Threading.Thread.Sleep(1000);

            }

        }

        /// <summary>
        /// Drop all databases used for the tests
        /// </summary>
        public void Dispose()
        {
            HelperDatabase.DropDatabase(this.ServerType, Server.DatabaseName);

            foreach (var client in Clients)
            {
                HelperDatabase.DropDatabase(client.ProviderType, client.DatabaseName);
            }

            this.kestrell.Dispose();

            this.stopwatch.Stop();

            var str = $"{test.TestCase.DisplayName} : {this.stopwatch.Elapsed.Minutes}:{this.stopwatch.Elapsed.Seconds}.{this.stopwatch.Elapsed.Milliseconds}";
            Console.WriteLine(str);
            Debug.WriteLine(str);

        }

        [Fact, TestPriority(1)]
        public virtual async Task SchemaIsCreated()
        {
            // create a server db without seed
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(Server, false, UseFallbackSchema);

            // configure server orchestrator
            this.Server.WebServerOrchestrator.Setup = new SyncSetup(Tables);

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, client.WebClientOrchestrator);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
            }


        }

        [Theory, TestPriority(2)]
        [ClassData(typeof(SyncOptionsData))]
        public virtual async Task RowsCount(SyncOptions options)
        {
            // create a server db and seed it
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // configure server orchestrator
            this.Server.WebServerOrchestrator.Setup = new SyncSetup(Tables);

            // Execute a sync on all clients and check results
            foreach (var client in this.Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, client.WebClientOrchestrator, null, options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
            }
        }

        /// <summary>
        /// Check a bad connection should raise correct error
        /// </summary>
        [Fact, TestPriority(3)]
        public async Task Bad_ConnectionFromServer_ShouldRaiseError()
        {
            // configure server orchestrator
            this.Server.WebServerOrchestrator.Setup = new SyncSetup(Tables);

            // change the remote orchestrator connection string
            Server.WebServerOrchestrator.Provider.ConnectionString = $@"Server=unknown;Database=unknown;UID=sa;PWD=unknown";

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, client.WebClientOrchestrator);

                var se = await Assert.ThrowsAnyAsync<SyncException>(async () =>
                {
                    var s = await agent.SynchronizeAsync();

                });
            }
        }

        [Fact, TestPriority(4)]
        public async Task Bad_TableWithoutPrimaryKeys_ShouldRaiseError()
        {

            string tableTestCreationScript = "Create Table TableTest (TestId int, TestName varchar(50))";

            // Create an empty server database
            await HelperDatabase.CreateDatabaseAsync(this.ServerType, this.Server.DatabaseName, true);

            // Create the table on the server
            await HelperDatabase.ExecuteScriptAsync(this.Server.ProviderType, this.Server.DatabaseName, tableTestCreationScript); ;

            // Create setup
            var setup = new SyncSetup(new string[] { "TableTest" });

            // configure server orchestrator
            this.Server.WebServerOrchestrator.Setup = setup;

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, client.WebClientOrchestrator);

                var se = await Assert.ThrowsAnyAsync<SyncException>(async () =>
                {
                    var s = await agent.SynchronizeAsync();
                });

                Assert.Equal(SyncExceptionSide.ServerSide, se.Side);
                Assert.Equal("MissingPrimaryKeyException", se.TypeName);
                Assert.Equal(this.Server.DatabaseName, se.InitialCatalog);

            }
        }

        [Fact, TestPriority(5)]
        public async Task Bad_ColumnSetup_DoesNotExistInSchema_ShouldRaiseError()
        {
            // create a server db without seed
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(Server, false, UseFallbackSchema);

            // Create setup
            var setup = new SyncSetup(Tables);

            // Add a malformatted column name
            setup.Tables["Employee"].Columns.AddRange(new string[] { "EmployeeID", "FirstName", "LastNam" });

            // configure server orchestrator
            this.Server.WebServerOrchestrator.Setup = setup;

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, client.WebClientOrchestrator);

                var se = await Assert.ThrowsAnyAsync<SyncException>(async () =>
                {
                    var s = await agent.SynchronizeAsync();
                });

                Assert.Equal(SyncExceptionSide.ServerSide, se.Side);
                Assert.Equal("MissingColumnException", se.TypeName);
            }
        }

        [Fact, TestPriority(6)]
        public async Task Bad_TableSetup_DoesNotExistInSchema_ShouldRaiseError()
        {
            // create a server db without seed
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(Server, false, UseFallbackSchema);

            // Add a fake table to setup tables
            var setup = new SyncSetup(this.Tables);
            setup.Tables.Add("WeirdTable");

            // configure server orchestrator
            this.Server.WebServerOrchestrator.Setup = setup;

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, client.WebClientOrchestrator);

                var se = await Assert.ThrowsAnyAsync<SyncException>(async () =>
                {
                    var s = await agent.SynchronizeAsync();
                });

                Assert.Equal(SyncExceptionSide.ServerSide, se.Side);
                Assert.Equal("MissingTableException", se.TypeName);
            }
        }

        /// <summary>
        /// Insert one row on server, should be correctly sync on all clients
        /// </summary>
        [Theory, TestPriority(7)]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Insert_OneTable_FromServer(SyncOptions options)
        {
            // create a server schema without seeding
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // configure server orchestrator
            this.Server.WebServerOrchestrator.Setup = new SyncSetup(Tables);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, client.WebClientOrchestrator);
                agent.Options = options;

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
            }

            // Create a new product on server
            var name = HelperDatabase.GetRandomName();
            var productNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

            var product = new Product { ProductId = Guid.NewGuid(), Name = name, ProductNumber = productNumber };

            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                serverDbCtx.Product.Add(product);
                await serverDbCtx.SaveChangesAsync();
            }

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, client.WebClientOrchestrator);
                agent.Options = options;

                var s = await agent.SynchronizeAsync();

                Assert.Equal(1, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
            }
        }

        /// <summary>
        /// Insert one row on each client, should be sync on server and clients
        /// </summary>
        [Theory, TestPriority(8)]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Insert_OneTable_FromClient(SyncOptions options)
        {
            // create a server schema without seeding
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // configure server orchestrator
            this.Server.WebServerOrchestrator.Setup = new SyncSetup(Tables);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, client.WebClientOrchestrator);
                agent.Options = options;

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
            }

            // Insert one line on each client
            foreach (var client in Clients)
            {
                var name = HelperDatabase.GetRandomName();
                var productNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

                var product = new Product { ProductId = Guid.NewGuid(), Name = name, ProductNumber = productNumber };

                using (var serverDbCtx = new AdventureWorksContext(client, this.UseFallbackSchema))
                {
                    serverDbCtx.Product.Add(product);
                    await serverDbCtx.SaveChangesAsync();
                }
            }

            // Sync all clients
            // First client  will upload one line and will download nothing
            // Second client will upload one line and will download one line
            // thrid client  will upload one line and will download two lines
            int download = 0;
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, client.WebClientOrchestrator);
                agent.Options = options;

                var s = await agent.SynchronizeAsync();

                Assert.Equal(download++, s.TotalChangesDownloaded);
                Assert.Equal(1, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
            }

        }

        /// <summary>
        /// Delete rows on server, should be correctly sync on all clients
        /// </summary>
        [Theory, TestPriority(9)]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Delete_OneTable_FromServer(SyncOptions options)
        {
            // create a server schema with seeding
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // get rows count
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // configure server orchestrator
            this.Server.WebServerOrchestrator.Setup = new SyncSetup(Tables);

            // part of the filter
            var employeeId = 1;
            // will be defined when address is inserted
            var addressId = 0;

            // Insert one address row and one addressemployee row
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                // Insert a new address for employee 1
                var city = "Paris " + HelperDatabase.GetRandomName();
                var addressline1 = "Rue Monthieu " + HelperDatabase.GetRandomName();
                var stateProvince = "Ile de France";
                var countryRegion = "France";
                var postalCode = "75001";

                var address = new Address
                {
                    AddressLine1 = addressline1,
                    City = city,
                    StateProvince = stateProvince,
                    CountryRegion = countryRegion,
                    PostalCode = postalCode

                };

                serverDbCtx.Add(address);
                await serverDbCtx.SaveChangesAsync();
                addressId = address.AddressId;

                var employeeAddress = new EmployeeAddress
                {
                    EmployeeId = employeeId,
                    AddressId = address.AddressId,
                    AddressType = "SERVER"
                };

                var ea = serverDbCtx.EmployeeAddress.Add(employeeAddress);
                await serverDbCtx.SaveChangesAsync();

            }

            // add 2 lines to rows count
            rowsCount += 2;

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, client.WebClientOrchestrator);
                agent.Options = options;

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);

                // check rows are create on client
                using (var ctx = new AdventureWorksContext(client, this.UseFallbackSchema))
                {
                    var finalAddressesCount = await ctx.Address.AsNoTracking().CountAsync(a => a.AddressId == addressId);
                    var finalEmployeeAddressesCount = await ctx.EmployeeAddress.AsNoTracking().CountAsync(a => a.AddressId == addressId && a.EmployeeId == employeeId);
                    Assert.Equal(1, finalAddressesCount);
                    Assert.Equal(1, finalEmployeeAddressesCount);
                }


            }

            // Delete those lines from server
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                // Get the addresses query
                var address = await serverDbCtx.Address.SingleAsync(a => a.AddressId == addressId);
                var empAddress = await serverDbCtx.EmployeeAddress.SingleAsync(a => a.AddressId == addressId && a.EmployeeId == employeeId);

                // remove them
                serverDbCtx.EmployeeAddress.Remove(empAddress);
                serverDbCtx.Address.Remove(address);

                // Execute query
                await serverDbCtx.SaveChangesAsync();
            }

            // Sync and check we have delete these lines on each server
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, client.WebClientOrchestrator);
                agent.Options = options;

                var s = await agent.SynchronizeAsync();

                Assert.Equal(2, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);

                // check row deleted on client values
                using (var ctx = new AdventureWorksContext(client, this.UseFallbackSchema))
                {
                    var finalAddressesCount = await ctx.Address.AsNoTracking().CountAsync(a => a.AddressId == addressId);
                    var finalEmployeeAddressesCount = await ctx.EmployeeAddress.AsNoTracking().CountAsync(a => a.AddressId == addressId && a.EmployeeId == employeeId);
                    Assert.Equal(0, finalAddressesCount);
                    Assert.Equal(0, finalEmployeeAddressesCount);
                }
            }
        }

        /// <summary>
        /// Generate a conflict when inserting one row on server and the same row on each client
        /// Server should wins the conflict since it's the default behavior
        /// </summary>
        [Theory, TestPriority(10)]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_DeleteClient_UpdateServer_ServerShouldWins(SyncOptions options)
        {
            var productId = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 6);
            var productCategoryName = HelperDatabase.GetRandomName("CLI");
            var productCategoryNameUpdated = HelperDatabase.GetRandomName("SRV");

            // configure server orchestrator
            this.Server.WebServerOrchestrator.Setup = new SyncSetup(Tables);

            // create a server schema without seeding
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // Insert a product category and sync it on all clients
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                ctx.Add(new ProductCategory { ProductCategoryId = productId, Name = productCategoryName });
                await ctx.SaveChangesAsync();
            }

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, client.WebClientOrchestrator);
                agent.Options = options;
                await agent.SynchronizeAsync();

            }

            // Delete product category on each client
            foreach (var client in Clients)
            {
                using (var ctx = new AdventureWorksContext(client, this.UseFallbackSchema))
                {
                    var pcdel = ctx.ProductCategory.Single(pc => pc.ProductCategoryId == productId);
                    ctx.ProductCategory.Remove(pcdel);
                    await ctx.SaveChangesAsync();
                }
            }

            // Update on Server
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                var pcupdated = ctx.ProductCategory.Single(pc => pc.ProductCategoryId == productId);
                pcupdated.Name = productCategoryNameUpdated;
                await ctx.SaveChangesAsync();
            }

            // Execute a sync on all clients and check results
            // Each client will upload its own deleted row (conflicting)
            // then download the updated row from server 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, client.WebClientOrchestrator);
                agent.Options = options;

                var s = await agent.SynchronizeAsync();

                Assert.Equal(1, s.TotalChangesDownloaded);
                Assert.Equal(1, s.TotalChangesUploaded);
                Assert.Equal(1, s.TotalSyncConflicts);
            }

            // check rows count on server and on each client
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                // get all product categories
                var serverPC = await ctx.ProductCategory.AsNoTracking().ToListAsync();

                foreach (var client in Clients)
                {
                    using (var cliCtx = new AdventureWorksContext(client, this.UseFallbackSchema))
                    {
                        // get all product categories
                        var clientPC = await cliCtx.ProductCategory.AsNoTracking().ToListAsync();

                        // check row count
                        Assert.Equal(serverPC.Count, clientPC.Count);

                        foreach (var cpc in clientPC)
                        {
                            var spc = serverPC.First(pc => pc.ProductCategoryId == cpc.ProductCategoryId);

                            // check column value
                            Assert.Equal(spc.ProductCategoryId, cpc.ProductCategoryId);
                            Assert.Equal(spc.Name, cpc.Name);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Generate a conflict when inserting one row on server and the same row on each client
        /// Server should wins the conflict since it's the default behavior
        /// </summary>
        [Theory, TestPriority(11)]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_Insert_Insert_ServerShouldWins(SyncOptions options)
        {
            // create a server schema without seeding
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // configure server orchestrator
            this.Server.WebServerOrchestrator.Setup = new SyncSetup(Tables);


            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, client.WebClientOrchestrator);
                agent.Options = options;

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
            }

            // Insert the conflict product category on each client
            foreach (var client in Clients)
            {
                var productId = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 6);
                var productCategoryNameClient = HelperDatabase.GetRandomName("CLI");
                var productCategoryNameServer = HelperDatabase.GetRandomName("SRV");

                using (var ctx = new AdventureWorksContext(client, this.UseFallbackSchema))
                {
                    ctx.Add(new ProductCategory
                    {
                        ProductCategoryId = productId,
                        Name = productCategoryNameClient
                    });
                    await ctx.SaveChangesAsync();
                }

                using (var ctx = new AdventureWorksContext(this.Server))
                {
                    ctx.Add(new ProductCategory
                    {
                        ProductCategoryId = productId,
                        Name = productCategoryNameServer
                    });
                    await ctx.SaveChangesAsync();
                }
            }

            // Execute a sync on all clients and check results
            // Each client will upload its row (conflicting)
            // then download the others client lines + the conflict (some Clients.count)
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, client.WebClientOrchestrator);
                agent.Options = options;

                var s = await agent.SynchronizeAsync();

                Assert.Equal(Clients.Count, s.TotalChangesDownloaded);
                Assert.Equal(1, s.TotalChangesUploaded);
                Assert.Equal(1, s.TotalSyncConflicts);
            }

            // check rows count on server and on each client
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                // get all product categories
                var serverPC = await ctx.ProductCategory.AsNoTracking().ToListAsync();

                foreach (var client in Clients)
                {
                    using (var cliCtx = new AdventureWorksContext(client, this.UseFallbackSchema))
                    {
                        // get all product categories
                        var clientPC = await cliCtx.ProductCategory.AsNoTracking().ToListAsync();

                        // check row count
                        Assert.Equal(serverPC.Count, clientPC.Count);

                        foreach (var cpc in clientPC)
                        {
                            var spc = serverPC.First(pc => pc.ProductCategoryId == cpc.ProductCategoryId);

                            // check column value
                            Assert.Equal(spc.ProductCategoryId, cpc.ProductCategoryId);
                            Assert.Equal(spc.Name, cpc.Name);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Generate a conflict when inserting one row on server and the same row on each client
        /// Server should wins the conflict because default behavior
        /// </summary>
        [Theory, TestPriority(12)]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_Update_Update_ServerShouldWins(SyncOptions options)
        {
            // create a server schema without seeding
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // configure server orchestrator
            this.Server.WebServerOrchestrator.Setup = new SyncSetup(Tables);

            // Conflict product category
            var conflictProductCategoryId = "BIKES";
            var productCategoryNameClient = "CLI BIKES " + HelperDatabase.GetRandomName();
            var productCategoryNameServer = "SRV BIKES " + HelperDatabase.GetRandomName();

            // Insert line on server
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                ctx.Add(new ProductCategory { ProductCategoryId = conflictProductCategoryId, Name = "BIKES" });
                await ctx.SaveChangesAsync();
            }

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, client.WebClientOrchestrator);
                agent.Options = options;

                var s = await agent.SynchronizeAsync();

                Assert.Equal(1, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
            }

            // Update each client to generate an update conflict
            foreach (var client in Clients)
            {
                using (var ctx = new AdventureWorksContext(client, this.UseFallbackSchema))
                {
                    var pc = ctx.ProductCategory.Find(conflictProductCategoryId);
                    pc.Name = productCategoryNameClient;
                    await ctx.SaveChangesAsync();
                }
            }

            // Update server
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                var pc = ctx.ProductCategory.Find(conflictProductCategoryId);
                pc.Name = productCategoryNameServer;
                await ctx.SaveChangesAsync();
            }

            // Execute a sync on all clients and check results
            // Each client will upload its row (conflicting)
            // then download the others client lines + conflict that should be ovewritten on client
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, client.WebClientOrchestrator);
                agent.Options = options;

                var s = await agent.SynchronizeAsync();

                Assert.Equal(1, s.TotalChangesDownloaded);
                Assert.Equal(1, s.TotalChangesUploaded);
                Assert.Equal(1, s.TotalSyncConflicts);
            }

            // check rows count on server and on each client
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                // get all product categories
                var serverPC = await ctx.ProductCategory.AsNoTracking().ToListAsync();

                foreach (var client in Clients)
                {
                    using (var cliCtx = new AdventureWorksContext(client, this.UseFallbackSchema))
                    {
                        // get all product categories
                        var clientPC = await cliCtx.ProductCategory.AsNoTracking().ToListAsync();

                        // check row count
                        Assert.Equal(serverPC.Count, clientPC.Count);

                        foreach (var cpc in clientPC)
                        {
                            var spc = serverPC.First(pc => pc.ProductCategoryId == cpc.ProductCategoryId);

                            // check column value
                            Assert.Equal(spc.ProductCategoryId, cpc.ProductCategoryId);
                            Assert.Equal(spc.Name, cpc.Name);
                            Assert.StartsWith("SRV", spc.Name);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Generate a conflict when inserting one row on server and the same row on each client
        /// Client should wins coz handler
        /// </summary>
        [Theory, TestPriority(13)]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_Update_Update_Resolved_ByMerge(SyncOptions options)
        {
            // create a server schema without seeding
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // configure server orchestrator
            this.Server.WebServerOrchestrator.Setup = new SyncSetup(Tables);

            // Conflict product category
            var conflictProductCategoryId = "BIKES";
            var productCategoryNameClient = "CLI BIKES " + HelperDatabase.GetRandomName();
            var productCategoryNameServer = "SRV BIKES " + HelperDatabase.GetRandomName();
            var productCategoryNameMerged = "BOTH BIKES";

            // Insert line on server
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                ctx.Add(new ProductCategory { ProductCategoryId = conflictProductCategoryId, Name = "BIKES" });
                await ctx.SaveChangesAsync();
            }

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, client.WebClientOrchestrator);
                agent.Options = options;

                var s = await agent.SynchronizeAsync();

                Assert.Equal(1, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
            }

            // Update each client to generate an update conflict
            foreach (var client in Clients)
            {
                using (var ctx = new AdventureWorksContext(client, this.UseFallbackSchema))
                {
                    var pc = ctx.ProductCategory.Find(conflictProductCategoryId);
                    pc.Name = productCategoryNameClient;
                    await ctx.SaveChangesAsync();
                }
            }

            // Update server
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                var pc = ctx.ProductCategory.Find(conflictProductCategoryId);
                pc.Name = productCategoryNameServer;
                await ctx.SaveChangesAsync();
            }

            var conflictIndex = 0;
            this.Server.WebServerOrchestrator.OnApplyChangesFailed(acf =>
            {
                // Check conflict is correctly set
                var localRow = acf.Conflict.LocalRow;
                var remoteRow = acf.Conflict.RemoteRow;

                // Merge row
                acf.Resolution = ConflictResolution.MergeRow;

                Assert.NotNull(acf.FinalRow);
                Assert.Equal(ConflictType.RemoteExistsLocalExists, acf.Conflict.Type);

                // On first apply, serveur product category is equal to SRV
                // then for the second client, the server
                // is set to BOTH because the previous conflict was resolved :)
                var srvStartString = conflictIndex == 0 ? "SRV" : "BOTH";
                Assert.StartsWith(srvStartString, localRow["Name"].ToString());
                Assert.StartsWith("CLI", remoteRow["Name"].ToString());

                acf.FinalRow["Name"] = productCategoryNameMerged;
                conflictIndex++;

            });



            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, client.WebClientOrchestrator);
                agent.Options = options;

                var s = await agent.SynchronizeAsync();

                Assert.Equal(1, s.TotalChangesDownloaded);
                Assert.Equal(1, s.TotalChangesUploaded);
                Assert.Equal(1, s.TotalSyncConflicts);

            }

            // check rows count on server and on each client
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                // get all product categories
                var serverPC = await ctx.ProductCategory.AsNoTracking().ToListAsync();

                foreach (var client in Clients)
                {
                    using (var cliCtx = new AdventureWorksContext(client, this.UseFallbackSchema))
                    {
                        // get all product categories
                        var clientPC = await cliCtx.ProductCategory.AsNoTracking().ToListAsync();

                        // check row count
                        Assert.Equal(serverPC.Count, clientPC.Count);

                        foreach (var cpc in clientPC)
                        {
                            var spc = serverPC.First(pc => pc.ProductCategoryId == cpc.ProductCategoryId);

                            // check column value
                            Assert.Equal(spc.ProductCategoryId, cpc.ProductCategoryId);
                            Assert.Equal(spc.Name, cpc.Name);
                            Assert.StartsWith("BOTH", spc.Name);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Insert thousand or rows. Check if batch mode works correctly
        /// </summary>
        [Theory, TestPriority(14)]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Insert_ThousandRows_FromClient(SyncOptions options)
        {
            // create a server schema without seeding
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // configure server orchestrator
            this.Server.WebServerOrchestrator.Setup = new SyncSetup(Tables);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, client.WebClientOrchestrator);
                agent.Options = options;
                await agent.SynchronizeAsync();
            }

            // Insert one thousand lines on each client
            foreach (var client in Clients)
            {
                using (var ctx = new AdventureWorksContext(client, this.UseFallbackSchema))
                {
                    for (var i = 0; i < 2000; i++)
                    {
                        var name = HelperDatabase.GetRandomName();
                        var productNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

                        var product = new Product { ProductId = Guid.NewGuid(), Name = name, ProductNumber = productNumber };

                        ctx.Product.Add(product);
                    }
                    await ctx.SaveChangesAsync();
                }
            }

            // Sync all clients
            // First client  will upload 2000 lines and will download nothing
            // Second client will upload 2000 lines and will download 2000 lines
            // Third client  will upload 2000 line and will download 4000 lines
            int download = 0;
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, client.WebClientOrchestrator);
                agent.Options = options;

                var s = await agent.SynchronizeAsync();

                Assert.Equal(download * 2000, s.TotalChangesDownloaded);
                Assert.Equal(2000, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);

                download++;
            }

        }

        /// <summary>
        /// Insert one row on each client, should be sync on server and clients
        /// </summary>
        [Theory, TestPriority(15)]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Reinitialize_Client(SyncOptions options)
        {
            // create a server schema with seeding
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // configure server orchestrator
            this.Server.WebServerOrchestrator.Setup = new SyncSetup(Tables);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, client.WebClientOrchestrator);
                agent.Options = options;

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
            }

            // Insert one line on each client
            foreach (var client in Clients)
            {
                var productCategoryName = HelperDatabase.GetRandomName();
                var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

                var productId = Guid.NewGuid();
                var productName = HelperDatabase.GetRandomName();
                var productNumber = productName.ToUpperInvariant().Substring(0, 10);

                using (var ctx = new AdventureWorksContext(client, this.UseFallbackSchema))
                {
                    var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                    ctx.Add(pc);
                    var product = new Product { ProductId = productId, Name = productName, ProductNumber = productNumber, ProductCategoryId = productCategoryId };
                    ctx.Add(product);
                    await ctx.SaveChangesAsync();
                }
            }

            // Sync all clients
            // inserted rows will be deleted 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, client.WebClientOrchestrator);
                agent.Options = options;

                var s = await agent.SynchronizeAsync(SyncType.Reinitialize);

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);

            }
        }

        /// <summary>
        /// Insert one row on each client, should be sync on server and clients
        /// </summary>
        [Theory, TestPriority(16)]
        [ClassData(typeof(SyncOptionsData))]
        public async Task ReinitializeWithUpload_Client(SyncOptions options)
        {
            // create a server schema with seeding
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // configure server orchestrator
            this.Server.WebServerOrchestrator.Setup = new SyncSetup(Tables);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, client.WebClientOrchestrator);
                agent.Options = options;

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
            }

            // Insert one line on each client
            foreach (var client in Clients)
            {
                var productCategoryName = HelperDatabase.GetRandomName();
                var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

                var productId = Guid.NewGuid();
                var productName = HelperDatabase.GetRandomName();
                var productNumber = productName.ToUpperInvariant().Substring(0, 10);

                using (var ctx = new AdventureWorksContext(client, this.UseFallbackSchema))
                {
                    var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                    ctx.Add(pc);
                    var product = new Product { ProductId = productId, Name = productName, ProductNumber = productNumber, ProductCategoryId = productCategoryId };
                    ctx.Add(product);
                    await ctx.SaveChangesAsync();
                }
            }

            // Sync all clients
            // client  will upload two lines and will download all + its two lines
            int download = 2;
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, client.WebClientOrchestrator);
                agent.Options = options;

                var s = await agent.SynchronizeAsync(SyncType.ReinitializeWithUpload);

                Assert.Equal(rowsCount + download, s.TotalChangesDownloaded);
                Assert.Equal(2, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
                download += 2;
            }


        }


        /// <summary>
        /// Insert one row on each client, should be sync on server and clients
        /// </summary>
        [Theory, TestPriority(17)]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Bad_Converter_NotRegisteredOnServer_ShouldRaiseError(SyncOptions options)
        {
            // create a server db and seed it
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // configure server orchestrator
            this.Server.WebServerOrchestrator.Setup = new SyncSetup(Tables);

            // Execute a sync on all clients and check results
            foreach (var client in this.Clients)
            {
                // Add a converter on the client.
                // But this converter is not register on the server side converters list.
                client.WebClientOrchestrator.Converter = new DateConverter();
                var agent = new SyncAgent(client.LocalOrchestrator, client.WebClientOrchestrator, null, options);

                var exception = await Assert.ThrowsAsync<SyncException>(async () =>
                {
                    var s = await agent.SynchronizeAsync();

                });

                Assert.Equal("HttpConverterNotConfiguredException", exception.TypeName);
            }
        }


        /// <summary>
        /// Check web interceptors are working correctly
        /// </summary>
        [Theory, TestPriority(18)]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Check_Interceptors_WebServerOrchestrator(SyncOptions options)
        {
            // create a server db and seed it
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // configure server orchestrator
            this.Server.WebServerOrchestrator.Setup = new SyncSetup(Tables);

            // Get response just before response with changes is send back from server
            this.Server.WebServerOrchestrator.OnSendingChanges(sra =>
            {
                var serializerFactory = this.Server.WebServerOrchestrator.Options.Serializers["json"];
                var serializer = serializerFactory.GetSerializer<HttpMessageSendChangesResponse>();

                using (var ms = new MemoryStream(sra.Content))
                {
                    var o = serializer.Deserialize(ms);

                    // check we have rows
                    Assert.True(o.Changes.HasRows);
                }
            });

            // Get response just before response with scope is send back from server
            this.Server.WebServerOrchestrator.OnSendingScopes(sra =>
            {
                var serializerFactory = this.Server.WebServerOrchestrator.Options.Serializers["json"];
                var serializer = serializerFactory.GetSerializer<HttpMessageEnsureScopesResponse>();

                using (var ms = new MemoryStream(sra.Content))
                {
                    var o = serializer.Deserialize(ms);

                    // check we have a schema
                    Assert.NotNull(o.Schema);
                }
            });


            // Execute a sync on all clients and check results
            foreach (var client in this.Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, client.WebClientOrchestrator, null, options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
            }
        }



        /// <summary>
        /// Check web interceptors are working correctly
        /// </summary>
        [Theory, TestPriority(19)]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Check_Interceptors_WebClientOrchestrator(SyncOptions options)
        {
            // create a server schema without seeding
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // configure server orchestrator
            this.Server.WebServerOrchestrator.Setup = new SyncSetup(Tables);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, client.WebClientOrchestrator);
                agent.Options = options;

                // Interceptor on sending scopes
                client.WebClientOrchestrator.OnSendingScopes(sra =>
                {
                    var serializerFactory = this.Server.WebServerOrchestrator.Options.Serializers["json"];
                    var serializer = serializerFactory.GetSerializer<HttpMessageEnsureScopesRequest>();

                    using (var ms = new MemoryStream(sra.Content))
                    {
                        var o = serializer.Deserialize(ms);

                        // check we a scope name
                        Assert.NotEmpty(o.ScopeName);
                    }
                });

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);

                client.WebClientOrchestrator.OnSendingScopes(null);
            }

            // Insert one line on each client
            foreach (var client in Clients)
            {
                var name = HelperDatabase.GetRandomName();
                var productNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

                var product = new Product { ProductId = Guid.NewGuid(), Name = name, ProductNumber = productNumber };

                using (var serverDbCtx = new AdventureWorksContext(client, this.UseFallbackSchema))
                {
                    serverDbCtx.Product.Add(product);
                    await serverDbCtx.SaveChangesAsync();
                }
            }

            // Sync all clients
            // First client  will upload one line and will download nothing
            // Second client will upload one line and will download one line
            // thrid client  will upload one line and will download two lines
            int download = 0;
            foreach (var client in Clients)
            {
                // Just before sending changes, get changes sent
                client.WebClientOrchestrator.OnSendingChanges(sra =>
                {
                    var serializerFactory = this.Server.WebServerOrchestrator.Options.Serializers["json"];
                    var serializer = serializerFactory.GetSerializer<HttpMessageSendChangesRequest>();

                    using (var ms = new MemoryStream(sra.Content))
                    {
                        var o = serializer.Deserialize(ms);

                        // check we have rows
                        Assert.True(o.Changes.HasRows);
                    }
                });

                var agent = new SyncAgent(client.LocalOrchestrator, client.WebClientOrchestrator);
                agent.Options = options;

                var s = await agent.SynchronizeAsync();

                Assert.Equal(download++, s.TotalChangesDownloaded);
                Assert.Equal(1, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);

                client.WebClientOrchestrator.OnSendingChanges(null);
            }

        }



        /// <summary>
        /// Insert one row on each client, should be sync on server and clients
        /// </summary>
        [Theory, TestPriority(20)]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Converter_Registered_ShouldConvertDateTime(SyncOptions options)
        {
            // create a server db and seed it
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // configure server orchestrator
            this.Server.WebServerOrchestrator.Setup = new SyncSetup(Tables);

            // Register converter on the server side
            this.Server.WebServerOrchestrator.Options.Converters.Add(new DateConverter());

            // Get response just before response sent back from server
            // Assert if datetime are correctly converted to long
            this.Server.WebServerOrchestrator.OnSendingChanges(sra =>
            {
                var serializerFactory = this.Server.WebServerOrchestrator.Options.Serializers["json"];
                var serializer = serializerFactory.GetSerializer<HttpMessageSendChangesResponse>();
                
                using (var ms = new MemoryStream(sra.Content))
                {
                    var o = serializer.Deserialize(ms);

                    // check we have rows
                    Assert.True(o.Changes.HasRows);

                    // getting a table where we know we have date time
                    var table = o.Changes.Tables.FirstOrDefault(t => t.TableName == "Employee");

                    Assert.NotNull(table);
                    Assert.NotEmpty(table.Rows);

                    foreach(var row in table.Rows)
                    {
                        var dateCell = row[6];

                        // check we have an integer here
                        Assert.IsType<long>(dateCell);
                    }
                   
                }
            });

            // Execute a sync on all clients and check results
            foreach (var client in this.Clients)
            {
                // Add a converter on the client.
                client.WebClientOrchestrator.Converter = new DateConverter();
                var agent = new SyncAgent(client.LocalOrchestrator, client.WebClientOrchestrator, null, options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
            }
        }


    }
}
