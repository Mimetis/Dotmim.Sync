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
using Microsoft.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.AspNetCore.Hosting;
using System.Net.Http;
using Microsoft.AspNetCore.Builder;
using System.Net.Http.Headers;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.IdentityModel.Tokens;
using System.IdentityModel.Tokens.Jwt;

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
        /// Service Uri provided by kestrell when starts
        /// </summary>
        public string ServiceUri { get; private set; }

        /// <summary>
        /// Gets the Web Server Orchestrator used for the tests
        /// </summary>
        public WebServerOrchestrator WebServerOrchestrator { get; }

        /// <summary>
        /// Get the server rows count
        /// </summary>
        public abstract int GetServerDatabaseRowsCount((string DatabaseName, ProviderType ProviderType, CoreProvider Provider) t);

        /// <summary>
        /// Create a provider
        /// </summary>
        public abstract CoreProvider CreateProvider(ProviderType providerType, string dbName);

        /// <summary>
        /// Create database, seed it, with or without schema
        /// </summary>
        public abstract Task EnsureDatabaseSchemaAndSeedAsync((string DatabaseName,
            ProviderType ProviderType, CoreProvider Provider) t, bool useSeeding = false, bool useFallbackSchema = false);


        /// <summary>
        /// Create an empty database
        /// </summary>
        public abstract Task CreateDatabaseAsync(ProviderType providerType, string dbName, bool recreateDb = true);


        // abstract fixture used to run the tests
        protected readonly HelperProvider fixture;

        // Current test running
        private ITest test;
        private KestrellTestServer kestrell;


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

            var serverProvider = this.CreateProvider(this.ServerType, serverDatabaseName);

            // create web remote orchestrator
            this.WebServerOrchestrator = new WebServerOrchestrator(serverProvider, new SyncOptions(), new WebServerOptions(), new SyncSetup());

            // public property
            this.Server = (serverDatabaseName, this.ServerType, serverProvider);

            // Create a kestrell server
            this.kestrell = new KestrellTestServer(this.WebServerOrchestrator, this.UseFiddler);

            // start server and get uri
            this.ServiceUri = this.kestrell.Run();

            // Get all clients providers
            Clients = new List<(string, ProviderType, CoreProvider)>(this.ClientsType.Count);

            // Generate Client database
            foreach (var clientType in this.ClientsType)
            {
                var dbCliName = HelperDatabase.GetRandomName("http_cli_");
                var localProvider = this.CreateProvider(clientType, dbCliName);
                this.Clients.Add((dbCliName, clientType, localProvider));
            }

        }

        /// <summary>
        /// Drop all databases used for the tests
        /// </summary>
        public void Dispose()
        {
            //HelperDatabase.DropDatabase(this.ServerType, Server.DatabaseName);

            //foreach (var client in Clients)
            //{
            //    HelperDatabase.DropDatabase(client.ProviderType, client.DatabaseName);
            //}

            if (this.kestrell != null)
            {
                this.kestrell.Dispose();
            }


            this.stopwatch.Stop();

            var str = $"{test.TestCase.DisplayName} : {this.stopwatch.Elapsed.Minutes}:{this.stopwatch.Elapsed.Seconds}.{this.stopwatch.Elapsed.Milliseconds}";
            Console.WriteLine(str);
            Debug.WriteLine(str);

        }

        [Fact, TestPriority(0)]
        public virtual async Task KestrellAsync()
        {
            var hostBuilder = new WebHostBuilder()
                                .UseKestrel()
                                .UseUrls("http://127.0.0.1:0/");

            var hellWorldString = "Hello world";

            hostBuilder.Configure(app =>
            {
                app.Run(async context => await context.Response.WriteAsync(hellWorldString));

            });

            var host = hostBuilder.Build();
            host.Start();
            string serviceUrl = $"http://localhost:{host.GetPort()}/";

            var client = new HttpClient();
            var s = await client.GetAsync(serviceUrl);

            var hellWorldStringResult = await s.Content.ReadAsStringAsync();

            Assert.Equal(hellWorldString, hellWorldStringResult);

            
        }

        [Fact, TestPriority(1)]
        public virtual async Task SchemaIsCreated()
        {
            // create a server db without seed
            await this.EnsureDatabaseSchemaAndSeedAsync(Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // configure server orchestrator
            this.WebServerOrchestrator.Setup.Tables.AddRange(Tables);

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebClientOrchestrator(this.ServiceUri));

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
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // configure server orchestrator
            this.WebServerOrchestrator.Setup.Tables.AddRange(Tables);

            // Execute a sync on all clients and check results
            foreach (var client in this.Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebClientOrchestrator(this.ServiceUri), options);

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

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            var onReconnect = new Action<ReConnectArgs>(args =>
                 Console.WriteLine($"Can't connect to database {args.Connection?.Database}. Retry N°{args.Retry}. Waiting {args.WaitingTimeSpan.Milliseconds}. Exception:{args.HandledException.Message}."));

            // configure server orchestrator
            this.WebServerOrchestrator.Setup.Tables.AddRange(Tables);
            // change the remote orchestrator connection string
            this.WebServerOrchestrator.Provider.ConnectionString = $@"Server=unknown;Database=unknown;UID=sa;PWD=unknown";

            this.WebServerOrchestrator.OnReConnect(onReconnect);

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebClientOrchestrator(this.ServiceUri));


                agent.LocalOrchestrator.OnReConnect(onReconnect);

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
            await this.CreateDatabaseAsync(this.ServerType, this.Server.DatabaseName, true);

            // Create the table on the server
            await HelperDatabase.ExecuteScriptAsync(this.Server.ProviderType, this.Server.DatabaseName, tableTestCreationScript); ;

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // configure server orchestrator
            this.WebServerOrchestrator.Setup.Tables.Add("TableTest");

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebClientOrchestrator(this.ServiceUri));

                var se = await Assert.ThrowsAnyAsync<SyncException>(async () =>
                {
                    var s = await agent.SynchronizeAsync();
                });

                Assert.Equal(SyncSide.ServerSide, se.Side);
                Assert.Equal("MissingPrimaryKeyException", se.TypeName);
                Assert.Equal(this.Server.DatabaseName, se.InitialCatalog);

            }
        }

        [Fact, TestPriority(5)]
        public async Task Bad_ColumnSetup_DoesNotExistInSchema_ShouldRaiseError()
        {
            // create a server db without seed
            await this.EnsureDatabaseSchemaAndSeedAsync(Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // configure server orchestrator
            this.WebServerOrchestrator.Setup.Tables.AddRange(Tables);
            // Add a malformatted column name
            this.WebServerOrchestrator.Setup.Tables["Employee"].Columns.AddRange(new string[] { "EmployeeID", "FirstName", "LastNam" });

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebClientOrchestrator(this.ServiceUri));

                var se = await Assert.ThrowsAnyAsync<SyncException>(async () =>
                {
                    var s = await agent.SynchronizeAsync();
                });

                Assert.Equal(SyncSide.ServerSide, se.Side);
                Assert.Equal("MissingColumnException", se.TypeName);
            }
        }

        [Fact, TestPriority(6)]
        public async Task Bad_TableSetup_DoesNotExistInSchema_ShouldRaiseError()
        {
            // create a server db without seed
            await this.EnsureDatabaseSchemaAndSeedAsync(Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // configure server orchestrator
            this.WebServerOrchestrator.Setup.Tables.AddRange(Tables);
            // Add a fake table to setup tables
            this.WebServerOrchestrator.Setup.Tables.Add("WeirdTable");

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebClientOrchestrator(this.ServiceUri));

                var se = await Assert.ThrowsAnyAsync<SyncException>(async () =>
                {
                    var s = await agent.SynchronizeAsync();
                });

                Assert.Equal(SyncSide.ServerSide, se.Side);
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
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // configure server orchestrator
            this.WebServerOrchestrator.Setup.Tables.AddRange(Tables);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebClientOrchestrator(this.ServiceUri), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
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
                var agent = new SyncAgent(client.Provider, new WebClientOrchestrator(this.ServiceUri), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(1, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
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
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // configure server orchestrator
            this.WebServerOrchestrator.Setup.Tables.AddRange(Tables);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebClientOrchestrator(this.ServiceUri), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

            // Insert one line on each client
            foreach (var client in Clients)
            {
                var name = HelperDatabase.GetRandomName();
                var productNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

                var product = new Product { ProductId = Guid.NewGuid(), Name = name, ProductNumber = productNumber };

                using var serverDbCtx = new AdventureWorksContext(client, this.UseFallbackSchema);
                serverDbCtx.Product.Add(product);
                await serverDbCtx.SaveChangesAsync();
            }

            // Sync all clients
            // First client  will upload one line and will download nothing
            // Second client will upload one line and will download one line
            // thrid client  will upload one line and will download two lines
            int download = 0;
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebClientOrchestrator(this.ServiceUri), options);
                var s = await agent.SynchronizeAsync();

                Assert.Equal(download++, s.TotalChangesDownloaded);
                Assert.Equal(1, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
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
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // get rows count
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // configure server orchestrator
            this.WebServerOrchestrator.Setup.Tables.AddRange(Tables);

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
                var agent = new SyncAgent(client.Provider, new WebClientOrchestrator(this.ServiceUri), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);

                // check rows are create on client
                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
                var finalAddressesCount = await ctx.Address.AsNoTracking().CountAsync(a => a.AddressId == addressId);
                var finalEmployeeAddressesCount = await ctx.EmployeeAddress.AsNoTracking().CountAsync(a => a.AddressId == addressId && a.EmployeeId == employeeId);
                Assert.Equal(1, finalAddressesCount);
                Assert.Equal(1, finalEmployeeAddressesCount);


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
                var agent = new SyncAgent(client.Provider, new WebClientOrchestrator(this.ServiceUri), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(2, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);

                // check row deleted on client values
                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
                var finalAddressesCount = await ctx.Address.AsNoTracking().CountAsync(a => a.AddressId == addressId);
                var finalEmployeeAddressesCount = await ctx.EmployeeAddress.AsNoTracking().CountAsync(a => a.AddressId == addressId && a.EmployeeId == employeeId);
                Assert.Equal(0, finalAddressesCount);
                Assert.Equal(0, finalEmployeeAddressesCount);
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
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // configure server orchestrator
            this.WebServerOrchestrator.Setup.Tables.AddRange(Tables);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebClientOrchestrator(this.ServiceUri), options);
                await agent.SynchronizeAsync();
            }

            // Insert one thousand lines on each client
            foreach (var client in Clients)
            {
                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
                for (var i = 0; i < 2000; i++)
                {
                    var name = HelperDatabase.GetRandomName();
                    var productNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

                    var product = new Product { ProductId = Guid.NewGuid(), Name = name, ProductNumber = productNumber };

                    ctx.Product.Add(product);
                }
                await ctx.SaveChangesAsync();
            }

            // Sync all clients
            // First client  will upload 2000 lines and will download nothing
            // Second client will upload 2000 lines and will download 2000 lines
            // Third client  will upload 2000 line and will download 4000 lines
            int download = 0;
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebClientOrchestrator(this.ServiceUri), options);
                var s = await agent.SynchronizeAsync();

                Assert.Equal(download * 2000, s.TotalChangesDownloaded);
                Assert.Equal(2000, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);

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
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // configure server orchestrator
            this.WebServerOrchestrator.Setup.Tables.AddRange(Tables);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebClientOrchestrator(this.ServiceUri), options);
                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

            // Insert one line on each client
            foreach (var client in Clients)
            {
                var productCategoryName = HelperDatabase.GetRandomName();
                var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

                var productId = Guid.NewGuid();
                var productName = HelperDatabase.GetRandomName();
                var productNumber = productName.ToUpperInvariant().Substring(0, 10);

                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
                var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                ctx.Add(pc);
                var product = new Product { ProductId = productId, Name = productName, ProductNumber = productNumber, ProductCategoryId = productCategoryId };
                ctx.Add(product);
                await ctx.SaveChangesAsync();
            }

            // Sync all clients
            // inserted rows will be deleted 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebClientOrchestrator(this.ServiceUri), options);

                var s = await agent.SynchronizeAsync(SyncType.Reinitialize);

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);

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
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // configure server orchestrator
            this.WebServerOrchestrator.Setup.Tables.AddRange(Tables);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebClientOrchestrator(this.ServiceUri), options);
                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

            // Insert one line on each client
            foreach (var client in Clients)
            {
                var productCategoryName = HelperDatabase.GetRandomName();
                var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

                var productId = Guid.NewGuid();
                var productName = HelperDatabase.GetRandomName();
                var productNumber = productName.ToUpperInvariant().Substring(0, 10);

                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
                var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                ctx.Add(pc);
                var product = new Product { ProductId = productId, Name = productName, ProductNumber = productNumber, ProductCategoryId = productCategoryId };
                ctx.Add(product);
                await ctx.SaveChangesAsync();
            }

            // Sync all clients
            // client  will upload two lines and will download all + its two lines
            int download = 2;
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebClientOrchestrator(this.ServiceUri), options);
                var s = await agent.SynchronizeAsync(SyncType.ReinitializeWithUpload);

                Assert.Equal(rowsCount + download, s.TotalChangesDownloaded);
                Assert.Equal(2, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
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
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // configure server orchestrator
            this.WebServerOrchestrator.Setup.Tables.AddRange(Tables);

            // Execute a sync on all clients and check results
            foreach (var client in this.Clients)
            {
                // Add a converter on the client.
                // But this converter is not register on the server side converters list.
                var webClientProvider = new WebClientOrchestrator(this.ServiceUri, null, new DateConverter());

                var agent = new SyncAgent(client.Provider, webClientProvider, options);

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
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // configure server orchestrator
            this.WebServerOrchestrator.Setup.Tables.AddRange(Tables);

            // Get response just before response with changes is send back from server
            this.WebServerOrchestrator.OnSendingChanges(async sra =>
            {
                var byteArray = sra.Content;

                var serializerFactory = this.WebServerOrchestrator.WebServerOptions.Serializers["json"];
                var serializer = serializerFactory.GetSerializer<HttpMessageSendChangesResponse>();

                using var ms = new MemoryStream(sra.Content);
                var o = await serializer.DeserializeAsync(ms);

                // check we have rows
                Assert.True(o.Changes.HasRows);
            });

            // Get response just before response with scope is send back from server
            this.WebServerOrchestrator.OnSendingScopes(async sra =>
            {
                var serializerFactory = this.WebServerOrchestrator.WebServerOptions.Serializers["json"];
                var serializer = serializerFactory.GetSerializer<HttpMessageEnsureSchemaResponse>();

                if (sra.Content == null)
                    return;

                using var ms = new MemoryStream(sra.Content);
                var o = await serializer.DeserializeAsync(ms);

                // check we have a schema
                Assert.NotNull(o.ServerScopeInfo.Schema);
            });


            // Execute a sync on all clients and check results
            foreach (var client in this.Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebClientOrchestrator(this.ServiceUri), options);

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
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // configure server orchestrator
            this.WebServerOrchestrator.Setup.Tables.AddRange(Tables);

            foreach (var client in Clients)
            {
                var wenClientOrchestrator = new WebClientOrchestrator(this.ServiceUri);
                var agent = new SyncAgent(client.Provider, wenClientOrchestrator, options);

                // Interceptor on sending scopes
                wenClientOrchestrator.OnSendingScopes(async sra =>
                {
                    var serializerFactory = this.WebServerOrchestrator.WebServerOptions.Serializers["json"];
                    var serializer = serializerFactory.GetSerializer<HttpMessageEnsureScopesRequest>();

                    using var ms = new MemoryStream(sra.Content);
                    var o = await serializer.DeserializeAsync(ms);

                    // check we a scope name
                    Assert.NotNull(o.SyncContext);
                });

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);

                wenClientOrchestrator.OnSendingScopes(null);
            }

            // Insert one line on each client
            foreach (var client in Clients)
            {
                var name = HelperDatabase.GetRandomName();
                var productNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

                var product = new Product { ProductId = Guid.NewGuid(), Name = name, ProductNumber = productNumber };

                using var serverDbCtx = new AdventureWorksContext(client, this.UseFallbackSchema);
                serverDbCtx.Product.Add(product);
                await serverDbCtx.SaveChangesAsync();
            }

            // Sync all clients
            // First client  will upload one line and will download nothing
            // Second client will upload one line and will download one line
            // thrid client  will upload one line and will download two lines
            int download = 0;
            foreach (var client in Clients)
            {
                var webClientOrchestrator = new WebClientOrchestrator(this.ServiceUri);
                var agent = new SyncAgent(client.Provider, webClientOrchestrator, options);

                // Just before sending changes, get changes sent
                webClientOrchestrator.OnSendingChanges(async sra =>
                {
                    var serializerFactory = this.WebServerOrchestrator.WebServerOptions.Serializers["json"];
                    var serializer = serializerFactory.GetSerializer<HttpMessageSendChangesRequest>();

                    using var ms = new MemoryStream(sra.Content);
                    var o = await serializer.DeserializeAsync(ms);

                    // check we have rows
                    Assert.True(o.Changes.HasRows);
                });


                var s = await agent.SynchronizeAsync();

                Assert.Equal(download++, s.TotalChangesDownloaded);
                Assert.Equal(1, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);

                webClientOrchestrator.OnSendingChanges(null);
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
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // configure server orchestrator
            this.WebServerOrchestrator.Setup.Tables.AddRange(Tables);

            // Register converter on the server side
            this.WebServerOrchestrator.WebServerOptions.Converters.Add(new DateConverter());

            // Get response just before response sent back from server
            // Assert if datetime are correctly converted to long
            this.WebServerOrchestrator.OnSendingChanges(async sra =>
            {
                var serializerFactory = this.WebServerOrchestrator.WebServerOptions.Serializers["json"];
                var serializer = serializerFactory.GetSerializer<HttpMessageSendChangesResponse>();

                using var ms = new MemoryStream(sra.Content);
                var o = await serializer.DeserializeAsync(ms);

                // check we have rows
                Assert.True(o.Changes.HasRows);

                // getting a table where we know we have date time
                var table = o.Changes.Tables.FirstOrDefault(t => t.TableName == "Employee");

                if (table != null)
                {
                    Assert.NotEmpty(table.Rows);

                    foreach (var row in table.Rows)
                    {
                        var dateCell = row[5];

                        // check we have an integer here
                        Assert.IsType<long>(dateCell);
                    }
                }
            });

            // Execute a sync on all clients and check results
            foreach (var client in this.Clients)
            {
                var webClientOrchestrator = new WebClientOrchestrator(this.ServiceUri, null, new DateConverter());
                var agent = new SyncAgent(client.Provider, webClientOrchestrator, options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
            }
        }



        /// <summary>
        /// Insert one row in two tables on server, should be correctly sync on all clients
        /// </summary>
        [Theory, TestPriority(21)]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Snapshot_Initialize(SyncOptions options)
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // snapshot directory
            var snapshotDirctoryName = HelperDatabase.GetRandomName();
            var snapshotDirectory = Path.Combine(Environment.CurrentDirectory, snapshotDirctoryName);

            // configure server orchestrator
            this.WebServerOrchestrator.Setup.Tables.AddRange(Tables);
            this.WebServerOrchestrator.Options.SnapshotsDirectory = snapshotDirectory;
            this.WebServerOrchestrator.Options.BatchSize = 2000;

            // ----------------------------------
            // Create a snapshot
            // ----------------------------------
            await this.WebServerOrchestrator.CreateSnapshotAsync();

            // ----------------------------------
            // Add rows on server AFTER snapshot
            // ----------------------------------
            var productId = Guid.NewGuid();
            var productName = HelperDatabase.GetRandomName();
            var productNumber = productName.ToUpperInvariant().Substring(0, 10);

            var productCategoryName = HelperDatabase.GetRandomName();
            var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

            using (var ctx = new AdventureWorksContext(this.Server))
            {
                var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                ctx.Add(pc);

                var product = new Product { ProductId = productId, Name = productName, ProductNumber = productNumber };
                ctx.Add(product);

                await ctx.SaveChangesAsync();
            }

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebClientOrchestrator(this.ServiceUri), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }
        }

        /// <summary>
        /// Insert one row on each client, should be sync on server and clients
        /// </summary>
        [Theory, TestPriority(22)]
        [ClassData(typeof(SyncOptionsData))]
        public async Task IsOutdated_ShouldWork_If_Correct_Action(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // configure server orchestrator
            this.WebServerOrchestrator.Setup.Tables.AddRange(Tables);


            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebClientOrchestrator(this.ServiceUri), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

            // Call a server delete metadata to update the last valid timestamp value in scope_info_server table
            var dmc = await this.WebServerOrchestrator.DeleteMetadatasAsync();

            // Insert one line on each client
            foreach (var client in Clients)
            {
                // Client side : Create a product category and a product
                var productId = Guid.NewGuid();
                var productName = HelperDatabase.GetRandomName();
                var productNumber = productName.ToUpperInvariant().Substring(0, 10);
                var productCategoryName = HelperDatabase.GetRandomName();
                var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

                using (var ctx = new AdventureWorksContext(client))
                {
                    var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                    ctx.Add(pc);

                    var product = new Product { ProductId = productId, Name = productName, ProductNumber = productNumber };
                    ctx.Add(product);

                    await ctx.SaveChangesAsync();
                }

                // Generate an outdated situation
                await HelperDatabase.ExecuteScriptAsync(client.ProviderType, client.DatabaseName,
                                    $"Update scope_info set scope_last_server_sync_timestamp={dmc.TimestampLimit - 1}");

                // create a new agent
                var agent = new SyncAgent(client.Provider, new WebClientOrchestrator(this.ServiceUri), options);

                //// Making a first sync, will initialize everything we need
                //var se = await Assert.ThrowsAsync<SyncException>(() => agent.SynchronizeAsync());

                //Assert.Equal(SyncSide.ClientSide, se.Side);
                //Assert.Equal("OutOfDateException", se.TypeName);

                // Intercept outdated event, and make a reinitialize with upload action
                agent.LocalOrchestrator.OnOutdated(oa => oa.Action = OutdatedAction.ReinitializeWithUpload);

                var r = await agent.SynchronizeAsync();
                var c = GetServerDatabaseRowsCount(this.Server);
                Assert.Equal(c, r.TotalChangesDownloaded);
                Assert.Equal(2, r.TotalChangesUploaded);

            }


        }

        [Theory, TestPriority(23)]
        [ClassData(typeof(SyncOptionsData))]
        public virtual async Task Handling_DifferentScopeNames(SyncOptions options)
        {
            // create a server db and seed it
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // configure server orchestrator
            this.WebServerOrchestrator.Setup.Tables.AddRange(Tables);
            this.WebServerOrchestrator.ScopeName = "customScope1";

            // Execute a sync on all clients and check results
            foreach (var client in this.Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebClientOrchestrator(this.ServiceUri), options, "customScope1");

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
            }
        }



        /// <summary>
        /// Try to get changes from server without making a first sync
        /// </summary>
        [Theory, TestPriority(24)]
        [ClassData(typeof(SyncOptionsData))]
        public async Task GetChanges_BeforeServerIsInitialized(SyncOptions options)
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // configure server orchestrator
            this.WebServerOrchestrator.Setup.Tables.AddRange(Tables);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // ----------------------------------
            // Get changes
            // ----------------------------------
            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var webClientOrchestrator = new WebClientOrchestrator(this.ServiceUri);
                var agent = new SyncAgent(client.Provider, webClientOrchestrator, options);

                // Ensure scope is created locally
                var clientScope = await agent.LocalOrchestrator.GetClientScopeAsync();

                // get changes from server, without any changes sent from client side
                var changes = await webClientOrchestrator.GetChangesAsync(clientScope);

                Assert.Equal(rowsCount, changes.ServerChangesSelected.TotalChangesSelected);
            }
        }

        /// <summary>
        /// Try to get changes from server without making a first sync
        /// </summary>
        [Theory, TestPriority(25)]
        [ClassData(typeof(SyncOptionsData))]
        public async Task GetChanges_AfterServerIsInitialized(SyncOptions options)
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // configure server orchestrator
            this.WebServerOrchestrator.Setup.Tables.AddRange(Tables);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebClientOrchestrator(this.ServiceUri), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }


            // ----------------------------------
            // Add rows on server AFTER first sync (so everything should be initialized)
            // ----------------------------------
            var productId = Guid.NewGuid();
            var productName = HelperDatabase.GetRandomName();
            var productNumber = productName.ToUpperInvariant().Substring(0, 10);

            var productCategoryName = HelperDatabase.GetRandomName();
            var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

            using (var ctx = new AdventureWorksContext(this.Server))
            {
                var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                ctx.Add(pc);

                var product = new Product { ProductId = productId, Name = productName, ProductNumber = productNumber };
                ctx.Add(product);

                await ctx.SaveChangesAsync();
            }

            // ----------------------------------
            // Get changes
            // ----------------------------------
            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var webClientOrchestrator = new WebClientOrchestrator(this.ServiceUri);
                var agent = new SyncAgent(client.Provider, webClientOrchestrator, options);

                // Ensure scope is created locally
                var clientScope = await agent.LocalOrchestrator.GetClientScopeAsync();

                // get changes from server, without any changes sent from client side
                var changes = await webClientOrchestrator.GetChangesAsync(clientScope);

                Assert.Equal(2, changes.ServerChangesSelected.TotalChangesSelected);

            }
        }


        /// <summary>
        /// Try to get changes from server without making a first sync
        /// </summary>
        [Theory, TestPriority(26)]
        [ClassData(typeof(SyncOptionsData))]
        public async Task GetEstimatedChangesCount_BeforeServerIsInitialized(SyncOptions options)
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // configure server orchestrator
            this.WebServerOrchestrator.Setup.Tables.AddRange(Tables);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // ----------------------------------
            // Get changes
            // ----------------------------------
            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var webClientOrchestrator = new WebClientOrchestrator(this.ServiceUri);
                var agent = new SyncAgent(client.Provider, webClientOrchestrator, options);

                // Ensure scope is created locally
                var clientScope = await agent.LocalOrchestrator.GetClientScopeAsync();

                // get changes from server, without any changes sent from client side
                var changes = await webClientOrchestrator.GetEstimatedChangesCountAsync(clientScope);

                Assert.Equal(rowsCount, changes.ServerChangesSelected.TotalChangesSelected);
            }
        }

        /// <summary>
        /// Try to get changes from server without making a first sync
        /// </summary>
        [Theory, TestPriority(27)]
        [ClassData(typeof(SyncOptionsData))]
        public async Task GetEstimatedChangesCount_AfterServerIsInitialized(SyncOptions options)
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // configure server orchestrator
            this.WebServerOrchestrator.Setup.Tables.AddRange(Tables);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebClientOrchestrator(this.ServiceUri), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }


            // ----------------------------------
            // Add rows on server AFTER first sync (so everything should be initialized)
            // ----------------------------------
            var productId = Guid.NewGuid();
            var productName = HelperDatabase.GetRandomName();
            var productNumber = productName.ToUpperInvariant().Substring(0, 10);

            var productCategoryName = HelperDatabase.GetRandomName();
            var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

            using (var ctx = new AdventureWorksContext(this.Server))
            {
                var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                ctx.Add(pc);

                var product = new Product { ProductId = productId, Name = productName, ProductNumber = productNumber };
                ctx.Add(product);

                await ctx.SaveChangesAsync();
            }

            // ----------------------------------
            // Get changes
            // ----------------------------------
            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var webClientOrchestrator = new WebClientOrchestrator(this.ServiceUri);
                var agent = new SyncAgent(client.Provider, webClientOrchestrator, options);

                // Ensure scope is created locally
                var clientScope = await agent.LocalOrchestrator.GetClientScopeAsync();

                // get changes from server, without any changes sent from client side
                var changes = await webClientOrchestrator.GetEstimatedChangesCountAsync(clientScope);

                Assert.Equal(2, changes.ServerChangesSelected.TotalChangesSelected);

            }
        }


        [Theory, TestPriority(28)]
        [ClassData(typeof(SyncOptionsData))]
        public virtual async Task SimpleAuthentication(SyncOptions options)
        {
            // stop running kestrell for creating my own one
            await this.kestrell.StopAsync();

            // create a server db and seed it
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            var hostBuilder = new WebHostBuilder()
                .UseKestrel()
                .UseUrls("http://127.0.0.1:0/")
                .ConfigureServices(services =>
                {
                    services.AddMemoryCache();
                    services.AddDistributedMemoryCache();
                    services.AddSession(options =>
                    {
                        // Set a long timeout for easy testing.
                        options.IdleTimeout = TimeSpan.FromDays(10);
                        options.Cookie.HttpOnly = true;
                    });

                    JwtSecurityTokenHandler.DefaultInboundClaimTypeMap.Clear(); // => remove default claims

                    services.AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
                    .AddJwtBearer(options =>
                    {
                        options.RequireHttpsMetadata = false;
                        options.SaveToken = true;
                        options.TokenValidationParameters = new TokenValidationParameters
                        {
                            ValidateIssuer = true,
                            ValidateAudience = true,
                            ValidateLifetime = true,
                            ValidateIssuerSigningKey = true,

                            ValidIssuer = AuthToken.Issuer,
                            ValidAudience = AuthToken.Audience,

                            ClockSkew = TimeSpan.Zero,

                            IssuerSigningKey = new SymmetricSecurityKey(Encoding.UTF8.GetBytes(AuthToken.SecurityKey))
                        };

                        options.Events = new JwtBearerEvents
                        {
                            OnAuthenticationFailed = context =>
                            {
                                Console.WriteLine("OnAuthenticationFailed: " + context.Exception.Message);
                                return Task.CompletedTask;
                            },
                            OnTokenValidated = context =>
                            {
                                Console.WriteLine("OnTokenValidated: " + context.SecurityToken);
                                return Task.CompletedTask;
                            }
                        };
                    });


                    // configure server orchestrator
                    this.WebServerOrchestrator.Setup.Tables.AddRange(Tables);

                    // add a SqlSyncProvider acting as the server hub
                    services.AddSyncServer(this.WebServerOrchestrator);

                });


            hostBuilder.Configure(app =>
            {
                app.UseSession();
                app.UseAuthentication();

                app.Run(async context =>
                {

                    if (context.User.Identity.IsAuthenticated)
                    {
                        var dict = new Dictionary<string, string>();

                        context.User.Claims.ToList()
                           .ForEach(item => dict.Add(item.Type, item.Value));

                        var webServerManager = context.RequestServices.GetService(typeof(WebServerManager)) as WebServerManager;
                        await webServerManager.HandleRequestAsync(context);
                    }


                });
            });

            var host = hostBuilder.Build();
            host.Start();

            string serviceUrl = $"http://localhost:{host.GetPort()}/";

            // Execute a sync on all clients and check results
            foreach (var client in this.Clients)
            {
                Guid userId = Guid.NewGuid();

                var token = AuthToken.GenerateJwtToken("spertus@microsoft.com", userId.ToString());
                HttpClient httpClient = new HttpClient();
                httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);

                var webClientOrchestrator = new WebClientOrchestrator(serviceUrl, client:httpClient);

                var agent = new SyncAgent(client.Provider, webClientOrchestrator, options);
                
                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
            }
        }
    }
}
