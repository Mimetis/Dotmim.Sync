using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;

using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Misc;
using Dotmim.Sync.Tests.Models;
using Dotmim.Sync.Tests.Serializers;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Http;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
#if NET5_0 || NET6_0 || NETCOREAPP3_1
using MySqlConnector;
#elif NETCOREAPP2_1
using MySql.Data.MySqlClient;
#endif

using System;
using System.Collections.Generic;
using System.Data;
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
    //[TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]

    public abstract class HttpFilterTests : IClassFixture<HelperProvider>, IDisposable
    {
        private Stopwatch stopwatch;

        /// <summary>
        /// Gets the sync filtered tables involved in the tests
        /// </summary>
        public abstract SyncSetup FilterSetup { get; }

        /// <summary>
        /// Gets the filter parameter value
        /// </summary>
        public abstract SyncParameters FilterParameters { get; }

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
        public abstract int GetServerDatabaseRowsCount((string DatabaseName, ProviderType ProviderType, CoreProvider Provider) t, Guid? customerId = null);

        /// <summary>
        /// Create a provider
        /// </summary>
        public abstract CoreProvider CreateProvider(ProviderType providerType, string dbName);

        /// <summary>
        /// Create database, seed it, with or without schema
        /// </summary>
        /// <param name="t"></param>
        /// <param name="useSeeding"></param>
        /// <param name="useFallbackSchema"></param>
        public abstract Task EnsureDatabaseSchemaAndSeedAsync((string DatabaseName,
            ProviderType ProviderType, CoreProvider Provider) t, bool useSeeding = false, bool useFallbackSchema = false);


        // abstract fixture used to run the tests
        protected readonly HelperProvider fixture;

        // Current test running
        private ITest test;

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
        /// Create an empty database
        /// </summary>
        public abstract Task CreateDatabaseAsync(ProviderType providerType, string dbName, bool recreateDb = true);


        public ITestOutputHelper Output { get; }
        public KestrellTestServer Kestrell { get; set; }

        /// <summary>
        /// For each test, Create a server database and some clients databases, depending on ProviderType provided in concrete class
        /// </summary>
        public HttpFilterTests(HelperProvider fixture, ITestOutputHelper output)
        {

            // Getting the test running
            this.Output = output;
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
            var serverDatabaseName = HelperDatabase.GetRandomName("httpfilt_sv_");

            // create remote orchestrator
            var serverProvider = this.CreateProvider(this.ServerType, serverDatabaseName);

            // public property
            this.Server = (serverDatabaseName, this.ServerType, serverProvider);

            // Create a kestrell server
            this.Kestrell = new KestrellTestServer(this.UseFiddler);

            // Get all clients providers
            Clients = new List<(string DatabaseName, ProviderType ProviderType, CoreProvider Provider)>(this.ClientsType.Count);

            // Generate Client database
            foreach (var clientType in this.ClientsType)
            {
                var dbCliName = HelperDatabase.GetRandomName("httpfilt_cli_");
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
                HelperDatabase.DropDatabase(this.ServerType, Server.DatabaseName);

                foreach (var client in Clients)
                    HelperDatabase.DropDatabase(client.ProviderType, client.DatabaseName);

            }
            catch (Exception) { }

            this.stopwatch.Stop();

            var str = $"{test.TestCase.DisplayName} : {this.stopwatch.Elapsed.Minutes}:{this.stopwatch.Elapsed.Seconds}.{this.stopwatch.Elapsed.Milliseconds}";
            Console.WriteLine(str);
            Debug.WriteLine(str);

        }

        [Fact]
        public virtual async Task SchemaIsCreated()
        {
            // create a server db without seed
            await this.EnsureDatabaseSchemaAndSeedAsync(Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // configure server orchestrator
            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName, this.FilterSetup);
            var serviceUri = this.Kestrell.Run();

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebRemoteOrchestrator(serviceUri));

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
            }
        }

        [Theory]
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
            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName, this.FilterSetup);
            var serviceUri = this.Kestrell.Run();

            // Execute a sync on all clients and check results
            foreach (var client in this.Clients)
            {
                var agent = new SyncAgent(client.Provider, new WebRemoteOrchestrator(serviceUri), options);
                var s = await agent.SynchronizeAsync(this.FilterParameters);

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
            }
        }


        /// <summary>
        /// Insert two rows on server, should be correctly sync on all clients
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Insert_TwoTables_FromServer(SyncOptions options)
        {
            // create a server schema and seed
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // configure server orchestrator
            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName, this.FilterSetup);
            var serviceUri = this.Kestrell.Run();

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, new WebRemoteOrchestrator(serviceUri), options);
                var s = await agent.SynchronizeAsync(this.FilterParameters);

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

            // Create a new address & customer address on server
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                var addressLine1 = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

                var newAddress = new Address { AddressLine1 = addressLine1 };

                serverDbCtx.Address.Add(newAddress);
                await serverDbCtx.SaveChangesAsync();

                var newCustomerAddress = new CustomerAddress
                {
                    AddressId = newAddress.AddressId,
                    CustomerId = AdventureWorksContext.CustomerId1ForFilter,
                    AddressType = "OTH"
                };

                serverDbCtx.CustomerAddress.Add(newCustomerAddress);
                await serverDbCtx.SaveChangesAsync();
            }

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, new WebRemoteOrchestrator(serviceUri), options);
                var s = await agent.SynchronizeAsync(this.FilterParameters);

                Assert.Equal(2, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }
        }


        /// <summary>
        /// Insert four rows on each client, should be sync on server and clients
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Insert_TwoTables_FromClient(SyncOptions options)
        {
            // create a server schema and seed
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // configure server orchestrator
            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName, this.FilterSetup);
            var serviceUri = this.Kestrell.Run();

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, new WebRemoteOrchestrator(serviceUri), options);
                var s = await agent.SynchronizeAsync(this.FilterParameters);

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

            // Insert 4 lines on each client
            foreach (var client in Clients)
            {
                var soh = new SalesOrderHeader
                {
                    SalesOrderNumber = $"SO-99999",
                    RevisionNumber = 1,
                    Status = 5,
                    OnlineOrderFlag = true,
                    PurchaseOrderNumber = "PO348186287",
                    AccountNumber = "10-4020-000609",
                    CustomerId = AdventureWorksContext.CustomerId1ForFilter,
                    ShipToAddressId = 4,
                    BillToAddressId = 5,
                    ShipMethod = "CAR TRANSPORTATION",
                    SubTotal = 6530.35M,
                    TaxAmt = 70.4279M,
                    Freight = 22.0087M,
                    TotalDue = 6530.35M + 70.4279M + 22.0087M
                };
                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
                var productId = ctx.Product.First().ProductId;

                var sod1 = new SalesOrderDetail { OrderQty = 1, ProductId = productId, UnitPrice = 3578.2700M };
                var sod2 = new SalesOrderDetail { OrderQty = 2, ProductId = productId, UnitPrice = 44.5400M };
                var sod3 = new SalesOrderDetail { OrderQty = 2, ProductId = productId, UnitPrice = 1431.5000M };

                soh.SalesOrderDetail.Add(sod1);
                soh.SalesOrderDetail.Add(sod2);
                soh.SalesOrderDetail.Add(sod3);

                ctx.SalesOrderHeader.Add(soh);
                await ctx.SaveChangesAsync();

            }

            // Sync all clients
            // First client  will upload 4 lines and will download nothing
            // Second client will upload 4 lines and will download 8 lines
            // thrid client  will upload 4 lines and will download 12 lines
            int download = 0;
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, new WebRemoteOrchestrator(serviceUri), options);
                var s = await agent.SynchronizeAsync(this.FilterParameters);

                //Assert.Equal(download, s.TotalChangesDownloaded);
                Assert.Equal(4, s.TotalChangesUploadedToServer);
                //Assert.Equal(0, s.TotalSyncConflicts);
                download += 4;
            }

            // Now sync again to be sure all clients have all lines
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, new WebRemoteOrchestrator(serviceUri), options);
                await agent.SynchronizeAsync(this.FilterParameters);
            }
        }

        /// <summary>
        /// Insert one row in two tables on server, should be correctly sync on all clients
        /// </summary>
        [Fact]
        public async Task Snapshot_Initialize()
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // snapshot directory
            var snapshotDirctory = HelperDatabase.GetRandomName();
            var directory = Path.Combine(Environment.CurrentDirectory, snapshotDirctory);
            // ----------------------------------
            // Setting correct options for sync agent to be able to reach snapshot
            // ----------------------------------
            var options = new SyncOptions
            {
                SnapshotsDirectory = directory,
                BatchSize = 200
            };

            // ----------------------------------
            // Create a snapshot
            // ----------------------------------
            var remoteOrchestrator = new RemoteOrchestrator(Server.Provider, options);
            await remoteOrchestrator.CreateSnapshotAsync(this.FilterSetup, this.FilterParameters);


            // ----------------------------------
            // Add rows on server AFTER snapshot
            // ----------------------------------
            // Create a new address & customer address on server
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                var addressLine1 = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

                var newAddress = new Address { AddressLine1 = addressLine1 };

                serverDbCtx.Address.Add(newAddress);
                await serverDbCtx.SaveChangesAsync();

                var newCustomerAddress = new CustomerAddress
                {
                    AddressId = newAddress.AddressId,
                    CustomerId = AdventureWorksContext.CustomerId1ForFilter,
                    AddressType = "OTH"
                };

                serverDbCtx.CustomerAddress.Add(newCustomerAddress);
                await serverDbCtx.SaveChangesAsync();
            }

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // configure server orchestrator
            var serverOptions = new SyncOptions { SnapshotsDirectory = directory, BatchSize = 200 };
            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName, this.FilterSetup, serverOptions);

            var serviceUri = this.Kestrell.Run();

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, new WebRemoteOrchestrator(serviceUri), options);

                var snapshotApplying = 0;
                var snapshotApplied = 0;

                agent.LocalOrchestrator.OnSnapshotApplying(saa => snapshotApplying++);
                agent.LocalOrchestrator.OnSnapshotApplied(saa => snapshotApplied++);

                var s = await agent.SynchronizeAsync(this.FilterParameters);

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                Assert.Equal(1, snapshotApplying);
                Assert.Equal(1, snapshotApplied);
            }
        }

        /// <summary>
        /// Insert two rows on server, should be correctly sync on all clients
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task CustomSeriazlizer_MessagePack(SyncOptions options)
        {
            // create a server schema and seed
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // configure server orchestrator
            var webServerOptions = new WebServerOptions();
            webServerOptions.SerializerFactories.Add(new CustomMessagePackSerializerFactory());
            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName, this.FilterSetup, null, webServerOptions);
            var serviceUri = this.Kestrell.Run();

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter and serializer message pack
                var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri)
                {
                    SerializerFactory = new CustomMessagePackSerializerFactory()
                };

                var agent = new SyncAgent(client.Provider, webRemoteOrchestrator, options);
                var s = await agent.SynchronizeAsync(this.FilterParameters);

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

            // Create a new address & customer address on server
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                var addressLine1 = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

                var newAddress = new Address { AddressLine1 = addressLine1 };

                serverDbCtx.Address.Add(newAddress);
                await serverDbCtx.SaveChangesAsync();

                var newCustomerAddress = new CustomerAddress
                {
                    AddressId = newAddress.AddressId,
                    CustomerId = AdventureWorksContext.CustomerId1ForFilter,
                    AddressType = "OTH"
                };

                serverDbCtx.CustomerAddress.Add(newCustomerAddress);
                await serverDbCtx.SaveChangesAsync();
            }

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter and serializer message pack
                var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri);
                var agent = new SyncAgent(client.Provider, webRemoteOrchestrator, options);
                var s = await agent.SynchronizeAsync(this.FilterParameters);

                Assert.Equal(2, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }
        }

        /// <summary>
        /// Insert one row in two tables on server, should be correctly sync on all clients
        /// </summary>
        [Fact]
        public async Task Snapshot_ShouldNot_Delete_Folders()
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // ----------------------------------
            // Setting correct options for sync agent to be able to reach snapshot
            // ----------------------------------
            var snapshotDirctory = HelperDatabase.GetRandomName();
            var directory = Path.Combine(Environment.CurrentDirectory, snapshotDirctory);
            var serverOptions = new SyncOptions
            {
                SnapshotsDirectory = directory,
                BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "srv"),
                BatchSize = 200
            };

            var clientOptions = new SyncOptions
            {
                BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "cli"),
                BatchSize = 200
            };

            // ----------------------------------
            // Create a snapshot
            // ----------------------------------
            var remoteOrchestrator = new RemoteOrchestrator(Server.Provider, serverOptions);

            // getting snapshot directory names
            var (rootDirectory, nameDirectory) = await remoteOrchestrator.GetSnapshotDirectoryAsync(this.FilterParameters).ConfigureAwait(false);

            Assert.False(Directory.Exists(rootDirectory));
            Assert.False(Directory.Exists(Path.Combine(rootDirectory, nameDirectory)));

            await remoteOrchestrator.CreateSnapshotAsync(this.FilterSetup, this.FilterParameters);

            Assert.True(Directory.Exists(rootDirectory));
            Assert.True(Directory.Exists(Path.Combine(rootDirectory, nameDirectory)));


            // ----------------------------------
            // Add rows on server AFTER snapshot
            // ----------------------------------
            // Create a new address & customer address on server
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                var addressLine1 = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

                var newAddress = new Address { AddressLine1 = addressLine1 };

                serverDbCtx.Address.Add(newAddress);
                await serverDbCtx.SaveChangesAsync();

                var newCustomerAddress = new CustomerAddress
                {
                    AddressId = newAddress.AddressId,
                    CustomerId = AdventureWorksContext.CustomerId1ForFilter,
                    AddressType = "OTH"
                };

                serverDbCtx.CustomerAddress.Add(newCustomerAddress);
                await serverDbCtx.SaveChangesAsync();
            }

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // configure server orchestrator
            var remoteOptions = new SyncOptions
            {
                SnapshotsDirectory = serverOptions.SnapshotsDirectory,
                BatchSize = serverOptions.BatchSize,
                BatchDirectory = serverOptions.BatchDirectory
            };

            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName, 
                this.FilterSetup, remoteOptions);

            var serviceUri = this.Kestrell.Run();

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, new WebRemoteOrchestrator(serviceUri), clientOptions);
                var s = await agent.SynchronizeAsync(this.FilterParameters);

                Assert.True(Directory.Exists(rootDirectory));
                Assert.True(Directory.Exists(Path.Combine(rootDirectory, nameDirectory)));

                var serverFiles = Directory.GetFiles(serverOptions.BatchDirectory, "*", SearchOption.AllDirectories);
                var clientFiles = Directory.GetFiles(clientOptions.BatchDirectory, "*", SearchOption.AllDirectories);

                Assert.Empty(serverFiles);
                Assert.Empty(clientFiles);
            }
        }


        /// <summary>
        /// Insert one row in two tables on server, should be correctly sync on all clients
        /// </summary>
        [Fact]
        public async Task Snapshot_Initialize_With_CustomSeriazlizer_MessagePack()
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // snapshot directory
            var snapshotDirctory = HelperDatabase.GetRandomName();
            var directory = Path.Combine(Environment.CurrentDirectory, snapshotDirctory);
            // ----------------------------------
            // Setting correct options for sync agent to be able to reach snapshot
            // ----------------------------------
            var options = new SyncOptions
            {
                SnapshotsDirectory = directory,
                BatchSize = 200,
            };

            // ----------------------------------
            // Create a snapshot
            // ----------------------------------
            var remoteOrchestrator = new RemoteOrchestrator(Server.Provider, options);
            await remoteOrchestrator.CreateSnapshotAsync(this.FilterSetup, this.FilterParameters);


            // ----------------------------------
            // Add rows on server AFTER snapshot
            // ----------------------------------
            // Create a new address & customer address on server
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                var addressLine1 = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

                var newAddress = new Address { AddressLine1 = addressLine1 };

                serverDbCtx.Address.Add(newAddress);
                await serverDbCtx.SaveChangesAsync();

                var newCustomerAddress = new CustomerAddress
                {
                    AddressId = newAddress.AddressId,
                    CustomerId = AdventureWorksContext.CustomerId1ForFilter,
                    AddressType = "OTH"
                };

                serverDbCtx.CustomerAddress.Add(newCustomerAddress);
                await serverDbCtx.SaveChangesAsync();
            }

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // configure server orchestrator
            // configure server orchestrator

            // configure server orchestrator
            var remoteOptions = new SyncOptions
            {
                SnapshotsDirectory = directory,
                BatchSize = 100,
            };

            var webServerOptions = new WebServerOptions();
            webServerOptions.SerializerFactories.Add(new CustomMessagePackSerializerFactory());

            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName,
                this.FilterSetup, remoteOptions, webServerOptions);


            var serviceUri = this.Kestrell.Run();

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri);
                webRemoteOrchestrator.SerializerFactory = new CustomMessagePackSerializerFactory();

                var agent = new SyncAgent(client.Provider, webRemoteOrchestrator, options);

                var snapshotApplying = 0;
                var snapshotApplied = 0;

                agent.LocalOrchestrator.OnSnapshotApplying(saa => snapshotApplying++);
                agent.LocalOrchestrator.OnSnapshotApplied(saa => snapshotApplied++);

                var s = await agent.SynchronizeAsync(this.FilterParameters);

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                Assert.Equal(1, snapshotApplying);
                Assert.Equal(1, snapshotApplied);
            }
        }




        /// <summary>
        /// Insert two rows on server, should be correctly sync on all clients
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Deprovision_Then_ProvisionAgain(SyncOptions options)
        {
            // create a server schema and seed
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // configure server orchestrator
            this.Kestrell.AddSyncServer(this.Server.Provider.GetType(), this.Server.Provider.ConnectionString, SyncOptions.DefaultScopeName, this.FilterSetup);
            var serviceUri = this.Kestrell.Run();

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, new WebRemoteOrchestrator(serviceUri), options);
                var s = await agent.SynchronizeAsync(this.FilterParameters);

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

            // Create a new address & customer address on server
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                var addressLine1 = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

                var newAddress = new Address { AddressLine1 = addressLine1 };

                serverDbCtx.Address.Add(newAddress);
                await serverDbCtx.SaveChangesAsync();

                var newCustomerAddress = new CustomerAddress
                {
                    AddressId = newAddress.AddressId,
                    CustomerId = AdventureWorksContext.CustomerId1ForFilter,
                    AddressType = "OTH"
                };

                serverDbCtx.CustomerAddress.Add(newCustomerAddress);
                await serverDbCtx.SaveChangesAsync();
            }

            // Deprovision clients
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var localOrchestrator = new LocalOrchestrator(client.Provider);
                await localOrchestrator.DeprovisionAsync(SyncProvision.StoredProcedures | SyncProvision.Triggers);
            }

            // Make a new sync on all clients
            foreach (var client in Clients)
            {
                // Create local orchestrator
                var localOrchestrator = new LocalOrchestrator(client.Provider);
                // Create a remote orchestrator
                var remoteOrchestrator = new WebRemoteOrchestrator(serviceUri);

                // Get the scope from server
                var serverScope = await remoteOrchestrator.GetScopeInfoAsync();

                // Apply scope locally to recreate everything we need
                await localOrchestrator.ProvisionAsync(serverScope);

                var agent = new SyncAgent(client.Provider, remoteOrchestrator, options);
                var s = await agent.SynchronizeAsync(this.FilterParameters);

                Assert.Equal(2, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

        }

    }
}
