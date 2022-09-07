using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;

using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Misc;
using Dotmim.Sync.Tests.Models;
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
    public abstract partial class TcpFilterTests : IClassFixture<HelperProvider>, IDisposable
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

        /// <summary>
        /// For each test, Create a server database and some clients databases, depending on ProviderType provided in concrete class
        /// </summary>
        public TcpFilterTests(HelperProvider fixture, ITestOutputHelper output)
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
            var serverDatabaseName = HelperDatabase.GetRandomName("tcpfilt_sv_");

            // create remote orchestrator
            var serverProvider = this.CreateProvider(this.ServerType, serverDatabaseName);

            this.Server = (serverDatabaseName, this.ServerType, serverProvider);

            // Get all clients providers
            Clients = new List<(string DatabaseName, ProviderType ProviderType, CoreProvider Provider)>(this.ClientsType.Count);

            // Generate Client database
            foreach (var clientType in this.ClientsType)
            {
                var dbCliName = HelperDatabase.GetRandomName("tcpfilt_cli_");
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
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, Server.Provider);
                var s = await agent.SynchronizeAsync(this.FilterSetup, this.FilterParameters);

                Assert.Equal(0, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);

                var scopeInfo = await agent.LocalOrchestrator.GetScopeInfoAsync();

                // Check we have the correct columns replicated
                using var c = client.Provider.CreateConnection();

                await c.OpenAsync();

                foreach (var setupTable in FilterSetup.Tables)
                {
                    var syncTable = new SyncTable(setupTable.TableName, setupTable.SchemaName);

                    var (tableName, trackingTableName) = client.Provider.GetParsers(syncTable, FilterSetup);

                    var tableBuilder = client.Provider.GetTableBuilder(syncTable, tableName, trackingTableName, this.FilterSetup, SyncOptions.DefaultScopeName);

                    var clientColumns = await tableBuilder.GetColumnsAsync(c, null);

                    // Check we have the same columns count
                    if (setupTable.Columns.Count == 0)
                    {
                        using var serverConnection = this.Server.Provider.CreateConnection();

                        serverConnection.Open();

                        var tableServerManagerFactory = this.Server.Provider.GetTableBuilder(syncTable, tableName, trackingTableName, this.FilterSetup, SyncOptions.DefaultScopeName);
                        var serverColumns = await tableServerManagerFactory.GetColumnsAsync(serverConnection, null);

                        serverConnection.Close();

                        Assert.Equal(serverColumns.Count(), clientColumns.Count());

                        // Check we have the same columns names
                        foreach (var serverColumn in serverColumns)
                            Assert.Contains(clientColumns, (col) => col.ColumnName == serverColumn.ColumnName);
                    }
                    else
                    {
                        Assert.Equal(setupTable.Columns.Count, clientColumns.Count());

                        // Check we have the same columns names
                        foreach (var setupColumn in setupTable.Columns)
                            Assert.Contains(clientColumns, (col) => col.ColumnName == setupColumn);
                    }
                }
                c.Close();
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

            // Execute a sync on all clients and check results
            foreach (var client in this.Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                var s = await agent.SynchronizeAsync(this.FilterSetup, this.FilterParameters);

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));

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

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                var s = await agent.SynchronizeAsync(this.FilterSetup, this.FilterParameters);

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
                Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));
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
                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                var s = await agent.SynchronizeAsync(this.FilterSetup, this.FilterParameters);

                Assert.Equal(2, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                Assert.Equal(rowsCount + 2, this.GetServerDatabaseRowsCount(client));

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

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                var s = await agent.SynchronizeAsync(this.FilterSetup, this.FilterParameters);

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
                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                var s = await agent.SynchronizeAsync(this.FilterSetup, this.FilterParameters);

                //Assert.Equal(download, s.TotalChangesDownloaded);
                Assert.Equal(4, s.TotalChangesUploadedToServer);
                //Assert.Equal(0, s.TotalSyncConflicts);
                download += 4;
            }

            // Now sync again to be sure all clients have all lines
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                await agent.SynchronizeAsync(this.FilterSetup, this.FilterParameters);
            }

            rowsCount = this.GetServerDatabaseRowsCount(this.Server);
            foreach (var client in Clients)
                Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));
        }


        /// <summary>
        /// Insert four rows on each client, should be sync on server and clients
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Delete_TwoTables_FromClient(SyncOptions options)
        {
            // create a server schema and seed
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                var s = await agent.SynchronizeAsync(this.FilterSetup, this.FilterParameters);

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

            // Insert 4 lines on each client
            foreach (var client in Clients)
            {
                var soh = new SalesOrderHeader
                {
                    SalesOrderNumber = $"SO-99099",
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
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                var s = await agent.SynchronizeAsync(this.FilterSetup, this.FilterParameters);

                //Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(4, s.TotalChangesUploadedToServer);
                //Assert.Equal(0, s.TotalSyncConflicts);
            }

            // Now sync again to be sure all clients have all lines
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                await agent.SynchronizeAsync(this.FilterSetup, this.FilterParameters);
            }


            // Delete lines from client
            // Now sync again to be sure all clients have all lines
            foreach (var client in Clients)
            {
                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
                ctx.SalesOrderDetail.RemoveRange(ctx.SalesOrderDetail.ToList());
                ctx.SalesOrderHeader.RemoveRange(ctx.SalesOrderHeader.ToList());
                await ctx.SaveChangesAsync();
            }

            // now sync

            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                var s = await agent.SynchronizeAsync(this.FilterSetup, this.FilterParameters);

                //Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(8, s.TotalChangesUploadedToServer);
                //Assert.Equal(0, s.TotalSyncConflicts);
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

            // ----------------------------------
            // Setting correct options for sync agent to be able to reach snapshot
            // ----------------------------------
            var snapshotDirctory = HelperDatabase.GetRandomName();
            var directory = Path.Combine(Environment.CurrentDirectory, snapshotDirctory);
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

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var snapshotApplying = 0;
                var snapshotApplied = 0;

                agent.LocalOrchestrator.OnSnapshotApplying(saa => snapshotApplying++);
                agent.LocalOrchestrator.OnSnapshotApplied(saa => snapshotApplied++);
                var s = await agent.SynchronizeAsync(this.FilterSetup, this.FilterParameters);

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);

                Assert.Equal(1, snapshotApplying);
                Assert.Equal(1, snapshotApplied);
            }
        }

        /// <summary>
        /// Insert rows on server, and ensure DISTINCT is applied correctly 
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Insert_TwoTables_EnsureDistinct(SyncOptions options)
        {
            // create a server schema and seed
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                var s = await agent.SynchronizeAsync(this.FilterSetup, this.FilterParameters);

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

                var addressLine2 = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);
                var newAddress2 = new Address { AddressLine1 = addressLine2 };
                serverDbCtx.Address.Add(newAddress2);

                await serverDbCtx.SaveChangesAsync();

                var newCustomerAddress = new CustomerAddress
                {
                    AddressId = newAddress.AddressId,
                    CustomerId = AdventureWorksContext.CustomerId1ForFilter,
                    AddressType = "Secondary Home 1"
                };

                serverDbCtx.CustomerAddress.Add(newCustomerAddress);

                var newCustomerAddress2 = new CustomerAddress
                {
                    AddressId = newAddress2.AddressId,
                    CustomerId = AdventureWorksContext.CustomerId1ForFilter,
                    AddressType = "Secondary Home 2"
                };

                serverDbCtx.CustomerAddress.Add(newCustomerAddress2);

                await serverDbCtx.SaveChangesAsync();

                // Update customer
                var customer = serverDbCtx.Customer.Find(AdventureWorksContext.CustomerId1ForFilter);
                customer.FirstName = "Orlanda";

                await serverDbCtx.SaveChangesAsync();
            }

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                var s = await agent.SynchronizeAsync(this.FilterSetup, this.FilterParameters);

                Assert.Equal(5, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }
        }




        /// <summary>
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Using_ExistingClientDatabase_Filter_With_NotSyncedColumn(SyncOptions options)
        {

            if (this.Server.ProviderType != ProviderType.Sql)
                return;

            var clients = this.Clients.Where(c => c.ProviderType == ProviderType.Sql || c.ProviderType == ProviderType.Sqlite);

            var setup = new SyncSetup(new string[] { "Customer" });

            // Filter columns. We are not syncing EmployeeID, BUT this column will be part of the filter
            setup.Tables["Customer"].Columns.AddRange(new string[] { "CustomerID", "NameStyle", "FirstName", "LastName" });

            // create a server schema and seed
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases WITH schema, and WITHOUT seeding
            foreach (var client in clients)
            {
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);
                await this.EnsureDatabaseSchemaAndSeedAsync(client, false, UseFallbackSchema);
            }


            var filter = new SetupFilter("Customer");
            filter.AddParameter("EmployeeID", DbType.Int32, true);
            filter.AddCustomWhere("EmployeeID = @EmployeeID or @EmployeeID is null");

            setup.Filters.Add(filter);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                var p = new SyncParameters(("EmployeeID", 1));
                var s = await agent.SynchronizeAsync(setup, p);

                Assert.Equal(2, s.ChangesAppliedOnClient.TotalAppliedChanges);

            }
        }

        /// <summary>
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Migration_Adding_Table(SyncOptions options)
        {
            var setup = new SyncSetup(new string[] { "Customer" });

            // Filter columns. We are not syncing EmployeeID, BUT this column will be part of the filter
            setup.Tables["Customer"].Columns.AddRange(new string[] { "CustomerID", "EmployeeID", "NameStyle", "FirstName", "LastName" });
            setup.Filters.Add("Customer", "EmployeeID");

            // create a server schema and seed
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);


            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                var p = new SyncParameters(("EmployeeID", 1));

                var s = await agent.SynchronizeAsync(setup, p);

                Assert.Equal(2, s.ChangesAppliedOnClient.TotalAppliedChanges);
            }

            var remoteOrchestrator = new RemoteOrchestrator(Server.Provider);

            // Adding a new scope on the server with a new table
            var setupv2 = new SyncSetup(new string[] { "Customer", "Employee" });
            setupv2.Tables["Customer"].Columns.AddRange(new string[] { "CustomerID", "EmployeeID", "NameStyle", "FirstName", "LastName" });
            setupv2.Filters.Add("Customer", "EmployeeID");

            var sScopeInfo = await remoteOrchestrator.ProvisionAsync("v2", setupv2);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                
                var parameters = new SyncParameters(("EmployeeID", 1));
                // Create the table on local database
                var localOrchestrator = new LocalOrchestrator(client.Provider);
                await localOrchestrator.CreateTableAsync(sScopeInfo, "Employee");

                // Once created we can provision the new scope, thanks to the serverScope instance we already have
                await localOrchestrator.ProvisionAsync(sScopeInfo);

                var cScopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync("v2", parameters);

                // IF we launch synchronize on this new scope, it will get all the rows from the server
                // We are making a shadow copy of previous scope to get the last synchronization metadata
                var oldCScopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync(syncParameters: parameters);
                cScopeInfoClient.ShadowScope(oldCScopeInfoClient);
                await localOrchestrator.SaveScopeInfoClientAsync(cScopeInfoClient);


                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync("v2", SyncType.Reinitialize, parameters);

                Assert.Equal(5, s.ChangesAppliedOnClient.TotalAppliedChanges);
            }

        }

        /// <summary>
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Migration_Modifying_Table(SyncOptions options)
        {

            var setup = new SyncSetup(new string[] { "Customer" });

            // Filter columns. We are not syncing EmployeeID, BUT this column will be part of the filter
            setup.Tables["Customer"].Columns.AddRange(new string[] { "CustomerID", "EmployeeID", "NameStyle", "FirstName", "LastName" });

            // create a server schema and seed
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            setup.Filters.Add("Customer", "EmployeeID");

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                var p = new SyncParameters(("EmployeeID", 1));

                var s = await agent.SynchronizeAsync(setup, p);

                Assert.Equal(2, s.ChangesAppliedOnClient.TotalAppliedChanges);
            }


            // Adding a new scope on the server with a the same table plus one column
            var setupv2 = new SyncSetup(new string[] { "Customer" });
            // Adding a new column to Customer
            setupv2.Tables["Customer"].Columns.AddRange(new string[] { "CustomerID", "EmployeeID", "NameStyle", "FirstName", "LastName", "EmailAddress" });
            setupv2.Filters.Add("Customer", "EmployeeID");

            var remoteOrchestrator = new RemoteOrchestrator(Server.Provider);
            var serverScope = await remoteOrchestrator.ProvisionAsync("v2", setupv2);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                // Adding the column on client side
                var commandText = client.ProviderType switch
                {
                    ProviderType.Sql => @"ALTER TABLE Customer ADD EmailAddress nvarchar(250) NULL;",
                    ProviderType.Sqlite => @"ALTER TABLE Customer ADD EmailAddress text NULL;",
                    ProviderType.MySql => @"ALTER TABLE `Customer` ADD `EmailAddress` nvarchar(250) NULL;",
                    ProviderType.MariaDB => @"ALTER TABLE `Customer` ADD `EmailAddress` nvarchar(250) NULL;",
                    _ => throw new NotImplementedException()
                };

                var connection = client.Provider.CreateConnection();
                connection.Open();
                var command = connection.CreateCommand();
                command.CommandText = commandText;
                command.Connection = connection;
                await command.ExecuteNonQueryAsync();
                connection.Close();

                // Once created we can provision the new scope, thanks to the serverScope instance we already have
                var clientScopeV2 = await agent.LocalOrchestrator.ProvisionAsync(serverScope);

                if (Server.ProviderType == ProviderType.MySql || Server.ProviderType == ProviderType.MariaDB)
                {
                    agent.RemoteOrchestrator.OnConnectionOpen(coa =>
                    {
                        // tracking https://github.com/mysql-net/MySqlConnector/issues/924
                        MySqlConnection.ClearPool(coa.Connection as MySqlConnection);
                    });
                }

                if (client.ProviderType == ProviderType.MySql || client.ProviderType == ProviderType.MariaDB)
                {
                    agent.LocalOrchestrator.OnConnectionOpen(coa =>
                    {
                        // tracking https://github.com/mysql-net/MySqlConnector/issues/924
                        MySqlConnection.ClearPool(coa.Connection as MySqlConnection);
                    });
                }

                // create agent with filtered tables and parameter
                var p = new SyncParameters(("EmployeeID", 1));

                var s = await agent.SynchronizeAsync("v2", SyncType.Reinitialize, p);

                Assert.Equal(2, s.ChangesAppliedOnClient.TotalAppliedChanges);
            }

        }


        /// <summary>
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Migration_Removing_Table(SyncOptions options)
        {
            var setup = new SyncSetup(new string[] { "Customer", "Employee" });

            // Filter columns. We are not syncing EmployeeID, BUT this column will be part of the filter
            setup.Tables["Customer"].Columns.AddRange(new string[] { "CustomerID", "EmployeeID", "NameStyle", "FirstName", "LastName" });
            setup.Filters.Add("Customer", "EmployeeID");

            var parameters = new SyncParameters(("EmployeeID", 1));

            // create a server schema and seed
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);


            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                var s = await agent.SynchronizeAsync(setup, parameters);

                Assert.Equal(5, s.ChangesAppliedOnClient.TotalAppliedChanges);
            }


            // Adding a new scope on the server with a the same table plus one column
            var setupv2 = new SyncSetup(new string[] { "Employee" });
            var remoteOrchestrator = new RemoteOrchestrator(Server.Provider);
            var serverScopeV2 = await remoteOrchestrator.ProvisionAsync("v2", setupv2);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, Server.Provider, options);

                // Once created we can provision the new scope, thanks to the serverScope instance we already have
                await agent.LocalOrchestrator.ProvisionAsync(serverScopeV2);
                var cScopeInfoClientV2 = await agent.LocalOrchestrator.GetScopeInfoClientAsync("v2");

                // IF we launch synchronize on this new scope, it will get all the rows from the server
                // We are making a shadow copy of previous scope client to get the last synchronization metadata
                var oldCScopeInfoClient = await agent.LocalOrchestrator.GetScopeInfoClientAsync(syncParameters: parameters);
                cScopeInfoClientV2.ShadowScope(oldCScopeInfoClient);
                await agent.LocalOrchestrator.SaveScopeInfoClientAsync(cScopeInfoClientV2);

                // Deprovision first scope
                await agent.LocalOrchestrator.DeprovisionAsync(SyncProvision.StoredProcedures);

                var s = await agent.SynchronizeAsync("v2");

                Assert.Equal(0, s.ChangesAppliedOnClient.TotalAppliedChanges);
            }

        }


        /// <summary>
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Deprovision_Should_Remove_Filtered_StoredProcedures(SyncOptions options)
        {
            var setup = new SyncSetup(new string[] { "Customer", "Employee" });

            // Filter columns. We are not syncing EmployeeID, BUT this column will be part of the filter
            setup.Tables["Customer"].Columns.AddRange(new string[] { "CustomerID", "EmployeeID", "NameStyle", "FirstName", "LastName" });

            // create a server schema and seed
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            setup.Filters.Add("Customer", "EmployeeID");

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                var p = new SyncParameters(("EmployeeID", 1));

                var s = await agent.SynchronizeAsync(setup, p);

                Assert.Equal(5, s.ChangesAppliedOnClient.TotalAppliedChanges);

                var scopeInfo = await agent.LocalOrchestrator.GetScopeInfoAsync();

                await agent.LocalOrchestrator.DeprovisionAsync(SyncProvision.StoredProcedures | SyncProvision.Triggers | SyncProvision.ScopeInfo | SyncProvision.TrackingTable);

                foreach (var setupTable in setup.Tables)
                {
                    Assert.False(await agent.LocalOrchestrator.ExistTriggerAsync(scopeInfo, setupTable.TableName, setupTable.SchemaName, DbTriggerType.Delete));
                    Assert.False(await agent.LocalOrchestrator.ExistTriggerAsync(scopeInfo, setupTable.TableName, setupTable.SchemaName, DbTriggerType.Insert));
                    Assert.False(await agent.LocalOrchestrator.ExistTriggerAsync(scopeInfo, setupTable.TableName, setupTable.SchemaName, DbTriggerType.Update));

                    if (client.ProviderType == ProviderType.Sql)
                    {
                        Assert.False(await agent.LocalOrchestrator.ExistStoredProcedureAsync(scopeInfo, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.BulkDeleteRows));
                        Assert.False(await agent.LocalOrchestrator.ExistStoredProcedureAsync(scopeInfo, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.BulkTableType));
                        Assert.False(await agent.LocalOrchestrator.ExistStoredProcedureAsync(scopeInfo, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.BulkUpdateRows));
                        Assert.False(await agent.LocalOrchestrator.ExistStoredProcedureAsync(scopeInfo, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.DeleteMetadata));
                        Assert.False(await agent.LocalOrchestrator.ExistStoredProcedureAsync(scopeInfo, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.DeleteRow));
                        Assert.False(await agent.LocalOrchestrator.ExistStoredProcedureAsync(scopeInfo, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.Reset));
                        Assert.False(await agent.LocalOrchestrator.ExistStoredProcedureAsync(scopeInfo, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectChanges));
                        Assert.False(await agent.LocalOrchestrator.ExistStoredProcedureAsync(scopeInfo, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectInitializedChanges));
                        Assert.False(await agent.LocalOrchestrator.ExistStoredProcedureAsync(scopeInfo, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectRow));
                        Assert.False(await agent.LocalOrchestrator.ExistStoredProcedureAsync(scopeInfo, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.UpdateRow));
                        Assert.False(await agent.LocalOrchestrator.ExistStoredProcedureAsync(scopeInfo, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectChangesWithFilters));
                        Assert.False(await agent.LocalOrchestrator.ExistStoredProcedureAsync(scopeInfo, setupTable.TableName, setupTable.SchemaName, DbStoredProcedureType.SelectInitializedChangesWithFilters));
                    }
                }

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
            var options = new SyncOptions
            {
                SnapshotsDirectory = directory,
                BatchSize = 200
            };
            // ----------------------------------
            // Create a snapshot
            // ----------------------------------
            var remoteOrchestrator = new RemoteOrchestrator(Server.Provider, options);

            // getting snapshot directory names
            var (rootDirectory, nameDirectory)
                = await remoteOrchestrator.GetSnapshotDirectoryAsync(SyncOptions.DefaultScopeName, this.FilterParameters).ConfigureAwait(false);

            Assert.False(Directory.Exists(rootDirectory));
            Assert.False(Directory.Exists(Path.Combine(rootDirectory, nameDirectory)));

            var setup = new SyncSetup(new string[] { "Customer" });

            await remoteOrchestrator.CreateSnapshotAsync(setup, this.FilterParameters);

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

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                var s = await agent.SynchronizeAsync("v2", this.FilterSetup, this.FilterParameters);

                Assert.True(Directory.Exists(rootDirectory));
                Assert.True(Directory.Exists(Path.Combine(rootDirectory, nameDirectory)));
            }

        }


        /// <summary>
        /// Insert one row in two tables on server, should be correctly sync on all clients
        /// </summary>
        [Fact]
        public async Task Snapshot_Initialize_ThenClientUploadSync_ThenReinitialize()
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // snapshot directory
            var snapshotDirctory = HelperDatabase.GetRandomName();
            var directory = Path.Combine(Environment.CurrentDirectory, snapshotDirctory);

            var options = new SyncOptions
            {
                SnapshotsDirectory = directory,
                BatchSize = 200
            };

            var remoteOrchestrator = new RemoteOrchestrator(Server.Provider, options);

            await remoteOrchestrator.CreateSnapshotAsync(this.FilterSetup, this.FilterParameters);

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


                var addressLine1 = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

                var newAddress = new Address { AddressLine1 = addressLine1 };

                ctx.Address.Add(newAddress);
                await ctx.SaveChangesAsync();

                var newCustomerAddress = new CustomerAddress
                {
                    AddressId = newAddress.AddressId,
                    CustomerId = AdventureWorksContext.CustomerId1ForFilter,
                    AddressType = "OTH"
                };

                ctx.CustomerAddress.Add(newCustomerAddress);

                await ctx.SaveChangesAsync();
            }

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);


            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                var s = await agent.SynchronizeAsync(this.FilterSetup, this.FilterParameters);

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

            // ----------------------------------
            // Now add rows on client
            // ----------------------------------

            foreach (var client in Clients)
            {
                var name = HelperDatabase.GetRandomName();
                var pn = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

                var product = new Product { ProductId = Guid.NewGuid(), ProductCategoryId = "A_BIKES", Name = name, ProductNumber = pn };

                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
                ctx.Product.Add(product);
                await ctx.SaveChangesAsync();
            }

            // Sync all clients
            // First client  will upload one line and will download nothing
            // Second client will upload one line and will download one line
            // thrid client  will upload one line and will download two lines
            int download = 0;
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                var s = await agent.SynchronizeAsync(this.FilterSetup, this.FilterParameters);

                Assert.Equal(download++, s.TotalChangesDownloadedFromServer);
                Assert.Equal(1, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

            // Get count of rows
            rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // ----------------------------------
            // Now Reinitialize
            // ----------------------------------

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                var s = await agent.SynchronizeAsync(this.FilterSetup, SyncType.Reinitialize, this.FilterParameters);

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }
        }


        /// <summary>
        /// </summary>
        [Fact]
        public async Task Synchronize_ThenDeprovision_ThenAddPrefixes()
        {
            var options = new SyncOptions();
            var setup = new SyncSetup(new string[] { "Customer" });

            // Filtered columns. 
            setup.Tables["Customer"].Columns.AddRange(new string[] { "CustomerID", "EmployeeID", "NameStyle", "FirstName", "LastName" });

            // create a server schema and seed
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            setup.Filters.Add("Customer", "EmployeeID");

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                var p = new SyncParameters(("EmployeeID", 1));

                var s = await agent.SynchronizeAsync(setup, p);

                Assert.Equal(2, s.ChangesAppliedOnClient.TotalAppliedChanges);

            }
            foreach (var client in Clients)
            {
                // Deprovision everything
                var localOrchestrator = new LocalOrchestrator(client.Provider, options);
                var clientScope = await localOrchestrator.GetScopeInfoAsync();

                await localOrchestrator.DeprovisionAsync(SyncProvision.StoredProcedures
                    | SyncProvision.Triggers
                    | SyncProvision.TrackingTable);

                await localOrchestrator.DeleteScopeInfoAsync(clientScope);
            }

            var remoteOrchestrator = new RemoteOrchestrator(Server.Provider, options);
            var serverScope = await remoteOrchestrator.GetScopeInfoAsync();

            await remoteOrchestrator.DeprovisionAsync(SyncProvision.StoredProcedures
                | SyncProvision.Triggers
                | SyncProvision.TrackingTable);

            await remoteOrchestrator.DeleteScopeInfoAsync(serverScope);


            // Adding a new table
            setup.Tables.Add("Employee");

            // Adding prefixes
            setup.StoredProceduresPrefix = "sync";
            setup.StoredProceduresSuffix = "sp";
            setup.TrackingTablesPrefix = "track";
            setup.TrackingTablesSuffix = "tbl";
            setup.TriggersPrefix = "trg";
            setup.TriggersSuffix = "tbl";

            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                var p = new SyncParameters(("EmployeeID", 1));


                var s = await agent.SynchronizeAsync(setup, SyncType.Reinitialize, p);
                Assert.Equal(5, s.ChangesAppliedOnClient.TotalAppliedChanges);
            }


        }



        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public virtual async Task MultiFiltersParameters(SyncOptions options)
        {
            // create a server db and seed it
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // Get count of rows for parameter 2
            var rowsCount2 = this.GetServerDatabaseRowsCount(this.Server, AdventureWorksContext.CustomerId2ForFilter);

            // Execute a sync on all clients and check results
            foreach (var client in this.Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, Server.Provider, options);
                var s = await agent.SynchronizeAsync(this.FilterSetup, this.FilterParameters);

                Assert.Equal(rowsCount, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));

                // create agent with filtered tables and second parameter
                var parameters2 = new SyncParameters(("CustomerID", AdventureWorksContext.CustomerId2ForFilter));
                agent = new SyncAgent(client.Provider, Server.Provider, options);
                s = await agent.SynchronizeAsync(this.FilterSetup, parameters2);

                Assert.Equal(rowsCount2, s.TotalChangesDownloadedFromServer);
                Assert.Equal(0, s.TotalChangesUploadedToServer);
                Assert.Equal(rowsCount2, this.GetServerDatabaseRowsCount(client, AdventureWorksContext.CustomerId2ForFilter));


            }
        }

    }
}
