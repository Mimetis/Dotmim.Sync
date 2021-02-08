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
using MySql.Data.MySqlClient;
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
    [TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]

    public abstract class TcpFilterTests : IClassFixture<HelperProvider>, IDisposable
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
        public abstract int GetServerDatabaseRowsCount((string DatabaseName, ProviderType ProviderType, CoreProvider Provider) t);

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
            HelperDatabase.DropDatabase(this.ServerType, Server.DatabaseName);

            foreach (var client in Clients)
                HelperDatabase.DropDatabase(client.ProviderType, client.DatabaseName);

            this.stopwatch.Stop();

            var str = $"{test.TestCase.DisplayName} : {this.stopwatch.Elapsed.Minutes}:{this.stopwatch.Elapsed.Seconds}.{this.stopwatch.Elapsed.Milliseconds}";
            Console.WriteLine(str);
            Debug.WriteLine(str);

        }


        // TODO : Test with provision and deprovision and ensure everything is correctly created/ dropped


        [Fact, TestPriority(1)]
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
                var agent = new SyncAgent(client.Provider, Server.Provider, new SyncOptions(), this.FilterSetup);
                agent.Parameters.AddRange(this.FilterParameters);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);

                var schema = await agent.LocalOrchestrator.GetSchemaAsync();

                // Check we have the correct columns replicated
                using var c = client.Provider.CreateConnection();

                await c.OpenAsync();

                foreach (var setupTable in FilterSetup.Tables)
                {
                    var syncTable = new SyncTable(setupTable.TableName, setupTable.SchemaName);

                    var tableBuilder = client.Provider.GetTableBuilder(syncTable, this.FilterSetup);

                    var clientColumns = await tableBuilder.GetColumnsAsync(c, null);

                    // Check we have the same columns count
                    if (setupTable.Columns.Count == 0)
                    {
                        using var serverConnection = this.Server.Provider.CreateConnection();

                        serverConnection.Open();

                        var tableServerManagerFactory = this.Server.Provider.GetTableBuilder(syncTable, this.FilterSetup);
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

            // Execute a sync on all clients and check results
            foreach (var client in this.Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, Server.Provider, options, this.FilterSetup);
                agent.Parameters.AddRange(this.FilterParameters);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
            }
        }


        /// <summary>
        /// Insert two rows on server, should be correctly sync on all clients
        /// </summary>
        [Theory, TestPriority(3)]
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
                var agent = new SyncAgent(client.Provider, Server.Provider, options, this.FilterSetup);

                agent.Parameters.AddRange(this.FilterParameters);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
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
                    CustomerId = AdventureWorksContext.CustomerIdForFilter,
                    AddressType = "OTH"
                };

                serverDbCtx.CustomerAddress.Add(newCustomerAddress);
                await serverDbCtx.SaveChangesAsync();
            }

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, Server.Provider, options, this.FilterSetup);

                agent.Parameters.AddRange(this.FilterParameters);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(2, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }
        }


        /// <summary>
        /// Insert four rows on each client, should be sync on server and clients
        /// </summary>
        [Theory, TestPriority(4)]
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
                var agent = new SyncAgent(client.Provider, Server.Provider, options, this.FilterSetup);

                agent.Parameters.AddRange(this.FilterParameters);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
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
                    CustomerId = AdventureWorksContext.CustomerIdForFilter,
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
                var agent = new SyncAgent(client.Provider, Server.Provider, options, this.FilterSetup);

                agent.Parameters.AddRange(this.FilterParameters);

                var s = await agent.SynchronizeAsync();

                //Assert.Equal(download, s.TotalChangesDownloaded);
                Assert.Equal(4, s.TotalChangesUploaded);
                //Assert.Equal(0, s.TotalSyncConflicts);
                download += 4;
            }

            // Now sync again to be sure all clients have all lines
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, Server.Provider, options, this.FilterSetup);

                agent.Parameters.AddRange(this.FilterParameters);

                await agent.SynchronizeAsync();
            }
        }


        /// <summary>
        /// Insert four rows on each client, should be sync on server and clients
        /// </summary>
        [Theory, TestPriority(5)]
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
                var agent = new SyncAgent(client.Provider, Server.Provider, options, this.FilterSetup);

                agent.Parameters.AddRange(this.FilterParameters);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
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
                    CustomerId = AdventureWorksContext.CustomerIdForFilter,
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
                var agent = new SyncAgent(client.Provider, Server.Provider, options, this.FilterSetup);

                agent.Parameters.AddRange(this.FilterParameters);

                var s = await agent.SynchronizeAsync();

                //Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(4, s.TotalChangesUploaded);
                //Assert.Equal(0, s.TotalSyncConflicts);
            }

            // Now sync again to be sure all clients have all lines
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, Server.Provider, options, this.FilterSetup);

                agent.Parameters.AddRange(this.FilterParameters);

                await agent.SynchronizeAsync();
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
                var agent = new SyncAgent(client.Provider, Server.Provider, options, this.FilterSetup);

                agent.Parameters.AddRange(this.FilterParameters);

                var s = await agent.SynchronizeAsync();

                //Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(8, s.TotalChangesUploaded);
                //Assert.Equal(0, s.TotalSyncConflicts);
            }

        }

        /// <summary>
        /// Insert one row in two tables on server, should be correctly sync on all clients
        /// </summary>
        [Fact, TestPriority(6)]
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
            var remoteOrchestrator = new RemoteOrchestrator(Server.Provider, options, this.FilterSetup);
            await remoteOrchestrator.CreateSnapshotAsync(this.FilterParameters);

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
                    CustomerId = AdventureWorksContext.CustomerIdForFilter,
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
                var agent = new SyncAgent(client.Provider, Server.Provider, options, this.FilterSetup);

                var snapshotApplying = 0;
                var snapshotApplied = 0;

                agent.LocalOrchestrator.OnSnapshotApplying(saa => snapshotApplying++);
                agent.LocalOrchestrator.OnSnapshotApplied(saa => snapshotApplied++);

                agent.Parameters.AddRange(this.FilterParameters);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);

                Assert.Equal(1, snapshotApplying);
                Assert.Equal(1, snapshotApplied);
            }
        }

        /// <summary>
        /// Insert rows on server, and ensure DISTINCT is applied correctly 
        /// </summary>
        [Theory, TestPriority(7)]
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
                var agent = new SyncAgent(client.Provider, Server.Provider, options, this.FilterSetup);

                agent.Parameters.AddRange(this.FilterParameters);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
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
                    CustomerId = AdventureWorksContext.CustomerIdForFilter,
                    AddressType = "Secondary Home 1"
                };

                serverDbCtx.CustomerAddress.Add(newCustomerAddress);

                var newCustomerAddress2 = new CustomerAddress
                {
                    AddressId = newAddress2.AddressId,
                    CustomerId = AdventureWorksContext.CustomerIdForFilter,
                    AddressType = "Secondary Home 2"
                };

                serverDbCtx.CustomerAddress.Add(newCustomerAddress2);

                await serverDbCtx.SaveChangesAsync();

                // Update customer
                var customer = serverDbCtx.Customer.Find(AdventureWorksContext.CustomerIdForFilter);
                customer.FirstName = "Orlanda";

                await serverDbCtx.SaveChangesAsync();
            }

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, Server.Provider, options, this.FilterSetup);

                agent.Parameters.AddRange(this.FilterParameters);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(5, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }
        }


        /// <summary>
        /// </summary>
        [Theory, TestPriority(8)]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Using_ExistingClientDatabase_ProvisionDeprovision(SyncOptions options)
        {
            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                // create a client schema without seeding
                await this.EnsureDatabaseSchemaAndSeedAsync(client, false, UseFallbackSchema);

                var localOrchestrator = new LocalOrchestrator(client.Provider, options, this.FilterSetup);

                var provision = SyncProvision.ClientScope | SyncProvision.TrackingTable | SyncProvision.StoredProcedures | SyncProvision.Triggers;

                // just check interceptor
                var onTableCreatedCount = 0;
                localOrchestrator.OnTableCreated(args => onTableCreatedCount++);

                // Read client schema
                var schema = await localOrchestrator.GetSchemaAsync();

                // Provision the database with all tracking tables, stored procedures, triggers and scope
                await localOrchestrator.ProvisionAsync(schema, provision);

                //--------------------------
                // ASSERTION
                //--------------------------

                // check if scope table is correctly created
                var scopeInfoTableExists = await localOrchestrator.ExistScopeInfoTableAsync(DbScopeType.Client, options.ScopeInfoTableName);
                Assert.True(scopeInfoTableExists);

                // get the db manager
                foreach (var setupTable in this.FilterSetup.Tables)
                {
                    Assert.True(await localOrchestrator.ExistTrackingTableAsync(setupTable));

                    Assert.True(await localOrchestrator.ExistTriggerAsync(setupTable, DbTriggerType.Delete));
                    Assert.True(await localOrchestrator.ExistTriggerAsync(setupTable, DbTriggerType.Insert));
                    Assert.True(await localOrchestrator.ExistTriggerAsync(setupTable, DbTriggerType.Update));

                    if (client.ProviderType == ProviderType.Sql)
                    {
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(setupTable, DbStoredProcedureType.BulkDeleteRows));
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(setupTable, DbStoredProcedureType.BulkTableType));
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(setupTable, DbStoredProcedureType.BulkUpdateRows));
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(setupTable, DbStoredProcedureType.DeleteMetadata));
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(setupTable, DbStoredProcedureType.DeleteRow));
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(setupTable, DbStoredProcedureType.Reset));
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(setupTable, DbStoredProcedureType.SelectChanges));
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(setupTable, DbStoredProcedureType.SelectInitializedChanges));
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(setupTable, DbStoredProcedureType.SelectRow));
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(setupTable, DbStoredProcedureType.UpdateRow));

                        // Filters here
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(setupTable, DbStoredProcedureType.SelectChangesWithFilters));
                        Assert.True(await localOrchestrator.ExistStoredProcedureAsync(setupTable, DbStoredProcedureType.SelectInitializedChangesWithFilters));
                    }

                }

                //localOrchestrator.OnTableProvisioned(null);

                //// Deprovision the database with all tracking tables, stored procedures, triggers and scope
                await localOrchestrator.DeprovisionAsync(schema, provision);

                // check if scope table is correctly created
                scopeInfoTableExists = await localOrchestrator.ExistScopeInfoTableAsync(DbScopeType.Client, options.ScopeInfoTableName);
                Assert.False(scopeInfoTableExists);

                // get the db manager
                foreach (var setupTable in this.FilterSetup.Tables)
                {
                    Assert.False(await localOrchestrator.ExistTrackingTableAsync(setupTable));

                    Assert.False(await localOrchestrator.ExistTriggerAsync(setupTable, DbTriggerType.Delete));
                    Assert.False(await localOrchestrator.ExistTriggerAsync(setupTable, DbTriggerType.Insert));
                    Assert.False(await localOrchestrator.ExistTriggerAsync(setupTable, DbTriggerType.Update));


                    if (client.ProviderType == ProviderType.Sql)
                    {
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(setupTable, DbStoredProcedureType.BulkDeleteRows));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(setupTable, DbStoredProcedureType.BulkTableType));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(setupTable, DbStoredProcedureType.BulkUpdateRows));

                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(setupTable, DbStoredProcedureType.DeleteMetadata));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(setupTable, DbStoredProcedureType.DeleteRow));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(setupTable, DbStoredProcedureType.Reset));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(setupTable, DbStoredProcedureType.SelectChanges));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(setupTable, DbStoredProcedureType.SelectInitializedChanges));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(setupTable, DbStoredProcedureType.SelectRow));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(setupTable, DbStoredProcedureType.UpdateRow));

                        // check filters are deleted
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(setupTable, DbStoredProcedureType.SelectChangesWithFilters));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(setupTable, DbStoredProcedureType.SelectInitializedChangesWithFilters));
                    }

                }


            }
        }


        /// <summary>
        /// </summary>
        [Theory, TestPriority(9)]
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
                var agent = new SyncAgent(client.Provider, Server.Provider, options, setup);
                agent.Parameters.Add("EmployeeID", 1);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(2, s.ChangesAppliedOnClient.TotalAppliedChanges);

            }
        }

        /// <summary>
        /// </summary>
        [Theory, TestPriority(10)]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Migration_Adding_Table(SyncOptions options)
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
                var agent = new SyncAgent(client.Provider, Server.Provider, options, setup);
                agent.Parameters.Add("EmployeeID", 1);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(2, s.ChangesAppliedOnClient.TotalAppliedChanges);
            }

            // Adding a new table
            setup.Tables.Add("Employee");

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, Server.Provider, options, setup);
                agent.Parameters.Add("EmployeeID", 1);

                var s = await agent.SynchronizeAsync(SyncType.Reinitialize);

                Assert.Equal(5, s.ChangesAppliedOnClient.TotalAppliedChanges);
            }

        }

        /// <summary>
        /// </summary>
        [Theory, TestPriority(11)]
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
                var agent = new SyncAgent(client.Provider, Server.Provider, options, setup);
                agent.Parameters.Add("EmployeeID", 1);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(2, s.ChangesAppliedOnClient.TotalAppliedChanges);
            }

            // Adding a new column to Customer
            setup.Tables["Customer"].Columns.Add("EmailAddress");



            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options, setup);

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
                agent.Parameters.Add("EmployeeID", 1);

                var s = await agent.SynchronizeAsync(SyncType.Reinitialize);

                Assert.Equal(2, s.ChangesAppliedOnClient.TotalAppliedChanges);
            }

        }


        /// <summary>
        /// </summary>
        [Theory, TestPriority(12)]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Migration_Removing_Table(SyncOptions options)
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
                var agent = new SyncAgent(client.Provider, Server.Provider, options, setup);
                agent.Parameters.Add("EmployeeID", 1);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(5, s.ChangesAppliedOnClient.TotalAppliedChanges);
            }

            // Adding a new column to Customer
            setup.Tables.Remove(setup.Tables["Customer"]);
            setup.Filters.Clear();

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, Server.Provider, options, setup);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.ChangesAppliedOnClient.TotalAppliedChanges);
            }

        }


        /// <summary>
        /// </summary>
        [Theory, TestPriority(13)]
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
                var agent = new SyncAgent(client.Provider, Server.Provider, options, setup);
                agent.Parameters.Add("EmployeeID", 1);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(5, s.ChangesAppliedOnClient.TotalAppliedChanges);

                await agent.LocalOrchestrator.DeprovisionAsync(SyncProvision.StoredProcedures | SyncProvision.TrackingTable | SyncProvision.Triggers);

                foreach (var setupTable in setup.Tables)
                {
                    Assert.False(await agent.LocalOrchestrator.ExistTriggerAsync(setupTable, DbTriggerType.Delete));
                    Assert.False(await agent.LocalOrchestrator.ExistTriggerAsync(setupTable, DbTriggerType.Insert));
                    Assert.False(await agent.LocalOrchestrator.ExistTriggerAsync(setupTable, DbTriggerType.Update));

                    if (client.ProviderType == ProviderType.Sql)
                    {
                        Assert.False(await agent.LocalOrchestrator.ExistStoredProcedureAsync(setupTable, DbStoredProcedureType.BulkDeleteRows));
                        Assert.False(await agent.LocalOrchestrator.ExistStoredProcedureAsync(setupTable, DbStoredProcedureType.BulkTableType));
                        Assert.False(await agent.LocalOrchestrator.ExistStoredProcedureAsync(setupTable, DbStoredProcedureType.BulkUpdateRows));
                        Assert.False(await agent.LocalOrchestrator.ExistStoredProcedureAsync(setupTable, DbStoredProcedureType.DeleteMetadata));
                        Assert.False(await agent.LocalOrchestrator.ExistStoredProcedureAsync(setupTable, DbStoredProcedureType.DeleteRow));
                        Assert.False(await agent.LocalOrchestrator.ExistStoredProcedureAsync(setupTable, DbStoredProcedureType.Reset));
                        Assert.False(await agent.LocalOrchestrator.ExistStoredProcedureAsync(setupTable, DbStoredProcedureType.SelectChanges));
                        Assert.False(await agent.LocalOrchestrator.ExistStoredProcedureAsync(setupTable, DbStoredProcedureType.SelectInitializedChanges));
                        Assert.False(await agent.LocalOrchestrator.ExistStoredProcedureAsync(setupTable, DbStoredProcedureType.SelectRow));
                        Assert.False(await agent.LocalOrchestrator.ExistStoredProcedureAsync(setupTable, DbStoredProcedureType.UpdateRow));
                        Assert.False(await agent.LocalOrchestrator.ExistStoredProcedureAsync(setupTable, DbStoredProcedureType.SelectChangesWithFilters));
                        Assert.False(await agent.LocalOrchestrator.ExistStoredProcedureAsync(setupTable, DbStoredProcedureType.SelectInitializedChangesWithFilters));
                    }
                }

            }
        }



        /// <summary>
        /// </summary>
        [Theory, TestPriority(14)]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Migration_Rename_TrackingTable(SyncOptions options)
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
                var agent = new SyncAgent(client.Provider, Server.Provider, options, setup);
                agent.Parameters.Add("EmployeeID", 1);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(2, s.ChangesAppliedOnClient.TotalAppliedChanges);
            }

            // Modifying pref and sufix
            setup.StoredProceduresPrefix = "sp__";

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, Server.Provider, options, setup);
                agent.Parameters.Add("EmployeeID", 1);

                var s = await agent.SynchronizeAsync(SyncType.Reinitialize);

                Assert.Equal(2, s.ChangesAppliedOnClient.TotalAppliedChanges);
            }

        }


        /// <summary>
        /// </summary>
        [Theory, TestPriority(15)]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Migration_Adding_Table_AndReinitialize_TableOnly(SyncOptions options)
        {
            var setup = new SyncSetup(new string[] { "[Customer]" });

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
                var agent = new SyncAgent(client.Provider, Server.Provider, options, setup);
                agent.Parameters.Add("EmployeeID", 1);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(2, s.ChangesAppliedOnClient.TotalAppliedChanges);
            }

            // Adding a new table
            setup.Tables.Add("Employee");

            // Trying to Hack the Reinitialize Thing
            foreach (var client in Clients)
            {
                // create an agent 
                var agent = new SyncAgent(client.Provider, Server.Provider, options, setup);

                // ON SERVER : When trying to get changes from the server, just replace the command with the Initialize command
                // and get ALL the rows for the migrated new table
                agent.RemoteOrchestrator.OnTableChangesSelecting(async tcs =>
                {
                    if (tcs.Context.AdditionalProperties == null || tcs.Context.AdditionalProperties.Count <= 0)
                        return;

                    if (tcs.Context.AdditionalProperties.ContainsKey(tcs.Table.GetFullName()))
                    {
                        var addProp = tcs.Context.AdditionalProperties[tcs.Table.GetFullName()];
                        if (addProp == "Reinitialize")
                        {
                            var adapter = agent.RemoteOrchestrator.GetSyncAdapter(tcs.Table, setup);
                            var command = await adapter.GetCommandAsync(DbCommandType.SelectInitializedChanges, tcs.Connection, tcs.Transaction, tcs.Table.GetFilter());
                            tcs.Command = command;
                        }
                    }
                });

                // On client
                agent.LocalOrchestrator.OnMigrated(ma =>
                {
                    // migrateTables are empty if not migration tables has been done.
                    var migratedTables = ma.Migration.Tables;

                    foreach (var migratedTable in migratedTables)
                    {
                        var tableName = migratedTable.SetupTable.GetFullName();

                        if (migratedTable.Table == MigrationAction.Create)
                        {
                            if (ma.Context.AdditionalProperties == null)
                                ma.Context.AdditionalProperties = new Dictionary<string, string>();

                            ma.Context.AdditionalProperties.Add(tableName, "Reinitialize");
                        }
                    }
                });

                // ON CLIENT : Forcing Reset of the table to be sure no conflicts will be raised
                // And all rows will be re-applied 
                agent.LocalOrchestrator.OnTableChangesApplying(async tca =>
                {
                    if (tca.Context.AdditionalProperties == null || tca.Context.AdditionalProperties.Count <= 0)
                        return;

                    if (tca.State != DataRowState.Modified)
                        return;

                    if (tca.Context.AdditionalProperties.ContainsKey(tca.Table.GetFullName()))
                    {
                        var addProp = tca.Context.AdditionalProperties[tca.Table.GetFullName()];
                        if (addProp == "Reinitialize")
                        {
                            await agent.LocalOrchestrator.ResetTableAsync(setup.Tables[tca.Table.TableName, tca.Table.SchemaName], tca.Connection, tca.Transaction);
                        }
                    }
                });


                agent.Parameters.Add("EmployeeID", 1);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(3, s.ChangesAppliedOnClient.TotalAppliedChanges);


                s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);

            }



        }


        /// <summary>
        /// Insert one row in two tables on server, should be correctly sync on all clients
        /// </summary>
        [Fact, TestPriority(16)]
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
            var remoteOrchestrator = new RemoteOrchestrator(Server.Provider, options, this.FilterSetup);

            // getting snapshot directory names
            var (rootDirectory, nameDirectory) = await remoteOrchestrator.GetSnapshotDirectoryAsync(this.FilterParameters).ConfigureAwait(false);

            Assert.False(Directory.Exists(rootDirectory));
            Assert.False(Directory.Exists(Path.Combine(rootDirectory, nameDirectory)));

            await remoteOrchestrator.CreateSnapshotAsync(this.FilterParameters);

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
                    CustomerId = AdventureWorksContext.CustomerIdForFilter,
                    AddressType = "OTH"
                };

                serverDbCtx.CustomerAddress.Add(newCustomerAddress);
                await serverDbCtx.SaveChangesAsync();
            }

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.Provider, Server.Provider, options, this.FilterSetup);

                agent.Parameters.AddRange(this.FilterParameters);

                var s = await agent.SynchronizeAsync();

                Assert.True(Directory.Exists(rootDirectory));
                Assert.True(Directory.Exists(Path.Combine(rootDirectory, nameDirectory)));
            }

        }


        /// <summary>
        /// Insert one row in two tables on server, should be correctly sync on all clients
        /// </summary>
        [Fact, TestPriority(44)]
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

            var remoteOrchestrator = new RemoteOrchestrator(Server.Provider, options, this.FilterSetup);

            await remoteOrchestrator.CreateSnapshotAsync(this.FilterParameters);

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
                    CustomerId = AdventureWorksContext.CustomerIdForFilter,
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
                var agent = new SyncAgent(client.Provider, Server.Provider, options, this.FilterSetup);

                agent.Parameters.AddRange(this.FilterParameters);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

            // ----------------------------------
            // Now add rows on client
            // ----------------------------------

            foreach (var client in Clients)
            {
                var name = HelperDatabase.GetRandomName();
                var pn = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

                var product = new Product { ProductId = Guid.NewGuid(), ProductCategoryId = "BIKES", Name = name, ProductNumber = pn };

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
                var agent = new SyncAgent(client.Provider, Server.Provider, options, this.FilterSetup);

                agent.Parameters.AddRange(this.FilterParameters);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(download++, s.TotalChangesDownloaded);
                Assert.Equal(1, s.TotalChangesUploaded);
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
                var agent = new SyncAgent(client.Provider, Server.Provider, options, this.FilterSetup);

                agent.Parameters.AddRange(this.FilterParameters);


                var s = await agent.SynchronizeAsync(SyncType.Reinitialize);

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }
        }



    }
}
