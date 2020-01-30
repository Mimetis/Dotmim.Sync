﻿using Dotmim.Sync.Builders;
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
        public abstract List<SyncParameter> FilterParameters { get; }

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
        public abstract int GetServerDatabaseRowsCount((string DatabaseName, ProviderType ProviderType, IOrchestrator Orchestrator) t);


        /// <summary>
        /// Create database, seed it, with or without schema
        /// </summary>
        /// <param name="t"></param>
        /// <param name="useSeeding"></param>
        /// <param name="useFallbackSchema"></param>
        public abstract Task EnsureDatabaseSchemaAndSeedAsync((string DatabaseName,
            ProviderType ProviderType, IOrchestrator Orchestrator) t, bool useSeeding = false, bool useFallbackSchema = false);


        // abstract fixture used to run the tests
        protected readonly HelperProvider fixture;

        // Current test running
        private ITest test;

        /// <summary>
        /// Gets the remote orchestrator and its database name
        /// </summary>
        public (string DatabaseName, ProviderType ProviderType, RemoteOrchestrator RemoteOrchestrator) Server { get; private set; }

        /// <summary>
        /// Gets the dictionary of all local orchestrators with database name as key
        /// </summary>
        public List<(string DatabaseName, ProviderType ProviderType, LocalOrchestrator LocalOrchestrator)> Clients { get; set; }

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
            var remoteOrchestrator = this.fixture.CreateOrchestrator<RemoteOrchestrator>(this.ServerType, serverDatabaseName);

            this.Server = (serverDatabaseName, this.ServerType, remoteOrchestrator);

            // Get all clients providers
            Clients = new List<(string DatabaseName, ProviderType ProviderType, LocalOrchestrator LocalOrhcestrator)>(this.ClientsType.Count);

            // Generate Client database
            foreach (var clientType in this.ClientsType)
            {
                var dbCliName = HelperDatabase.GetRandomName("tcpfilt_cli_");
                var localOrchestrator = this.fixture.CreateOrchestrator<LocalOrchestrator>(clientType, dbCliName);

                this.Clients.Add((dbCliName, clientType, localOrchestrator));
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
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator, this.FilterSetup);
                agent.Parameters.AddRange(this.FilterParameters);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);

                // Check we have the correct columns replicated
                using (var c = client.LocalOrchestrator.Provider.CreateConnection())
                {
                    await c.OpenAsync();

                    foreach (var setupTable in FilterSetup.Tables)
                    {
                        var tableClientManagerFactory = client.LocalOrchestrator.Provider.GetTableManagerFactory(setupTable.TableName, setupTable.SchemaName);
                        var tableClientManager = tableClientManagerFactory.CreateManagerTable(c);
                        var clientColumns = tableClientManager.GetColumns();

                        // Check we have the same columns count
                        if (setupTable.Columns.Count == 0)
                        {
                            using (var serverConnection = this.Server.RemoteOrchestrator.Provider.CreateConnection())
                            {
                                serverConnection.Open();
                                var tableServerManagerFactory = this.Server.RemoteOrchestrator.Provider.GetTableManagerFactory(setupTable.TableName, setupTable.SchemaName);
                                var tableServerManager = tableServerManagerFactory.CreateManagerTable(serverConnection);
                                var serverColumns = tableClientManager.GetColumns();

                                serverConnection.Close();

                                Assert.Equal(serverColumns.Count(), clientColumns.Count());

                                // Check we have the same columns names
                                foreach (var serverColumn in serverColumns)
                                    Assert.Contains(clientColumns, (col) => col.ColumnName == serverColumn.ColumnName);
                            }
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
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          this.FilterSetup, options);
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
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          this.FilterSetup, options);
                agent.Parameters.AddRange(this.FilterParameters);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
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
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          this.FilterSetup, options);
                agent.Parameters.AddRange(this.FilterParameters);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(2, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
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
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          this.FilterSetup, options);
                agent.Parameters.AddRange(this.FilterParameters);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
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
                using (var ctx = new AdventureWorksContext(client, this.UseFallbackSchema))
                {
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

            }

            // Sync all clients
            // First client  will upload 4 lines and will download nothing
            // Second client will upload 4 lines and will download 8 lines
            // thrid client  will upload 4 lines and will download 12 lines
            int download = 0;
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          this.FilterSetup, options);
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
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          this.FilterSetup, options);
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
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          this.FilterSetup, options);
                agent.Parameters.AddRange(this.FilterParameters);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
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

                using (var ctx = new AdventureWorksContext(client, this.UseFallbackSchema))
                {

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

            }
            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          this.FilterSetup, options);
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
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          this.FilterSetup, options);
                agent.Parameters.AddRange(this.FilterParameters);

                await agent.SynchronizeAsync();
            }


            // Delete lines from client
            // Now sync again to be sure all clients have all lines
            foreach (var client in Clients)
            {
                using (var ctx = new AdventureWorksContext(client, this.UseFallbackSchema))
                {
                    ctx.SalesOrderDetail.RemoveRange(ctx.SalesOrderDetail.Where(sod => sod.SalesOrder.SalesOrderNumber == "SO-99099"));
                    ctx.SalesOrderHeader.RemoveRange(ctx.SalesOrderHeader.Where(soh => soh.SalesOrderNumber == "SO-99099"));
                    await ctx.SaveChangesAsync();

                }
            }

            // now sync

            foreach (var client in Clients)
            {
                // create agent with filtered tables and parameter
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          this.FilterSetup, options);
                agent.Parameters.AddRange(this.FilterParameters);

                var s = await agent.SynchronizeAsync();

                //Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(4, s.TotalChangesUploaded);
                //Assert.Equal(0, s.TotalSyncConflicts);
            }

        }


    }
}
