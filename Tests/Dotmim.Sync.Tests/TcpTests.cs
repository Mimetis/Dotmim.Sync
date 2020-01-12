using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.SqlServer.Manager;
using Dotmim.Sync.Tests.Core;
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
    public abstract class TcpTests : IClassFixture<HelperProvider>, IDisposable
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
        public abstract int GetServerDatabaseRowsCount((string DatabaseName, ProviderType ProviderType, IOrchestrator Orchestrator) t);


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

        public ITestOutputHelper Output { get; }

        /// <summary>
        /// For each test, Create a server database and some clients databases, depending on ProviderType provided in concrete class
        /// </summary>
        public TcpTests(HelperProvider fixture, ITestOutputHelper output)
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
            var serverDatabaseName = HelperDatabase.GetRandomName("tcp_sv_");

            // create remote orchestrator
            var remoteOrchestrator = this.fixture.CreateOrchestrator<RemoteOrchestrator>(this.ServerType, serverDatabaseName);

            this.Server = (serverDatabaseName, this.ServerType, remoteOrchestrator);

            // Get all clients providers
            Clients = new List<(string DatabaseName, ProviderType ProviderType, LocalOrchestrator LocalOrhcestrator)>(this.ClientsType.Count);

            // Generate Client database
            foreach (var clientType in this.ClientsType)
            {
                var dbCliName = HelperDatabase.GetRandomName("tcp_cli_");
                var localOrchestrator = this.fixture.CreateOrchestrator<LocalOrchestrator>(clientType, dbCliName);

                HelperDatabase.CreateDatabaseAsync(clientType, dbCliName, true);

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
            {
                HelperDatabase.DropDatabase(client.ProviderType, client.DatabaseName);
            }

            this.stopwatch.Stop();

            var str = $"{test.TestCase.DisplayName} : {this.stopwatch.Elapsed.Minutes}:{this.stopwatch.Elapsed.Seconds}.{this.stopwatch.Elapsed.Milliseconds}";
            Console.WriteLine(str);
            Debug.WriteLine(str);

        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public virtual async Task RowsCount(SyncOptions options)
        {
            // create a server db and seed it
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // Execute a sync on all clients and check results
            foreach (var client in this.Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(this.Tables), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
            }
        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public virtual async Task SchemaIsCreated(SyncOptions options)
        {
            // create a server db without seed
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);

                // Check we have the correct columns replicated
                using (var clientConnection = client.LocalOrchestrator.Provider.CreateConnection())
                {
                    await clientConnection.OpenAsync();

                    foreach (var setupTable in agent.Setup.Tables)
                    {
                        var tableClientManagerFactory = client.LocalOrchestrator.Provider.GetTableManagerFactory(setupTable.TableName, setupTable.SchemaName);
                        var tableClientManager = tableClientManagerFactory.CreateManagerTable(clientConnection);
                        var clientColumns = tableClientManager.GetColumns();

                        using (var serverConnection = this.Server.RemoteOrchestrator.Provider.CreateConnection())
                        {
                            serverConnection.Open();

                            var tableServerManagerFactory = this.Server.RemoteOrchestrator.Provider.GetTableManagerFactory(setupTable.TableName, setupTable.SchemaName);
                            var tableServerManager = tableServerManagerFactory.CreateManagerTable(serverConnection);
                            var serverColumns = tableServerManager.GetColumns();

                            serverConnection.Close();

                            var serverColumnsCount = serverColumns.Count();
                            var clientColumnsCount = clientColumns.Count();

                            Assert.Equal(serverColumnsCount, clientColumnsCount);

                            // Check we have the same columns names
                            foreach (var serverColumn in serverColumns)
                            {
                                var clientColumn = clientColumns.FirstOrDefault(c => c.ColumnName == serverColumn.ColumnName);
                                
                                Assert.NotNull(clientColumn);

                                if (this.ServerType == client.ProviderType && this.ServerType == ProviderType.Sql)
                                {
                                    Assert.Equal(serverColumn.DataType, clientColumn.DataType);
                                    Assert.Equal(serverColumn.IsUnicode, clientColumn.IsUnicode);
                                    Assert.Equal(serverColumn.IsUnsigned, clientColumn.IsUnsigned);

                                    var maxPrecision = Math.Min(SqlDbMetadata.PRECISION_MAX, serverColumn.Precision);
                                    var maxScale = Math.Min(SqlDbMetadata.SCALE_MAX, serverColumn.Scale);

                                    // dont assert max length since numeric reset this value
                                    //Assert.Equal(serverColumn.MaxLength, clientColumn.MaxLength);

                                    Assert.Equal(maxPrecision, clientColumn.Precision);
                                    Assert.Equal(serverColumn.PrecisionSpecified, clientColumn.PrecisionSpecified);
                                    Assert.Equal(maxScale, clientColumn.Scale);
                                    Assert.Equal(serverColumn.ScaleSpecified, clientColumn.ScaleSpecified);

                                    Assert.Equal(serverColumn.DefaultValue, clientColumn.DefaultValue);
                                    Assert.Equal(serverColumn.OriginalDbType, clientColumn.OriginalDbType);
                                    
                                    // We don't replicate unique indexes
                                    //Assert.Equal(serverColumn.IsUnique, clientColumn.IsUnique);
                                    
                                    Assert.Equal(serverColumn.AutoIncrementSeed, clientColumn.AutoIncrementSeed);
                                    Assert.Equal(serverColumn.AutoIncrementStep, clientColumn.AutoIncrementStep);
                                    Assert.Equal(serverColumn.IsAutoIncrement, clientColumn.IsAutoIncrement);
                                    
                                    //Assert.Equal(serverColumn.OriginalTypeName, clientColumn.OriginalTypeName);
                                    
                                    //Assert.Equal(serverColumn.IsCompute, clientColumn.IsCompute);
                                    
                                    Assert.Equal(serverColumn.IsReadOnly, clientColumn.IsReadOnly);
                                    Assert.Equal(serverColumn.DbType, clientColumn.DbType);
                                    Assert.Equal(serverColumn.Ordinal, clientColumn.Ordinal);
                                    Assert.Equal(serverColumn.AllowDBNull, clientColumn.AllowDBNull);
                                }

                                Assert.Equal(serverColumn.ColumnName, clientColumn.ColumnName);

                            }
                        }
                    }
                    clientConnection.Close();

                }

            }
        }

        /// <summary>
        /// Check a bad connection should raise correct error
        /// </summary>
        [Fact]
        public async Task Bad_ConnectionFromServer_ShouldRaiseError()
        {
            // change the remote orchestrator connection string
            Server.RemoteOrchestrator.Provider.ConnectionString = $@"Server=unknown;Database=unknown;UID=sa;PWD=unknown";

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator, new SyncSetup(Tables));

                var se = await Assert.ThrowsAnyAsync<SyncException>(async () =>
                {
                    var s = await agent.SynchronizeAsync();
                });
            }
        }

        /// <summary>
        /// Check a bad connection should raise correct error
        /// </summary>
        [Fact]
        public async Task Bad_ConnectionFromClient_ShouldRaiseError()
        {
            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                // change the local orchestrator connection string
                // Set a connection string that will faile everywhere (event Sqlite)
                client.LocalOrchestrator.Provider.ConnectionString = $@"Data Source=D;";

                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator, new SyncSetup(Tables));

                var se = await Assert.ThrowsAnyAsync<SyncException>(async () =>
                {
                    var s = await agent.SynchronizeAsync();
                });
            }
        }


        [Fact]
        public async Task Bad_TableWithoutPrimaryKeys_ShouldRaiseError()
        {
            string tableTestCreationScript = "Create Table TableTest (TestId int, TestName varchar(50))";

            // Create an empty server database
            await HelperDatabase.CreateDatabaseAsync(this.ServerType, this.Server.DatabaseName, true);

            // Create the table on the server
            await HelperDatabase.ExecuteScriptAsync(this.Server.ProviderType, this.Server.DatabaseName, tableTestCreationScript); ;

            // Create setup
            var setup = new SyncSetup(new string[] { "TableTest" });

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator, setup);


                var se = await Assert.ThrowsAnyAsync<SyncException>(async () =>
                {
                    var s = await agent.SynchronizeAsync();
                });

                Assert.Equal(SyncExceptionSide.ServerSide, se.Side);
                Assert.Equal("MissingPrimaryKeyException", se.TypeName);
                Assert.Equal(this.Server.DatabaseName, se.InitialCatalog);

            }
        }

        [Fact]
        public async Task Bad_ColumnSetup_DoesNotExistInSchema_ShouldRaiseError()
        {
            // create a server db without seed
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // Create setup
            var setup = new SyncSetup(Tables);

            // Add a malformatted column name
            setup.Tables["Employee"].Columns.AddRange(new string[] { "EmployeeID", "FirstName", "LastNam" });

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator, setup);

                var se = await Assert.ThrowsAnyAsync<SyncException>(async () =>
                {
                    var s = await agent.SynchronizeAsync();
                });

                Assert.Equal(SyncExceptionSide.ServerSide, se.Side);
                Assert.Equal("MissingColumnException", se.TypeName);
            }
        }

        [Fact]
        public async Task Bad_TableSetup_DoesNotExistInSchema_ShouldRaiseError()
        {
            // create a server db without seed
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // Add a fake table to setup tables
            var setup = new SyncSetup(this.Tables);
            setup.Tables.Add("WeirdTable");

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator, setup);

                var se = await Assert.ThrowsAnyAsync<SyncException>(async () =>
                {
                    var s = await agent.SynchronizeAsync();
                });

                Assert.Equal(SyncExceptionSide.ServerSide, se.Side);
                Assert.Equal("MissingTableException", se.TypeName);
            }
        }

        /// <summary>
        /// Check interceptors are correctly called
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public virtual async Task Check_Interceptors(SyncOptions options)
        {
            // create a server schema without seeding
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // replicate schema with a no rows sync
            foreach (var client in this.Clients)
                await new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator, Tables).SynchronizeAsync();

            var productId = Guid.NewGuid();
            var productName = HelperDatabase.GetRandomName();
            var productNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

            var productCategoryName = HelperDatabase.GetRandomName();
            var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

            // insert 2 rows
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                serverDbCtx.Add(pc);

                var product = new Product { ProductId = productId, Name = productName, ProductNumber = productNumber };
                serverDbCtx.Add(product);

                await serverDbCtx.SaveChangesAsync();
            }

            int productDownloads = 1;
            int productCategoryDownloads = 1;

            foreach (var client in this.Clients)
            {
                var clientProductCategoryName = HelperDatabase.GetRandomName();
                var clientProductCategoryId = clientProductCategoryName.ToUpperInvariant().Substring(0, 6);

                var clientProductId = Guid.NewGuid();
                var clientProductName = HelperDatabase.GetRandomName();
                var clientProductNumber = clientProductName.ToUpperInvariant().Substring(0, 10);

                using (var ctx = new AdventureWorksContext(client, this.UseFallbackSchema))
                {
                    var pc = new ProductCategory { ProductCategoryId = clientProductCategoryId, Name = clientProductCategoryName };
                    ctx.Add(pc);
                    var product = new Product { ProductId = clientProductId, Name = clientProductName, ProductNumber = clientProductNumber, ProductCategoryId = clientProductCategoryId };
                    ctx.Add(product);

                    await ctx.SaveChangesAsync();
                }

                string sessionString = "";

                var interceptors = new Interceptors();
                interceptors.OnSessionBegin(sba => sessionString += "begin");
                interceptors.OnSessionEnd(sea => sessionString += "end");
                interceptors.OnTableChangesApplying(args =>
                {
                    if (args.SchemaTable.TableName == "ProductCategory")
                        Assert.Equal(DataRowState.Modified, args.State);

                    if (args.SchemaTable.TableName == "Product")
                        Assert.Equal(DataRowState.Modified, args.State);
                });
                interceptors.OnTableChangesApplied(args =>
                {
                    if (args.TableChangesApplied.Table.TableName == "ProductCategory")
                    {
                        Assert.Equal(DataRowState.Modified, args.TableChangesApplied.State);
                        Assert.Equal(productCategoryDownloads, args.TableChangesApplied.Applied);
                    }

                    if (args.TableChangesApplied.Table.TableName == "Product")
                    {
                        Assert.Equal(DataRowState.Modified, args.TableChangesApplied.State);
                        Assert.Equal(productDownloads, args.TableChangesApplied.Applied);
                    }
                });

                interceptors.OnTableChangesSelected(args =>
                {
                    if (args.TableChangesSelected.TableName == "ProductCategory")
                        Assert.Equal(1, args.TableChangesSelected.Upserts);

                    if (args.TableChangesSelected.TableName == "Product")
                        Assert.Equal(1, args.TableChangesSelected.Upserts);
                });

                interceptors.OnSchema(args =>
                {
                    Assert.True(args.Schema.HasTables);
                });

                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                              new SyncSetup(Tables), options);

                agent.SetInterceptors(interceptors);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(productDownloads + productCategoryDownloads, s.TotalChangesDownloaded);
                Assert.Equal(2, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);

                productDownloads++;
                productCategoryDownloads++;

                Assert.Equal("beginend", sessionString);

                agent.SetInterceptors(null);

            }
        }

        /// <summary>
        /// Insert one row on server, should be correctly sync on all clients
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Insert_OneTable_FromServer(SyncOptions options)
        {
            // create a server schema without seeding
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

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
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(1, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
            }
        }

        /// <summary>
        /// Insert one row on server, should be correctly sync on all clients
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Insert_OneTable_ThenDelete_FromServer(SyncOptions options)
        {
            // create a server schema without seeding
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
            }

            // Create a new product on server
            var name = HelperDatabase.GetRandomName().ToLowerInvariant();
            var productNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);
            var productId = Guid.NewGuid();

            var product = new Product { ProductId = productId, Name = name, ProductNumber = productNumber };

            // Add Product
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                serverDbCtx.Product.Add(product);
                await serverDbCtx.SaveChangesAsync();
            }

            // Then delete it
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                var pc = await serverDbCtx.Product.SingleAsync(o => o.ProductId == productId);
                serverDbCtx.Remove(pc);
                await serverDbCtx.SaveChangesAsync();
            }

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(1, s.TotalSyncConflicts);
            }
        }

        /// <summary>
        /// Insert one row on server, should be correctly sync on all clients
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Insert_OneTable_ThenUpdate_FromServer(SyncOptions options)
        {
            // create a server schema without seeding
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
            }

            // Create a new product on server
            var name = HelperDatabase.GetRandomName().ToLowerInvariant();
            var nameUpdated = HelperDatabase.GetRandomName().ToLowerInvariant();
            var productNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);
            var productId = Guid.NewGuid();

            var product = new Product { ProductId = productId, Name = name, ProductNumber = productNumber };

            // Add Product
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                serverDbCtx.Product.Add(product);
                await serverDbCtx.SaveChangesAsync();
            }

            // Then Update it
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                var pc = await serverDbCtx.Product.SingleAsync(o => o.ProductId == productId);
                pc.Name = nameUpdated;
                await serverDbCtx.SaveChangesAsync();
            }

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(1, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
            }
        }

        /// <summary>
        /// Insert one row on each client, should be sync on server and clients
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Insert_OneTable_FromClient(SyncOptions options)
        {
            // create a server schema without seeding
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

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
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(download++, s.TotalChangesDownloaded);
                Assert.Equal(1, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
            }

        }

        /// <summary>
        /// Insert one row in two tables on server, should be correctly sync on all clients
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Insert_TwoTables_FromServer(SyncOptions options)
        {
            // create a server schema without seeding
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
            }

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

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(2, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
            }
        }

        /// <summary>
        /// Insert one row on each client, should be sync on server and clients
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Insert_TwoTables_FromClient(SyncOptions options)
        {
            // create a server schema without seeding
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
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
            // First client  will upload two lines and will download nothing
            // Second client will upload two lines and will download two lines
            // thrid client  will upload two lines and will download four lines
            int download = 0;
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(download, s.TotalChangesDownloaded);
                Assert.Equal(2, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
                download += 2;
            }

            // Now sync again to be sure all clients have all lines
            foreach (var client in Clients)
            {
                await new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                    new SyncSetup(Tables), options).SynchronizeAsync();
            }

            // check rows count on server and on each client
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                var productRowCount = await ctx.Product.AsNoTracking().CountAsync();
                var productCategoryCount = await ctx.ProductCategory.AsNoTracking().CountAsync();
                foreach (var client in Clients)
                {
                    using (var cliCtx = new AdventureWorksContext(client, this.UseFallbackSchema))
                    {
                        var pCount = await cliCtx.Product.AsNoTracking().CountAsync();
                        Assert.Equal(productRowCount, pCount);

                        var pcCount = await cliCtx.ProductCategory.AsNoTracking().CountAsync();
                        Assert.Equal(productCategoryCount, pcCount);
                    }
                }
            }
        }

        /// <summary>
        /// Update one row on server, should be correctly sync on all clients
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Update_OneTable_FromServer(SyncOptions options)
        {
            // Generate a random city name
            var cityName = HelperDatabase.GetRandomName("City");
            var addressLine = HelperDatabase.GetRandomName("Address");

            // create a server schema with seeding
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // get rows count
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
            }

            // Update one address on server
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                var address = await serverDbCtx.Address.SingleAsync(a => a.AddressId == 1);

                // Update at least two properties
                address.City = cityName;
                address.AddressLine1 = addressLine;

                await serverDbCtx.SaveChangesAsync();
            }

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(1, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);

                // check row updated values
                using (var ctx = new AdventureWorksContext(client, this.UseFallbackSchema))
                {
                    var cliAddress = await ctx.Address.AsNoTracking().SingleAsync(a => a.AddressId == 1);
                    Assert.Equal(cityName, cliAddress.City);
                    Assert.Equal(addressLine, cliAddress.AddressLine1);
                }
            }
        }

        /// <summary>
        /// Update one row on client, should be correctly sync on server then all clients
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Update_OneTable_FromClient(SyncOptions options)
        {
            // create a server schema with seeding
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // get rows count
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
            }

            // Update one address on each client
            // To avoid conflicts, each client will update differents lines
            // each address id is generated from the foreach index
            var addressId = 1;
            foreach (var client in Clients)
            {
                using (var ctx = new AdventureWorksContext(client, this.UseFallbackSchema))
                {
                    var address = await ctx.Address.SingleAsync(a => a.AddressId == addressId);

                    // Update at least two properties
                    address.City = HelperDatabase.GetRandomName("City");
                    address.AddressLine1 = HelperDatabase.GetRandomName("Address");

                    await ctx.SaveChangesAsync();
                }
                addressId++;
            }
            // Execute a sync on all clients and check results
            // Each client will download the "upload from previous client"
            int download = 0;
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(download++, s.TotalChangesDownloaded);
                Assert.Equal(1, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
            }

            // Now sync again to be sure all clients have all lines
            foreach (var client in Clients)
            {
                await new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                    new SyncSetup(Tables), options).SynchronizeAsync();
            }

            // check rows count on server and on each client
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                // get all addresses
                var serverAddresses = await ctx.Address.AsNoTracking().ToListAsync();

                foreach (var client in Clients)
                {
                    using (var cliCtx = new AdventureWorksContext(client, this.UseFallbackSchema))
                    {
                        // get all addresses
                        var clientAddresses = await cliCtx.Address.AsNoTracking().ToListAsync();

                        // check row count
                        Assert.Equal(serverAddresses.Count, clientAddresses.Count);

                        foreach (var clientAddress in clientAddresses)
                        {
                            var serverAddress = serverAddresses.First(a => a.AddressId == clientAddress.AddressId);

                            // check column value
                            Assert.Equal(serverAddress.StateProvince, clientAddress.StateProvince);
                            Assert.Equal(serverAddress.AddressLine2, clientAddress.AddressLine2);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Update one row on client, should be correctly sync on server then all clients
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Update_NullValue_FromClient(SyncOptions options)
        {
            // create a server schema with seeding
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // get rows count
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
            }

            // Update one address on each client, with null value on addressline2 (which is not null when seed)
            // To avoid conflicts, each client will update differents lines
            var addressId = 1;
            foreach (var client in Clients)
            {
                using (var ctx = new AdventureWorksContext(client, this.UseFallbackSchema))
                {
                    var address = await ctx.Address.SingleAsync(a => a.AddressId == addressId);

                    // update to null value
                    address.AddressLine2 = null;

                    await ctx.SaveChangesAsync();
                }
                addressId++;
            }
            // Execute a sync on all clients and check results
            // Each client will download the "upload from previous client"
            int download = 0;
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(download++, s.TotalChangesDownloaded);
                Assert.Equal(1, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
            }

            // Now sync again to be sure all clients have all lines
            foreach (var client in Clients)
            {
                await new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                    new SyncSetup(Tables), options).SynchronizeAsync();
            }


            // check rows count on server and on each client
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                // get all addresses
                var serverAddresses = await ctx.Address.AsNoTracking().ToListAsync();

                foreach (var client in Clients)
                {
                    using (var cliCtx = new AdventureWorksContext(client, this.UseFallbackSchema))
                    {
                        // get all addresses
                        var clientAddresses = await cliCtx.Address.AsNoTracking().ToListAsync();

                        // check row count
                        Assert.Equal(serverAddresses.Count, clientAddresses.Count);

                        foreach (var clientAddress in clientAddresses)
                        {
                            var serverAddress = serverAddresses.First(a => a.AddressId == clientAddress.AddressId);

                            // check column value
                            Assert.Equal(serverAddress.AddressLine2, clientAddress.AddressLine2);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Update one row on server, should be correctly sync on all clients
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Update_NullValue_FromServer(SyncOptions options)
        {
            // create a server schema with seeding
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // get rows count
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
            }

            // Update one address on server with a null value which was not null before
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                var address = await serverDbCtx.Address.SingleAsync(a => a.AddressId == 1);

                // set null to a previous value which was not null
                address.AddressLine2 = null;

                await serverDbCtx.SaveChangesAsync();
            }

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(1, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);

                // check row updated values
                using (var ctx = new AdventureWorksContext(client, this.UseFallbackSchema))
                {
                    var cliAddress = await ctx.Address.AsNoTracking().SingleAsync(a => a.AddressId == 1);
                    Assert.Null(cliAddress.AddressLine2);
                }
            }


            // Update one address on server with a non null value (on a value which was null before)
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                var address = await serverDbCtx.Address.SingleAsync(a => a.AddressId == 1);

                // set not null value to a previous value which was null
                address.AddressLine2 = "NoT a null value !";

                await serverDbCtx.SaveChangesAsync();
            }

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(1, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);

                // check row updated values
                using (var ctx = new AdventureWorksContext(client, this.UseFallbackSchema))
                {
                    var cliAddress = await ctx.Address.AsNoTracking().SingleAsync(a => a.AddressId == 1);
                    Assert.Equal("NoT a null value !", cliAddress.AddressLine2);
                }
            }

        }

        /// <summary>
        /// Delete rows on server, should be correctly sync on all clients
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Delete_OneTable_FromServer(SyncOptions options)
        {
            // create a server schema with seeding
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // get rows count
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

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
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

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
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

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
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_DeleteClient_UpdateServer_ServerShouldWins(SyncOptions options)
        {
            var productId = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 6);
            var productCategoryName = HelperDatabase.GetRandomName("CLI");
            var productCategoryNameUpdated = HelperDatabase.GetRandomName("SRV");

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
                await new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                    new SyncSetup(Tables), options).SynchronizeAsync();

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
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

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
        /// Deleting a client row and sync, let the tracking table row on the client database
        /// When downloading the same row from server, the tracking table should be aligned with this new row
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Delete_Client_Sync_InsertServer_Sync_ClientShouldHaveRow(SyncOptions options)
        {
            var productCategoryId = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 6);
            var productCategoryNameClient = HelperDatabase.GetRandomName("CLI_");
            var productCategoryNameServer = HelperDatabase.GetRandomName("SRV_");

            // create a server schema without seeding
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
                await new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                    new SyncSetup(Tables), options).SynchronizeAsync();

            foreach (var client in Clients)
            {
                // Insert product category on each client
                using (var ctx = new AdventureWorksContext(client, this.UseFallbackSchema))
                {
                    ctx.Add(new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryNameClient });
                    await ctx.SaveChangesAsync();
                }

                // Then delete it
                using (var ctx = new AdventureWorksContext(client, this.UseFallbackSchema))
                {
                    var pcdel = ctx.ProductCategory.Single(pc => pc.ProductCategoryId == productCategoryId);
                    ctx.ProductCategory.Remove(pcdel);
                    await ctx.SaveChangesAsync();
                }
                // Now we have a tracking shadow row 
            }

            // Execute a sync on all clients to not avoid any conflict
            foreach (var client in Clients)
            {
                var s = await new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                            new SyncSetup(Tables), options).SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(1, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
            }

            // Insert same product on server
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                ctx.Add(new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryNameServer });
                await ctx.SaveChangesAsync();
            }

            // Execute a sync on all clients and check results
            // Each client will upload its deleted row (conflicting)
            // then download the updated row from server 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(1, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
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
                        Assert.Single(clientPC);

                        foreach (var cpc in clientPC)
                        {
                            var spc = serverPC.First(pc => pc.ProductCategoryId == cpc.ProductCategoryId);

                            // check column value
                            Assert.Equal(spc.ProductCategoryId, cpc.ProductCategoryId);
                            Assert.Equal(spc.Name, cpc.Name);
                            Assert.Equal(productCategoryNameServer, spc.Name);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Generate a conflict when inserting one row on server and the same row on each client
        /// Server should wins the conflict since it's the default behavior
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_Insert_Insert_ServerShouldWins(SyncOptions options)
        {
            // create a server schema without seeding
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

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
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

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
        /// Client should wins the conflict because configuration set to ClientWins
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_Insert_Insert_ClientShouldWins_CozConfiguration(SyncOptions options)
        {
            // create a server schema without seeding
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // set correct ConflictResolution
            options.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
            }
             
            // Insert the conflict product category on each client and the server
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
            // then download the others client lines (and not the conflict since it's resolved)
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(Clients.Count - 1, s.TotalChangesDownloaded);
                Assert.Equal(1, s.TotalChangesUploaded);
                Assert.Equal(1, s.TotalSyncConflicts);
            }


            // Now sync again to be sure all clients download all the lines from the server
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator, new SyncSetup(Tables), options);
                await agent.SynchronizeAsync();
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
                            Assert.StartsWith("CLI", spc.Name);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Generate a conflict when inserting one row on server and the same row on each client
        /// Client should wins the conflict because we have an event raised
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_Insert_Insert_ClientShouldWins_CozHandler(SyncOptions options)
        {
            // create a server schema without seeding
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

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
                    ctx.Add(new ProductCategory { ProductCategoryId = productId, Name = productCategoryNameClient });
                    await ctx.SaveChangesAsync();
                }

                using (var ctx = new AdventureWorksContext(this.Server))
                {
                    ctx.Add(new ProductCategory { ProductCategoryId = productId, Name = productCategoryNameServer });
                    await ctx.SaveChangesAsync();
                }
            }

            // Execute a sync on all clients and check results
            // Each client will upload its row (conflicting)
            // then download the others client lines (and not the conflict since it's resolved)
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

                agent.OnApplyChangesFailed(acf =>
                {
                    // Check conflict is correctly set
                    var localRow = acf.Conflict.LocalRow;
                    var remoteRow = acf.Conflict.RemoteRow;

                    Assert.Equal(ConflictType.RemoteExistsLocalExists, acf.Conflict.Type);
                    Assert.StartsWith("SRV", localRow["Name"].ToString());
                    Assert.StartsWith("CLI", remoteRow["Name"].ToString());

                    // Client should wins
                    acf.Resolution = ConflictResolution.ClientWins;
                });

                var s = await agent.SynchronizeAsync();

                Assert.Equal(Clients.Count - 1, s.TotalChangesDownloaded);
                Assert.Equal(1, s.TotalChangesUploaded);
                Assert.Equal(1, s.TotalSyncConflicts);
            }

            // Now sync again to be sure all clients download all the lines from the server
            foreach (var client in Clients)
                await new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator, new SyncSetup(Tables), options)
                    .SynchronizeAsync();


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
                            Assert.StartsWith("CLI", spc.Name);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Generate a conflict when inserting one row on server and the same row on each client
        /// Server should wins the conflict because default behavior
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_Update_Update_ServerShouldWins(SyncOptions options)
        {
            // create a server schema without seeding
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

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
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

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
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

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
        /// Server should wins the conflict because default behavior
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_Update_Update_ClientShouldWins_CozConfiguration(SyncOptions options)
        {
            // create a server schema without seeding
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // Set conflict resolution
            options.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;

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
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

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
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
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
                            Assert.StartsWith("CLI", spc.Name);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Generate a conflict when inserting one row on server and the same row on each client
        /// Client should wins coz handler
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_Update_Update_ClientShouldWins_CozHandler(SyncOptions options)
        {
            // create a server schema without seeding
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

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
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

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
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

                agent.OnApplyChangesFailed(acf =>
                {
                    // Check conflict is correctly set
                    var localRow = acf.Conflict.LocalRow;
                    var remoteRow = acf.Conflict.RemoteRow;

                    Assert.Equal(ConflictType.RemoteExistsLocalExists, acf.Conflict.Type);
                    // first time it's SRV, second time it's CLI !!
                    // Assert.StartsWith("SRV", localRow["Name"].ToString());

                    Assert.StartsWith("CLI", remoteRow["Name"].ToString());

                    // Client should wins
                    acf.Resolution = ConflictResolution.ClientWins;
                });

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
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
                            Assert.StartsWith("CLI", spc.Name);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Generate a conflict when inserting one row on server and the same row on each client
        /// Client should wins coz handler
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Conflict_Update_Update_Resolved_ByMerge(SyncOptions options)
        {
            // create a server schema without seeding
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

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
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

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
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

                agent.OnApplyChangesFailed(acf =>
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

                });

                var s = await agent.SynchronizeAsync();

                Assert.Equal(1, s.TotalChangesDownloaded);
                Assert.Equal(1, s.TotalChangesUploaded);
                Assert.Equal(1, s.TotalSyncConflicts);

                conflictIndex++;
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
        /// Generate a conflict when inserting one row on server and the same row on each client
        /// Client should wins coz handler
        /// </summary>
        //[Fact]
        public async Task Using_ExistingClientDatabase_ProvisionDeprovision()
        {
            // generate a sync conf to host the schema
            var setup = new SyncSetup(this.Tables);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                // create a client schema without seeding
                await this.fixture.EnsureDatabaseSchemaAndSeedAsync(client, false, UseFallbackSchema);

                // just check interceptor
                client.LocalOrchestrator.On<TableProvisioningArgs>(args =>
                {
                    Assert.Equal(SyncProvision.All, args.Provision);
                });

                // Read client schema
                SyncSet schema = null;

                using (var dbConnection = client.LocalOrchestrator.Provider.CreateConnection())
                {
                    schema = client.LocalOrchestrator.Provider.ReadSchema(setup, dbConnection, null);
                };

                // Provision the database with all tracking tables, stored procedures, triggers and scope
                await client.LocalOrchestrator.Provider.ProvisionAsync(schema, SyncProvision.All);


                //--------------------------
                // ASSERTION
                //--------------------------

                // check if scope table is correctly created
                var scopeBuilderFactory = client.LocalOrchestrator.Provider.GetScopeBuilder();
                SyncSet syncSchema;

                using (var dbConnection = client.LocalOrchestrator.Provider.CreateConnection())
                {
                    syncSchema = client.LocalOrchestrator.Provider.ReadSchema(setup, dbConnection, null);
                    var scopeBuilder = scopeBuilderFactory.CreateScopeInfoBuilder(SyncOptions.DefaultScopeInfoTableName, dbConnection);
                    Assert.False(scopeBuilder.NeedToCreateScopeInfoTable());
                }

                // get the db manager
                foreach (var syncTable in syncSchema.Tables)
                {
                    var tableName = syncTable.TableName;
                    using (var dbConnection = client.LocalOrchestrator.Provider.CreateConnection())
                    {
                        // get the database manager factory then the db manager itself
                        var dbTableBuilder = client.LocalOrchestrator.Provider.GetDatabaseBuilder(syncTable);

                        // get builders
                        var trackingTablesBuilder = dbTableBuilder.CreateTrackingTableBuilder(dbConnection);
                        var triggersBuilder = dbTableBuilder.CreateTriggerBuilder(dbConnection);
                        var spBuider = dbTableBuilder.CreateProcBuilder(dbConnection);

                        await dbConnection.OpenAsync();

                        Assert.False(trackingTablesBuilder.NeedToCreateTrackingTable());

                        Assert.False(triggersBuilder.NeedToCreateTrigger(Builders.DbTriggerType.Insert));
                        Assert.False(triggersBuilder.NeedToCreateTrigger(Builders.DbTriggerType.Delete));
                        Assert.False(triggersBuilder.NeedToCreateTrigger(Builders.DbTriggerType.Update));

                        // sqlite does not have stored procedures
                        if (spBuider != null)
                        {
                            Assert.False(spBuider.NeedToCreateProcedure(Builders.DbCommandType.Reset));
                            Assert.False(spBuider.NeedToCreateProcedure(Builders.DbCommandType.SelectChanges));
                            Assert.False(spBuider.NeedToCreateProcedure(Builders.DbCommandType.SelectRow));
                            Assert.False(spBuider.NeedToCreateProcedure(Builders.DbCommandType.DeleteMetadata));
                            Assert.False(spBuider.NeedToCreateProcedure(Builders.DbCommandType.DeleteRow));

                            // Check if we have mutables columns to see if the update row / metadata have been generated
                            if (dbTableBuilder.TableDescription.GetMutableColumns(false).Any())
                            {
                                Assert.False(spBuider.NeedToCreateProcedure(Builders.DbCommandType.UpdateRow));
                                Assert.False(spBuider.NeedToCreateProcedure(Builders.DbCommandType.UpdateMetadata));
                            }
                        }

                        if (client.ProviderType == ProviderType.Sql)
                        {
                            Assert.False(spBuider.NeedToCreateProcedure(Builders.DbCommandType.BulkDeleteRows));
                            Assert.False(spBuider.NeedToCreateProcedure(Builders.DbCommandType.BulkUpdateRows));
                        }

                        dbConnection.Close();

                    }
                }

                client.LocalOrchestrator.Provider.On<TableProvisioningArgs>(null);


                // just check interceptor
                client.LocalOrchestrator.Provider.On<TableDeprovisioningArgs>(args => Assert.Equal(SyncProvision.All, args.Provision));


                // Provision the database with all tracking tables, stored procedures, triggers and scope
                await client.LocalOrchestrator.Provider.DeprovisionAsync(schema, SyncProvision.All);

                // get the db manager
                foreach (var dmTable in syncSchema.Tables)
                {
                    var tableName = dmTable.TableName;
                    using (var dbConnection = client.LocalOrchestrator.Provider.CreateConnection())
                    {
                        // get the database manager factory then the db manager itself
                        var dbTableBuilder = client.LocalOrchestrator.Provider.GetDatabaseBuilder(dmTable);

                        // get builders
                        var trackingTablesBuilder = dbTableBuilder.CreateTrackingTableBuilder(dbConnection);
                        var triggersBuilder = dbTableBuilder.CreateTriggerBuilder(dbConnection);
                        var spBuider = dbTableBuilder.CreateProcBuilder(dbConnection);

                        await dbConnection.OpenAsync();

                        Assert.True(trackingTablesBuilder.NeedToCreateTrackingTable());
                        Assert.True(triggersBuilder.NeedToCreateTrigger(Builders.DbTriggerType.Insert));
                        Assert.True(triggersBuilder.NeedToCreateTrigger(Builders.DbTriggerType.Delete));
                        Assert.True(triggersBuilder.NeedToCreateTrigger(Builders.DbTriggerType.Update));
                        if (spBuider != null)
                        {

                            Assert.True(spBuider.NeedToCreateProcedure(Builders.DbCommandType.Reset));
                            Assert.True(spBuider.NeedToCreateProcedure(Builders.DbCommandType.SelectChanges));
                            Assert.True(spBuider.NeedToCreateProcedure(Builders.DbCommandType.SelectRow));
                            Assert.True(spBuider.NeedToCreateProcedure(Builders.DbCommandType.DeleteMetadata));
                            Assert.True(spBuider.NeedToCreateProcedure(Builders.DbCommandType.DeleteRow));

                            // Check if we have mutables columns to see if the update row / metadata have been generated
                            if (dbTableBuilder.TableDescription.GetMutableColumns(false).Any())
                            {
                                Assert.True(spBuider.NeedToCreateProcedure(Builders.DbCommandType.UpdateRow));
                                Assert.True(spBuider.NeedToCreateProcedure(Builders.DbCommandType.UpdateMetadata));
                            }
                        }

                        if (client.ProviderType == ProviderType.Sql)
                        {
                            Assert.True(spBuider.NeedToCreateProcedure(Builders.DbCommandType.BulkDeleteRows));
                            Assert.True(spBuider.NeedToCreateProcedure(Builders.DbCommandType.BulkUpdateRows));
                        }

                        dbConnection.Close();

                    }
                }


                client.LocalOrchestrator.Provider.On<TableDeprovisioningArgs>(null);


            }
        }

        /// <summary>
        /// Check foreign keys existence
        /// </summary>
        [Fact]
        public async Task Check_Composite_ForeignKey_Existence()
        {
            // create a server schema without seeding
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator, Tables);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);


                using (var dbConnection = client.LocalOrchestrator.Provider.CreateConnection())
                {
                    var tablePricesListCategory = client.LocalOrchestrator.Provider.GetTableManagerFactory("PricesListCategory", "")?.CreateManagerTable(dbConnection);

                    Assert.NotNull(tablePricesListCategory);

                    var relations = tablePricesListCategory.GetRelations().ToArray();
                    Assert.Single(relations);

                    if (client.ProviderType != ProviderType.Sqlite)
                        Assert.StartsWith("FK_PricesListCategory_PricesList_PriceListId", relations[0].ForeignKey);

                    Assert.Single(relations[0].Columns);

                    var tablePricesListDetail = client.LocalOrchestrator.Provider.GetTableManagerFactory("PricesListDetail", "")?.CreateManagerTable(dbConnection);

                    Assert.NotNull(tablePricesListDetail);

                    var relations2 = tablePricesListDetail.GetRelations().ToArray();
                    Assert.Single(relations2);

                    if (client.ProviderType != ProviderType.Sqlite)
                        Assert.StartsWith("FK_PricesListDetail_PricesListCategory_PriceListId", relations2[0].ForeignKey);

                    Assert.Equal(2, relations2[0].Columns.Count);

                    var tableEmployeeAddress = client.LocalOrchestrator.Provider.GetTableManagerFactory("EmployeeAddress", "")?.CreateManagerTable(dbConnection);
                    Assert.NotNull(tableEmployeeAddress);

                    var relations3 = tableEmployeeAddress.GetRelations().ToArray();
                    Assert.Equal(2, relations3.Count());

                    if (client.ProviderType != ProviderType.Sqlite)
                    {
                        Assert.StartsWith("FK_EmployeeAddress_Address_AddressID", relations3[0].ForeignKey);
                        Assert.StartsWith("FK_EmployeeAddress_Employee_EmployeeID", relations3[1].ForeignKey);

                    }

                    Assert.Single(relations3[0].Columns);
                    Assert.Single(relations3[1].Columns);

                }

            }
        }

        /// <summary>
        /// Be sure we continue to trakc correctly rows even during a sync process
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Insert_Record_Then_Insert_During_GetChanges(SyncOptions options)
        {
            // create a server schema without seeding
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
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
                    var priceList = new PriceList { PriceListId = Guid.NewGuid(), Description = HelperDatabase.GetRandomName() };
                    ctx.Add(priceList);
                    await ctx.SaveChangesAsync();
                }
            }

            // Sync all clients
            // First client  will upload 3 lines and will download nothing
            // Second client will upload 3 lines and will download 3 lines
            // thrid client  will upload 3 lines and will download 6 lines
            int download = 0;
            foreach (var client in Clients)
            {

                // Sleep during a selecting changes on first sync
                Task tableChangesSelected(TableChangesSelectedArgs changes)
                {
                    if (changes.TableChangesSelected.TableName != "PricesList")
                        return Task.CompletedTask;

                    // Insert on same connection as current sync.
                    // Using same connection to avoid lock, especially on SQlite
                    var command = changes.Connection.CreateCommand();
                    command.CommandText = "INSERT INTO PricesList (PriceListId, Description) Values (@PriceListId, @Description);";

                    var p = command.CreateParameter();
                    p.ParameterName = "@PriceListId";
                    p.Value = Guid.NewGuid();
                    command.Parameters.Add(p);

                    p = command.CreateParameter();
                    p.ParameterName = "@Description";
                    p.Value = HelperDatabase.GetRandomName();
                    command.Parameters.Add(p);

                    command.Transaction = changes.Transaction;
                    try
                    {
                        var inserted = command.ExecuteNonQuery();
                        Debug.WriteLine($"Execution result: {inserted}");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine(ex.Message);
                        throw;
                    }
                    return Task.CompletedTask;
                };

                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

                // Intercept TableChangesSelected
                agent.LocalOrchestrator.OnTableChangesSelected(tableChangesSelected);

                var s = await agent.SynchronizeAsync();

                agent.LocalOrchestrator.OnTableChangesSelected(null);

                Assert.Equal(download, s.TotalChangesDownloaded);
                Assert.Equal(3, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
                download += 3;
            }

            // CLI1 (6 rows) : CLI1 will upload 1 row and download 3 rows from CLI2 and 3 rows from CLI3
            // CLI2 (4 rows) : CLI2 will upload 1 row and download 3 rows from CLI3 and 1 row from CLI1
            // CLI3 (2 rows) : CLI3 will upload 1 row and download 1 row from CLI1 and 1 row from CLI2
            download = 3 * (Clients.Count - 1);
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                    new SyncSetup(Tables), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(download, s.TotalChangesDownloaded);
                Assert.Equal(1, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
                download -= 2;
            }

            // CLI1 (6) : CLI1 will download 1 row from CLI3 and 1 rows from CLI2
            // CLI2 (4) : CLI2 will download 1 row from CLI3
            // CLI3 (2) : CLI3 will download nothing
            download = Clients.Count - 1;
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                    new SyncSetup(Tables), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(download--, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
            }



            // check rows count on server and on each client
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                var productRowCount = await ctx.Product.AsNoTracking().CountAsync();
                var productCategoryCount = await ctx.ProductCategory.AsNoTracking().CountAsync();
                var priceListCount = await ctx.PricesList.AsNoTracking().CountAsync();
                foreach (var client in Clients)
                {
                    using (var cliCtx = new AdventureWorksContext(client, this.UseFallbackSchema))
                    {
                        var pCount = await cliCtx.Product.AsNoTracking().CountAsync();
                        Assert.Equal(productRowCount, pCount);

                        var pcCount = await cliCtx.ProductCategory.AsNoTracking().CountAsync();
                        Assert.Equal(productCategoryCount, pcCount);

                        var plCount = await cliCtx.PricesList.AsNoTracking().CountAsync();
                        Assert.Equal(priceListCount, plCount);
                    }
                }
            }
        }


        /// <summary>
        /// Insert thousand or rows. Check if batch mode works correctly
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Insert_ThousandRows_FromClient(SyncOptions options)
        {
            // create a server schema without seeding
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
                await new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options).SynchronizeAsync();

            // Insert one thousand lines on each client
            foreach (var client in Clients)
            {
                using (var ctx = new AdventureWorksContext(client, this.UseFallbackSchema))
                {
                    for (var i = 0; i < 1000; i++)
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
            // First client  will upload 1000 lines and will download nothing
            // Second client will upload 1000 lines and will download 1000 lines
            // Third client  will upload 1000 line and will download 3000 lines
            int download = 0;
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(download * 1000, s.TotalChangesDownloaded);
                Assert.Equal(1000, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);

                download++;
            }

        }

        /// <summary>
        /// Force failing constraints.
        /// But since we set the correct options, shoudl work correctly
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Force_Failing_Constraints_ButWorks_WithOptions(SyncOptions options)
        {
            // create a server schema without seeding
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // Disable check constraints
            // "true" is the default value, but set for information purpose
            options.DisableConstraintsOnApplyChanges = true;

            // product category and product items
            var productCategoryName = HelperDatabase.GetRandomName();
            var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

            var productId = Guid.NewGuid();
            var productName = HelperDatabase.GetRandomName();
            var productNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);


            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
                await new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options).SynchronizeAsync();


            // Add a product and its product category
            using (var ctx = new AdventureWorksContext(this.Server))
            {

                var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                ctx.ProductCategory.Add(pc);

                // Create a product and affect ProductCategory
                var product = new Product
                {
                    ProductId = productId,
                    Name = productName,
                    ProductNumber = productNumber,
                    ProductCategory = pc
                };

                ctx.Product.Add(product);

                await ctx.SaveChangesAsync();
            }

            // Sync all clients 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(2, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
            }

            // Creating the fail constraint 
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                // Get the ProductCategory and the Product
                var pc = ctx.ProductCategory.Find(productCategoryId);
                var p = ctx.Product.Find(productId);

                // Update Product to remove foreign key to Product Category
                p.ProductCategory = null;
                // Delete the ProductCategory row
                ctx.ProductCategory.Remove(pc);

                // Save
                await ctx.SaveChangesAsync();

            }

            // Sync all clients. Should raise an error
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);


                var s = await agent.SynchronizeAsync();

                Assert.Equal(2, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);

            }
        }


        /// <summary>
        /// Force failing constraints.
        /// Try to solve with interceptors
        /// </summary>
        /// <param name="options"></param>
        /// <returns></returns>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Force_Failing_Constraints_ButWorks_WithInterceptors(SyncOptions options)
        {
            // create a server schema without seeding
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // Enable check constraints
            options.DisableConstraintsOnApplyChanges = false;

            // product category and product items
            var productCategoryName = HelperDatabase.GetRandomName();
            var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

            var productId = Guid.NewGuid();
            var productName = HelperDatabase.GetRandomName();
            var productNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);


            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
                await new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options).SynchronizeAsync();


            // Add a product and its product category
            using (var ctx = new AdventureWorksContext(this.Server))
            {

                var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                ctx.ProductCategory.Add(pc);


                // Create a product and affect ProductCategory
                var product = new Product
                {
                    ProductId = productId,
                    Name = productName,
                    ProductNumber = productNumber,
                    ProductCategory = pc
                };

                ctx.Product.Add(product);

                await ctx.SaveChangesAsync();
            }

            // Sync all clients 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(2, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
            }


            // Creating the fail constraint 
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                // Get the ProductCategory and the Product
                var pc = ctx.ProductCategory.Find(productCategoryId);
                var p = ctx.Product.Find(productId);

                // Update Product to remove foreign key to Product Category
                p.ProductCategory = null;
                // Delete the ProductCategory row
                ctx.ProductCategory.Remove(pc);

                // Save
                await ctx.SaveChangesAsync();

            }

            // Sync all clients. Should raise an error
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

                //await Assert.ThrowsAsync<SyncException>(async () =>
                //{
                //    var res = await agent.SynchronizeAsync();
                //});

                // NOTE : Using interceptors to disable constraint
                // but options.DisableConstraintsOnApplyChanges = true; DOES THE SAME 
                // Using interceptors just to test if it's possible "on the fly"

                // Setting PRAGMA only works outside a transaction
                // So setting the pragma on the connection opening
                // No need to reaffect PRAGMA at the end, since the modification leaves
                // only during the connection open time
                agent.LocalOrchestrator.OnConnectionOpen(coa =>
                {
                    if (client.ProviderType != ProviderType.Sqlite)
                        return;

                    var cmd = coa.Connection.CreateCommand();
                    cmd.Connection = coa.Connection;

                    cmd.CommandText = "PRAGMA foreign_keys = OFF;";
                    object res = cmd.ExecuteScalar();

                });

                agent.LocalOrchestrator.OnDatabaseChangesApplying(tca =>
                {
                    if (client.ProviderType == ProviderType.Sqlite)
                        return;

                    if (client.ProviderType == ProviderType.MySql)
                    {
                        var cmd = tca.Connection.CreateCommand();
                        cmd.CommandText = "SET FOREIGN_KEY_CHECKS = 0;";
                        cmd.Connection = tca.Connection;
                        cmd.Transaction = tca.Transaction;
                        cmd.ExecuteNonQuery();

                        return;
                    }
                    if (client.ProviderType == ProviderType.Sql)
                    {
                        foreach (var table in agent.Schema.Tables.Where(t => t.TableName == "Product" || t.TableName == "ProductCategory"))
                        {
                            var cmd = tca.Connection.CreateCommand();
                            var tableAndSchemaName = ParserName.Parse(table).Schema().Quoted().ToString();
                            var tableName = ParserName.Parse(table).Schema().Quoted().ToString();
                            cmd.CommandText = $"ALTER TABLE {tableAndSchemaName} NOCHECK CONSTRAINT ALL";
                            cmd.Connection = tca.Connection;
                            cmd.Transaction = tca.Transaction;
                            cmd.ExecuteNonQuery();

                        }
                    }
                });

                agent.LocalOrchestrator.OnDatabaseChangesApplied(tca =>
                {

                    if (client.ProviderType == ProviderType.Sqlite)
                        return;


                    if (client.ProviderType == ProviderType.MySql)
                    {
                        var cmd = tca.Connection.CreateCommand();
                        cmd.CommandText = "SET FOREIGN_KEY_CHECKS = 1;";
                        cmd.Connection = tca.Connection;
                        cmd.Transaction = tca.Transaction;
                        cmd.ExecuteNonQuery();

                        return;
                    }

                    if (client.ProviderType == ProviderType.Sql)
                    {
                        foreach (var table in agent.Schema.Tables.Where(t => t.TableName == "Product" || t.TableName == "ProductCategory"))
                        {
                            var cmd = tca.Connection.CreateCommand();
                            var tableAndSchemaName = ParserName.Parse(table).Schema().Quoted().ToString();
                            var tableName = ParserName.Parse(table).Schema().Quoted().ToString();
                            cmd.CommandText = $"ALTER TABLE {tableAndSchemaName} CHECK CONSTRAINT ALL";
                            cmd.Connection = tca.Connection;
                            cmd.Transaction = tca.Transaction;
                            cmd.ExecuteNonQuery();
                        }
                    }

                });



                var s = await agent.SynchronizeAsync();

                Assert.Equal(2, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);

                agent.LocalOrchestrator.OnDatabaseChangesApplying(null);
                agent.LocalOrchestrator.OnDatabaseChangesApplied(null);
                agent.LocalOrchestrator.OnConnectionOpen(null);

            }


        }


        /// <summary>
        /// Insert one row on each client, should be sync on server and clients
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Reinitialize_Client(SyncOptions options)
        {
            // create a server schema with seeding
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

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
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

                var s = await agent.SynchronizeAsync(SyncType.Reinitialize);

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);

            }
        }

        /// <summary>
        /// Insert one row on each client, should be sync on server and clients
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task ReinitializeWithUpload_Client(SyncOptions options)
        {
            // create a server schema with seeding
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

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
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator,
                                          new SyncSetup(Tables), options);

                var s = await agent.SynchronizeAsync(SyncType.ReinitializeWithUpload);

                Assert.Equal(rowsCount + download, s.TotalChangesDownloaded);
                Assert.Equal(2, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
                download += 2;
            }


        }


        /// <summary>
        /// Configuring tables to be upload only
        /// Server should receive lines but will not send back its own lines
        /// </summary>
        [Fact]
        public async Task UploadOnly()
        {
            // create a server schema without seeding
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);


            // Set Employee, Address, EmployeeAddress to Upload only
            // All others are Bidirectional by default.
            var setup = new SyncSetup(Tables);
            setup.Tables["Employee"].SyncDirection = SyncDirection.UploadOnly;
            setup.Tables["Address"].SyncDirection = SyncDirection.UploadOnly;
            setup.Tables["EmployeeAddress"].SyncDirection = SyncDirection.UploadOnly;


            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator, setup);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
            }

            // Insert one line on each client
            int index = 10;
            foreach (var client in Clients)
            {
                // Insert one employee, address, employeeaddress
                using (var ctx = new AdventureWorksContext(client, this.UseFallbackSchema))
                {

                    ctx.Database.OpenConnection();

                    // Insert an employee
                    var employee = new Employee
                    {
                        EmployeeId = index,
                        FirstName = "John",
                        LastName = "Doe"
                    };

                    ctx.Add(employee);

                    if (client.ProviderType == ProviderType.Sql)
                        ctx.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Employee ON;");

                    await ctx.SaveChangesAsync();

                    if (client.ProviderType == ProviderType.Sql)
                        ctx.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Employee OFF");

                    // Insert a new address for employee 
                    var city = "Paris " + HelperDatabase.GetRandomName();
                    var addressline1 = "Rue Monthieu " + HelperDatabase.GetRandomName();
                    var stateProvince = "Ile de France";
                    var countryRegion = "France";
                    var postalCode = "75001";

                    var address = new Address
                    {
                        AddressId = index,
                        AddressLine1 = addressline1,
                        City = city,
                        StateProvince = stateProvince,
                        CountryRegion = countryRegion,
                        PostalCode = postalCode

                    };

                    ctx.Add(address);
                    if (client.ProviderType == ProviderType.Sql)
                        ctx.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Address ON;");

                    await ctx.SaveChangesAsync();

                    if (client.ProviderType == ProviderType.Sql)
                        ctx.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Address OFF");


                    var employeeAddress = new EmployeeAddress
                    {
                        EmployeeId = employee.EmployeeId,
                        AddressId = address.AddressId,
                        AddressType = "CLIENT"
                    };

                    ctx.EmployeeAddress.Add(employeeAddress);
                    await ctx.SaveChangesAsync();


                    ctx.Database.CloseConnection();

                    index++;
                }


            }

            // Insert one ProductCategory, Employee, Address, EmployeeAddress on server
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                ctx.Database.OpenConnection();

                var productId = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 6);
                var productCategoryNameServer = HelperDatabase.GetRandomName("SRV");


                // ProductCategory
                ctx.Add(new ProductCategory
                {
                    ProductCategoryId = productId,
                    Name = productCategoryNameServer
                });
                await ctx.SaveChangesAsync();

                // Insert an employee
                var employee = new Employee
                {
                    EmployeeId = 1000,
                    FirstName = "John",
                    LastName = "Doe"
                };

                ctx.Add(employee);

                if (this.ServerType == ProviderType.Sql)
                    ctx.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Employee ON;");

                await ctx.SaveChangesAsync();

                if (this.ServerType == ProviderType.Sql)
                    ctx.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Employee OFF");

                // Insert a new address for employee 
                var city = "Lyon " + HelperDatabase.GetRandomName();
                var addressline1 = HelperDatabase.GetRandomName();
                var stateProvince = "Rhones";
                var countryRegion = "France";
                var postalCode = "69001";

                var address = new Address
                {
                    AddressId = 1000,
                    AddressLine1 = addressline1,
                    City = city,
                    StateProvince = stateProvince,
                    CountryRegion = countryRegion,
                    PostalCode = postalCode

                };

                ctx.Add(address);
                if (this.ServerType == ProviderType.Sql)
                    ctx.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Address ON;");

                await ctx.SaveChangesAsync();

                if (this.ServerType == ProviderType.Sql)
                    ctx.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Address OFF");

                var employeeAddress = new EmployeeAddress
                {
                    EmployeeId = employee.EmployeeId,
                    AddressId = address.AddressId,
                    AddressType = "SERVER"
                };

                ctx.EmployeeAddress.Add(employeeAddress);
                await ctx.SaveChangesAsync();

                ctx.Database.CloseConnection();
            }

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator, setup);


                var s = await agent.SynchronizeAsync();

                // Server shoud not sent back lines, so download equals 1 (just product category)
                Assert.Equal(1, s.TotalChangesDownloaded);
                Assert.Equal(3, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
            }

            // check rows count on server and on each client
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                // get all product categories
                var serverPC = await ctx.ProductCategory.AsNoTracking().ToListAsync();
                Assert.Single(serverPC);

                // get all employees
                var employees = await ctx.Employee.AsNoTracking().ToListAsync();
                Assert.Equal(Clients.Count + 1, employees.Count);
                // get all employees address
                var employeesAddresses = await ctx.EmployeeAddress.AsNoTracking().ToListAsync();
                Assert.Equal(Clients.Count + 1, employeesAddresses.Count);
                // get all addresses
                var addresses = await ctx.Address.AsNoTracking().ToListAsync();
                Assert.Equal(Clients.Count + 1, addresses.Count);

            }
            foreach (var client in Clients)
            {
                using (var cliCtx = new AdventureWorksContext(client, this.UseFallbackSchema))
                {
                    // get all product categories
                    var clientPC = await cliCtx.ProductCategory.AsNoTracking().ToListAsync();
                    Assert.Single(clientPC);

                    // get all employees
                    var employees = await cliCtx.Employee.AsNoTracking().ToListAsync();
                    Assert.Single(employees);
                    // get all employees address
                    var employeesAddresses = await cliCtx.EmployeeAddress.AsNoTracking().ToListAsync();
                    Assert.Single(employeesAddresses);
                    // get all addresses
                    var addresses = await cliCtx.Address.AsNoTracking().ToListAsync();
                    Assert.Single(addresses);

                }
            }
        }

        /// <summary>
        /// Configuring tables to be upload only
        /// Server should receive lines but will not send back its own lines
        /// </summary>
        [Fact]
        public async Task DownloadOnly()
        {
            // create a server schema without seeding
            await this.fixture.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);


            // Set Employee, Address, EmployeeAddress to Upload only
            // All others are Bidirectional by default.
            var setup = new SyncSetup(Tables);
            setup.Tables["Employee"].SyncDirection = SyncDirection.DownloadOnly;
            setup.Tables["Address"].SyncDirection = SyncDirection.DownloadOnly;
            setup.Tables["EmployeeAddress"].SyncDirection = SyncDirection.DownloadOnly;


            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator, setup);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
            }

            // Insert one line on each client
            int index = 10;
            foreach (var client in Clients)
            {
                // Insert one employee, address, employeeaddress
                using (var ctx = new AdventureWorksContext(client, this.UseFallbackSchema))
                {

                    ctx.Database.OpenConnection();

                    // Insert an employee
                    var employee = new Employee
                    {
                        EmployeeId = index,
                        FirstName = "John",
                        LastName = "Doe"
                    };

                    ctx.Add(employee);

                    if (client.ProviderType == ProviderType.Sql)
                        ctx.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Employee ON;");

                    await ctx.SaveChangesAsync();

                    if (client.ProviderType == ProviderType.Sql)
                        ctx.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Employee OFF");

                    // Insert a new address for employee 
                    var city = "Paris " + HelperDatabase.GetRandomName();
                    var addressline1 = "Rue Monthieu " + HelperDatabase.GetRandomName();
                    var stateProvince = "Ile de France";
                    var countryRegion = "France";
                    var postalCode = "75001";

                    var address = new Address
                    {
                        AddressId = index,
                        AddressLine1 = addressline1,
                        City = city,
                        StateProvince = stateProvince,
                        CountryRegion = countryRegion,
                        PostalCode = postalCode

                    };

                    ctx.Add(address);
                    if (client.ProviderType == ProviderType.Sql)
                        ctx.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Address ON;");

                    await ctx.SaveChangesAsync();

                    if (client.ProviderType == ProviderType.Sql)
                        ctx.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Address OFF");


                    var employeeAddress = new EmployeeAddress
                    {
                        EmployeeId = employee.EmployeeId,
                        AddressId = address.AddressId,
                        AddressType = "CLIENT"
                    };

                    ctx.EmployeeAddress.Add(employeeAddress);
                    await ctx.SaveChangesAsync();


                    ctx.Database.CloseConnection();

                    index++;
                }


            }

            // Insert one ProductCategory, Employee, Address, EmployeeAddress on server
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                ctx.Database.OpenConnection();

                var productId = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 6);
                var productCategoryNameServer = HelperDatabase.GetRandomName("SRV");


                // ProductCategory
                ctx.Add(new ProductCategory
                {
                    ProductCategoryId = productId,
                    Name = productCategoryNameServer
                });
                await ctx.SaveChangesAsync();

                // Insert an employee
                var employee = new Employee
                {
                    EmployeeId = 1000,
                    FirstName = "John",
                    LastName = "Doe"
                };

                ctx.Add(employee);

                if (this.ServerType == ProviderType.Sql)
                    ctx.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Employee ON;");

                await ctx.SaveChangesAsync();

                if (this.ServerType == ProviderType.Sql)
                    ctx.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Employee OFF");

                // Insert a new address for employee 
                var city = "Lyon " + HelperDatabase.GetRandomName();
                var addressline1 = HelperDatabase.GetRandomName();
                var stateProvince = "Rhones";
                var countryRegion = "France";
                var postalCode = "69001";

                var address = new Address
                {
                    AddressId = 1000,
                    AddressLine1 = addressline1,
                    City = city,
                    StateProvince = stateProvince,
                    CountryRegion = countryRegion,
                    PostalCode = postalCode

                };

                ctx.Add(address);
                if (this.ServerType == ProviderType.Sql)
                    ctx.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Address ON;");

                await ctx.SaveChangesAsync();

                if (this.ServerType == ProviderType.Sql)
                    ctx.Database.ExecuteSqlRaw("SET IDENTITY_INSERT Address OFF");

                var employeeAddress = new EmployeeAddress
                {
                    EmployeeId = employee.EmployeeId,
                    AddressId = address.AddressId,
                    AddressType = "SERVER"
                };

                ctx.EmployeeAddress.Add(employeeAddress);
                await ctx.SaveChangesAsync();

                ctx.Database.CloseConnection();
            }

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.LocalOrchestrator, Server.RemoteOrchestrator, setup);


                var s = await agent.SynchronizeAsync();

                // Server send lines, but clients don't
                Assert.Equal(4, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalSyncConflicts);
            }

            // check rows count on server and on each client
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                // get all product categories
                var serverPC = await ctx.ProductCategory.AsNoTracking().ToListAsync();
                Assert.Single(serverPC);

                // get all employees
                var employees = await ctx.Employee.AsNoTracking().ToListAsync();
                Assert.Single(employees);
                // get all employees address
                var employeesAddresses = await ctx.EmployeeAddress.AsNoTracking().ToListAsync();
                Assert.Single(employeesAddresses);
                // get all addresses
                var addresses = await ctx.Address.AsNoTracking().ToListAsync();
                Assert.Single(addresses);

            }
            foreach (var client in Clients)
            {
                using (var cliCtx = new AdventureWorksContext(client, this.UseFallbackSchema))
                {
                    // get all product categories
                    var clientPC = await cliCtx.ProductCategory.AsNoTracking().ToListAsync();
                    Assert.Single(clientPC);

                    // get all employees
                    var employees = await cliCtx.Employee.AsNoTracking().ToListAsync();
                    Assert.Equal(2, employees.Count);
                    // get all employees address
                    var employeesAddresses = await cliCtx.EmployeeAddress.AsNoTracking().ToListAsync();
                    Assert.Equal(2, employeesAddresses.Count);
                    // get all addresses
                    var addresses = await cliCtx.Address.AsNoTracking().ToListAsync();
                    Assert.Equal(2, addresses.Count);

                }
            }
        }

    }
}
