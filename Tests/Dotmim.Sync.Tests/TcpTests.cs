using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.SqlServer.Manager;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Misc;
using Dotmim.Sync.Tests.Models;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Http;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using MySql.Data.MySqlClient;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Dotmim.Sync.Tests
{
    //[TestCaseOrderer("Dotmim.Sync.Tests.Misc.PriorityOrderer", "Dotmim.Sync.Tests")]
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

        /// <summary>
        /// Create an empty database
        /// </summary>
        public abstract Task CreateDatabaseAsync(ProviderType providerType, string dbName, bool recreateDb = true);

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
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // Execute a sync on all clients and check results
            foreach (var client in this.Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options,
                    new SyncSetup(this.Tables) { StoredProceduresPrefix = "cli", StoredProceduresSuffix = "", TrackingTablesPrefix = "tr" });

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));
            }
        }

        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public virtual async Task SchemaIsCreated(SyncOptions options)
        {
            // create a server db without seed
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(this.Tables));

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);

                // Check we have the correct columns replicated
                using var clientConnection = client.Provider.CreateConnection();
                await clientConnection.OpenAsync();

                var clientSchema = await agent.LocalOrchestrator.GetSchemaAsync();
                var serverSchema = await agent.RemoteOrchestrator.GetSchemaAsync();

                foreach (var setupTable in agent.Setup.Tables)
                {
                    var clientTable = client.ProviderType == ProviderType.Sql ? clientSchema.Tables[setupTable.TableName, setupTable.SchemaName] : clientSchema.Tables[setupTable.TableName];
                    var serverTable = serverSchema.Tables[setupTable.TableName, setupTable.SchemaName];

                    Assert.Equal(clientTable.Columns.Count, serverTable.Columns.Count);


                    foreach (var serverColumn in serverTable.Columns)
                    {
                        var clientColumn = clientTable.Columns.FirstOrDefault(c => c.ColumnName == serverColumn.ColumnName);

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

                            // IsCompute is not replicated, because we are not able to replicate formulat
                            // Instead, we are allowing null for the column
                            //Assert.Equal(serverColumn.IsCompute, clientColumn.IsCompute);

                            // Readonly is not replicated, because we are not able to replicate formulat
                            // Instead, we are allowing null for the column
                            //Assert.Equal(serverColumn.IsReadOnly, clientColumn.IsReadOnly);

                            // Decimal is conflicting with Numeric
                            //Assert.Equal(serverColumn.DbType, clientColumn.DbType);

                            Assert.Equal(serverColumn.Ordinal, clientColumn.Ordinal);
                            Assert.Equal(serverColumn.AllowDBNull, clientColumn.AllowDBNull);
                        }

                        Assert.Equal(serverColumn.ColumnName, clientColumn.ColumnName);

                    }

                }
                clientConnection.Close();

            }
        }

        /// <summary>
        /// Check a bad connection should raise correct error
        /// </summary>
        [Fact]
        public async Task Bad_ConnectionFromServer_ShouldRaiseError()
        {
            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // change the remote orchestrator connection string
            Server.Provider.ConnectionString = $@"Server=unknown;Database=unknown;UID=sa;PWD=unknown";

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var str = $"{test.TestCase.DisplayName}. Client {client.ProviderType}";
                Console.WriteLine(str);


                var agent = new SyncAgent(client.Provider, Server.Provider, this.Tables);

                var onReconnect = new Action<ReConnectArgs>(args =>
                    Console.WriteLine($"[Retry Connection] Can't connect to database {args.Connection?.Database}. Retry N°{args.Retry}. Waiting {args.WaitingTimeSpan.Milliseconds}. Exception:{args.HandledException.Message}."));

                agent.LocalOrchestrator.OnReConnect(onReconnect);
                agent.RemoteOrchestrator.OnReConnect(onReconnect);

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
                var str = $"{test.TestCase.DisplayName}. Client {client.ProviderType}";
                Console.WriteLine(str);
                // change the local orchestrator connection string
                // Set a connection string that will faile everywhere (event Sqlite)
                client.Provider.ConnectionString = $@"Data Source=D;";

                var agent = new SyncAgent(client.Provider, Server.Provider, this.Tables);

                var onReconnect = new Action<ReConnectArgs>(args =>
                    Console.WriteLine($"[Retry Connection] Can't connect to database {args.Connection?.Database}. Retry N°{args.Retry}. Waiting {args.WaitingTimeSpan.Milliseconds}. Exception:{args.HandledException.Message}."));

                agent.LocalOrchestrator.OnReConnect(onReconnect);
                agent.RemoteOrchestrator.OnReConnect(onReconnect);


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
            await this.CreateDatabaseAsync(this.ServerType, this.Server.DatabaseName, true);

            // Create the table on the server
            await HelperDatabase.ExecuteScriptAsync(this.Server.ProviderType, this.Server.DatabaseName, tableTestCreationScript); ;

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, new string[] { "TableTest" });

                var se = await Assert.ThrowsAnyAsync<SyncException>(async () =>
                {
                    var s = await agent.SynchronizeAsync();
                });

                Assert.Equal(SyncSide.ServerSide, se.Side);
                Assert.Equal("MissingPrimaryKeyException", se.TypeName);
                Assert.Equal(this.Server.DatabaseName, se.InitialCatalog);

            }
        }

        [Fact]
        public async Task Bad_ColumnSetup_DoesNotExistInSchema_ShouldRaiseError()
        {
            // create a server db without seed
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Create setup
            var setup = new SyncSetup(Tables);

            // Add a malformatted column name
            setup.Tables["Employee"].Columns.AddRange(new string[] { "EmployeeID", "FirstName", "LastNam" });

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, new SyncOptions(), setup);

                var se = await Assert.ThrowsAnyAsync<SyncException>(async () =>
                {
                    var s = await agent.SynchronizeAsync();
                });

                Assert.Equal(SyncSide.ServerSide, se.Side);
                Assert.Equal("MissingColumnException", se.TypeName);
            }
        }

        [Fact]
        public async Task Bad_TableSetup_DoesNotExistInSchema_ShouldRaiseError()
        {
            // create a server db without seed
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Add a fake table to setup tables
            var setup = new SyncSetup(this.Tables);
            setup.Tables.Add("WeirdTable");

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, new SyncOptions(), setup);

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
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Insert_OneTable_FromServer(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables));

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
                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables));

                var s = await agent.SynchronizeAsync();

                Assert.Equal(1, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
                Assert.Equal(1, this.GetServerDatabaseRowsCount(client));

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
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables));

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
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
                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables));

                var s = await agent.SynchronizeAsync();

                Assert.Equal(1, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
                Assert.Equal(1, this.GetServerDatabaseRowsCount(client));
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
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables));

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
                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables));

                var s = await agent.SynchronizeAsync();

                Assert.Equal(download++, s.TotalChangesDownloaded);
                Assert.Equal(1, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);

                Assert.Equal(this.GetServerDatabaseRowsCount(Server), this.GetServerDatabaseRowsCount(client));
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
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables));

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
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
                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables));

                var s = await agent.SynchronizeAsync();

                Assert.Equal(2, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
                Assert.Equal(2, this.GetServerDatabaseRowsCount(client));
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
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables));

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
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
            // First client  will upload two lines and will download nothing
            // Second client will upload two lines and will download two lines
            // thrid client  will upload two lines and will download four lines
            int download = 0;
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables));

                var s = await agent.SynchronizeAsync();

                Assert.Equal(download, s.TotalChangesDownloaded);
                Assert.Equal(2, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
                download += 2;
                Assert.Equal(this.GetServerDatabaseRowsCount(Server), this.GetServerDatabaseRowsCount(client));
            }

            // Now sync again to be sure all clients have all lines
            foreach (var client in Clients)
            {
                await new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables)).SynchronizeAsync();
            }

            // check rows count on server and on each client
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                var productRowCount = await ctx.Product.AsNoTracking().CountAsync();
                var productCategoryCount = await ctx.ProductCategory.AsNoTracking().CountAsync();
                foreach (var client in Clients)
                {
                    using var cliCtx = new AdventureWorksContext(client, this.UseFallbackSchema);
                    var pCount = await cliCtx.Product.AsNoTracking().CountAsync();
                    Assert.Equal(productRowCount, pCount);

                    var pcCount = await cliCtx.ProductCategory.AsNoTracking().CountAsync();
                    Assert.Equal(productCategoryCount, pcCount);
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
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // get rows count
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables));

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
                Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));
            }

            int addressId;
            using (var ctx = new AdventureWorksContext(Clients[0], this.UseFallbackSchema))
            {
                addressId = ctx.Address.First().AddressId;
            }

            // Update one address on server
            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                var address = serverDbCtx.Address.Single(a => a.AddressId == addressId);

                // Update at least two properties
                address.City = cityName;
                address.AddressLine1 = addressLine;

                await serverDbCtx.SaveChangesAsync();
            }

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables));

                var s = await agent.SynchronizeAsync();

                Assert.Equal(1, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);

                // check row updated values
                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
                var cliAddress = await ctx.Address.AsNoTracking().SingleAsync(a => a.AddressId == 1);
                Assert.Equal(cityName, cliAddress.City);
                Assert.Equal(addressLine, cliAddress.AddressLine1);
            }

            // get rows count
            rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            foreach (var client in Clients)
                Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));

        }

        /// <summary>
        /// Update one row on client, should be correctly sync on server then all clients
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Update_OneTable_FromClient(SyncOptions options)
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // get rows count
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables));

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
                Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));
            }


            // Update one address on each client
            // To avoid conflicts, each client will update differents lines
            // each address id is generated from the foreach index
            int addressId = 0;
            foreach (var client in Clients)
            {
                using (var ctx = new AdventureWorksContext(client, this.UseFallbackSchema))
                {
                    var addresses = ctx.Address.OrderBy(a => a.AddressId).Take(Clients.Count).ToList();
                    var address = addresses[addressId];

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
                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables));

                var s = await agent.SynchronizeAsync();

                Assert.Equal(download++, s.TotalChangesDownloaded);
                Assert.Equal(1, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

            // Now sync again to be sure all clients have all lines
            foreach (var client in Clients)
            {
                await new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables)).SynchronizeAsync();
            }

            // get rows count
            rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // check rows count on server and on each client
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                // get all addresses
                var serverAddresses = await ctx.Address.AsNoTracking().ToListAsync();

                foreach (var client in Clients)
                {
                    Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));

                    using var cliCtx = new AdventureWorksContext(client, this.UseFallbackSchema);
                    // get all addresses
                    var clientAddresses = await cliCtx.Address.AsNoTracking().ToListAsync();

                    // check row count
                    Assert.Equal(serverAddresses.Count, clientAddresses.Count);

                    foreach (var clientAddress in clientAddresses)
                    {
                        var serverAddress = serverAddresses.First(a => a.AddressId == clientAddress.AddressId);

                        // check column value
                        Assert.Equal(serverAddress.StateProvince, clientAddress.StateProvince);
                        Assert.Equal(serverAddress.AddressLine1, clientAddress.AddressLine1);
                        Assert.Equal(serverAddress.AddressLine2, clientAddress.AddressLine2);
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
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // get rows count
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables));

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
                Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));
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
                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables));

                var s = await agent.SynchronizeAsync();

                Assert.Equal(download++, s.TotalChangesDownloaded);
                Assert.Equal(1, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

            // Now sync again to be sure all clients have all lines
            foreach (var client in Clients)
            {
                await new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables)).SynchronizeAsync();
            }

            rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // check rows count on server and on each client
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                // get all addresses
                var serverAddresses = await ctx.Address.AsNoTracking().ToListAsync();

                foreach (var client in Clients)
                {
                    Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));

                    using var cliCtx = new AdventureWorksContext(client, this.UseFallbackSchema);
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

        /// <summary>
        /// Update one row on server, should be correctly sync on all clients
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Update_NullValue_FromServer(SyncOptions options)
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // get rows count
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables));

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
                Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));
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
                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables));

                var s = await agent.SynchronizeAsync();

                Assert.Equal(1, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);

                // check row updated values
                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
                var cliAddress = await ctx.Address.AsNoTracking().SingleAsync(a => a.AddressId == 1);
                Assert.Null(cliAddress.AddressLine2);
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
                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables));

                var s = await agent.SynchronizeAsync();

                Assert.Equal(1, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);

                // check row updated values
                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
                var cliAddress = await ctx.Address.AsNoTracking().SingleAsync(a => a.AddressId == 1);
                Assert.Equal("NoT a null value !", cliAddress.AddressLine2);
            }

            rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            foreach (var client in Clients)
                Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));

        }

        /// <summary>
        /// Delete rows on server, should be correctly sync on all clients
        /// </summary>
        [Theory]
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
                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables));

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
                Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));

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
                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables));

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

            rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            foreach (var client in Clients)
                Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));


        }

        /// <summary>
        /// </summary>
        [Fact]
        public async Task Using_ExistingClientDatabase_ProvisionDeprovision()
        {
            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // generate a sync conf to host the schema
            var setup = new SyncSetup(this.Tables);

            // options
            var options = new SyncOptions();

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                // create a client schema without seeding
                await this.EnsureDatabaseSchemaAndSeedAsync(client, false, UseFallbackSchema);

                var localOrchestrator = new LocalOrchestrator(client.Provider, options, setup);
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
                foreach (var setupTable in setup.Tables)
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

                        // No filters here
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(setupTable, DbStoredProcedureType.SelectChangesWithFilters));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(setupTable, DbStoredProcedureType.SelectInitializedChangesWithFilters));
                    }

                }

                //localOrchestrator.OnTableProvisioned(null);

                //// Deprovision the database with all tracking tables, stored procedures, triggers and scope
                await localOrchestrator.DeprovisionAsync(schema, provision);

                // check if scope table is correctly created
                scopeInfoTableExists = await localOrchestrator.ExistScopeInfoTableAsync(DbScopeType.Client, options.ScopeInfoTableName);
                Assert.False(scopeInfoTableExists);

                // get the db manager
                foreach (var setupTable in setup.Tables)
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

                        // No filters here
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(setupTable, DbStoredProcedureType.SelectChangesWithFilters));
                        Assert.False(await localOrchestrator.ExistStoredProcedureAsync(setupTable, DbStoredProcedureType.SelectInitializedChangesWithFilters));
                    }

                }


            }
        }

        /// <summary>
        /// Check foreign keys existence
        /// </summary>
        [Fact]
        public async Task Check_Composite_ForeignKey_Existence()
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, Tables);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);

                using var connection = client.Provider.CreateConnection();
                await connection.OpenAsync();
                using var transaction = connection.BeginTransaction();

                var schema = await agent.LocalOrchestrator.GetSchemaAsync(connection, transaction);

                var tablePricesListCategory = agent.LocalOrchestrator.GetTableBuilder(schema.Tables["PricesListCategory"], agent.Setup);
                Assert.NotNull(tablePricesListCategory);

                var relations = (await tablePricesListCategory.GetRelationsAsync(connection, transaction)).ToList();
                Assert.Single(relations);

                if (client.ProviderType != ProviderType.Sqlite)
                    Assert.StartsWith("FK_PricesListCategory_PricesList_PriceListId", relations[0].ForeignKey);

                Assert.Single(relations[0].Columns);

                var tablePricesListDetail = agent.LocalOrchestrator.GetTableBuilder(schema.Tables["PricesListDetail"], agent.Setup);

                Assert.NotNull(tablePricesListDetail);

                var relations2 = (await tablePricesListDetail.GetRelationsAsync(connection, transaction)).ToArray();
                Assert.Single(relations2);

                if (client.ProviderType != ProviderType.Sqlite)
                    Assert.StartsWith("FK_PricesListDetail_PricesListCategory_PriceListId", relations2[0].ForeignKey);

                Assert.Equal(2, relations2[0].Columns.Count);

                var tableEmployeeAddress = agent.LocalOrchestrator.GetTableBuilder(schema.Tables["EmployeeAddress"], agent.Setup);
                Assert.NotNull(tableEmployeeAddress);

                var relations3 = (await tableEmployeeAddress.GetRelationsAsync(connection, transaction)).ToArray();
                Assert.Equal(2, relations3.Count());

                if (client.ProviderType != ProviderType.Sqlite)
                {
                    Assert.StartsWith("FK_EmployeeAddress_Address_AddressID", relations3[0].ForeignKey);
                    Assert.StartsWith("FK_EmployeeAddress_Employee_EmployeeID", relations3[1].ForeignKey);

                }
                Assert.Single(relations3[0].Columns);
                Assert.Single(relations3[1].Columns);

                transaction.Commit();
                connection.Close();
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
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables));

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
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
                var priceList = new PriceList { PriceListId = Guid.NewGuid(), Description = HelperDatabase.GetRandomName() };
                ctx.Add(priceList);
                await ctx.SaveChangesAsync();
            }

            // Sync all clients
            // First client  will upload 3 lines and will download nothing
            // Second client will upload 3 lines and will download 3 lines
            // thrid client  will upload 3 lines and will download 6 lines
            int download = 0;
            foreach (var client in Clients)
            {

                // Sleep during a selecting changes on first sync
                void tableChangesSelected(TableChangesSelectedArgs changes)
                {
                    if (changes.TableChangesSelected.TableName != "PricesList")
                        return;

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
                    return;
                };

                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables));

                // Intercept TableChangesSelected
                agent.LocalOrchestrator.OnTableChangesSelected(tableChangesSelected);

                var s = await agent.SynchronizeAsync();

                agent.LocalOrchestrator.OnTableChangesSelected(null);

                Assert.Equal(download, s.TotalChangesDownloaded);
                Assert.Equal(3, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
                download += 3;

            }

            // CLI1 (6 rows) : CLI1 will upload 1 row and download 3 rows from CLI2 and 3 rows from CLI3
            // CLI2 (4 rows) : CLI2 will upload 1 row and download 3 rows from CLI3 and 1 row from CLI1
            // CLI3 (2 rows) : CLI3 will upload 1 row and download 1 row from CLI1 and 1 row from CLI2
            download = 3 * (Clients.Count - 1);
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables));

                var s = await agent.SynchronizeAsync();

                Assert.Equal(download, s.TotalChangesDownloaded);
                Assert.Equal(1, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
                download -= 2;
            }


            // CLI1 (6) : CLI1 will download 1 row from CLI3 and 1 rows from CLI2
            // CLI2 (4) : CLI2 will download 1 row from CLI3
            // CLI3 (2) : CLI3 will download nothing
            download = Clients.Count - 1;
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables));

                var s = await agent.SynchronizeAsync();

                Assert.Equal(download--, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

            // check rows count on server and on each client
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                var productRowCount = await ctx.Product.AsNoTracking().CountAsync();
                var productCategoryCount = await ctx.ProductCategory.AsNoTracking().CountAsync();
                var priceListCount = await ctx.PricesList.AsNoTracking().CountAsync();
                var rowsCount = this.GetServerDatabaseRowsCount(this.Server);
                foreach (var client in Clients)
                {
                    Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));

                    using var cliCtx = new AdventureWorksContext(client, this.UseFallbackSchema);
                    var pCount = await cliCtx.Product.AsNoTracking().CountAsync();
                    Assert.Equal(productRowCount, pCount);

                    var pcCount = await cliCtx.ProductCategory.AsNoTracking().CountAsync();
                    Assert.Equal(productCategoryCount, pcCount);

                    var plCount = await cliCtx.PricesList.AsNoTracking().CountAsync();
                    Assert.Equal(priceListCount, plCount);
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
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
                await new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables)).SynchronizeAsync();

            // Insert one thousand lines on each client
            foreach (var client in Clients)
            {
                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);

                for (var i = 0; i < 1000; i++)
                {
                    var name = HelperDatabase.GetRandomName();
                    var productNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

                    var product = new Product { ProductId = Guid.NewGuid(), Name = name, ProductNumber = productNumber };

                    ctx.Product.Add(product);
                }
                await ctx.SaveChangesAsync();
            }

            // Sync all clients
            // First client  will upload 1000 lines and will download nothing
            // Second client will upload 1000 lines and will download 1000 lines
            // Third client  will upload 1000 line and will download 3000 lines
            int download = 0;
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables));

                var s = await agent.SynchronizeAsync();

                Assert.Equal(download * 1000, s.TotalChangesDownloaded);
                Assert.Equal(1000, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);

                download++;

                var innerRowsCount = this.GetServerDatabaseRowsCount(this.Server);
                Assert.Equal(innerRowsCount, this.GetServerDatabaseRowsCount(client));
            }

            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);
            foreach (var client in Clients)
            {
                await new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables)).SynchronizeAsync();
                Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));
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
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

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
                await new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables)).SynchronizeAsync();


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
                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables));

                var s = await agent.SynchronizeAsync();

                Assert.Equal(2, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
                Assert.Equal(2, this.GetServerDatabaseRowsCount(client));
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

            // Sync all clients. Should not raise an error, because we disable constraint check
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables));

                var s = await agent.SynchronizeAsync();

                Assert.Equal(2, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);

                // we removed a product category
                Assert.Equal(1, this.GetServerDatabaseRowsCount(client));
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
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

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
                await new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables)).SynchronizeAsync();


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
                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables));

                var s = await agent.SynchronizeAsync();

                Assert.Equal(2, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
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
                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables));

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

                    if (client.ProviderType == ProviderType.MySql || client.ProviderType == ProviderType.MariaDB)
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


                    if (client.ProviderType == ProviderType.MySql || client.ProviderType == ProviderType.MariaDB)
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
                Assert.Equal(0, s.TotalResolvedConflicts);

                agent.LocalOrchestrator.OnDatabaseChangesApplying(null);
                agent.LocalOrchestrator.OnDatabaseChangesApplied(null);
                agent.LocalOrchestrator.OnConnectionOpen(null);

            }

            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);
            foreach (var client in Clients)
                Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));
        }


        /// <summary>
        /// Insert one row on each client, should be sync on server and clients
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Reinitialize_Client(SyncOptions options)
        {
            // create a server schema with seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables));

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);

                Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));
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
                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables));

                var s = await agent.SynchronizeAsync(SyncType.Reinitialize);

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);

                Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));
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
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables));

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
                Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));
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
                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables));

                var s = await agent.SynchronizeAsync(SyncType.ReinitializeWithUpload);

                Assert.Equal(rowsCount + download, s.TotalChangesDownloaded);
                Assert.Equal(2, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
                download += 2;
            }

            rowsCount = this.GetServerDatabaseRowsCount(this.Server);
            foreach (var client in Clients)
            {
                await new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables)).SynchronizeAsync().ConfigureAwait(false);
                Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));
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
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Set Employee, Address, EmployeeAddress to Upload only
            // All others are Bidirectional by default.
            var setup = new SyncSetup(Tables);
            setup.Tables["Employee"].SyncDirection = SyncDirection.UploadOnly;
            setup.Tables["Address"].SyncDirection = SyncDirection.UploadOnly;
            setup.Tables["EmployeeAddress"].SyncDirection = SyncDirection.UploadOnly;


            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, new SyncOptions(), setup);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

            // Insert one line on each client
            int index = 10;
            foreach (var client in Clients)
            {
                // Insert one employee, address, employeeaddress
                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);

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
                var agent = new SyncAgent(client.Provider, Server.Provider, new SyncOptions(), setup);

                var s = await agent.SynchronizeAsync();

                // Server shoud not sent back lines, so download equals 1 (just product category)
                Assert.Equal(1, s.TotalChangesDownloaded);
                Assert.Equal(3, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
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
                using var cliCtx = new AdventureWorksContext(client, this.UseFallbackSchema);
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

        /// <summary>
        /// Configuring tables to be upload only
        /// Server should receive lines but will not send back its own lines
        /// </summary>
        [Fact]
        public async Task DownloadOnly()
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Set Employee, Address, EmployeeAddress to Upload only
            // All others are Bidirectional by default.
            var setup = new SyncSetup(Tables);
            setup.Tables["Employee"].SyncDirection = SyncDirection.DownloadOnly;
            setup.Tables["Address"].SyncDirection = SyncDirection.DownloadOnly;
            setup.Tables["EmployeeAddress"].SyncDirection = SyncDirection.DownloadOnly;


            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, new SyncOptions(), setup); ;

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

            // Insert one line on each client
            int index = 10;
            foreach (var client in Clients)
            {
                // Insert one employee, address, employeeaddress
                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);

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
                var agent = new SyncAgent(client.Provider, Server.Provider, new SyncOptions(), setup);


                var s = await agent.SynchronizeAsync();

                // Server send lines, but clients don't
                Assert.Equal(4, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
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
                using var cliCtx = new AdventureWorksContext(client, this.UseFallbackSchema);
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


        /// <summary>
        /// Deleting a client row and sync, let the tracking table row on the client database
        /// When downloading the same row from server, the tracking table should be aligned with this new row
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Delete_OneTable_FromClient(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Execute a sync on all clients to initialize client and server schema 
            foreach (var client in Clients)
                await new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables)).SynchronizeAsync();

            // Insert a new product category on each client
            foreach (var client in Clients)
            {
                // Insert product category on each client
                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
                var productCategoryId = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 6);
                var productCategoryNameClient = HelperDatabase.GetRandomName("CLI_");

                ctx.Add(new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryNameClient });
                await ctx.SaveChangesAsync();

            }

            // Execute a sync on all clients to not avoid any conflict
            var download = 0;
            foreach (var client in Clients)
            {
                var s = await new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables)).SynchronizeAsync();

                Assert.Equal(download, s.TotalChangesDownloaded);
                Assert.Equal(download, s.TotalChangesApplied);
                Assert.Equal(1, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
                download++;
            }

            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);
            // Execute a sync on all clients to be sure all clients have download all others clients product
            foreach (var client in Clients)
            {
                await new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables)).SynchronizeAsync();
                Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));
            }


            // Now delete rows on each client
            foreach (var client in Clients)
            {
                // Then delete all product category items
                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
                foreach (var pc in ctx.ProductCategory)
                    ctx.ProductCategory.Remove(pc);
                await ctx.SaveChangesAsync();
            }

            var cpt = 0; // first client won't have any conflicts, but others will upload their deleted rows that are ALREADY deleted
            foreach (var client in Clients)
            {
                var s = await new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables)).SynchronizeAsync();

                // we may download deleted rows from server
                Assert.Equal(cpt, s.TotalChangesDownloaded);
                // but we should not have any rows applied locally
                Assert.Equal(0, s.TotalChangesApplied);
                // anyway we are always uploading our deleted rows
                Assert.Equal(Clients.Count, s.TotalChangesUploaded);
                // w may have resolved conflicts locally
                Assert.Equal(cpt, s.TotalResolvedConflicts);

                cpt = Clients.Count;
            }

            // check rows count on server and on each client
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                // get all product categories
                var serverPC = await ctx.ProductCategory.AsNoTracking().CountAsync();
                Assert.Equal(0, serverPC);

                foreach (var client in Clients)
                {
                    using var cliCtx = new AdventureWorksContext(client, this.UseFallbackSchema);
                    // get all product categories
                    var clientPC = await cliCtx.ProductCategory.AsNoTracking().CountAsync();

                    // check row count
                    Assert.Equal(0, clientPC);
                }
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

            var setup = new SyncSetup(Tables);

            // snapshot directory
            var snapshotDirctory = HelperDatabase.GetRandomName();
            var directory = Path.Combine(Environment.CurrentDirectory, snapshotDirctory);

            var options = new SyncOptions
            {
                SnapshotsDirectory = directory,
                BatchSize = 3000
            };

            var remoteOrchestrator = new RemoteOrchestrator(Server.Provider, options, setup);

            // ----------------------------------
            // Create a snapshot
            // ----------------------------------
            await remoteOrchestrator.CreateSnapshotAsync();

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
                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables));

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);

                Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));
            }
        }



        [Fact]
        public async Task Serialize_And_Deserialize()
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            var scopeName = "scopesnap1";

            var myRijndael = new RijndaelManaged();
            myRijndael.GenerateKey();
            myRijndael.GenerateIV();

            // Create action for serializing and deserialzing for both remote and local orchestrators
            var deserializing = new Action<DeserializingSetArgs>(dsa =>
            {
                // Create an encryptor to perform the stream transform.
                var decryptor = myRijndael.CreateDecryptor(myRijndael.Key, myRijndael.IV);

                using var csDecrypt = new CryptoStream(dsa.FileStream, decryptor, CryptoStreamMode.Read);
                using var swDecrypt = new StreamReader(csDecrypt);
                //Read all data to the ContainerSet
                var str = swDecrypt.ReadToEnd();
                dsa.Result = JsonConvert.DeserializeObject<ContainerSet>(str);
            });


            var serializing = new Action<SerializingSetArgs>(ssa =>
            {
                // Create an encryptor to perform the stream transform.
                var encryptor = myRijndael.CreateEncryptor(myRijndael.Key, myRijndael.IV);

                using var msEncrypt = new MemoryStream();
                using var csEncrypt = new CryptoStream(msEncrypt, encryptor, CryptoStreamMode.Write);
                using (var swEncrypt = new StreamWriter(csEncrypt))
                {
                    //Write all data to the stream.
                    var strSet = JsonConvert.SerializeObject(ssa.Set);
                    swEncrypt.Write(strSet);
                }
                ssa.Result = msEncrypt.ToArray();

            });

            foreach (var client in this.Clients)
            {
                // Defining options with Batchsize to enable serialization on disk
                var options = new SyncOptions { BatchSize = 1000 };

                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables), scopeName);

                // Get the orchestrators
                var localOrchestrator = agent.LocalOrchestrator;
                var remoteOrchestrator = agent.RemoteOrchestrator;

                // Encrypting / decrypting data on disk
                localOrchestrator.OnSerializingSet(serializing);
                localOrchestrator.OnDeserializingSet(deserializing);
                remoteOrchestrator.OnSerializingSet(serializing);
                remoteOrchestrator.OnDeserializingSet(deserializing);

                // Making a first sync, will initialize everything we need
                var result = await agent.SynchronizeAsync();

                Assert.Equal(GetServerDatabaseRowsCount(this.Server), result.TotalChangesDownloaded);

                // Client side : Create a product category and a product
                // Create a productcategory item
                // Create a new product on server
                var productId = Guid.NewGuid();
                var productName = HelperDatabase.GetRandomName();
                var productNumber = productName.ToUpperInvariant().Substring(0, 10);

                var productCategoryName = HelperDatabase.GetRandomName();
                var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

                using (var ctx = new AdventureWorksContext(client, this.UseFallbackSchema))
                {
                    var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                    ctx.Add(pc);

                    var product = new Product { ProductId = productId, Name = productName, ProductNumber = productNumber };
                    ctx.Add(product);

                    await ctx.SaveChangesAsync();
                }

                // Making a first sync, will initialize everything we need
                var r = await agent.SynchronizeAsync();

                Assert.Equal(2, r.TotalChangesUploaded);
            }
        }


        [Fact]
        public async Task IsOutdated_ShouldWork_If_Correct_Action()
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            var scopeName = "scopesnap1";


            foreach (var client in this.Clients)
            {
                // Defining options with Batchsize to enable serialization on disk
                var options = new SyncOptions { BatchSize = 1000 };

                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables), scopeName);

                // Making a first sync, will initialize everything we need
                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

            foreach (var client in this.Clients)
            {

                // Defining options with Batchsize to enable serialization on disk
                var options = new SyncOptions { BatchSize = 1000 };

                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables), scopeName);

                // Call a server delete metadata to update the last valid timestamp value in scope_info_server table
                var dmc = await agent.RemoteOrchestrator.DeleteMetadatasAsync();

                // Client side : Create a product category and a product
                var productId = Guid.NewGuid();
                var productName = HelperDatabase.GetRandomName();
                var productNumber = productName.ToUpperInvariant().Substring(0, 10);
                var productCategoryName = HelperDatabase.GetRandomName();
                var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

                using (var ctx = new AdventureWorksContext(client, this.UseFallbackSchema))
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

                // Making a first sync, will initialize everything we need
                var se = await Assert.ThrowsAsync<SyncException>(() => agent.SynchronizeAsync());

                Assert.Equal(SyncSide.ClientSide, se.Side);
                Assert.Equal("OutOfDateException", se.TypeName);

                // Intercept outdated event, and make a reinitialize with upload action
                agent.LocalOrchestrator.OnOutdated(oa => oa.Action = OutdatedAction.ReinitializeWithUpload);

                var r = await agent.SynchronizeAsync();
                var c = GetServerDatabaseRowsCount(this.Server);
                Assert.Equal(c, r.TotalChangesDownloaded);
                Assert.Equal(2, r.TotalChangesUploaded);

                Assert.Equal(c, this.GetServerDatabaseRowsCount(client));


            }
        }


        /// <summary>
        /// Configuring tables to be upload only
        /// Server should receive lines but will not send back its own lines
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Change_Bidirectional_To_UploadOnly_ShouldWork(SyncOptions options)
        {
            // create a server schema without seeding
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            var setup = new SyncSetup(this.Tables) { StoredProceduresPrefix = "cli", StoredProceduresSuffix = "", TrackingTablesPrefix = "tr" };

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            int addressesCount = 0;
            int employeesAddressesCount = 0;
            int employeesCount = 0;
            int productCategoriesCount = 0;

            // check rows count on server and on each client
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                addressesCount = ctx.Address.AsNoTracking().Count();
                employeesAddressesCount = ctx.EmployeeAddress.AsNoTracking().Count();
                employeesCount = ctx.Employee.AsNoTracking().Count();
                productCategoriesCount = ctx.ProductCategory.AsNoTracking().Count();
            }


            // Execute a sync on all clients and check results
            foreach (var client in this.Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, options, setup);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));
            }

            // Change Employee, Address, EmployeeAddress to Upload only
            // All others stay Bidirectional
            setup.Tables["Employee"].SyncDirection = SyncDirection.UploadOnly;
            setup.Tables["Address"].SyncDirection = SyncDirection.UploadOnly;
            setup.Tables["EmployeeAddress"].SyncDirection = SyncDirection.UploadOnly;

            // Insert one line on each client
            int index = 10;
            foreach (var client in Clients)
            {
                // Insert one employee, address, employeeaddress
                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);

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
                var agent = new SyncAgent(client.Provider, Server.Provider, options, setup);

                var s = await agent.SynchronizeAsync();

                // Server shoud not sent back lines, so download equals 1 (just product category)
                Assert.Equal(1, s.TotalChangesDownloaded);
                Assert.Equal(3, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);
            }

            var remoteOrchestrator = new RemoteOrchestrator(Server.Provider, options, setup);
            var remoteScope = await remoteOrchestrator.GetServerScopeAsync();

            foreach (var client in Clients)
            {
                var localOrchestrator = new LocalOrchestrator(client.Provider, options, setup);
                var localScope = await localOrchestrator.GetClientScopeAsync();

                Assert.True(localScope.Setup.Equals(remoteScope.Setup));
            }



            // check rows count on server and on each client
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                // get all product categories
                var serverPC = ctx.ProductCategory.AsNoTracking().Count();
                Assert.Equal(productCategoriesCount + 1, serverPC);

                // get all employees
                var employees = ctx.Employee.AsNoTracking().Count();
                Assert.Equal(employeesCount + Clients.Count + 1, employees);
                // get all employees address
                var employeesAddresses = ctx.EmployeeAddress.AsNoTracking().Count();
                Assert.Equal(employeesAddressesCount + Clients.Count + 1, employeesAddresses);
                // get all addresses
                var addresses = ctx.Address.AsNoTracking().Count();
                Assert.Equal(addressesCount + Clients.Count + 1, addresses);

            }

            foreach (var client in Clients)
            {
                using var cliCtx = new AdventureWorksContext(client, this.UseFallbackSchema);
                // get all product categories
                var clientPC = cliCtx.ProductCategory.AsNoTracking().Count();
                Assert.Equal(productCategoriesCount + 1, clientPC);

                // get all employees
                var employees = cliCtx.Employee.AsNoTracking().Count();
                Assert.Equal(employeesCount + 1, employees);

                // get all employees address
                var employeesAddresses = cliCtx.EmployeeAddress.AsNoTracking().Count();
                Assert.Equal(employeesAddressesCount + 1, employeesAddresses);

                // get all addresses
                var addresses = cliCtx.Address.AsNoTracking().Count();
                Assert.Equal(addressesCount + 1, addresses);
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

            var setup = new SyncSetup(Tables);

            // snapshot directory
            var snapshotDirctory = HelperDatabase.GetRandomName();
            var directory = Path.Combine(Environment.CurrentDirectory, snapshotDirctory);

            var options = new SyncOptions
            {
                SnapshotsDirectory = directory,
                BatchSize = 3000
            };

            var remoteOrchestrator = new RemoteOrchestrator(Server.Provider, options, setup);

            // ----------------------------------
            // Create a snapshot
            // ----------------------------------
            await remoteOrchestrator.CreateSnapshotAsync();

            // ----------------------------------
            // Add rows on server AFTER snapshot
            // ----------------------------------

            using (var ctx = new AdventureWorksContext(this.Server))
            {
                var productId = Guid.NewGuid();
                var productName = HelperDatabase.GetRandomName();
                var productNumber = productName.ToUpperInvariant().Substring(0, 10);

                var productCategoryName = HelperDatabase.GetRandomName();
                var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

                var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                ctx.Add(pc);

                var product = new Product { ProductId = productId, Name = productName, ProductNumber = productNumber };
                ctx.Add(product);

                await ctx.SaveChangesAsync();
            }

            using (var ctx = new AdventureWorksContext(this.Server))
            {
                var productId = Guid.NewGuid();
                var productName = HelperDatabase.GetRandomName();
                var productNumber = productName.ToUpperInvariant().Substring(0, 10);

                var productCategoryName = HelperDatabase.GetRandomName();
                var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

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
                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables));

                var s = await agent.SynchronizeAsync();

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);

                Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));

            }

            // ----------------------------------
            // Now add rows on client
            // ----------------------------------

            foreach (var client in Clients)
            {
                var name = HelperDatabase.GetRandomName();
                var pn = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

                var product = new Product { ProductId = Guid.NewGuid(), Name = name, ProductNumber = pn };

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
                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables));

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
                var agent = new SyncAgent(client.Provider, Server.Provider, options, new SyncSetup(Tables));

                var s = await agent.SynchronizeAsync(SyncType.Reinitialize);

                Assert.Equal(rowsCount, s.TotalChangesDownloaded);
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);

                Assert.Equal(rowsCount, this.GetServerDatabaseRowsCount(client));
            }
        }


        /// <summary>
        /// Insert one row on server, should be correctly sync on all clients
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async Task Parallel_Sync_For_TwentyClients(SyncOptions options)
        {
            // create a server database
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, true, UseFallbackSchema);

            // Get count of rows
            var rowsCount = this.GetServerDatabaseRowsCount(this.Server);

            // Provision server, to be sure no clients will try to do something that could break server
            var remoteOrchestrator = new RemoteOrchestrator(this.Server.Provider, options, new SyncSetup(Tables));

            // Ensure schema is ready on server side. Will create everything we need (triggers, tracking, stored proc, scopes)
            var scope = await remoteOrchestrator.EnsureSchemaAsync();

            var providers = this.Clients.Select(c => c.ProviderType).Distinct();

            var clientProviders = new List<CoreProvider>();
            var createdDatabases = new List<(ProviderType ProviderType, string DatabaseName)>();
            foreach (var provider in providers)
            {
                for (int i = 0; i < 10; i++)
                {
                    // Create the provider
                    var dbCliName = HelperDatabase.GetRandomName("tcp_cli_");
                    var localProvider = this.CreateProvider(provider, dbCliName);

                    clientProviders.Add(localProvider);

                    // Create the database
                    await this.CreateDatabaseAsync(provider, dbCliName, true);

                    createdDatabases.Add((provider, dbCliName));
                }
            }

            var allTasks = new List<Task<SyncResult>>();

            // Execute a sync on all clients and add the task to a list of tasks
            foreach (var clientProvider in clientProviders)
            {
                var agent = new SyncAgent(clientProvider, Server.Provider, options, new SyncSetup(Tables));
                allTasks.Add(agent.SynchronizeAsync());
            }

            // Await all tasks
            await Task.WhenAll(allTasks);

            foreach (var s in allTasks)
            {
                Assert.Equal(rowsCount, s.Result.TotalChangesDownloaded);
                Assert.Equal(0, s.Result.TotalChangesUploaded);
                Assert.Equal(0, s.Result.TotalResolvedConflicts);
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

            allTasks = new List<Task<SyncResult>>();

            // Execute a sync on all clients to get the new server row
            foreach (var clientProvider in clientProviders)
            {
                var agent = new SyncAgent(clientProvider, Server.Provider, options, new SyncSetup(Tables));
                allTasks.Add(agent.SynchronizeAsync());
            }

            // Await all tasks
            await Task.WhenAll(allTasks);

            foreach (var s in allTasks)
            {
                Assert.Equal(1, s.Result.TotalChangesDownloaded);
                Assert.Equal(0, s.Result.TotalChangesUploaded);
                Assert.Equal(0, s.Result.TotalResolvedConflicts);
            }

            foreach (var db in createdDatabases)
            {
                HelperDatabase.DropDatabase(db.ProviderType, db.DatabaseName);
            }
        }


        /// <summary>
        /// Testing an insert / update on a table where a column is not part of the sync setup, and should stay alive after a sync
        /// </summary>
        [Theory]
        [ClassData(typeof(SyncOptionsData))]
        public async virtual Task OneColumn_NotInSetup_ShouldNotBe_UploadToServer(SyncOptions options)
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
                setup.Tables["Address"].Columns.AddRange(new string[] { "AddressId", "AddressLine1", "AddressLine2", "City", "StateProvince", "CountryRegion", "PostalCode" });

                var agent = new SyncAgent(client.Provider, Server.Provider, options, setup);

                var s = await agent.SynchronizeAsync();

                // Editing Rowguid on client. This column is not part of the setup
                // So far, it should not be uploaded to server
                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);

                var cliAddress = await ctx.Address.SingleAsync(a => a.AddressId == 1);

                // Now Update on client this address with a rowGuid
                cliAddress.Rowguid = clientGuid;

                await ctx.SaveChangesAsync();
            }

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var setup = new SyncSetup(new string[] { "Address" });

                // Add all columns to address except Rowguid and ModifiedDate
                setup.Tables["Address"].Columns.AddRange(new string[] { "AddressId", "AddressLine1", "AddressLine2", "City", "StateProvince", "CountryRegion", "PostalCode" });

                var agent = new SyncAgent(client.Provider, Server.Provider, options, setup);
                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);

                // No upload since Rowguid is not part of SyncSetup (and trigger shoul not add a line)
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);

                // check row on client should not have been updated 
                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
                var cliAddress = await ctx.Address.AsNoTracking().SingleAsync(a => a.AddressId == 1);

                Assert.Equal(clientGuid, cliAddress.Rowguid);
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
        public async virtual Task OneColumn_NotInSetup_AfterCleanMetadata_ShouldNotBe_Tracked_AND_ShouldNotBe_UploadedToServer(SyncOptions options)
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
                setup.Tables["Address"].Columns.AddRange(new string[] { "AddressId", "AddressLine1", "AddressLine2", "City", "StateProvince", "CountryRegion", "PostalCode" });

                var agent = new SyncAgent(client.Provider, Server.Provider, options, setup);

                var s = await agent.SynchronizeAsync();

                // call CleanMetadata to be sure we don't have row in tracking
                var ts = await agent.LocalOrchestrator.GetLocalTimestampAsync();

                // be sure we are deleting ALL rows from tracking table
                var dc = await agent.LocalOrchestrator.DeleteMetadatasAsync(ts + 1);


                // checking if there is no rows in tracking table for address
                var connection = client.Provider.CreateConnection();
                var command = connection.CreateCommand();
                command.CommandText = "SELECT COUNT(*) FROM Address_tracking";
                command.Connection = connection;
                await connection.OpenAsync();
                var count = await command.ExecuteScalarAsync();
                var countRows = Convert.ToInt32(count);
                Assert.Equal(0, countRows);
                await connection.CloseAsync();

                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);

                // Editing Rowguid on client. This column is not part of the setup
                // So far, it should not be uploaded to server
                var cliAddress = await ctx.Address.SingleAsync(a => a.AddressId == 1);

                // Now Update on client this address with a rowGuid
                cliAddress.Rowguid = clientGuid;

                await ctx.SaveChangesAsync();

                // Check again no rows has been inserted
                // this test ensure an existing row does not execute the second part of the UPDATE Trigger
                // if the updated column is not part of the setup
                await connection.OpenAsync();
                count = await command.ExecuteScalarAsync();
                countRows = Convert.ToInt32(count);
                Assert.Equal(0, countRows);
                await connection.CloseAsync();


            }

            // Execute a sync on all clients and check results
            foreach (var client in Clients)
            {
                var setup = new SyncSetup(new string[] { "Address" });

                // Add all columns to address except Rowguid and ModifiedDate
                setup.Tables["Address"].Columns.AddRange(new string[] { "AddressId", "AddressLine1", "AddressLine2", "City", "StateProvince", "CountryRegion", "PostalCode" });

                var agent = new SyncAgent(client.Provider, Server.Provider, options, setup);

                var s = await agent.SynchronizeAsync();

                Assert.Equal(0, s.TotalChangesDownloaded);

                // No upload since Rowguid is not part of SyncSetup (and trigger shoul not add a line)
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);

                // check row on client should not have been updated 
                using var ctx = new AdventureWorksContext(client, this.UseFallbackSchema);
                var cliAddress = await ctx.Address.AsNoTracking().SingleAsync(a => a.AddressId == 1);

                Assert.Equal(clientGuid, cliAddress.Rowguid);
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
                setup.Tables["Address"].Columns.AddRange(new string[] { "AddressId", "AddressLine1", "AddressLine2", "City", "StateProvince", "CountryRegion", "PostalCode" });

                var agent = new SyncAgent(client.Provider, Server.Provider, options, setup);

                var s = await agent.SynchronizeAsync();

                // Editing Rowguid on client. This column is not part of the setup
                // So far, it should not be uploaded to server
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
                var setup = new SyncSetup(new string[] { "Address" });

                // Add all columns to address except Rowguid and ModifiedDate
                setup.Tables["Address"].Columns.AddRange(new string[] { "AddressId", "AddressLine1", "AddressLine2", "City", "StateProvince", "CountryRegion", "PostalCode" });

                var agent = new SyncAgent(client.Provider, Server.Provider, options, setup);
                var s = await agent.SynchronizeAsync();

                // "Mimecity" change should be received from server
                Assert.Equal(1, s.TotalChangesDownloaded);

                // No upload since Rowguid is not part of SyncSetup (and trigger shoul not add a line)
                Assert.Equal(0, s.TotalChangesUploaded);
                Assert.Equal(0, s.TotalResolvedConflicts);

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
        /// Testing if blob are consistent across sync
        /// </summary>
        [Fact]
        public virtual async Task Blob_ShouldBeConsistent_AndSize_ShouldBeMaintained()
        {
            // create a server db and seed it
            await this.EnsureDatabaseSchemaAndSeedAsync(this.Server, false, UseFallbackSchema);

            // create empty client databases
            foreach (var client in this.Clients)
                await this.CreateDatabaseAsync(client.ProviderType, client.DatabaseName, true);

            // Execute a sync to initialize all clients
            foreach (var client in this.Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, new SyncOptions(),
                    new SyncSetup(this.Tables) { StoredProceduresPrefix = "cli", StoredProceduresSuffix = "", TrackingTablesPrefix = "tr" });

                var s = await agent.SynchronizeAsync();
            }

            // Create a new product on server with a big thumbnail photo
            var name = HelperDatabase.GetRandomName();
            var productNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

            var product = new Product
            {
                ProductId = Guid.NewGuid(),
                Name = name,
                ProductNumber = productNumber,
                ThumbNailPhoto = new byte[20000]
            };

            using (var serverDbCtx = new AdventureWorksContext(this.Server))
            {
                serverDbCtx.Product.Add(product);
                await serverDbCtx.SaveChangesAsync();
            }

            // Create a new product on client with a big thumbnail photo
            foreach (var client in this.Clients)
            {
                var clientName = HelperDatabase.GetRandomName();
                var clientProductNumber = HelperDatabase.GetRandomName().ToUpperInvariant().Substring(0, 10);

                var clientProduct = new Product
                {
                    ProductId = Guid.NewGuid(),
                    Name = clientName,
                    ProductNumber = clientProductNumber,
                    ThumbNailPhoto = new byte[20000]
                };

                using (var clientDbCtx = new AdventureWorksContext(client, UseFallbackSchema))
                {
                    clientDbCtx.Product.Add(product);
                    await clientDbCtx.SaveChangesAsync();
                }
            }
            // Two sync to be sure all clients have all rows from all
            foreach (var client in this.Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, new SyncOptions(),
                    new SyncSetup(this.Tables) { StoredProceduresPrefix = "cli", StoredProceduresSuffix = "", TrackingTablesPrefix = "tr" });

                await agent.SynchronizeAsync();
            }
            foreach (var client in this.Clients)
            {
                var agent = new SyncAgent(client.Provider, Server.Provider, new SyncOptions(),
                    new SyncSetup(this.Tables) { StoredProceduresPrefix = "cli", StoredProceduresSuffix = "", TrackingTablesPrefix = "tr" });

                await agent.SynchronizeAsync();
            }


            // check rows count on server and on each client
            using (var ctx = new AdventureWorksContext(this.Server))
            {
                var products = await ctx.Product.AsNoTracking().ToListAsync();
                foreach (var p in products)
                {
                    Assert.Equal(20000, p.ThumbNailPhoto.Length);
                }

            }
            
            foreach (var client in Clients)
            {
                using var cliCtx = new AdventureWorksContext(client, this.UseFallbackSchema);

                var products = await cliCtx.Product.AsNoTracking().ToListAsync();
                foreach (var p in products)
                {
                    Assert.Equal(20000, p.ThumbNailPhoto.Length);
                }
            }
        }

    }
}
