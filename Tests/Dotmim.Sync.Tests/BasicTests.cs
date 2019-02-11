using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Test.Misc;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Models;
using Microsoft.EntityFrameworkCore;
using System;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Dotmim.Sync.Tests
{

    /// <summary>
    /// This abstract class implements all the required tests.
    /// All the tests are agonistic to any provider 
    /// This class does not use XUnit annotations, since it can't be used as is.
    /// Each provider should inherits from this class and override methods, with XUnit annotations
    /// </summary>
    public abstract class BasicTestsBase
    {
        // Runner for my tests, that will run each tests on each client and on tcp then http
        protected readonly TestRunner testRunner;

        // abstract fixture used to run the tests
        protected readonly ProviderFixture fixture;


        protected virtual AdventureWorksContext GetServerDbContext() => new AdventureWorksContext(this.fixture);

        protected virtual AdventureWorksContext GetClientDbContext(ProviderRun providerRun) => new AdventureWorksContext(providerRun);

        protected virtual AdventureWorksContext GetClientDbContext(ProviderRun providerRun, DbConnection connection) => new AdventureWorksContext(providerRun, connection);

        public static Action<ProviderFixture> Configure { get; set; }
        private static bool isConfigured = false;

        private static void OnConfigure(ProviderFixture fixture)
        {
            if (isConfigured)
                return;

            // launch fixture configuration on first launch
            Configure?.Invoke(fixture);

            // Configure fixture
            fixture.Configure();

            isConfigured = true;

        }


        /// <summary>
        /// on ctor, set the tables we want to use
        /// </summary>
        public BasicTestsBase(ProviderFixture fixture)
        {
            this.fixture = fixture;

            // Configure this tests
            OnConfigure(fixture);

            // create a test runner based on my server fixture
            this.testRunner = new TestRunner(fixture, this.fixture.ServerProvider);


        }


        public virtual async Task CheckHealthDatabase()
        {
            if (this.fixture.ProviderType == ProviderType.Sql)
            {
                var serverDbConnectioString = HelperDB.GetConnectionString(this.fixture.ProviderType, this.fixture.DatabaseName);

                using (var sqlConnection = new SqlConnection(serverDbConnectioString))
                {
                    using (var sqlCommand = new SqlCommand())
                    {
                        var commandText =
                                $"Select tbl.name as tableName,  col.name as columnName, typ.name as [type], col.max_length " +
                                $"from sys.columns as col " +
                                $"Inner join sys.tables as tbl on tbl.object_id = col.object_id " +
                                $"Inner Join sys.systypes typ on typ.xusertype = col.system_type_id " +
                                $"Left outer join sys.indexes ind on ind.object_id = col.object_id and ind.index_id = col.column_id";

                        sqlCommand.Connection = sqlConnection;
                        sqlCommand.CommandText = commandText;

                        await sqlConnection.OpenAsync();
                        var dbReader = sqlCommand.ExecuteReader();
                        Console.WriteLine($"Check Health Database on {this.fixture.DatabaseName}");
                        while (dbReader.Read())
                        {
                            var debugLine = $"{(string)dbReader["tableName"]}\t{(string)dbReader["columnName"]}\t{(string)dbReader["type"]}";
                            Console.WriteLine(debugLine);
                            Debug.WriteLine(debugLine);
                        }
                        sqlConnection.Close();
                    }
                }

            }

            Assert.Equal(0, 0);

        }

        /// <summary>
        /// Initialize should be always called.
        /// It creates the clients schemas and make a first sync
        /// Once Initialize() is done, both server and all clients should be equivalent
        /// </summary>
        public virtual async Task Initialize()
        {
            try
            {
                var s = new Action<SyncConfiguration>(c => { });

                var results = await this.testRunner.RunTestsAsync(s);

                foreach (var trr in results)
                {
                    Assert.Equal(this.fixture.RowsCount, trr.Results.TotalChangesDownloaded);
                    Assert.Equal(0, trr.Results.TotalChangesUploaded);
                }

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                throw;
            }

        }

        /// <summary>
        /// Should raise a correct error when a bad connection is defined
        /// </summary>
        public virtual async Task Bad_Server_Connection_Should_Raise_Error()
        {
            var provider = this.fixture.NewServerProvider($@"Server=unknown;Database=unknown;UID=sa;PWD=unknown");
            // create a new runner with a provider with bad connection string
            var tempTestRunner = new TestRunner(this.fixture, this.fixture.ServerProvider);

            var results = await tempTestRunner.RunTestsAsync(false);

            foreach (var trr in results)
            {
                Assert.IsType<SyncException>(trr.Exception);
                var se = trr.Exception as SyncException;
                Assert.Equal(SyncExceptionType.Data, se.Type);
            }

        }

        /// <summary>
        /// Should raise a correct error when a bad connection is defined
        /// </summary>
        public virtual async Task Bad_Client_Connection_Should_Raise_Error()
        {
            // set a bad connection string
            this.fixture.ClientRuns.ForEach(tr => tr.ConnectionString = $@"Server=unknown;Database=unknown;UID=sa;PWD=unknown");

            var results = await this.testRunner.RunTestsAsync(false);

            foreach (var trr in results)
            {
                Assert.IsType<SyncException>(trr.Exception);
                var se = trr.Exception as SyncException;
            }
        }

        /// <summary>
        /// Insert one row on server, should be correctly sync on all clients
        /// </summary>
        public virtual async Task Insert_One_Table_From_Server()
        {
            foreach (var conf in TestConfigurations.GetConfigurations())
            {
                var name = Path.GetRandomFileName().Replace(".", "").ToLowerInvariant();
                var productNumber = Path.GetRandomFileName().Replace(".", "").ToUpperInvariant().Substring(0, 10);

                var product = new Product { ProductId = Guid.NewGuid(), Name = name, ProductNumber = productNumber };

                using (var serverDbCtx = this.GetServerDbContext())
                {
                    serverDbCtx.Product.Add(product);
                    await serverDbCtx.SaveChangesAsync();
                }

                var results = await this.testRunner.RunTestsAsync(conf);

                foreach (var trr in results)
                {
                    Assert.Equal(1, trr.Results.TotalChangesDownloaded);
                    Assert.Equal(0, trr.Results.TotalChangesUploaded);
                }
            }
        }

        /// <summary>
        /// Insert one row on each client, should be correctly sync on server and all clients
        /// </summary>
        public virtual async Task Insert_One_Table_From_Client()
        {
            foreach (var conf in TestConfigurations.GetConfigurations())
            {
                foreach (var clientRun in this.fixture.ClientRuns)
                {
                    var name = Path.GetRandomFileName().Replace(".", "");
                    var id = name.ToUpperInvariant().Substring(0, 6);

                    using (var ctx = this.GetClientDbContext(clientRun))
                    {
                        var pc = new ProductCategory { Name = name, ProductCategoryId = id };
                        ctx.ProductCategory.Add(pc);
                        await ctx.SaveChangesAsync();
                    }
                }

                // first sync. each client will send its own row to the server
                // so the first client won't get anything from the second
                // the last one will have all the rows, as well as the server
                var results = await this.testRunner.RunTestsAsync(conf);

                for (var i = 0; i < this.fixture.ClientRuns.Count; i++)
                {
                    var testRunner = this.fixture.ClientRuns[i];

                    Assert.Equal(0 + i, testRunner.Results.TotalChangesDownloaded);
                    Assert.Equal(1, testRunner.Results.TotalChangesUploaded);
                }

                // now a last sync to be sure all clients have all rows
                var results2 = await this.testRunner.RunTestsAsync(conf);

                // check rows count on server
                var productCategoryRowCount = 0;
                using (var serverDbCtx = this.GetServerDbContext())
                {
                    productCategoryRowCount = await serverDbCtx.ProductCategory.AsNoTracking().CountAsync();
                }

                // check rows number on all product category table
                foreach (var clientRun in this.fixture.ClientRuns)
                {
                    using (var ctx = this.GetClientDbContext(clientRun))
                    {
                        var rowCount = await ctx.ProductCategory.AsNoTracking().CountAsync();
                        Assert.Equal(productCategoryRowCount, rowCount);
                    }
                }
            }
        }

        /// <summary>
        /// Insert several rows on server, with a foreign key involved, should be correctly sync on all clients
        /// </summary>
        public virtual async Task Insert_Multiple_Tables_From_Server()
        {
            foreach (var conf in TestConfigurations.GetConfigurations())
            {
                var productId = Guid.NewGuid();
                var productName = Path.GetRandomFileName().Replace(".", "");
                var productNumber = productName.ToUpperInvariant().Substring(0, 10);

                var productCategoryName = Path.GetRandomFileName().Replace(".", "");
                var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

                // insert 2 rows
                using (var serverDbCtx = this.GetServerDbContext())
                {
                    var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                    serverDbCtx.Add(pc);

                    var product = new Product { ProductId = productId, Name = productName, ProductNumber = productNumber };
                    serverDbCtx.Add(product);

                    await serverDbCtx.SaveChangesAsync();
                }


                // run tests
                var results = await this.testRunner.RunTestsAsync(conf);

                // get results
                foreach (var trr in results)
                {
                    Assert.Equal(2, trr.Results.TotalChangesDownloaded);
                    Assert.Equal(0, trr.Results.TotalChangesUploaded);
                }
            }
        }

        /// <summary>
        /// Insert several rows on each client, with a foreign key involved, should be correctly sync on server and all clients
        /// </summary>
        public virtual async Task Insert_Multiple_Tables_From_Client()
        {
            foreach (var conf in TestConfigurations.GetConfigurations())
            {
                foreach (var testRun in this.fixture.ClientRuns)
                {
                    var productCategoryName = Path.GetRandomFileName().Replace(".", "");
                    var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

                    var productId = Guid.NewGuid();
                    var productName = Path.GetRandomFileName().Replace(".", "");
                    var productNumber = productName.ToUpperInvariant().Substring(0, 10);


                    using (var ctx = this.GetClientDbContext(testRun))
                    {
                        var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                        ctx.Add(pc);
                        var product = new Product { ProductId = productId, Name = productName, ProductNumber = productNumber, ProductCategoryId = productCategoryId };
                        ctx.Add(product);

                        await ctx.SaveChangesAsync();
                    }
                }

                // first sync. each client will send its own row to the server
                // so the first client won't get anything from the second
                // the last one will have all the rows, as well as the server
                var results = await this.testRunner.RunTestsAsync(conf);

                for (var i = 0; i < this.fixture.ClientRuns.Count; i++)
                {
                    var testRunner = this.fixture.ClientRuns[i];

                    Assert.Equal(0 + (i * 2), testRunner.Results.TotalChangesDownloaded);
                    Assert.Equal(2, testRunner.Results.TotalChangesUploaded);
                }

                // now a last sync to be sure all clients have all rows
                var results2 = await this.testRunner.RunTestsAsync(conf);

                // check rows count on server
                using (var serverDbCtx = this.GetServerDbContext())
                {
                    var productRowCount = await serverDbCtx.Product.AsNoTracking().CountAsync();
                    var productCategoryCount = await serverDbCtx.ProductCategory.AsNoTracking().CountAsync();

                    // check rows number on all product category table
                    foreach (var testRun in this.fixture.ClientRuns)
                    {
                        using (var ctx = this.GetClientDbContext(testRun))
                        {
                            var pCount = await ctx.Product.AsNoTracking().CountAsync();
                            Assert.Equal(productRowCount, pCount);

                            var pcCount = await ctx.ProductCategory.AsNoTracking().CountAsync();
                            Assert.Equal(productCategoryCount, pcCount);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Update one row from server. Should be updated on each client
        /// </summary>
        public virtual async Task Update_One_Table_From_Server()
        {
            foreach (var conf in TestConfigurations.GetConfigurations())
            {
                string randomAddressLine;
                string city;
                string stateProvince;

                // get the first address with ID=1
                using (var serverDbCtx = this.GetServerDbContext())
                {
                    var address = await serverDbCtx.Address.SingleAsync(a => a.AddressId == 1);

                    // Update at least two properties
                    // Use a random string for each configuration
                    randomAddressLine = Path.GetRandomFileName().Replace(".", "");
                    city = "Miami";
                    stateProvince = "Floridia";

                    address.City = city;
                    address.StateProvince = stateProvince;
                    address.AddressLine2 = randomAddressLine;

                    await serverDbCtx.SaveChangesAsync();
                }


                var results = await this.testRunner.RunTestsAsync(conf);

                foreach (var trr in results)
                {
                    Assert.Equal(1, trr.Results.TotalChangesDownloaded);
                    Assert.Equal(0, trr.Results.TotalChangesUploaded);

                    // check row updated values
                    using (var ctx = this.GetClientDbContext(trr))
                    {
                        var cliAddress = await ctx.Address.AsNoTracking().SingleAsync(a => a.AddressId == 1);
                        Assert.Equal(city, cliAddress.City);
                        Assert.Equal(stateProvince, cliAddress.StateProvince);
                        Assert.Equal(randomAddressLine, cliAddress.AddressLine2);
                    }

                }
            }
        }

        /// <summary>
        /// Update one row from client. Should be update on server and on each client
        /// To avoid conflicts, each client should update a different row
        /// </summary>
        public virtual async Task Update_One_Table_From_Client()
        {
            foreach (var conf in TestConfigurations.GetConfigurations())
            {
                // get a for instead of foreach to have an index corresponding to the Address pkeyid, ensuring I won't have conflicts beetween clients
                // start on "Index-Base-1"
                for (var i = 1; i <= this.fixture.ClientRuns.Count; i++)
                {
                    var testRun = this.fixture.ClientRuns[i - 1];
                    using (var ctx = this.GetClientDbContext(testRun))
                    {
                        // get the client address with ID=i
                        var address = await ctx.Address.SingleAsync(a => a.AddressId == i);

                        // Update at least two properties
                        // Use a random string for each configuration
                        var randomAddressLine = Path.GetRandomFileName().Replace(".", "");
                        var stateProvince = "Floridia";

                        address.StateProvince = stateProvince;
                        address.AddressLine2 = randomAddressLine;

                        await ctx.SaveChangesAsync();

                    }
                }

                // Each client will update its own row. Server will so update multiples lines
                // First    client will have 1 upload and then 0 download
                // Second   client will have 1 upload and then 1 download
                // Third    client will have 1 upload and then 2 downloads
                // and so on ...
                var results = await this.testRunner.RunTestsAsync(conf);

                for (var i = 0; i < this.fixture.ClientRuns.Count; i++)
                {
                    var testRunner = this.fixture.ClientRuns[i];

                    Assert.Equal(0 + i, testRunner.Results.TotalChangesDownloaded);
                    Assert.Equal(1, testRunner.Results.TotalChangesUploaded);
                }

                // Sync again to be sure all clients have the exact same lines
                await this.testRunner.RunTestsAsync(conf);

                // get all address from server
                using (var serverDbCtx = this.GetServerDbContext())
                {
                    var serverAddresses = await serverDbCtx.Address.AsNoTracking().ToListAsync();

                    for (var i = 0; i < this.fixture.ClientRuns.Count; i++)
                    {
                        var clientRun = this.fixture.ClientRuns[i];

                        using (var ctx = this.GetClientDbContext(clientRun))
                        {
                            var clientAddresses = await ctx.Address.AsNoTracking().ToListAsync();

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

            // launch a last sync to be sure everything is up to date for next test
            var endOfTestResults = await this.testRunner.RunTestsAsync();
        }

        /// <summary>
        /// Conflict resolution on insert-insert. 
        /// Use the default behavior where server should always wins conflict.
        /// </summary>
        public virtual async Task Conflict_Insert_Insert_Server_Should_Wins()
        {
            foreach (var conf in TestConfigurations.GetConfigurations())
            {
                // reset
                await this.testRunner.RunTestsAsync(conf);
                // generate a conflict product category id
                var conflictProductCategoryId = Path.GetRandomFileName().Replace(".", "").ToUpperInvariant().Substring(0, 6);

                var productCategoryNameClient = Path.GetRandomFileName().Replace(".", "").ToUpperInvariant().Substring(3, 6);
                var productCategoryNameServer = Path.GetRandomFileName().Replace(".", "").ToUpperInvariant().Substring(3, 6);

                // Insert a conflict product category and a product
                foreach (var testRun in this.fixture.ClientRuns)
                {
                    using (var ctx = this.GetClientDbContext(testRun))
                    {
                        ctx.Add(new ProductCategory
                        {
                            ProductCategoryId = conflictProductCategoryId,
                            Name = productCategoryNameClient
                        });

                        await ctx.SaveChangesAsync();
                    }
                }

                // insert conflict rows on server
                using (var serverDbCtx = this.GetServerDbContext())
                {
                    serverDbCtx.Add(new ProductCategory
                    {
                        ProductCategoryId = conflictProductCategoryId,
                        Name = productCategoryNameServer
                    });

                    await serverDbCtx.SaveChangesAsync();
                }

                var results = await this.testRunner.RunTestsAsync(conf);

                foreach (var testRunner in this.fixture.ClientRuns)
                {
                    //Assert.Equal(1, testRunner.Results.TotalChangesDownloaded);
                    Assert.Equal(1, testRunner.Results.TotalChangesUploaded);
                    Assert.Equal(1, testRunner.Results.TotalSyncConflicts);
                }

                using (var serverDbCtx = this.GetServerDbContext())
                {
                    // check server product category
                    var checkProductCategoryServer = await serverDbCtx.ProductCategory.AsNoTracking().SingleAsync(pc => pc.ProductCategoryId == conflictProductCategoryId);

                    Assert.Equal(productCategoryNameServer, checkProductCategoryServer.Name);
                }
                // check client product category row
                foreach (var testRun in this.fixture.ClientRuns)
                {
                    using (var ctx = this.GetClientDbContext(testRun))
                    {
                        // check client product category
                        var checkProductCategoryClient = await ctx.ProductCategory.AsNoTracking().SingleAsync(pc => pc.ProductCategoryId == conflictProductCategoryId);
                        Assert.Equal(productCategoryNameServer, checkProductCategoryClient.Name);
                    }
                }
            }
        }

        /// <summary>
        /// Conflict resolution on insert-insert. 
        /// Use the configuration behavior where client should always wins conflict.
        /// </summary>
        public virtual async Task Conflict_Insert_Insert_Client_Should_Wins_Coz_Configuration()
        {

            // reset
            await this.testRunner.RunTestsAsync();

            var conf = new Action<SyncConfiguration>(sc => sc.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins);

            // generate a conflict product category id
            var conflictProductCategoryId = Path.GetRandomFileName().Replace(".", "").ToUpperInvariant().Substring(0, 6);

            var productCategoryNameClient = "Client " + Path.GetRandomFileName().Replace(".", "");
            var productCategoryNameServer = "Server " + Path.GetRandomFileName().Replace(".", "");

            // Insert a conflict product category and a product
            foreach (var testRun in this.fixture.ClientRuns)
            {
                using (var ctx = this.GetClientDbContext(testRun))
                {
                    ctx.Add(new ProductCategory
                    {
                        ProductCategoryId = conflictProductCategoryId,
                        Name = productCategoryNameClient
                    });

                    await ctx.SaveChangesAsync();

                }
            }

            using (var serverDbCtx = this.GetServerDbContext())
            {
                // insert conflict rows on server
                serverDbCtx.Add(new ProductCategory
                {
                    ProductCategoryId = conflictProductCategoryId,
                    Name = productCategoryNameServer
                });

                await serverDbCtx.SaveChangesAsync();
            }

            // use a new agent since we modify conf
            var results = await this.testRunner.RunTestsAsync(conf, false);

            for (var i = 0; i < this.fixture.ClientRuns.Count; i++)
            {
                var testRunner = this.fixture.ClientRuns[i];

                // 0+i : Download is false on sqlite
                //Assert.Equal(0 + i, testRunner.Results.TotalChangesDownloaded);
                Assert.Equal(1, testRunner.Results.TotalChangesUploaded);
                Assert.Equal(1, testRunner.Results.TotalSyncConflicts);
            }

            using (var serverDbCtx = this.GetServerDbContext())
            {
                // check server product category
                var checkProductCategoryServer = await serverDbCtx.ProductCategory.AsNoTracking().SingleAsync(pc => pc.ProductCategoryId == conflictProductCategoryId);

                Assert.Equal(productCategoryNameClient, checkProductCategoryServer.Name);
            }

            // check client product category row
            foreach (var testRun in this.fixture.ClientRuns)
            {
                using (var ctx = this.GetClientDbContext(testRun))
                {
                    // check client product category
                    var checkProductCategoryClient = await ctx.ProductCategory.AsNoTracking().SingleAsync(pc => pc.ProductCategoryId == conflictProductCategoryId);

                    Assert.Equal(productCategoryNameClient, checkProductCategoryClient.Name);
                }
            }

        }

        /// <summary>
        /// Conflict resolution on insert-insert. 
        /// Use the handler to resolve the conflict using the action [ConflictAction.ClientWins].
        /// </summary>
        public virtual async Task Conflict_Insert_Insert_Client_Should_Wins_Coz_Handler_Raised()
        {
            foreach (var conf in TestConfigurations.GetConfigurations())
            {
                // reset
                await this.testRunner.RunTestsAsync(conf);
                // generate a conflict product category id
                var conflictProductCategoryId = Path.GetRandomFileName().Replace(".", "").ToUpperInvariant().Substring(0, 6);

                var productCategoryNameClient = "Client " + Path.GetRandomFileName().Replace(".", "");
                var productCategoryNameServer = "Server " + Path.GetRandomFileName().Replace(".", "");
                // Insert a conflict product category and a product

                foreach (var testRun in this.fixture.ClientRuns)
                {
                    using (var ctx = this.GetClientDbContext(testRun))
                    {
                        ctx.Add(new ProductCategory
                        {
                            ProductCategoryId = conflictProductCategoryId,
                            Name = productCategoryNameClient
                        });

                        await ctx.SaveChangesAsync();
                    }
                }

                using (var serverDbCtx = this.GetServerDbContext())
                {
                    // insert conflict rows on server
                    serverDbCtx.Add(new ProductCategory
                    {
                        ProductCategoryId = conflictProductCategoryId,
                        Name = productCategoryNameServer
                    });

                    await serverDbCtx.SaveChangesAsync();
                }


                this.testRunner.BeginRun = provider
                    => provider.SetInterceptor(new Interceptor<ApplyChangesFailedArgs>(c => c.Resolution = ConflictResolution.ClientWins));
                this.testRunner.EndRun = provider => provider.SetInterceptor(null);

                var results = await this.testRunner.RunTestsAsync(conf);

                foreach (var testRunner in this.fixture.ClientRuns)
                {
                    Assert.Equal(1, testRunner.Results.TotalChangesUploaded);
                    Assert.Equal(1, testRunner.Results.TotalSyncConflicts);
                }

                using (var serverDbCtx = this.GetServerDbContext())
                {
                    // check server product category
                    var checkProductCategoryServer = await serverDbCtx.ProductCategory.AsNoTracking().SingleAsync(pc => pc.ProductCategoryId == conflictProductCategoryId);
                    Assert.Equal(productCategoryNameClient, checkProductCategoryServer.Name);
                }

                // check client product category row
                foreach (var testRun in this.fixture.ClientRuns)
                {
                    using (var ctx = this.GetClientDbContext(testRun))
                    {
                        // check client product category
                        var checkProductCategoryClient = await ctx.ProductCategory.AsNoTracking().SingleAsync(pc => pc.ProductCategoryId == conflictProductCategoryId);

                        Assert.Equal(productCategoryNameClient, checkProductCategoryClient.Name);
                    }
                }
            }
        }

        /// <summary>
        /// Conflict resolution on update-update. 
        /// Use the configuration behavior where client should always wins conflict.
        public virtual async Task Conflict_Update_Update_Client_Should_Wins_Coz_Configuration()
        {
            // reset
            await this.testRunner.RunTestsAsync();

            var conf = new Action<SyncConfiguration>(sc => sc.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins);

            // generate a conflict product category id
            var conflictProductCategoryId = "BIKES";

            var productCategoryNameClient = "Client BIKES " + Path.GetRandomFileName().Replace(".", "");
            var productCategoryNameServer = "Server BIKES " + Path.GetRandomFileName().Replace(".", "");

            // Insert a conflict product category and a product
            foreach (var testRun in this.fixture.ClientRuns)
            {
                using (var ctx = this.GetClientDbContext(testRun))
                {
                    ctx.ProductCategory.Update(new ProductCategory
                    {
                        ProductCategoryId = conflictProductCategoryId,
                        Name = productCategoryNameClient
                    });

                    await ctx.SaveChangesAsync();

                }
            }

            using (var serverDbCtx = this.GetServerDbContext())
            {
                // insert conflict rows on server
                serverDbCtx.Update(new ProductCategory
                {
                    ProductCategoryId = conflictProductCategoryId,
                    Name = productCategoryNameServer
                });

                await serverDbCtx.SaveChangesAsync();
            }

            // use a new agent since we modify conf
            var results = await this.testRunner.RunTestsAsync(conf, false);

            for (var i = 0; i < this.fixture.ClientRuns.Count; i++)
            {
                var testRunner = this.fixture.ClientRuns[i];

                Assert.Equal(1, testRunner.Results.TotalChangesUploaded);
                Assert.Equal(1, testRunner.Results.TotalSyncConflicts);
            }

            using (var serverDbCtx = this.GetServerDbContext())
            {
                // check server product category
                var checkProductCategoryServer = await serverDbCtx.ProductCategory.AsNoTracking().SingleAsync(pc => pc.ProductCategoryId == conflictProductCategoryId);

                Assert.Equal(productCategoryNameClient, checkProductCategoryServer.Name);
            }
            // check client product category row
            foreach (var testRun in this.fixture.ClientRuns)
            {
                using (var ctx = this.GetClientDbContext(testRun))
                {
                    // check client product category
                    var checkProductCategoryClient = await ctx.ProductCategory.AsNoTracking().SingleAsync(pc => pc.ProductCategoryId == conflictProductCategoryId);

                    Assert.Equal(productCategoryNameClient, checkProductCategoryClient.Name);
                }
            }
        }

        /// <summary>
        /// Conflict resolution on update-update. 
        /// Use the handler to resolve the conflict using the action [ConflictAction.ClientWins].
        /// </summary>
        public virtual async Task Conflict_Update_Update_Client_Should_Wins_Coz_Handler_Raised()
        {
            foreach (var conf in TestConfigurations.GetConfigurations())
            {
                // reset
                await this.testRunner.RunTestsAsync(conf);
                // generate a conflict product category id
                var conflictProductCategoryId = "BIKES";

                var productCategoryNameClient = "Client BIKES " + Path.GetRandomFileName().Replace(".", "");
                var productCategoryNameServer = "Server BIKES " + Path.GetRandomFileName().Replace(".", "");

                // Insert a conflict product category and a product
                foreach (var testRun in this.fixture.ClientRuns)
                {
                    using (var ctx = this.GetClientDbContext(testRun))
                    {

                        ctx.ProductCategory.Update(new ProductCategory
                        {
                            ProductCategoryId = conflictProductCategoryId,
                            Name = productCategoryNameClient
                        });

                        await ctx.SaveChangesAsync();

                    }
                }

                using (var serverDbCtx = this.GetServerDbContext())
                {
                    // insert conflict rows on server
                    serverDbCtx.Update(new ProductCategory
                    {
                        ProductCategoryId = conflictProductCategoryId,
                        Name = productCategoryNameServer
                    });

                    await serverDbCtx.SaveChangesAsync();
                }


                this.testRunner.BeginRun = provider =>
                    provider.SetInterceptor(new Interceptor<ApplyChangesFailedArgs>(args =>
                            args.Resolution = ConflictResolution.ClientWins));

                var results = await this.testRunner.RunTestsAsync(conf);

                for (var i = 0; i < this.fixture.ClientRuns.Count; i++)
                {
                    var testRunner = this.fixture.ClientRuns[i];

                    Assert.Equal(1, testRunner.Results.TotalChangesUploaded);
                    Assert.Equal(1, testRunner.Results.TotalSyncConflicts);
                }

                using (var serverDbCtx = this.GetServerDbContext())
                {
                    // check server product category
                    var checkProductCategoryServer = await serverDbCtx.ProductCategory.AsNoTracking().SingleAsync(pc => pc.ProductCategoryId == conflictProductCategoryId);

                    Assert.Equal(productCategoryNameClient, checkProductCategoryServer.Name);
                }
                // check client product category row
                foreach (var testRun in this.fixture.ClientRuns)
                {
                    using (var ctx = this.GetClientDbContext(testRun))
                    {
                        // check client product category
                        var checkProductCategoryClient = await ctx.ProductCategory.AsNoTracking().SingleAsync(pc => pc.ProductCategoryId == conflictProductCategoryId);

                        Assert.Equal(productCategoryNameClient, checkProductCategoryClient.Name);
                    }
                }
            }
        }

        /// <summary>
        /// Conflict resolved by merging the entity on both server and client
        /// </summary>
        public virtual async Task Conflict_Update_Update_Resolve_By_Merge()
        {

            foreach (var conf in TestConfigurations.GetConfigurations())
            {
                // reset
                await this.testRunner.RunTestsAsync();

                // generate a conflict product category id
                var conflictProductCategoryId = "BIKES";

                var productCategoryNameClient = "Client BIKES " + Path.GetRandomFileName().Replace(".", "");
                var productCategoryNameServer = "Server BIKES " + Path.GetRandomFileName().Replace(".", "");
                var productCategoryNameMerged = "BOTH BIKES" + Path.GetRandomFileName().Replace(".", "");

                // Insert a conflict product category and a product
                foreach (var testRun in this.fixture.ClientRuns)
                {
                    using (var ctx = this.GetClientDbContext(testRun))
                    {

                        ctx.ProductCategory.Update(new ProductCategory
                        {
                            ProductCategoryId = conflictProductCategoryId,
                            Name = productCategoryNameClient
                        });

                        await ctx.SaveChangesAsync();

                    }
                }

                using (var serverDbCtx = this.GetServerDbContext())
                {
                    // insert conflict rows on server
                    serverDbCtx.Update(new ProductCategory
                    {
                        ProductCategoryId = conflictProductCategoryId,
                        Name = productCategoryNameServer
                    });

                    await serverDbCtx.SaveChangesAsync();
                }

                this.testRunner.BeginRun = provider
                    => provider.SetInterceptor(new Interceptor<ApplyChangesFailedArgs>(args =>
                    {
                        args.Resolution = ConflictResolution.MergeRow;
                        args.FinalRow["Name"] = productCategoryNameMerged;

                    }));


                this.testRunner.EndRun = provider => provider.SetInterceptor(null);


                var results = await this.testRunner.RunTestsAsync(conf);

                for (var i = 0; i < this.fixture.ClientRuns.Count; i++)
                {
                    var testRunner = this.fixture.ClientRuns[i];

                    Assert.Equal(1, testRunner.Results.TotalChangesUploaded);
                    Assert.Equal(1, testRunner.Results.TotalSyncConflicts);
                }

                using (var serverDbCtx = this.GetServerDbContext())
                {
                    // check server product category
                    var checkProductCategoryServer = await serverDbCtx.ProductCategory.AsNoTracking().SingleAsync(pc => pc.ProductCategoryId == conflictProductCategoryId);

                    Assert.Equal(productCategoryNameMerged, checkProductCategoryServer.Name);
                }
                // check client product category row
                foreach (var testRun in this.fixture.ClientRuns)
                {
                    using (var ctx = this.GetClientDbContext(testRun))
                    {
                        // check client product category
                        var checkProductCategoryClient = await ctx.ProductCategory.AsNoTracking().SingleAsync(pc => pc.ProductCategoryId == conflictProductCategoryId);

                        Assert.Equal(productCategoryNameMerged, checkProductCategoryClient.Name);
                    }
                }
            }

        }

        /// <summary>
        /// Conflict resolution by default. Server wins the resolution and all clients should be updated correctly
        /// </summary>
        /// <returns></returns>
        public virtual async Task Conflict_Update_Update_Server_Should_Wins()
        {
            foreach (var conf in TestConfigurations.GetConfigurations())
            {
                // reset
                await this.testRunner.RunTestsAsync(conf);

                // generate a conflict product category id
                var conflictProductCategoryId = "BIKES";

                var productCategoryNameClient = "Client BIKES " + Path.GetRandomFileName().Replace(".", "");
                var productCategoryNameServer = "Server BIKES " + Path.GetRandomFileName().Replace(".", "");

                // Insert a conflict product category and a product
                foreach (var testRun in this.fixture.ClientRuns)
                {
                    using (var ctx = this.GetClientDbContext(testRun))
                    {
                        ctx.ProductCategory.Update(new ProductCategory
                        {
                            ProductCategoryId = conflictProductCategoryId,
                            Name = productCategoryNameClient
                        });

                        await ctx.SaveChangesAsync();

                    }
                }

                using (var serverDbCtx = this.GetServerDbContext())
                {
                    // insert conflict rows on server
                    serverDbCtx.Update(new ProductCategory
                    {
                        ProductCategoryId = conflictProductCategoryId,
                        Name = productCategoryNameServer
                    });

                    await serverDbCtx.SaveChangesAsync();
                }

                // use a new agent since we modify conf
                var results = await this.testRunner.RunTestsAsync(conf, false);

                for (var i = 0; i < this.fixture.ClientRuns.Count; i++)
                {
                    var testRunner = this.fixture.ClientRuns[i];

                    Assert.Equal(1, testRunner.Results.TotalChangesUploaded);
                    Assert.Equal(1, testRunner.Results.TotalChangesDownloaded);
                    Assert.Equal(1, testRunner.Results.TotalSyncConflicts);
                }

                using (var serverDbCtx = this.GetServerDbContext())
                {
                    // check server product category
                    var checkProductCategoryServer = await serverDbCtx.ProductCategory.AsNoTracking().SingleAsync(pc => pc.ProductCategoryId == conflictProductCategoryId);

                    Assert.Equal(productCategoryNameServer, checkProductCategoryServer.Name);
                }
                // check client product category row
                foreach (var testRun in this.fixture.ClientRuns)
                {
                    using (var ctx = this.GetClientDbContext(testRun))
                    {
                        // check client product category
                        var checkProductCategoryClient = await ctx.ProductCategory.AsNoTracking().SingleAsync(pc => pc.ProductCategoryId == conflictProductCategoryId);

                        Assert.Equal(productCategoryNameServer, checkProductCategoryClient.Name);
                    }
                }
            }
        }

        /// <summary>
        /// Delete some rows from one table. Address from primary key 6 and +, are not used
        /// </summary>
        /// <returns></returns>
        public virtual async Task Delete_From_Server()
        {
            var configurations = TestConfigurations.GetConfigurations();

            // part of the filter
            var employeeId = 1;
            // will be defined when address is inserted
            var addressId = 0;

            for (var i = 0; i < configurations.Count; i++)
            {
                var conf = configurations[i];
                // insert in db server
                // first of all, delete the line from server
                using (var serverDbCtx = this.GetServerDbContext())
                {

                    // Insert a new address for employee 1
                    var city = "Paris " + Path.GetRandomFileName().Replace(".", "");
                    var addressline1 = "Rue Monthieu " + Path.GetRandomFileName().Replace(".", "");
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

                // Upload this new address / employee address
                var resultsInsertServer = await this.testRunner.RunTestsAsync(conf);

                // check the download to client
                foreach (var trr in resultsInsertServer)
                {
                    Assert.Equal(2, trr.Results.TotalChangesDownloaded);
                    Assert.Equal(0, trr.Results.TotalChangesUploaded);
                }

                // Delete those lines from server
                using (var serverDbCtx = this.GetServerDbContext())
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

                var resultsDeleteOnServer = await this.testRunner.RunTestsAsync(conf);

                foreach (var trr in resultsDeleteOnServer)
                {
                    Assert.Equal(2, trr.Results.TotalChangesDownloaded);
                    Assert.Equal(0, trr.Results.TotalChangesUploaded);

                    // check row deleted on client values
                    using (var ctx = this.GetClientDbContext(trr))
                    {
                        var finalAddressesCount = await ctx.Address.AsNoTracking().CountAsync(a => a.AddressId == addressId);
                        var finalEmployeeAddressesCount = await ctx.EmployeeAddress.AsNoTracking().CountAsync(a => a.AddressId == addressId && a.EmployeeId == employeeId);
                        Assert.Equal(0, finalAddressesCount);
                        Assert.Equal(0, finalEmployeeAddressesCount);
                    }
                }

                // reset all clients
                await this.testRunner.RunTestsAsync(conf);
            }

        }

        /// <summary>
        /// The idea is to insert an item, then delete it, then sync.
        /// To see how the client is interacting with something to delete he does not have 
        /// </summary>
        public virtual async Task Insert_Then_Delete_From_Server_Then_Sync()
        {
            foreach (var conf in TestConfigurations.GetConfigurations())
            {
                var productCategoryId = Path.GetRandomFileName().Replace(".", "").ToUpperInvariant().Substring(0, 6);
                var productCategoryNameServer = Path.GetRandomFileName().Replace(".", "").ToUpperInvariant().Substring(3, 6);

                // insert the row on the server
                using (var serverDbCtx = this.GetServerDbContext())
                {
                    serverDbCtx.Add(new ProductCategory
                    {
                        ProductCategoryId = productCategoryId,
                        Name = productCategoryNameServer
                    });

                    await serverDbCtx.SaveChangesAsync();
                }

                // then delete it
                using (var serverDbCtx = this.GetServerDbContext())
                {
                    var pc = await serverDbCtx.ProductCategory.SingleAsync(o => o.ProductCategoryId == productCategoryId);

                    serverDbCtx.Remove(pc);

                    await serverDbCtx.SaveChangesAsync();
                }

                var results = await this.testRunner.RunTestsAsync();

                foreach (var trr in results)
                {
                    Assert.Equal(1, trr.Results.TotalChangesDownloaded);
                    Assert.Equal(0, trr.Results.TotalChangesUploaded);
                }
            }

        }

        /// <summary>
        /// The idea is to insert an item, then update it, then sync.
        /// To see how the client is interacting with something to delete he does not have 
        /// </summary>
        public virtual async Task Insert_Then_Update_From_Server_Then_Sync()
        {
            foreach (var conf in TestConfigurations.GetConfigurations())
            {
                var productCategoryId = Path.GetRandomFileName().Replace(".", "").ToUpperInvariant().Substring(0, 6);
                var productCategoryNameServer = Path.GetRandomFileName().Replace(".", "").ToUpperInvariant().Substring(3, 6);
                var productCategoryNameServerUpdated = Path.GetRandomFileName().Replace(".", "").ToUpperInvariant().Substring(3, 6);

                // insert the row on the server
                using (var serverDbCtx = this.GetServerDbContext())
                {
                    serverDbCtx.Add(new ProductCategory
                    {
                        ProductCategoryId = productCategoryId,
                        Name = productCategoryNameServer
                    });

                    await serverDbCtx.SaveChangesAsync();
                }

                // Then update the row
                using (var serverDbCtx = this.GetServerDbContext())
                {
                    var pc = await serverDbCtx.ProductCategory.SingleAsync(o => o.ProductCategoryId == productCategoryId);

                    pc.Name = productCategoryNameServerUpdated;

                    await serverDbCtx.SaveChangesAsync();
                }


                var results = await this.testRunner.RunTestsAsync(conf);

                foreach (var trr in results)
                {
                    Assert.Equal(1, trr.Results.TotalChangesDownloaded);
                    Assert.Equal(0, trr.Results.TotalChangesUploaded);

                    // Check
                    using (var ctx = this.GetClientDbContext(trr))
                    {
                        var pc = await ctx.ProductCategory.SingleAsync(o => o.ProductCategoryId == productCategoryId);
                        Assert.Equal(productCategoryNameServerUpdated, pc.Name);
                    }
                }
            }
        }


        public Task Delete_One_Table_From_Client() => throw new NotImplementedException();
        public Task Delete_Multiple_Tables_From_Client() => throw new NotImplementedException();

        public Task Insert_Update_Delete_From_Server() => throw new NotImplementedException();
        public Task No_Rows() => throw new NotImplementedException();
        public Task Update_Multiple_Rows_From_Client() => throw new NotImplementedException();
        public Task Update_Multiple_Rows_From_Server() => throw new NotImplementedException();

        /// <summary>
        /// Should be able to Deprovision a whole database
        /// </summary>
        public virtual async Task Use_Existing_Client_Database_Provision_Deprosivion()
        {
            // Generate a new temp database and a local provider
            var dbName = this.fixture.GetRandomDatabaseName();
            var connectionString = HelperDB.GetConnectionString(this.fixture.ProviderType, dbName);

            // create a local provider (the provider we want to test, obviously)
            var localProvider = this.fixture.NewServerProvider(connectionString);

            var providerRun = new ProviderRun(dbName, localProvider, this.fixture.ProviderType, NetworkType.Tcp);

            try
            {
                // create an empty AdventureWorks client database
                using (var ctx = new AdventureWorksContext(providerRun, providerRun.ClientProviderType == ProviderType.Sql, false))
                    await ctx.Database.EnsureCreatedAsync();

                // generate a sync conf to host the schema
                var conf = new SyncConfiguration(this.fixture.Tables);


                // just check interceptor
                localProvider.SetInterceptor(new Interceptor<TableProvisioningArgs>(args =>
                {
                    Assert.Equal(SyncProvision.All, args.Provision);
                }));


                // Provision the database with all tracking tables, stored procedures, triggers and scope
                await localProvider.ProvisionAsync(conf, SyncProvision.All);

                //--------------------------
                // ASSERTION
                //--------------------------

                // check if scope table is correctly created
                var scopeBuilderFactory = localProvider.GetScopeBuilder();

                using (var dbConnection = localProvider.CreateConnection())
                {
                    var scopeBuilder = scopeBuilderFactory.CreateScopeInfoBuilder(conf.ScopeInfoTableName, dbConnection);
                    Assert.False(scopeBuilder.NeedToCreateScopeInfoTable());
                }

                // get the db manager
                foreach (var dmTable in conf.Schema.Tables)
                {
                    var tableName = dmTable.TableName;
                    using (var dbConnection = localProvider.CreateConnection())
                    {
                        // get the database manager factory then the db manager itself
                        var dbTableBuilder = localProvider.GetDatabaseBuilder(dmTable);

                        // get builders
                        var trackingTablesBuilder = dbTableBuilder.CreateTrackingTableBuilder(dbConnection);
                        var triggersBuilder = dbTableBuilder.CreateTriggerBuilder(dbConnection);
                        var spBuider = dbTableBuilder.CreateProcBuilder(dbConnection);

                        await dbConnection.OpenAsync();

                        Assert.False(trackingTablesBuilder.NeedToCreateTrackingTable());

                        Assert.False(triggersBuilder.NeedToCreateTrigger(Builders.DbTriggerType.Insert));
                        Assert.False(triggersBuilder.NeedToCreateTrigger(Builders.DbTriggerType.Delete));
                        Assert.False(triggersBuilder.NeedToCreateTrigger(Builders.DbTriggerType.Update));

                        Assert.False(spBuider.NeedToCreateProcedure(Builders.DbCommandType.InsertMetadata));
                        Assert.False(spBuider.NeedToCreateProcedure(Builders.DbCommandType.InsertRow));
                        Assert.False(spBuider.NeedToCreateProcedure(Builders.DbCommandType.Reset));
                        Assert.False(spBuider.NeedToCreateProcedure(Builders.DbCommandType.SelectChanges));
                        Assert.False(spBuider.NeedToCreateProcedure(Builders.DbCommandType.SelectRow));
                        Assert.False(spBuider.NeedToCreateProcedure(Builders.DbCommandType.DeleteMetadata));
                        Assert.False(spBuider.NeedToCreateProcedure(Builders.DbCommandType.DeleteRow));

                        // Check if we have mutables columns to see if the update row / metadata have been generated
                        if (dbTableBuilder.TableDescription.MutableColumnsAndNotAutoInc.Any())
                        {
                            Assert.False(spBuider.NeedToCreateProcedure(Builders.DbCommandType.UpdateRow));
                            Assert.False(spBuider.NeedToCreateProcedure(Builders.DbCommandType.UpdateMetadata));
                        }

                        if (this.fixture.ProviderType == ProviderType.Sql)
                        {
                            Assert.False(spBuider.NeedToCreateProcedure(Builders.DbCommandType.BulkDeleteRows));
                            Assert.False(spBuider.NeedToCreateProcedure(Builders.DbCommandType.BulkInsertRows));
                            Assert.False(spBuider.NeedToCreateProcedure(Builders.DbCommandType.BulkInsertRows));
                        }

                        dbConnection.Close();

                    }
                }

                // just check interceptor
                localProvider.SetInterceptor(new Interceptor<TableDeprovisioningArgs>(args =>
                {
                    Assert.Equal(SyncProvision.All, args.Provision);
                }));


                // Provision the database with all tracking tables, stored procedures, triggers and scope
                await localProvider.DeprovisionAsync(conf, SyncProvision.All);

                // get the db manager
                foreach (var dmTable in conf.Schema.Tables)
                {
                    var tableName = dmTable.TableName;
                    using (var dbConnection = localProvider.CreateConnection())
                    {
                        // get the database manager factory then the db manager itself
                        var dbTableBuilder = localProvider.GetDatabaseBuilder(dmTable);

                        // get builders
                        var trackingTablesBuilder = dbTableBuilder.CreateTrackingTableBuilder(dbConnection);
                        var triggersBuilder = dbTableBuilder.CreateTriggerBuilder(dbConnection);
                        var spBuider = dbTableBuilder.CreateProcBuilder(dbConnection);

                        await dbConnection.OpenAsync();

                        Assert.True(trackingTablesBuilder.NeedToCreateTrackingTable());
                        Assert.True(triggersBuilder.NeedToCreateTrigger(Builders.DbTriggerType.Insert));
                        Assert.True(triggersBuilder.NeedToCreateTrigger(Builders.DbTriggerType.Delete));
                        Assert.True(triggersBuilder.NeedToCreateTrigger(Builders.DbTriggerType.Update));
                        Assert.True(spBuider.NeedToCreateProcedure(Builders.DbCommandType.InsertMetadata));
                        Assert.True(spBuider.NeedToCreateProcedure(Builders.DbCommandType.InsertRow));
                        Assert.True(spBuider.NeedToCreateProcedure(Builders.DbCommandType.Reset));
                        Assert.True(spBuider.NeedToCreateProcedure(Builders.DbCommandType.SelectChanges));
                        Assert.True(spBuider.NeedToCreateProcedure(Builders.DbCommandType.SelectRow));
                        Assert.True(spBuider.NeedToCreateProcedure(Builders.DbCommandType.DeleteMetadata));
                        Assert.True(spBuider.NeedToCreateProcedure(Builders.DbCommandType.DeleteRow));

                        // Check if we have mutables columns to see if the update row / metadata have been generated
                        if (dbTableBuilder.TableDescription.MutableColumnsAndNotAutoInc.Any())
                        {
                            Assert.True(spBuider.NeedToCreateProcedure(Builders.DbCommandType.UpdateRow));
                            Assert.True(spBuider.NeedToCreateProcedure(Builders.DbCommandType.UpdateMetadata));
                        }

                        if (this.fixture.ProviderType == ProviderType.Sql)
                        {
                            Assert.True(spBuider.NeedToCreateProcedure(Builders.DbCommandType.BulkDeleteRows));
                            Assert.True(spBuider.NeedToCreateProcedure(Builders.DbCommandType.BulkInsertRows));
                            Assert.True(spBuider.NeedToCreateProcedure(Builders.DbCommandType.BulkInsertRows));
                        }

                        dbConnection.Close();

                    }
                }


                localProvider.SetInterceptor(null);


            }
            finally
            {
                // create an empty AdventureWorks client database
                using (var ctx = new AdventureWorksContext(providerRun, providerRun.ClientProviderType == ProviderType.Sql, false))
                    await ctx.Database.EnsureDeletedAsync();

            }
        }



        //[Fact, TestPriority(99)]
        //public async Task Reserved_Keyword_Should_Raise_Error()
        //{
        //    var tables = new string[] { "dbo.Log" };
        //    var results = await this.testRunner.RunTestsAsync("errorscope", tables, null, false);

        //    foreach (var trr in results)
        //    {
        //        Assert.IsType(typeof(SyncException), trr.Exception);
        //        SyncException se = trr.Exception as SyncException;
        //        Assert.Equal(SyncExceptionType.NotSupported, se.Type);
        //    }

        //}

        public virtual async Task Check_Composite_ForeignKey_Existence()
        {
            var runners = await this.testRunner.RunTestsAsync();
            foreach (var runner in runners)
            {
                var provider = runner.ClientProvider as CoreProvider;
                var connection = provider.CreateConnection();
                using (connection)
                {
                    connection.Open();
                    var tableManger = provider
                        .GetDbManager("PriceListCategory")
                        ?.CreateManagerTable(connection);

                    if (tableManger == null)
                        continue;

                    var relations = tableManger.GetTableRelations().ToArray();
                    Assert.Single(relations);
                    Assert.StartsWith("FK_PriceListDetail_PriceListCategory_", relations[0].ForeignKey);
                    Assert.Equal(2, relations[0].Columns.Count());
                }
            }
        }

        /// <summary>
        /// Insert one row on each client and update FK on another table that references that row,
        /// should be correctly sync on server and all clients
        /// </summary>
        public virtual async Task Insert_New_Table_Then_Update_Existing_Table_From_Client()
        {
            // create new ProductCategory on server
            foreach (var conf in TestConfigurations.GetConfigurations())
            {
                // reset to be sure everything is downloaded from every clients
                await this.testRunner.RunTestsAsync(conf);

                var productId = Guid.NewGuid();
                var productNameServer = Path.GetRandomFileName().Replace(".", "");
                var productNumber = Path.GetRandomFileName().Replace(".", "").ToUpperInvariant().Substring(0, 10);

                // insert the row on the server
                using (var serverDbCtx = this.GetServerDbContext())
                {
                    serverDbCtx.Product.Add(new Product
                    {
                        ProductId = productId,
                        Name = productNameServer,
                        ProductNumber = productNumber
                    });

                    await serverDbCtx.SaveChangesAsync();
                }

                var results = await this.testRunner.RunTestsAsync(conf);

                foreach (var trr in results)
                {
                    Assert.Equal(1, trr.Results.TotalChangesDownloaded);
                }

                foreach (var clientRun in this.fixture.ClientRuns)
                {
                    var name = Path.GetRandomFileName().Replace(".", "");
                    var id = name.ToUpperInvariant().Substring(0, 6);

                    using (var clientDbCtx = this.GetClientDbContext(clientRun))
                    {
                        // create a new product category
                        clientDbCtx.ProductCategory.Add(new ProductCategory
                        {
                            Name = name,
                            ProductCategoryId = id
                        });

                        // update the synced product with this new product category
                        var product = await clientDbCtx.Product.SingleAsync(a => a.ProductId == productId);
                        product.ProductCategoryId = id;

                        await clientDbCtx.SaveChangesAsync();
                    }

                    var trr = await clientRun.RunAsync(this.fixture, null, conf, false);
                    Assert.Equal(2, trr.Results.TotalChangesUploaded);
                }

            }
        }


        public virtual async Task Insert_Record_Then_Insert_During_GetChanges()
        {
            // create new ProductCategory on server
            foreach (var conf in TestConfigurations.GetConfigurations())
            {
                // reset
                await this.testRunner.RunTestsAsync(conf);

                var cpt = 0;
                foreach (var clientRun in this.fixture.ClientRuns)
                {
                    var name = Path.GetRandomFileName().Replace(".", "");
                    var id = name.ToUpperInvariant().Substring(0, 6);

                    // Add a row on the client
                    using (var ctx = this.GetClientDbContext(clientRun))
                    {
                        var pc = new ProductCategory { Name = name, ProductCategoryId = id };
                        ctx.ProductCategory.Add(pc);
                        await ctx.SaveChangesAsync();
                    }

                    // Sleep during a selecting changes on first sync
                    Task tableChangesSelected(TableChangesSelectedArgs changes)
                    {
                        if (changes.TableChangesSelected.TableName != "PricesList")
                            return Task.CompletedTask;

                        var randomString = Path.GetRandomFileName().Replace(".", "");
                        var randomGuid = Guid.NewGuid();
                        // Insert on same connection as current sync.
                        // Using same connection to avoid lock, especially on SQlite

                        var command = changes.Connection.CreateCommand();
                        command.CommandText = "INSERT INTO PricesList (PriceListId, Description) Values (@PriceListId, @Description);";

                        var p = command.CreateParameter();
                        p.ParameterName = "@PriceListId";
                        p.Value = randomGuid;
                        command.Parameters.Add(p);

                        p = command.CreateParameter();
                        p.ParameterName = "@Description";
                        p.Value = randomString;
                        command.Parameters.Add(p);

                        command.Transaction = changes.Transaction;
                        try
                        {
                            Debug.WriteLine($"Insert new value in {changes.Connection.ConnectionString}. {randomGuid.ToString()}={randomString}");
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

                    // during first run, add a new row during selection on client (very first step of whole sync process)
                    clientRun.ClientProvider.SetInterceptor
                        (new Interceptor<TableChangesSelectedArgs>(tableChangesSelected));

                    var trr = await clientRun.RunAsync(this.fixture, null, conf, false);

                    Assert.Equal(cpt, trr.Results.TotalChangesDownloaded);
                    Assert.Equal(1, trr.Results.TotalChangesUploaded);
                    cpt = cpt + 2;

                    clientRun.ClientProvider.SetInterceptor(null);

                    var trr2 = await clientRun.RunAsync(this.fixture, null, conf, false);
                    Debug.WriteLine($"{trr2.ClientProvider.ConnectionString}: Upload={trr2.Results.TotalChangesUploaded}");

                    Assert.Equal(0, trr2.Results.TotalChangesDownloaded);
                    Assert.Equal(1, trr2.Results.TotalChangesUploaded);


                }
            }
        }


        public virtual async Task Check_Interceptors()
        {
            // create new ProductCategory on server
            foreach (var conf in TestConfigurations.GetConfigurations())
            {
                foreach (var clientRun in this.fixture.ClientRuns)
                {
                    // reset all

                    await this.testRunner.RunTestsAsync(conf);

                    var productId = Guid.NewGuid();
                    var productName = Path.GetRandomFileName().Replace(".", "");
                    var productNumber = productName.ToUpperInvariant().Substring(0, 10);

                    var productCategoryName = Path.GetRandomFileName().Replace(".", "");
                    var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

                    // insert 2 rows
                    using (var serverDbCtx = this.GetServerDbContext())
                    {
                        var pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                        serverDbCtx.Add(pc);

                        var product = new Product { ProductId = productId, Name = productName, ProductNumber = productNumber };
                        serverDbCtx.Add(product);

                        await serverDbCtx.SaveChangesAsync();
                    }

                    var clientProductCategoryName = Path.GetRandomFileName().Replace(".", "");
                    var clientProductCategoryId = clientProductCategoryName.ToUpperInvariant().Substring(0, 6);

                    var clientProductId = Guid.NewGuid();
                    var clientProductName = Path.GetRandomFileName().Replace(".", "");
                    var clientProductNumber = clientProductName.ToUpperInvariant().Substring(0, 10);

                    using (var ctx = this.GetClientDbContext(clientRun))
                    {
                        var pc = new ProductCategory { ProductCategoryId = clientProductCategoryId, Name = clientProductCategoryName };
                        ctx.Add(pc);
                        var product = new Product { ProductId = clientProductId, Name = clientProductName, ProductNumber = clientProductNumber, ProductCategoryId = clientProductCategoryId };
                        ctx.Add(product);

                        await ctx.SaveChangesAsync();
                    }

                    string sessionString = "";

                    var interceptor = new Interceptors();
                    interceptor.OnSessionBegin(sba => sessionString += "begin");
                    interceptor.OnSessionEnd(sea => sessionString += "end");
                    interceptor.OnTableChangesApplying(args =>
                    {
                        if (args.TableName == "ProductCategory")
                            Assert.Equal(DmRowState.Added, args.State);

                        if (args.TableName == "Product")
                            Assert.Equal(DmRowState.Added, args.State);
                    });
                    interceptor.OnTableChangesApplied(args =>
                    {
                        if (args.TableChangesApplied.TableName == "ProductCategory")
                        {
                            Assert.Equal(DmRowState.Added, args.TableChangesApplied.State);
                            Assert.Equal(1, args.TableChangesApplied.Applied);
                        }

                        if (args.TableChangesApplied.TableName == "Product")
                        {
                            Assert.Equal(DmRowState.Added, args.TableChangesApplied.State);
                            Assert.Equal(1, args.TableChangesApplied.Applied);
                        }
                    });

                    interceptor.OnTableChangesSelected(args =>
                    {
                        if (args.TableChangesSelected.TableName == "ProductCategory")
                            Assert.Equal(1, args.TableChangesSelected.Inserts);

                        if (args.TableChangesSelected.TableName == "Product")
                            Assert.Equal(1, args.TableChangesSelected.Inserts);
                    });

                    interceptor.OnSchema(args =>
                    {
                        Assert.True(args.Schema.HasTables);
                    });


                    clientRun.Agent.SetInterceptor(interceptor);

                    await clientRun.RunAsync(this.fixture, null, conf, false);

                    //Assert we have go through begin and end session
                    Assert.Equal("beginend", sessionString);

                    clientRun.Agent.SetInterceptor(null);

                }

            }
        }
    }
}
