using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Test.Misc;
using Dotmim.Sync.Tests.Core;
using Dotmim.Sync.Tests.Misc;
using Dotmim.Sync.Tests.Models;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
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
        protected readonly ProviderFixture<CoreProvider> fixture;

        // the server provider
        protected readonly CoreProvider ServerProvider;

        protected virtual AdventureWorksContext GetServerDbContext()
        {
            return new AdventureWorksContext(fixture.ProviderType, ServerProvider.ConnectionString);
        }

        protected virtual AdventureWorksContext GetClientDbContext(ProviderRun providerRun)
        {
            return new AdventureWorksContext(providerRun);
        }

        /// <summary>
        /// on ctor, set the tables we want to use
        /// </summary>
        public BasicTestsBase(ProviderFixture<CoreProvider> fixture)
        {
            this.fixture = fixture;

            // launc fixture configuration on first launch
            this.fixture.Configure();

            // gets the server provider
            this.ServerProvider = this.fixture.NewServerProvider(HelperDB.GetConnectionString(this.fixture.ProviderType, this.fixture.DatabaseName));

            // create a test runner based on my server fixture
            this.testRunner = new TestRunner(fixture, this.ServerProvider);
        }


        /// <summary>
        /// Initialize should be always called.
        /// It creates the clients schemas and make a first sync
        /// Once Initialize() is done, both server and all clients should be equivalent
        /// </summary>
        public async virtual Task Initialize()
        {
            var results = await this.testRunner.RunTestsAsync();

            foreach (var trr in results)
            {
                Assert.Equal(82, trr.Results.TotalChangesDownloaded);
                Assert.Equal(0, trr.Results.TotalChangesUploaded);
            }
        }

        /// <summary>
        /// Should be able to Deprovision a whole database
        /// </summary>
        public async virtual Task Use_Existing_Client_Database_Provision_Deprosivion()
        {
            // Generate a new temp database and a local provider
            var dbName = fixture.GetRandomDatabaseName();
            var connectionString = HelperDB.GetConnectionString(fixture.ProviderType, dbName);

            // create a local provider (the provider we want to test, obviously)
            var localProvider = fixture.NewServerProvider(connectionString);

            try
            {
                // create an empty AdventureWorks client database
                using (var ctx = new AdventureWorksContext(fixture.ProviderType, connectionString, (fixture.ProviderType == ProviderType.Sql), false))
                    await ctx.Database.EnsureCreatedAsync();

                // generate a sync conf to host the schema
                var conf = new SyncConfiguration(fixture.Tables);

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
                    Assert.Equal(false, scopeBuilder.NeedToCreateScopeInfoTable());
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

                        Assert.Equal(false, trackingTablesBuilder.NeedToCreateTrackingTable());

                        Assert.Equal(false, triggersBuilder.NeedToCreateTrigger(Builders.DbTriggerType.Insert));
                        Assert.Equal(false, triggersBuilder.NeedToCreateTrigger(Builders.DbTriggerType.Delete));
                        Assert.Equal(false, triggersBuilder.NeedToCreateTrigger(Builders.DbTriggerType.Update));

                        Assert.Equal(false, spBuider.NeedToCreateProcedure(Builders.DbCommandType.InsertMetadata));
                        Assert.Equal(false, spBuider.NeedToCreateProcedure(Builders.DbCommandType.InsertRow));
                        Assert.Equal(false, spBuider.NeedToCreateProcedure(Builders.DbCommandType.Reset));
                        Assert.Equal(false, spBuider.NeedToCreateProcedure(Builders.DbCommandType.SelectChanges));
                        Assert.Equal(false, spBuider.NeedToCreateProcedure(Builders.DbCommandType.SelectRow));
                        Assert.Equal(false, spBuider.NeedToCreateProcedure(Builders.DbCommandType.DeleteMetadata));
                        Assert.Equal(false, spBuider.NeedToCreateProcedure(Builders.DbCommandType.DeleteRow));

                        // Check if we have mutables columns to see if the update row / metadata have been generated
                        if (dbTableBuilder.TableDescription.MutableColumnsAndNotAutoInc.Any())
                        {
                            Assert.Equal(false, spBuider.NeedToCreateProcedure(Builders.DbCommandType.UpdateRow));
                            Assert.Equal(false, spBuider.NeedToCreateProcedure(Builders.DbCommandType.UpdateMetadata));
                        }

                        if (this.fixture.ProviderType == ProviderType.Sql)
                        {
                            Assert.Equal(false, spBuider.NeedToCreateProcedure(Builders.DbCommandType.BulkDeleteRows));
                            Assert.Equal(false, spBuider.NeedToCreateProcedure(Builders.DbCommandType.BulkInsertRows));
                            Assert.Equal(false, spBuider.NeedToCreateProcedure(Builders.DbCommandType.BulkInsertRows));
                        }

                        dbConnection.Close();

                    }
                }

                // Try to make a sync 

                var syncAgent = new SyncAgent(localProvider, this.ServerProvider, this.fixture.Tables);
                var results = await syncAgent.SynchronizeAsync();
                Assert.Equal(82, results.TotalChangesDownloaded);



            }
            finally
            {
                // ensure database is created and filled with some data
                using (var ctx = new AdventureWorksContext(fixture.ProviderType, connectionString))
                {
                    await ctx.Database.EnsureDeletedAsync();
                }

            }
        }

        /// <summary>
        /// Should raise a correct error when a bad connection is defined
        /// </summary>
        public async virtual Task Bad_Server_Connection_Should_Raise_Error()
        {
            var provider = this.fixture.NewServerProvider($@"Server=unknown;Database=unknown;UID=sa;PWD=unknown");
            // create a new runner with a provider with bad connection string
            var tempTestRunner = new TestRunner(fixture, this.ServerProvider);

            var results = await tempTestRunner.RunTestsAsync(false);

            foreach (var trr in results)
            {
                Assert.IsType(typeof(SyncException), trr.Exception);
                SyncException se = trr.Exception as SyncException;
                Assert.Equal(SyncExceptionType.Data, se.Type);
            }

        }

        /// <summary>
        /// Should raise a correct error when a bad connection is defined
        /// </summary>
        public async virtual Task Bad_Client_Connection_Should_Raise_Error()
        {
            // set a bad connection string
            this.fixture.ClientRuns.ForEach(tr => tr.ConnectionString = $@"Server=unknown;Database=unknown;UID=sa;PWD=unknown");

            var results = await this.testRunner.RunTestsAsync(false);

            foreach (var trr in results)
            {
                Assert.IsType(typeof(SyncException), trr.Exception);
                SyncException se = trr.Exception as SyncException;
            }
        }

        /// <summary>
        /// Insert one row on server, should be correctly sync on all clients
        /// </summary>
        public async virtual Task Insert_One_Table_From_Server()
        {
            foreach (var conf in TestConfigurations.GetConfigurations())
            {
                var name = Path.GetRandomFileName().Replace(".", "").ToLowerInvariant();
                var productNumber = Path.GetRandomFileName().Replace(".", "").ToUpperInvariant().Substring(0, 10);

                Product product = new Product { ProductId = Guid.NewGuid(), Name = name, ProductNumber = productNumber };

                using (var serverDbCtx = GetServerDbContext())
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
        public async virtual Task Insert_One_Table_From_Client()
        {
            foreach (var conf in TestConfigurations.GetConfigurations())
            {
                foreach (var clientRun in this.fixture.ClientRuns)
                {
                    var name = Path.GetRandomFileName().Replace(".", "");
                    var id = name.ToUpperInvariant().Substring(0, 6);

                    using (var ctx = GetClientDbContext(clientRun))
                    {
                        ProductCategory pc = new ProductCategory { Name = name, ProductCategoryId = id };
                        ctx.ProductCategory.Add(pc);
                        await ctx.SaveChangesAsync();
                    }
                }

                // first sync. each client will send its own row to the server
                // so the first client won't get anything from the second
                // the last one will have all the rows, as well as the server
                var results = await this.testRunner.RunTestsAsync(conf);

                for (int i = 0; i < this.fixture.ClientRuns.Count; i++)
                {
                    var testRunner = this.fixture.ClientRuns[i];

                    Assert.Equal(0 + i, testRunner.Results.TotalChangesDownloaded);
                    Assert.Equal(1, testRunner.Results.TotalChangesUploaded);
                }

                // now a last sync to be sure all clients have all rows
                var results2 = await this.testRunner.RunTestsAsync(conf);

                // check rows count on server
                int productCategoryRowCount = 0;
                using (var serverDbCtx = GetServerDbContext())
                {
                    productCategoryRowCount = await serverDbCtx.ProductCategory.AsNoTracking().CountAsync();
                }

                // check rows number on all product category table
                foreach (var clientRun in this.fixture.ClientRuns)
                {
                    using (var ctx = GetClientDbContext(clientRun))
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
        public async virtual Task Insert_Multiple_Tables_From_Server()
        {
            foreach (var conf in TestConfigurations.GetConfigurations())
            {
                var productId = Guid.NewGuid();
                var productName = Path.GetRandomFileName().Replace(".", "");
                var productNumber = productName.ToUpperInvariant().Substring(0, 10);

                var productCategoryName = Path.GetRandomFileName().Replace(".", "");
                var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

                // insert 2 rows
                using (var serverDbCtx = GetServerDbContext())
                {
                    ProductCategory pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                    serverDbCtx.Add(pc);

                    Product product = new Product { ProductId = productId, Name = productName, ProductNumber = productNumber };
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
        public async virtual Task Insert_Multiple_Tables_From_Client()
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


                    using (var ctx = GetClientDbContext(testRun))
                    {
                        ProductCategory pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                        ctx.Add(pc);
                        Product product = new Product { ProductId = productId, Name = productName, ProductNumber = productNumber, ProductCategoryId = productCategoryId };
                        ctx.Add(product);

                        await ctx.SaveChangesAsync();
                    }
                }

                // first sync. each client will send its own row to the server
                // so the first client won't get anything from the second
                // the last one will have all the rows, as well as the server
                var results = await this.testRunner.RunTestsAsync(conf);

                for (int i = 0; i < this.fixture.ClientRuns.Count; i++)
                {
                    var testRunner = this.fixture.ClientRuns[i];

                    Assert.Equal(0 + (i * 2), testRunner.Results.TotalChangesDownloaded);
                    Assert.Equal(2, testRunner.Results.TotalChangesUploaded);
                }

                // now a last sync to be sure all clients have all rows
                var results2 = await this.testRunner.RunTestsAsync(conf);

                // check rows count on server
                using (var serverDbCtx = GetServerDbContext())
                {
                    var productRowCount = await serverDbCtx.Product.AsNoTracking().CountAsync();
                    var productCategoryCount = await serverDbCtx.ProductCategory.AsNoTracking().CountAsync();

                    // check rows number on all product category table
                    foreach (var testRun in this.fixture.ClientRuns)
                    {
                        using (var ctx = GetClientDbContext(testRun))
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
        public async virtual Task Update_One_Table_From_Server()
        {
            foreach (var conf in TestConfigurations.GetConfigurations())
            {
                string randomAddressLine;
                string city;
                string stateProvince;

                // get the first address with ID=1
                using (var serverDbCtx = GetServerDbContext())
                {
                    Address address = await serverDbCtx.Address.SingleAsync(a => a.AddressId == 1);

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
                    using (var ctx = GetClientDbContext(trr))
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
        public async virtual Task Update_One_Table_From_Client()
        {
            foreach (var conf in TestConfigurations.GetConfigurations())
            {
                // get a for instead of foreach to have an index corresponding to the Address pkeyid, ensuring I won't have conflicts beetween clients
                // start on "Index-Base-1"
                for (int i = 1; i <= fixture.ClientRuns.Count; i++)
                {
                    ProviderRun testRun = this.fixture.ClientRuns[i - 1];
                    using (var ctx = GetClientDbContext(testRun))
                    {
                        // get the client address with ID=i
                        Address address = await ctx.Address.SingleAsync(a => a.AddressId == i);

                        // Update at least two properties
                        // Use a random string for each configuration
                        string randomAddressLine = Path.GetRandomFileName().Replace(".", "");
                        string stateProvince = "Floridia";

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

                for (int i = 0; i < this.fixture.ClientRuns.Count; i++)
                {
                    var testRunner = this.fixture.ClientRuns[i];

                    Assert.Equal(0 + i, testRunner.Results.TotalChangesDownloaded);
                    Assert.Equal(1, testRunner.Results.TotalChangesUploaded);
                }

                // Sync again to be sure all clients have the exact same lines
                await this.testRunner.RunTestsAsync(conf);

                // get all address from server
                using (var serverDbCtx = GetServerDbContext())
                {
                    var serverAddresses = await serverDbCtx.Address.AsNoTracking().ToListAsync();

                    for (int i = 0; i < this.fixture.ClientRuns.Count; i++)
                    {
                        var clientRun = this.fixture.ClientRuns[i];

                        using (var ctx = GetClientDbContext(clientRun))
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
        public async virtual Task Conflict_Insert_Insert_Server_Should_Wins()
        {
            foreach (var conf in TestConfigurations.GetConfigurations())
            {
                // generate a conflict product category id
                var conflictProductCategoryId = Path.GetRandomFileName().Replace(".", "").ToUpperInvariant().Substring(0, 6);

                var productCategoryNameClient = Path.GetRandomFileName().Replace(".", "").ToUpperInvariant().Substring(3, 6);
                var productCategoryNameServer = Path.GetRandomFileName().Replace(".", "").ToUpperInvariant().Substring(3, 6);

                // Insert a conflict product category and a product
                foreach (var testRun in this.fixture.ClientRuns)
                {
                    using (var ctx = GetClientDbContext(testRun))
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
                using (var serverDbCtx = GetServerDbContext())
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

                using (var serverDbCtx = GetServerDbContext())
                {
                    // check server product category
                    var checkProductCategoryServer = await serverDbCtx.ProductCategory.AsNoTracking().SingleAsync(pc => pc.ProductCategoryId == conflictProductCategoryId);

                    Assert.Equal(productCategoryNameServer, checkProductCategoryServer.Name);
                }
                // check client product category row
                foreach (var testRun in this.fixture.ClientRuns)
                {
                    using (var ctx = GetClientDbContext(testRun))
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
        public async virtual Task Conflict_Insert_Insert_Client_Should_Wins_Coz_Configuration()
        {
            foreach (var conf in TestConfigurations.GetConfigurations())
            {
                // set manually conf resolution to client wins
                conf.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;

                // generate a conflict product category id
                var conflictProductCategoryId = Path.GetRandomFileName().Replace(".", "").ToUpperInvariant().Substring(0, 6);

                var productCategoryNameClient = "Client " + Path.GetRandomFileName().Replace(".", "");
                var productCategoryNameServer = "Server " + Path.GetRandomFileName().Replace(".", "");

                // Insert a conflict product category and a product
                foreach (var testRun in this.fixture.ClientRuns)
                {
                    using (var ctx = GetClientDbContext(testRun))
                    {
                        ctx.Add(new ProductCategory
                        {
                            ProductCategoryId = conflictProductCategoryId,
                            Name = productCategoryNameClient
                        });

                        await ctx.SaveChangesAsync();

                    }
                }

                using (var serverDbCtx = GetServerDbContext())
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

                for (int i = 0; i < fixture.ClientRuns.Count; i++)
                {
                    ProviderRun testRunner = this.fixture.ClientRuns[i];

                    // 0+i : Download is false on sqlite
                    //Assert.Equal(0 + i, testRunner.Results.TotalChangesDownloaded);
                    Assert.Equal(1, testRunner.Results.TotalChangesUploaded);
                    Assert.Equal(1, testRunner.Results.TotalSyncConflicts);
                }

                using (var serverDbCtx = GetServerDbContext())
                {
                    // check server product category
                    var checkProductCategoryServer = await serverDbCtx.ProductCategory.AsNoTracking().SingleAsync(pc => pc.ProductCategoryId == conflictProductCategoryId);

                    Assert.Equal(productCategoryNameClient, checkProductCategoryServer.Name);
                }

                // check client product category row
                foreach (var testRun in this.fixture.ClientRuns)
                {
                    using (var ctx = GetClientDbContext(testRun))
                    {
                        // check client product category
                        var checkProductCategoryClient = await ctx.ProductCategory.AsNoTracking().SingleAsync(pc => pc.ProductCategoryId == conflictProductCategoryId);

                        Assert.Equal(productCategoryNameClient, checkProductCategoryClient.Name);
                    }
                }
            }
        }

        /// <summary>
        /// Conflict resolution on insert-insert. 
        /// Use the handler to resolve the conflict using the action [ConflictAction.ClientWins].
        /// </summary>
        public async virtual Task Conflict_Insert_Insert_Client_Should_Wins_Coz_Handler_Raised()
        {
            foreach (var conf in TestConfigurations.GetConfigurations())
            {
                // generate a conflict product category id
                var conflictProductCategoryId = Path.GetRandomFileName().Replace(".", "").ToUpperInvariant().Substring(0, 6);

                var productCategoryNameClient = "Client " + Path.GetRandomFileName().Replace(".", "");
                var productCategoryNameServer = "Server " + Path.GetRandomFileName().Replace(".", "");
                // Insert a conflict product category and a product

                foreach (var testRun in this.fixture.ClientRuns)
                {
                    using (var ctx = GetClientDbContext(testRun))
                    {
                        ctx.Add(new ProductCategory
                        {
                            ProductCategoryId = conflictProductCategoryId,
                            Name = productCategoryNameClient
                        });

                        await ctx.SaveChangesAsync();
                    }
                }

                using (var serverDbCtx = GetServerDbContext())
                {
                    // insert conflict rows on server
                    serverDbCtx.Add(new ProductCategory
                    {
                        ProductCategoryId = conflictProductCategoryId,
                        Name = productCategoryNameServer
                    });

                    await serverDbCtx.SaveChangesAsync();
                }

                // register applychangedfailed to all sync agent 
                void applyChangedFailed(object s, ApplyChangeFailedEventArgs changeFailedEventAgrs)
                {
                    changeFailedEventAgrs.Action = ConflictAction.ClientWins;
                }

                this.testRunner.BeginRun = provider => provider.ApplyChangedFailed += applyChangedFailed;
                this.testRunner.EndRun = provider => provider.ApplyChangedFailed -= applyChangedFailed;

                var results = await this.testRunner.RunTestsAsync(conf);

                foreach (var testRunner in this.fixture.ClientRuns)
                {
                    Assert.Equal(1, testRunner.Results.TotalChangesUploaded);
                    Assert.Equal(1, testRunner.Results.TotalSyncConflicts);
                }

                using (var serverDbCtx = GetServerDbContext())
                {
                    // check server product category
                    var checkProductCategoryServer = await serverDbCtx.ProductCategory.AsNoTracking().SingleAsync(pc => pc.ProductCategoryId == conflictProductCategoryId);
                    Assert.Equal(productCategoryNameClient, checkProductCategoryServer.Name);
                }

                // check client product category row
                foreach (var testRun in this.fixture.ClientRuns)
                {
                    using (var ctx = GetClientDbContext(testRun))
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
        public async virtual Task Conflict_Update_Update_Client_Should_Wins_Coz_Configuration()
        {
            foreach (var conf in TestConfigurations.GetConfigurations())
            {
                // set manually conf resolution to client wins
                conf.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;

                // generate a conflict product category id
                var conflictProductCategoryId = "BIKES";

                var productCategoryNameClient = "Client BIKES " + Path.GetRandomFileName().Replace(".", "");
                var productCategoryNameServer = "Server BIKES " + Path.GetRandomFileName().Replace(".", "");

                // Insert a conflict product category and a product
                foreach (var testRun in this.fixture.ClientRuns)
                {
                    using (var ctx = GetClientDbContext(testRun))
                    {
                        ctx.ProductCategory.Update(new ProductCategory
                        {
                            ProductCategoryId = conflictProductCategoryId,
                            Name = productCategoryNameClient
                        });

                        await ctx.SaveChangesAsync();

                    }
                }

                using (var serverDbCtx = GetServerDbContext())
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

                for (int i = 0; i < fixture.ClientRuns.Count; i++)
                {
                    ProviderRun testRunner = this.fixture.ClientRuns[i];

                    Assert.Equal(1, testRunner.Results.TotalChangesUploaded);
                    Assert.Equal(1, testRunner.Results.TotalSyncConflicts);
                }

                using (var serverDbCtx = GetServerDbContext())
                {
                    // check server product category
                    var checkProductCategoryServer = await serverDbCtx.ProductCategory.AsNoTracking().SingleAsync(pc => pc.ProductCategoryId == conflictProductCategoryId);

                    Assert.Equal(productCategoryNameClient, checkProductCategoryServer.Name);
                }
                // check client product category row
                foreach (var testRun in this.fixture.ClientRuns)
                {
                    using (var ctx = GetClientDbContext(testRun))
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
        /// Use the handler to resolve the conflict using the action [ConflictAction.ClientWins].
        /// </summary>
        public async virtual Task Conflict_Update_Update_Client_Should_Wins_Coz_Handler_Raised()
        {
            foreach (var conf in TestConfigurations.GetConfigurations())
            {
                // generate a conflict product category id
                var conflictProductCategoryId = "BIKES";

                var productCategoryNameClient = "Client BIKES " + Path.GetRandomFileName().Replace(".", "");
                var productCategoryNameServer = "Server BIKES " + Path.GetRandomFileName().Replace(".", "");

                // Insert a conflict product category and a product
                foreach (var testRun in this.fixture.ClientRuns)
                {
                    using (var ctx = GetClientDbContext(testRun))
                    {

                        ctx.ProductCategory.Update(new ProductCategory
                        {
                            ProductCategoryId = conflictProductCategoryId,
                            Name = productCategoryNameClient
                        });

                        await ctx.SaveChangesAsync();

                    }
                }

                using (var serverDbCtx = GetServerDbContext())
                {
                    // insert conflict rows on server
                    serverDbCtx.Update(new ProductCategory
                    {
                        ProductCategoryId = conflictProductCategoryId,
                        Name = productCategoryNameServer
                    });

                    await serverDbCtx.SaveChangesAsync();
                }

                // register applychangedfailed to all sync agent 
                void applyChangedFailed(object s, ApplyChangeFailedEventArgs changeFailedEventAgrs)
                {
                    changeFailedEventAgrs.Action = ConflictAction.ClientWins;
                }

                this.testRunner.BeginRun = provider => provider.ApplyChangedFailed += applyChangedFailed;
                this.testRunner.EndRun = provider => provider.ApplyChangedFailed -= applyChangedFailed;

                var results = await this.testRunner.RunTestsAsync(conf);

                for (int i = 0; i < fixture.ClientRuns.Count; i++)
                {
                    ProviderRun testRunner = this.fixture.ClientRuns[i];

                    Assert.Equal(1, testRunner.Results.TotalChangesUploaded);
                    Assert.Equal(1, testRunner.Results.TotalSyncConflicts);
                }

                using (var serverDbCtx = GetServerDbContext())
                {
                    // check server product category
                    var checkProductCategoryServer = await serverDbCtx.ProductCategory.AsNoTracking().SingleAsync(pc => pc.ProductCategoryId == conflictProductCategoryId);

                    Assert.Equal(productCategoryNameClient, checkProductCategoryServer.Name);
                }
                // check client product category row
                foreach (var testRun in this.fixture.ClientRuns)
                {
                    using (var ctx = GetClientDbContext(testRun))
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
        public async virtual Task Conflict_Update_Update_Resolve_By_Merge()
        {
            foreach (var conf in TestConfigurations.GetConfigurations())
            {
                // generate a conflict product category id
                var conflictProductCategoryId = "BIKES";

                var productCategoryNameClient = "Client BIKES " + Path.GetRandomFileName().Replace(".", "");
                var productCategoryNameServer = "Server BIKES " + Path.GetRandomFileName().Replace(".", "");
                var productCategoryNameMerged = "BOTH BIKES" + Path.GetRandomFileName().Replace(".", "");

                // Insert a conflict product category and a product
                foreach (var testRun in this.fixture.ClientRuns)
                {
                    using (var ctx = GetClientDbContext(testRun))
                    {

                        ctx.ProductCategory.Update(new ProductCategory
                        {
                            ProductCategoryId = conflictProductCategoryId,
                            Name = productCategoryNameClient
                        });

                        await ctx.SaveChangesAsync();

                    }
                }

                using (var serverDbCtx = GetServerDbContext())
                {
                    // insert conflict rows on server
                    serverDbCtx.Update(new ProductCategory
                    {
                        ProductCategoryId = conflictProductCategoryId,
                        Name = productCategoryNameServer
                    });

                    await serverDbCtx.SaveChangesAsync();
                }

                void applyChangedFailed(object s, ApplyChangeFailedEventArgs changeFailedEventAgrs)
                {
                    changeFailedEventAgrs.Action = ConflictAction.MergeRow;
                    changeFailedEventAgrs.FinalRow["Name"] = productCategoryNameMerged;
                };

                this.testRunner.BeginRun = provider => provider.ApplyChangedFailed += applyChangedFailed;
                this.testRunner.EndRun = provider => provider.ApplyChangedFailed -= applyChangedFailed;

                var results = await this.testRunner.RunTestsAsync(conf);

                for (int i = 0; i < fixture.ClientRuns.Count; i++)
                {
                    ProviderRun testRunner = this.fixture.ClientRuns[i];

                    Assert.Equal(1, testRunner.Results.TotalChangesUploaded);
                    Assert.Equal(1, testRunner.Results.TotalSyncConflicts);
                }

                using (var serverDbCtx = GetServerDbContext())
                {
                    // check server product category
                    var checkProductCategoryServer = await serverDbCtx.ProductCategory.AsNoTracking().SingleAsync(pc => pc.ProductCategoryId == conflictProductCategoryId);

                    Assert.Equal(productCategoryNameMerged, checkProductCategoryServer.Name);
                }
                // check client product category row
                foreach (var testRun in this.fixture.ClientRuns)
                {
                    using (var ctx = GetClientDbContext(testRun))
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
        public async virtual Task Conflict_Update_Update_Server_Should_Wins()
        {
            foreach (var conf in TestConfigurations.GetConfigurations())
            {
                // generate a conflict product category id
                var conflictProductCategoryId = "BIKES";

                var productCategoryNameClient = "Client BIKES " + Path.GetRandomFileName().Replace(".", "");
                var productCategoryNameServer = "Server BIKES " + Path.GetRandomFileName().Replace(".", "");

                // Insert a conflict product category and a product
                foreach (var testRun in this.fixture.ClientRuns)
                {
                    using (var ctx = GetClientDbContext(testRun))
                    {
                        ctx.ProductCategory.Update(new ProductCategory
                        {
                            ProductCategoryId = conflictProductCategoryId,
                            Name = productCategoryNameClient
                        });

                        await ctx.SaveChangesAsync();

                    }
                }

                using (var serverDbCtx = GetServerDbContext())
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

                for (int i = 0; i < fixture.ClientRuns.Count; i++)
                {
                    ProviderRun testRunner = this.fixture.ClientRuns[i];

                    Assert.Equal(1, testRunner.Results.TotalChangesUploaded);
                    Assert.Equal(1, testRunner.Results.TotalChangesDownloaded);
                    Assert.Equal(1, testRunner.Results.TotalSyncConflicts);
                }

                using (var serverDbCtx = GetServerDbContext())
                {
                    // check server product category
                    var checkProductCategoryServer = await serverDbCtx.ProductCategory.AsNoTracking().SingleAsync(pc => pc.ProductCategoryId == conflictProductCategoryId);

                    Assert.Equal(productCategoryNameServer, checkProductCategoryServer.Name);
                }
                // check client product category row
                foreach (var testRun in this.fixture.ClientRuns)
                {
                    using (var ctx = GetClientDbContext(testRun))
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
        public async virtual Task Delete_One_Table_From_Server()
        {
            var configurations = TestConfigurations.GetConfigurations();

            for (int i = 0; i < configurations.Count; i++)
            {
                SyncConfiguration conf = configurations[i];

                // get a new item to delete
                var id = 6 + i;

                // first of all, delete the line from server
                using (var serverDbCtx = GetServerDbContext())
                {
                    // Get the addresses query
                    var address = await serverDbCtx.Address.SingleAsync(a => a.AddressId == id);

                    // remove them
                    serverDbCtx.Address.Remove(address);

                    // Execute query
                    await serverDbCtx.SaveChangesAsync();
                }

                var results = await this.testRunner.RunTestsAsync(conf);

                int finalServerAddressesCount = 0;
                using (var serverDbCtx = GetServerDbContext())
                {
                    finalServerAddressesCount = await serverDbCtx.Address.AsNoTracking().CountAsync();
                }

                foreach (var trr in results)
                {
                    Assert.Equal(1, trr.Results.TotalChangesDownloaded);
                    Assert.Equal(0, trr.Results.TotalChangesUploaded);

                    int finalClientAddressesCount = 0;

                    // check row updated values
                    using (var ctx = GetClientDbContext(trr))
                    {
                        finalClientAddressesCount = await ctx.Address.AsNoTracking().CountAsync();
                    }

                    Assert.Equal(finalServerAddressesCount, finalClientAddressesCount);

                }
            }
        }

        /// <summary>
        /// Delete some rows in multiple tables. Choose rows in table with foreign key constraint
        /// </summary>
        public async virtual Task Delete_Multiple_Tables_From_Server()
        {
            var configurations = TestConfigurations.GetConfigurations();

            for (int i = 0; i < configurations.Count; i++)
            {
                SyncConfiguration conf = configurations[i];

                // first of all, insert a product category and a product
                var productId = Guid.NewGuid();
                var productName = Path.GetRandomFileName().Replace(".", "");
                var productNumber = productName.ToUpperInvariant().Substring(0, 10);

                var productCategoryName = Path.GetRandomFileName().Replace(".", "");
                var productCategoryId = productCategoryName.ToUpperInvariant().Substring(0, 6);

                // insert 2 rows
                using (var serverDbCtx = GetServerDbContext())
                {
                    ProductCategory pc = new ProductCategory { ProductCategoryId = productCategoryId, Name = productCategoryName };
                    serverDbCtx.Add(pc);

                    Product product = new Product { ProductId = productId, Name = productName, ProductNumber = productNumber };
                    serverDbCtx.Add(product);

                    await serverDbCtx.SaveChangesAsync();
                }

                // Sync to be sure everything is correct on both side
                await this.testRunner.RunTestsAsync(conf);

                // Then delete these lines from server
                using (var serverDbCtx = GetServerDbContext())
                {
                    // Get the lines
                    var sproduct = await serverDbCtx.Product.SingleAsync(p => p.ProductId == productId);
                    var sproductCat = await serverDbCtx.ProductCategory.SingleAsync(pc => pc.ProductCategoryId == productCategoryId);

                    // remove them
                    serverDbCtx.Remove(sproduct);
                    serverDbCtx.Remove(sproductCat);

                    // Execute query
                    await serverDbCtx.SaveChangesAsync();
                }

                var results = await this.testRunner.RunTestsAsync(conf);

                int finalServerProductCount = 0;
                int finalServerProductCategoryCount = 0;

                using (var serverDbCtx = GetServerDbContext())
                {
                    finalServerProductCount = await serverDbCtx.Product.AsNoTracking().CountAsync();
                    finalServerProductCategoryCount = await serverDbCtx.ProductCategory.AsNoTracking().CountAsync();
                }

                foreach (var trr in results)
                {
                    Assert.Equal(2, trr.Results.TotalChangesDownloaded);
                    Assert.Equal(0, trr.Results.TotalChangesUploaded);

                    int finalClientProductCount = 0;
                    int finalClientProductCategoryCount = 0;

                    // check row updated values
                    using (var ctx = GetClientDbContext(trr))
                    {
                        finalClientProductCount = await ctx.Product.AsNoTracking().CountAsync();
                        finalClientProductCategoryCount = await ctx.ProductCategory.AsNoTracking().CountAsync();
                    }

                    Assert.Equal(finalServerProductCount, finalClientProductCount);
                    Assert.Equal(finalServerProductCategoryCount, finalClientProductCategoryCount);

                }
            }
        }

        /// <summary>
        /// The idea is to insert an item, then delete it, then sync.
        /// To see how the client is interacting with something to delete he does not have 
        /// </summary>
        public async virtual Task Insert_Then_Delete_From_Server_Then_Sync()
        {
            foreach (var conf in TestConfigurations.GetConfigurations())
            {
                var productCategoryId = Path.GetRandomFileName().Replace(".", "").ToUpperInvariant().Substring(0, 6);
                var productCategoryNameServer = Path.GetRandomFileName().Replace(".", "").ToUpperInvariant().Substring(3, 6);

                // insert the row on the server
                using (var serverDbCtx = GetServerDbContext())
                {
                    serverDbCtx.Add(new ProductCategory
                    {
                        ProductCategoryId = productCategoryId,
                        Name = productCategoryNameServer
                    });

                    await serverDbCtx.SaveChangesAsync();
                }

                // then delete it
                using (var serverDbCtx = GetServerDbContext())
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
        public async virtual Task Insert_Then_Update_From_Server_Then_Sync()
        {
            foreach (var conf in TestConfigurations.GetConfigurations())
            {
                var productCategoryId = Path.GetRandomFileName().Replace(".", "").ToUpperInvariant().Substring(0, 6);
                var productCategoryNameServer = Path.GetRandomFileName().Replace(".", "").ToUpperInvariant().Substring(3, 6);
                var productCategoryNameServerUpdated = Path.GetRandomFileName().Replace(".", "").ToUpperInvariant().Substring(3, 6);

                // insert the row on the server
                using (var serverDbCtx = GetServerDbContext())
                {
                    serverDbCtx.Add(new ProductCategory
                    {
                        ProductCategoryId = productCategoryId,
                        Name = productCategoryNameServer
                    });

                    await serverDbCtx.SaveChangesAsync();
                }

                // Then update the row
                using (var serverDbCtx = GetServerDbContext())
                {
                    var pc = await serverDbCtx.ProductCategory.SingleAsync(o => o.ProductCategoryId == productCategoryId);

                    pc.Name = productCategoryNameServerUpdated;

                    await serverDbCtx.SaveChangesAsync();
                }


                var results = await this.testRunner.RunTestsAsync();

                foreach (var trr in results)
                {
                    Assert.Equal(1, trr.Results.TotalChangesDownloaded);
                    Assert.Equal(0, trr.Results.TotalChangesUploaded);

                    // Check
                    using (var ctx = GetClientDbContext(trr))
                    {
                        var pc = await ctx.ProductCategory.SingleAsync(o => o.ProductCategoryId == productCategoryId);
                        Assert.Equal(productCategoryNameServerUpdated, pc.Name);
                    }
                }
            }
        }


        public Task Delete_One_Table_From_Client()
        {
            throw new NotImplementedException();
        }
        public Task Delete_Multiple_Tables_From_Client()
        {
            throw new NotImplementedException();
        }

        public Task Insert_Update_Delete_From_Server()
        {
            throw new NotImplementedException();
        }
        public Task No_Rows()
        {
            throw new NotImplementedException();
        }
        public Task Update_Multiple_Rows_From_Client()
        {
            throw new NotImplementedException();
        }
        public Task Update_Multiple_Rows_From_Server()
        {
            throw new NotImplementedException();
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

    }



}
