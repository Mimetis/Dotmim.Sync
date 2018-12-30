using Dotmim.Sync;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SampleConsole;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Http;
using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using SerializationFormat = Dotmim.Sync.Enumerations.SerializationFormat;

internal class Program
{
    private static void Main(string[] args)
    {
        TestSync().GetAwaiter().GetResult();

        Console.ReadLine();
    }

    public static string GetDatabaseConnectionString(string dbName) =>
        System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(System.Runtime.InteropServices.OSPlatform.Windows) ?
        $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog={dbName}; Integrated Security=true;" :
        $"Data Source=localhost; Database={dbName}; User=sa; Password=QWE123qwe";

    public static string GetMySqlDatabaseConnectionString(string dbName) =>
        $@"Server=127.0.0.1; Port=3306; Database={dbName}; Uid=root; Pwd=azerty31$;";

    public static async Task SyncHttpThroughKestellAsync()
    {
        // server provider
        var serverProvider = new SqlSyncProvider(GetDatabaseConnectionString("AdventureWorks"));
        // proxy server based on server provider
        var proxyServerProvider = new WebProxyServerProvider(serverProvider);

        // client provider
        var client1Provider = new SqlSyncProvider(GetDatabaseConnectionString("Client"));
        // proxy client provider 
        var proxyClientProvider = new WebProxyClientProvider();

        var tables = new string[] {"ProductCategory",
                "ProductDescription", "ProductModel",
                "Product", "ProductModelProductDescription",
                "Address", "Customer", "CustomerAddress",
                "SalesOrderHeader", "SalesOrderDetail" };

        var configuration = new SyncConfiguration(tables)
        {
            ScopeName = "AdventureWorks",
            ScopeInfoTableName = "tscopeinfo",
            SerializationFormat = Dotmim.Sync.Enumerations.SerializationFormat.Binary,
            StoredProceduresPrefix = "s",
            StoredProceduresSuffix = "",
            TrackingTablesPrefix = "t",
            TrackingTablesSuffix = "",
        };

        var optionsClient = new SyncOptions
        {
            BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "client"),
            BatchSize = 400,
            CleanMetadatas = true,
            UseBulkOperations = true,
            UseVerboseErrors = false
        };

        var optionsServer = new SyncOptions
        {
            BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "server"),
            BatchSize = 400,
            CleanMetadatas = false,
            UseBulkOperations = true,
            UseVerboseErrors = false
        };


        var serverHandler = new RequestDelegate(async context =>
        {
            proxyServerProvider.Configuration = configuration;

            await proxyServerProvider.HandleRequestAsync(context);
        });
        using (var server = new KestrellTestServer())
        {
            var clientHandler = new ResponseDelegate(async (serviceUri) =>
            {
                proxyClientProvider.ServiceUri = new Uri(serviceUri);

                var syncAgent = new SyncAgent(client1Provider, proxyClientProvider);

                do
                {
                    Console.Clear();
                    Console.WriteLine("Sync Start");
                    try
                    {
                        var cts = new CancellationTokenSource();

                        Console.WriteLine("--------------------------------------------------");
                        Console.WriteLine("1 : Normal synchronization.");
                        Console.WriteLine("2 : Fill configuration from server side");
                        Console.WriteLine("3 : Synchronization with reinitialize");
                        Console.WriteLine("4 : Synchronization with upload and reinitialize");
                        Console.WriteLine("5 : Deprovision everything from client side (tables included)");
                        Console.WriteLine("6 : Deprovision everything from client side (tables not included)");
                        Console.WriteLine("7 : Deprovision everything from server side (tables not included)");
                        Console.WriteLine("8 : Provision everything on the client side (tables included)");
                        Console.WriteLine("9 : Provision everything on the server side (tables not included)");
                        Console.WriteLine("10 : Insert datas on client");
                        Console.WriteLine("--------------------------------------------------");
                        Console.WriteLine("What's your choice ? ");
                        Console.WriteLine("--------------------------------------------------");
                        var choice = Console.ReadLine();

                        if (int.TryParse(choice, out var choiceNumber))
                        {
                            Console.WriteLine($"You choose {choice}. Start operation....");
                            switch (choiceNumber)
                            {
                                case 1:
                                    var s1 = await syncAgent.SynchronizeAsync(cts.Token);
                                    Console.WriteLine(s1);
                                    break;
                                case 2:
                                    var ctx = new SyncContext(Guid.NewGuid());
                                    var syncConfigProvider = new SqlSyncProvider(GetDatabaseConnectionString("AdventureWorks"));
                                    (ctx, configuration.Schema) = await syncConfigProvider.EnsureSchemaAsync(ctx, new Dotmim.Sync.Messages.MessageEnsureSchema
                                    {
                                        Schema = configuration.Schema,
                                        SerializationFormat = Dotmim.Sync.Enumerations.SerializationFormat.Json
                                    });
                                    break;
                                case 3:
                                    s1 = await syncAgent.SynchronizeAsync(SyncType.Reinitialize, cts.Token);
                                    Console.WriteLine(s1);
                                    break;
                                case 4:
                                    s1 = await syncAgent.SynchronizeAsync(SyncType.ReinitializeWithUpload, cts.Token);
                                    Console.WriteLine(s1);
                                    break;
                                case 5:
                                    var clientSyncProvider = syncAgent.LocalProvider as SqlSyncProvider;
                                    await clientSyncProvider.DeprovisionAsync(configuration, SyncProvision.All | SyncProvision.Table);
                                    Console.WriteLine("Deprovision complete on client");
                                    break;
                                case 6:
                                    var client2SyncProvider = syncAgent.LocalProvider as SqlSyncProvider;
                                    await client2SyncProvider.DeprovisionAsync(configuration, SyncProvision.All);
                                    Console.WriteLine("Deprovision complete on client");
                                    break;
                                case 7:
                                    var remoteSyncProvider = new SqlSyncProvider(GetDatabaseConnectionString("AdventureWorks"));
                                    await remoteSyncProvider.DeprovisionAsync(configuration, SyncProvision.All);
                                    Console.WriteLine("Deprovision complete on remote");
                                    break;

                                case 8:
                                    var clientSyncProvider2 = syncAgent.LocalProvider as SqlSyncProvider;
                                    await clientSyncProvider2.ProvisionAsync(configuration, SyncProvision.All | SyncProvision.Table);
                                    Console.WriteLine("Provision complete on client");
                                    break;
                                case 9:
                                    var remoteSyncProvider2 = new SqlSyncProvider(GetDatabaseConnectionString("AdventureWorks"));
                                    await remoteSyncProvider2.ProvisionAsync(configuration, SyncProvision.All);
                                    Console.WriteLine("Provision complete on remote");
                                    break;
                                case 10:
                                    var c = GetDatabaseConnectionString("Adv");
                                    var catId = await InsertProductCategory(c);
                                    var modelId = await InsertProductModel(c);
                                    await InsertProduct(c, catId, modelId);
                                    Console.WriteLine("Inserted a model, a category and a product.");
                                    break;
                                default:
                                    break;

                            }
                        }


                    }
                    catch (SyncException e)
                    {
                        Console.WriteLine(e.ToString());
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("UNKNOW EXCEPTION : " + e.Message);
                    }


                    Console.WriteLine("--------------------------------------------------");
                    Console.WriteLine("Press a key to choose again, or Escapte to end");

                } while (Console.ReadKey().Key != ConsoleKey.Escape);


            });
            await server.Run(serverHandler, clientHandler);
        }

    }

    private static async Task<int> InsertProductModel(string connectionString)
    {
        var insertCategory = @"
            INSERT INTO [dbo].[ProductModel] ([Name],[rowguid], [ModifiedDate]) VALUES (@Name, @rowguid, @ModifiedDate);
            SELECT @@IDENTITY;
        ";

        int categoryId;
        using (var connection = new SqlConnection(connectionString))
        {
            using (var cmd = new SqlCommand(insertCategory, connection))
            {
                var p = new SqlParameter("@Name", SqlDbType.NVarChar)
                {
                    Value = "Paris LL Round Bike " + Guid.NewGuid().ToString().Substring(0, 4)
                };
                cmd.Parameters.Add(p);

                p = new SqlParameter("@rowguid", SqlDbType.UniqueIdentifier)
                {
                    Value = Guid.NewGuid()
                };
                cmd.Parameters.Add(p);

                p = new SqlParameter("@ModifiedDate", SqlDbType.DateTime)
                {
                    Value = DateTime.Now
                };
                cmd.Parameters.Add(p);


                connection.Open();
                categoryId = (int)(await cmd.ExecuteScalarAsync());
                connection.Close();
            }
        }

        return categoryId;
    }

    private static async Task<int> InsertProductCategory(string connectionString)
    {
        var insertCategory = @"
            INSERT INTO [dbo].[ProductCategory] ([Name], [rowguid], [ModifiedDate]) VALUES (@Name, @rowguid, @ModifiedDate);
            SELECT @@IDENTITY;
        ";

        int categoryId;
        using (var connection = new SqlConnection(connectionString))
        {
            using (var cmd = new SqlCommand(insertCategory, connection))
            {
                var p = new SqlParameter("@Name", SqlDbType.NVarChar)
                {
                    Value = "Paris Bikes " + Guid.NewGuid().ToString().Substring(0, 4)
                };
                cmd.Parameters.Add(p);

                p = new SqlParameter("@rowguid", SqlDbType.UniqueIdentifier)
                {
                    Value = Guid.NewGuid()
                };
                cmd.Parameters.Add(p);

                p = new SqlParameter("@ModifiedDate", SqlDbType.DateTime)
                {
                    Value = DateTime.Now
                };
                cmd.Parameters.Add(p);

                connection.Open();
                categoryId = Convert.ToInt32((await cmd.ExecuteScalarAsync()));
                connection.Close();
            }
        }

        return categoryId;
    }
    private static async Task InsertProduct(string connectionString, int categoryId, int productModelId)
    {
        var insertProduct = @"
        INSERT INTO [Product]
        ([Name] ,[ProductNumber] ,[StandardCost] ,[ListPrice]
        ,[ProductCategoryID] ,[ProductModelID] ,[SellStartDate])
        VALUES
        (@Name, @ProductNumber, @StandardCost , @ListPrice
        ,@ProductCategoryID ,@ProductModelID ,@SellStartDate)";

        using (var connection = new SqlConnection(connectionString))
        {
            using (var cmd = new SqlCommand(insertProduct, connection))
            {
                var p = new SqlParameter("@Name", SqlDbType.NVarChar)
                {
                    Value = "Paris Road Byke"
                };
                cmd.Parameters.Add(p);

                p = new SqlParameter("@ProductNumber", SqlDbType.NVarChar)
                {
                    Value = "FR-PRB-001"
                };
                cmd.Parameters.Add(p);

                p = new SqlParameter("@StandardCost", SqlDbType.Money)
                {
                    Value = 856.45
                };
                cmd.Parameters.Add(p);

                p = new SqlParameter("@ListPrice", SqlDbType.Money)
                {
                    Value = 912.99
                };
                cmd.Parameters.Add(p);

                p = new SqlParameter("@ProductCategoryID", SqlDbType.Int)
                {
                    Value = categoryId
                };
                cmd.Parameters.Add(p);

                p = new SqlParameter("@ProductModelID", SqlDbType.Int)
                {
                    Value = productModelId
                };
                cmd.Parameters.Add(p);

                p = new SqlParameter("@SellStartDate", SqlDbType.DateTime)
                {
                    Value = DateTime.Now
                };
                cmd.Parameters.Add(p);

                connection.Open();
                await cmd.ExecuteNonQueryAsync();
                connection.Close();
            }
        }
    }

    /// <summary>
    /// Test a client syncing through a web api
    /// </summary>
    private static async Task TestSyncThroughWebApi()
    {
        var clientProvider = new SqlSyncProvider(GetDatabaseConnectionString("NW1"));

        var proxyClientProvider = new WebProxyClientProvider(
            new Uri("http://localhost:54347/api/values"));

        var agent = new SyncAgent(clientProvider, proxyClientProvider);

        Console.WriteLine("Press a key to start (be sure web api is running ...)");
        Console.ReadKey();
        do
        {
            Console.Clear();
            Console.WriteLine("Web sync start");
            try
            {
                var s = await agent.SynchronizeAsync();

                Console.WriteLine(s);

            }
            catch (SyncException e)
            {
                Console.WriteLine(e.ToString());
            }
            catch (Exception e)
            {
                Console.WriteLine("UNKNOW EXCEPTION : " + e.Message);
            }


            Console.WriteLine("Sync Ended. Press a key to start again, or Escapte to end");
        } while (Console.ReadKey().Key != ConsoleKey.Escape);

        Console.WriteLine("End");

    }

    private static async Task TestSync()
    {
        //CreateDatabase("NW1", true);
        var serverProvider = new SqlSyncProvider(GetDatabaseConnectionString("AdventureWorks"));
        var clientProvider = new SqlSyncProvider(GetDatabaseConnectionString("Client"));

        // Tables involved in the sync process:
        var tables = new string[] {"ProductCategory",
                "ProductDescription", "ProductModel",
                "Product", "ProductModelProductDescription",
                "Address", "Customer", "CustomerAddress",
                "SalesOrderHeader", "SalesOrderDetail" };

        var agent = new SyncAgent(clientProvider, serverProvider, tables);

        agent.Configuration.StoredProceduresPrefix = "sp";
        agent.Configuration.TrackingTablesPrefix = "sync";
        agent.Configuration.ScopeInfoTableName = "syncscope";
        agent.Configuration.SerializationFormat = SerializationFormat.Binary;

        agent.Options = new SyncOptions
        {
            BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "test"),
            BatchSize = 1000,
            CleanMetadatas = true,
            UseBulkOperations = true,
            UseVerboseErrors = false
        };

        do
        {
            Console.Clear();
            Console.WriteLine("Sync Start");
            try
            {
                var cts = new CancellationTokenSource();
                var token = cts.Token;

                var s1 = await agent.SynchronizeAsync(SyncType.ReinitializeWithUpload, token);

                Console.WriteLine(s1);
            }
            catch (SyncException e)
            {
                Console.WriteLine(e.ToString());
            }
            catch (Exception e)
            {
                Console.WriteLine("UNKNOW EXCEPTION : " + e.Message);
            }


            //Console.WriteLine("Sync Ended. Press a key to start again, or Escapte to end");
        } while (Console.ReadKey().Key != ConsoleKey.Escape);

        Console.WriteLine("End");
    }

    public static void DeleteDatabase(string dbName)
    {
        SqlConnection masterConnection = null;
        SqlCommand cmdDb = null;
        masterConnection = new SqlConnection(GetDatabaseConnectionString("master"));

        masterConnection.Open();
        cmdDb = new SqlCommand(GetDeleteDatabaseScript(dbName), masterConnection);
        cmdDb.ExecuteNonQuery();
        masterConnection.Close();
    }

    private static string GetDeleteDatabaseScript(string dbName) => $@"if (exists (Select * from sys.databases where name = '{dbName}'))
                    begin
	                    alter database [{dbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
	                    drop database {dbName}
                    end";

    private static string GetCreationDBScript(string dbName, bool recreateDb = true)
    {
        if (recreateDb)
            return $@"if (exists (Select * from sys.databases where name = '{dbName}'))
                    begin
	                    alter database [{dbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
	                    drop database {dbName}
                    end
                    Create database {dbName}";
        else
            return $@"if not (exists (Select * from sys.databases where name = '{dbName}')) 
                          Create database {dbName}";

    }

    public static void CreateDatabase(string dbName, bool recreateDb = true)
    {
        SqlConnection masterConnection = null;
        SqlCommand cmdDb = null;
        masterConnection = new SqlConnection(GetDatabaseConnectionString("master"));

        masterConnection.Open();
        cmdDb = new SqlCommand(GetCreationDBScript(dbName, recreateDb), masterConnection);
        cmdDb.ExecuteNonQuery();
        masterConnection.Close();
    }


}