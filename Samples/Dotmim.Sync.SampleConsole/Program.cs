using Dotmim.Sync;
using Dotmim.Sync.Data;
using Dotmim.Sync.Data.Surrogate;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Web;
using Newtonsoft.Json;
using System;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

class Program
{
    static void Main(string[] args)
    {
        Console.ReadLine();

        //// Reset DB
        //CreateDatabase("NW1", true);

        //// Make a first sync
        //TestSync().GetAwaiter().GetResult();

        //// Alter the server / client schemas
        //AlterSchemasAsync().GetAwaiter().GetResult();

        //// make another sync
        //TestSync().GetAwaiter().GetResult();

        SynchronizeAdventureWorksAsync().GetAwaiter().GetResult();

        Console.ReadLine();
    }

    public static String GetDatabaseConnectionString(string dbName) =>
        $"Data Source=.\\SQLEXPRESS; Initial Catalog={dbName}; Integrated Security=true;";

    /// <summary>
    /// Test a client syncing through a web api
    /// </summary>
    private static async Task TestSyncThroughWebApi()
    {
        var clientProvider = new SqlSyncProvider(GetDatabaseConnectionString("NW1"));

        var proxyClientProvider = new WebProxyClientProvider(
            new Uri("http://localhost:56782/api/values"));

        var agent = new SyncAgent(clientProvider, proxyClientProvider);

        agent.ApplyChangedFailed += ApplyChangedFailed;

        Console.WriteLine("Press a key to start...");
        Console.ReadKey();
        do
        {
            Console.Clear();
            Console.WriteLine("Sync Start");
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


    private static async Task AlterSchemasAsync()
    {
        SqlSyncProvider serverProvider = new SqlSyncProvider(GetDatabaseConnectionString("Northwind"));
        SqlSyncProvider clientProvider = new SqlSyncProvider(GetDatabaseConnectionString("NW1"));

        // tables to edit
        var tables = new string[] { "Customers" };

        // delete triggers and sp
        await serverProvider.DeprovisionAsync(tables, SyncProvision.StoredProcedures | SyncProvision.Triggers);


        await clientProvider.DeprovisionAsync(tables, SyncProvision.StoredProcedures | SyncProvision.Triggers);

        // use whatever you want to edit your schema
        // add column on server
        using (SqlConnection cs = serverProvider.CreateConnection() as SqlConnection)
        {
            cs.Open();
            SqlCommand cmd = new SqlCommand("ALTER TABLE dbo.Customers ADD Comments nvarchar(50) NULL", cs);
            cmd.ExecuteNonQuery();
            cs.Close();
        }
        // add column on client
        using (SqlConnection cs = clientProvider.CreateConnection() as SqlConnection)
        {
            cs.Open();
            SqlCommand cmd = new SqlCommand("ALTER TABLE dbo.Customers ADD Comments nvarchar(50) NULL", cs);
            cmd.ExecuteNonQuery();
            cs.Close();
        }

        // re apply server conf
        await serverProvider.ProvisionAsync(tables, SyncProvision.StoredProcedures | SyncProvision.Triggers);
        await clientProvider.ProvisionAsync(tables, SyncProvision.StoredProcedures | SyncProvision.Triggers);

    }

    public static async Task SynchronizeAdventureWorksAsync()
    {
        CreateDatabase("Adv", true);
        SqlSyncProvider serverProvider = new SqlSyncProvider(GetDatabaseConnectionString("AdventureWorks"));
        SqlSyncProvider client1Provider = new SqlSyncProvider(GetDatabaseConnectionString("Adv"));

        // "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail"

        SyncConfiguration configuration = new SyncConfiguration(new string[] {
        "ProductDescription", "ProductModel",
        "ProductModelProductDescription", "ProductCategory", "Product", "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail"});

        configuration.DownloadBatchSizeInKB = 5000;

        SyncAgent syncAgent = new SyncAgent(client1Provider, serverProvider, configuration);
        syncAgent.SyncProgress += (s, a) => Console.WriteLine($"Progress: {a.Message} {a.PropertiesMessage}");

        //syncAgent.BeginSession += (s, a) => Console.WriteLine("Begin session");
        //syncAgent.ScopeLoading += (s, a) => Console.WriteLine("Scope loading");
        //syncAgent.ConfigurationApplying += (s, a) => Console.WriteLine("Applying configuration");
        //syncAgent.ConfigurationApplied += (s, a) => Console.WriteLine("Configuration applied");
        //syncAgent.DatabaseApplying += (s, a) => a.GenerateScript = false;
        //syncAgent.DatabaseApplied += (s, a) => Console.WriteLine("Database schemas applied");
        //syncAgent.ChangesSelecting+= (s, a) => Console.WriteLine($"Selecting changes for table {a.TableName}");
        //syncAgent.ChangesSelected += (s, a) => Console.WriteLine($"Changes selected for table {a.TableChangesSelected.TableName}: {a.TableChangesSelected.TotalChanges}");
        //serverProvider.TableChangesSelected += (s, a) => Console.WriteLine($"SERVER Changes selected for table {a.TableChangesSelected.TableName}: {a.TableChangesSelected.TotalChanges}");
        //syncAgent.ChangesApplying += (s, a) => Console.WriteLine($"Applying changes for table {a.TableName}");
        //syncAgent.ChangesApplied += (s, a) => Console.WriteLine($"Changes applied for table {a.TableChangesApplied.TableName}: [{a.TableChangesApplied.State}] {a.TableChangesApplied.Applied}");
        //serverProvider.TableChangesApplied += (s, a) => Console.WriteLine($"SERVER Changes applied for table {a.TableChangesApplied.TableName}: [{a.TableChangesApplied.State}] {a.TableChangesApplied.Applied}");
        //syncAgent.EndSession += (s, a) => Console.WriteLine("end session");
        
        var context = await syncAgent.SynchronizeAsync();

        Console.WriteLine(context);
        // do stuff ..
    }

    private static async Task TestSync()
    {
        SqlSyncProvider serverProvider = new SqlSyncProvider(GetDatabaseConnectionString("Northwind"));
        SqlSyncProvider client1Provider = new SqlSyncProvider(GetDatabaseConnectionString("NW1"));

        // With a config when we are in local mode (no proxy)
        SyncConfiguration configuration = new SyncConfiguration(new string[] {
        "Customers", "Region"});

        SyncAgent agent1 = new SyncAgent(client1Provider, serverProvider, configuration);

        do
        {
            Console.Clear();
            Console.WriteLine("Sync Start");
            try
            {
                CancellationTokenSource cts = new CancellationTokenSource();
                CancellationToken token = cts.Token;

                void selected(object s, TableChangesSelectedEventArgs a) => Console.WriteLine($"Changes selected for table {a.TableChangesSelected.TableName}: {a.TableChangesSelected.TotalChanges}");
                void applied(object s, TableChangesAppliedEventArgs a) => Console.WriteLine($"Changes applied for table {a.TableChangesApplied.TableName}: [{a.TableChangesApplied.State}] {a.TableChangesApplied.Applied}");

                agent1.ChangesSelected += selected;
                agent1.ChangesApplied += applied;

                var s1 = await agent1.SynchronizeAsync(token);

                Console.WriteLine(s1);

                agent1.ChangesSelected -= selected;
                agent1.ChangesApplied -= applied;
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

    /// <summary>
    /// Simple Sync test
    /// </summary>
    private static async Task TestMultipleSync()
    {
        SqlSyncProvider serverProvider = new SqlSyncProvider(GetDatabaseConnectionString("Northwind"));
        SqlSyncProvider client1Provider = new SqlSyncProvider(GetDatabaseConnectionString("NW1"));
        SqlSyncProvider client2Provider = new SqlSyncProvider(GetDatabaseConnectionString("NW2"));
        SqlSyncProvider client3Provider = new SqlSyncProvider(GetDatabaseConnectionString("NW3"));

        // With a config when we are in local mode (no proxy)
        SyncConfiguration configuration = new SyncConfiguration(new string[] {
        "Customers", "Region"});

        SyncAgent agent1 = new SyncAgent(client1Provider, serverProvider, configuration);
        SyncAgent agent2 = new SyncAgent(client2Provider, serverProvider, configuration);
        SyncAgent agent3 = new SyncAgent(client3Provider, serverProvider, configuration);

        //agent1.SyncProgress += SyncProgress;
        //agent1.ApplyChangedFailed += ApplyChangedFailed;
        //agent2.SyncProgress += SyncProgress;
        //agent2.ApplyChangedFailed += ApplyChangedFailed;


        do
        {
            Console.Clear();
            Console.WriteLine("Sync Start");
            try
            {
                CancellationTokenSource cts = new CancellationTokenSource();
                CancellationToken token = cts.Token;

                CreateDatabase("NW1", true);
                CreateDatabase("NW2", true);
                CreateDatabase("NW3", true);

                UpdateRegion("Server", GetDatabaseConnectionString("Northwind"));

                var s1 = await agent1.SynchronizeAsync(token);
                var s2 = await agent2.SynchronizeAsync(token);
                var s3 = await agent3.SynchronizeAsync(token);
                Console.WriteLine($"S1 Upload changes {s1.TotalChangesUploaded}. Download changes {s1.TotalChangesDownloaded}");
                Console.WriteLine($"S2 Upload changes {s2.TotalChangesUploaded}. Download changes {s2.TotalChangesDownloaded}");
                Console.WriteLine($"S3 Upload changes {s3.TotalChangesUploaded}. Download changes {s3.TotalChangesDownloaded}");

                Console.WriteLine($"Server  Region desc : {GetRegion(GetDatabaseConnectionString("Northwind"))}.");
                Console.WriteLine($"Client1 Region desc : {GetRegion(GetDatabaseConnectionString("NW1"))}.");
                Console.WriteLine($"Client2 Region desc : {GetRegion(GetDatabaseConnectionString("NW2"))}.");
                Console.WriteLine($"Client3 Region desc : {GetRegion(GetDatabaseConnectionString("NW3"))}.");

                UpdateRegion("Client 1", GetDatabaseConnectionString("NW1"));
                UpdateRegion("Client 2", GetDatabaseConnectionString("NW2"));
                UpdateRegion("Client 3", GetDatabaseConnectionString("NW3"));

                s1 = await agent1.SynchronizeAsync(token);
                Console.WriteLine($"S1 Upload changes {s1.TotalChangesUploaded}. Download changes {s1.TotalChangesDownloaded}. Conflicts {s1.TotalSyncConflicts}");
                s2 = await agent2.SynchronizeAsync(token);
                Console.WriteLine($"S2 Upload changes {s2.TotalChangesUploaded}. Download changes {s2.TotalChangesDownloaded}. Conflicts {s2.TotalSyncConflicts}");
                s3 = await agent3.SynchronizeAsync(token);
                Console.WriteLine($"S3 Upload changes {s3.TotalChangesUploaded}. Download changes {s3.TotalChangesDownloaded}. Conflicts {s3.TotalSyncConflicts}");

                Console.WriteLine($"Server  Region desc : {GetRegion(GetDatabaseConnectionString("Northwind"))}.");
                Console.WriteLine($"Client1 Region desc : {GetRegion(GetDatabaseConnectionString("NW1"))}.");
                Console.WriteLine($"Client2 Region desc : {GetRegion(GetDatabaseConnectionString("NW2"))}.");
                Console.WriteLine($"Client3 Region desc : {GetRegion(GetDatabaseConnectionString("NW3"))}.");


                UpdateRegion("Client 2", GetDatabaseConnectionString("NW2"));
                UpdateRegion("Client 3", GetDatabaseConnectionString("NW3"));

                s1 = await agent1.SynchronizeAsync(token);
                Console.WriteLine($"S1 Upload changes {s1.TotalChangesUploaded}. Download changes {s1.TotalChangesDownloaded}. Conflicts {s1.TotalSyncConflicts}");
                s2 = await agent2.SynchronizeAsync(token);
                Console.WriteLine($"S2 Upload changes {s2.TotalChangesUploaded}. Download changes {s2.TotalChangesDownloaded}. Conflicts {s2.TotalSyncConflicts}");
                s3 = await agent3.SynchronizeAsync(token);
                Console.WriteLine($"S3 Upload changes {s3.TotalChangesUploaded}. Download changes {s3.TotalChangesDownloaded}. Conflicts {s3.TotalSyncConflicts}");

                Console.WriteLine($"Server  Region desc : {GetRegion(GetDatabaseConnectionString("Northwind"))}.");
                Console.WriteLine($"Client1 Region desc : {GetRegion(GetDatabaseConnectionString("NW1"))}.");
                Console.WriteLine($"Client2 Region desc : {GetRegion(GetDatabaseConnectionString("NW2"))}.");
                Console.WriteLine($"Client3 Region desc : {GetRegion(GetDatabaseConnectionString("NW3"))}.");
                s1 = await agent1.SynchronizeAsync(token);
                Console.WriteLine($"S1 Upload changes {s1.TotalChangesUploaded}. Download changes {s1.TotalChangesDownloaded}. Conflicts {s1.TotalSyncConflicts}");
                Console.WriteLine($"Client1 Region desc : {GetRegion(GetDatabaseConnectionString("NW1"))}.");

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


    private static void UpdateRegion(string description, string connectionString)
    {
        using (SqlConnection connection = new SqlConnection(connectionString))
        {
            using (SqlCommand command = new SqlCommand())
            {
                string text = $"Update Region Set RegionDescription = '{description}' Where RegionID = 1";
                command.Connection = connection;
                command.CommandText = text;
                connection.Open();
                command.ExecuteNonQuery();
                connection.Close();
            }
        }
    }

    private static string GetRegion(string connectionString)
    {
        string res = null;
        using (SqlConnection connection = new SqlConnection(connectionString))
        {
            using (SqlCommand command = new SqlCommand())
            {
                string text = $"Select RegionDescription from Region Where RegionID = 1";
                command.Connection = connection;
                command.CommandText = text;
                connection.Open();
                res = (string)command.ExecuteScalar();
                connection.Close();
            }
        }
        return res;
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

    private static string GetDeleteDatabaseScript(string dbName)
    {
        return $@"if (exists (Select * from sys.databases where name = '{dbName}'))
                    begin
	                    alter database [{dbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
	                    drop database {dbName}
                    end";
    }

    private static string GetCreationDBScript(string dbName, Boolean recreateDb = true)
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

    private static void GenerateConflict()
    {
        using (SqlConnection connection = new SqlConnection(GetDatabaseConnectionString("Northwind")))
        {
            using (SqlCommand command = new SqlCommand())
            {
                string text = "Update Region Set RegionDescription = 'Eastern Server' Where RegionID = 1";
                command.Connection = connection;
                command.CommandText = text;
                connection.Open();
                command.ExecuteNonQuery();
                connection.Close();
            }
        }

        using (SqlConnection connection = new SqlConnection(GetDatabaseConnectionString("NW1")))
        {
            using (SqlCommand command = new SqlCommand())
            {
                string text = "Update Region Set RegionDescription = 'Eastern Client' Where RegionID = 1";
                command.Connection = connection;
                command.CommandText = text;
                connection.Open();
                command.ExecuteNonQuery();
                connection.Close();
            }
        }
    }


    /// <summary>
    /// Sync apply changed, deciding who win
    /// </summary>
    static void ApplyChangedFailed(object sender, ApplyChangeFailedEventArgs e)
    {
        e.Action = ConflictAction.MergeRow;
        e.FinalRow["RegionDescription"] = "Eastern alone !";
    }
}