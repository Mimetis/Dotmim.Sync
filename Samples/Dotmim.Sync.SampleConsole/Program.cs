using Dotmim.Sync;
using Dotmim.Sync.Data;
using Dotmim.Sync.Data.Surrogate;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.MySql;
using Dotmim.Sync.Oracle;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Web;
using MySql.Data.MySqlClient;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

class Program
{
    static void Main(string[] args)
    {
        TestOracleSyncThroughWebApi().GetAwaiter().GetResult();

        Console.ReadLine();
    }

    public static String GetDatabaseConnectionString(string dbName) =>
        $"Data Source=.\\SQLEXPRESS; Initial Catalog={dbName}; Integrated Security=true;";

    public static string GetMySqlDatabaseConnectionString(string dbName) =>
        $@"Server=127.0.0.1; Port=3306; Database={dbName}; Uid=root; Pwd=azerty31$;";

    public static string GetOracleDatabaseConnectionString() =>
        $"Data Source =(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=192.168.99.100)(PORT=32774)))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=xe)));User Id=test_base_personnel;Password=test_base_personnel;";

    public static MySqlConnectionStringBuilder GetMySQLConnectionBuilder() => new MySqlConnectionStringBuilder()
    {
        Port = 32775,
        Server = "192.168.99.100",
        UserID = "root",
        Password = "secret",
        Database = "test",
        MaximumPoolSize = 50
    };

    /// <summary>
    /// Test a client syncing through a web api
    /// </summary>
    private static async Task TestOracleSyncThroughWebApi()
    {
        string line = "";
        var oracleClientProvider = new OracleSyncProvider(GetOracleDatabaseConnectionString());
        var mySqlClientProvider = new MySqlSyncProvider(GetMySQLConnectionBuilder());

        var agent = new SyncAgent(mySqlClientProvider, oracleClientProvider, new string[] { "T_PERSONNE", "T_ENFANT" });
        agent.Configuration.UseBulkOperations = false;

        Console.WriteLine("Press a key to start synchronization ... ");
        Console.ReadKey();
        do
        {
            Console.Clear();
            Console.WriteLine("Sync start");
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


            Console.WriteLine("Sync Ended. Press a key to start again, or EXIT to end");
            line = Console.ReadLine();
        } while (!line.ToUpper().Contains("EXIT"));

        Console.WriteLine("End");

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
        SqlSyncProvider serverProvider = new SqlSyncProvider(GetDatabaseConnectionString("Northwind"));
        SqlSyncProvider clientProvider = new SqlSyncProvider(GetDatabaseConnectionString("NW1"));

        SyncAgent agent = new SyncAgent(clientProvider, serverProvider, new string[] {
        "Customers", "Region"});

        do
        {
            Console.Clear();
            Console.WriteLine("Sync Start");
            try
            {
                CancellationTokenSource cts = new CancellationTokenSource();
                CancellationToken token = cts.Token;

                //void selected(object s, TableChangesSelectedEventArgs a) => Console.WriteLine($"Changes selected for table {a.TableChangesSelected.TableName}: {a.TableChangesSelected.TotalChanges}");
                //void applied(object s, TableChangesAppliedEventArgs a) => Console.WriteLine($"Changes applied for table {a.TableChangesApplied.TableName}: [{a.TableChangesApplied.State}] {a.TableChangesApplied.Applied}");

                //agent.TableChangesSelected += selected;
                //agent.TableChangesApplied += applied;

                var s1 = await agent.SynchronizeAsync(SyncType.ReinitializeWithUpload, token);

                Console.WriteLine(s1);

                //agent.TableChangesSelected -= selected;
                //agent.TableChangesApplied -= applied;
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


}