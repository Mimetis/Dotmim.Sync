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

        TestSync().GetAwaiter().GetResult();

        Console.ReadLine();
    }

    public static String GetDatabaseConnectionString(string dbName) => 
        $"Data Source=.\\SQLEXPRESS; Initial Catalog={dbName}; Integrated Security=true;";

    //private static string sqlConnectionStringServer = 
    //    "Data Source=.\\SQLEXPRESS; Initial Catalog=Northwind; Integrated Security=true;";

    //private static string sqlConnectionStringClient1 =
    //    "Data Source=.\\SQLEXPRESS; Initial Catalog=NW1; Integrated Security=true;";

    //private static string sqlConnectionStringClient2 =
    //    "Data Source=.\\SQLEXPRESS; Initial Catalog=NW2; Integrated Security=true;";

    /// <summary>
    /// Test a client syncing through a web api
    /// </summary>
    private static async Task TestSyncThroughWebApi()
    {
        var clientProvider = new SqlSyncProvider(GetDatabaseConnectionString("NW1"));

        var proxyClientProvider = new WebProxyClientProvider(
            new Uri("http://localhost:56782/api/values"));

        var agent = new SyncAgent(clientProvider, proxyClientProvider);

        agent.SyncProgress += SyncProgress;
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

                Console.WriteLine(GetResultString(s));

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
    private static async Task TestSync()
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
                res  = (string)command.ExecuteScalar();
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
    /// Write results
    /// </summary>
    private static string GetResultString(SyncContext s)
    {
        var tsEnded = TimeSpan.FromTicks(s.CompleteTime.Ticks);
        var tsStarted = TimeSpan.FromTicks(s.StartTime.Ticks);

        var durationTs = tsEnded.Subtract(tsStarted);
        var durationstr = $"{durationTs.Hours}:{durationTs.Minutes}:{durationTs.Seconds}.{durationTs.Milliseconds}";

        return ($"Synchronization done. " + Environment.NewLine +
                $"\tTotal changes downloaded: {s.TotalChangesDownloaded} " + Environment.NewLine +
                $"\tTotal changes uploaded: {s.TotalChangesUploaded}" + Environment.NewLine +
                $"\tTotal duration :{durationstr} ");
    }

    /// <summary>
    /// Sync progression
    /// </summary>
    private static void SyncProgress(object sender, SyncProgressEventArgs e)
    {
        var sessionId = e.Context.SessionId.ToString();

        switch (e.Context.SyncStage)
        {
            case SyncStage.BeginSession:
                Console.WriteLine($"Begin Session.");
                break;
            case SyncStage.EndSession:
                Console.WriteLine($"End Session.");
                break;
            case SyncStage.EnsureScopes:
                Console.WriteLine($"Ensure Scopes");
                break;
            case SyncStage.EnsureConfiguration:
                Console.WriteLine($"Ensure Configuration");
                if (e.Configuration != null)
                {
                    var ds = e.Configuration.ScopeSet;

                    Console.WriteLine($"Configuration readed. {ds.Tables.Count} table(s) involved.");

                    Func<JsonSerializerSettings> settings = new Func<JsonSerializerSettings>(() =>
                    {
                        var s = new JsonSerializerSettings
                        {
                            Formatting = Formatting.Indented,
                            StringEscapeHandling = StringEscapeHandling.Default
                        };
                        return s;
                    });
                    JsonConvert.DefaultSettings = settings;
                    var dsString = JsonConvert.SerializeObject(new DmSetSurrogate(ds));

                    //Console.WriteLine(dsString);
                }
                break;
            case SyncStage.EnsureDatabase:
                Console.WriteLine($"Ensure Database");
                break;
            case SyncStage.SelectingChanges:
                Console.WriteLine($"Selecting changes...");
                break;
            case SyncStage.SelectedChanges:
                Console.WriteLine($"Changes selected : {e.ChangesStatistics.TotalSelectedChanges}");
                break;
            case SyncStage.ApplyingChanges:
                Console.WriteLine($"Applying changes...");
                break;
            case SyncStage.ApplyingInserts:
                Console.WriteLine($"\tApplying Inserts : {e.ChangesStatistics.AppliedChanges.Where(ac => ac.State == DmRowState.Added).Sum(ac => ac.ChangesApplied) }");
                break;
            case SyncStage.ApplyingDeletes:
                Console.WriteLine($"\tApplying Deletes : {e.ChangesStatistics.AppliedChanges.Where(ac => ac.State == DmRowState.Deleted).Sum(ac => ac.ChangesApplied) }");
                break;
            case SyncStage.ApplyingUpdates:
                Console.WriteLine($"\tApplying Updates : {e.ChangesStatistics.AppliedChanges.Where(ac => ac.State == DmRowState.Modified).Sum(ac => ac.ChangesApplied) }");
                break;
            case SyncStage.AppliedChanges:
                Console.WriteLine($"Changes applied : {e.ChangesStatistics.TotalAppliedChanges}");
                break;
            case SyncStage.WriteMetadata:
                if (e.Scopes != null)
                {
                    Console.WriteLine($"Writing Scopes : ");
                    e.Scopes.ForEach(sc => Console.WriteLine($"\t{sc.Id} synced at {sc.LastSync}. "));
                }
                break;
            case SyncStage.CleanupMetadata:
                Console.WriteLine($"CleanupMetadata");
                break;
        }

        Console.ResetColor();
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