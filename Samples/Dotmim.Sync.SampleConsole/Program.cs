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

    private static string sqlConnectionStringServer = 
        "Data Source=.\\SQLEXPRESS; Initial Catalog=Northwind; Integrated Security=true;";

    private static string sqlConnectionStringClient =
        "Data Source=.\\SQLEXPRESS; Initial Catalog=NorthwindClient; Integrated Security=true;";

    /// <summary>
    /// Test a client syncing through a web api
    /// </summary>
    private static async Task TestSyncThroughWebApi()
    {
        var clientProvider = new SqlSyncProvider(sqlConnectionStringClient);

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
        SqlSyncProvider serverProvider = new SqlSyncProvider(sqlConnectionStringServer);
        SqlSyncProvider clientProvider = new SqlSyncProvider(sqlConnectionStringClient);

        // With a config when we are in local mode (no proxy)
        SyncConfiguration configuration = new SyncConfiguration(new string[] {
        "Customers", "Region"});

        SyncAgent agent = new SyncAgent(clientProvider, serverProvider, configuration);

        agent.SyncProgress += SyncProgress;
        agent.ApplyChangedFailed += ApplyChangedFailed;


        do
        {
            Console.Clear();
            Console.WriteLine("Sync Start");
            try
            {
                // if you want to test some conflicts resolution
                // GenerateConflict();

                CancellationTokenSource cts = new CancellationTokenSource();
                CancellationToken token = cts.Token;
                var s = await agent.SynchronizeAsync(token);

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

    private static void GenerateConflict()
    {
        using (SqlConnection connection = new SqlConnection(sqlConnectionStringServer))
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

        using (SqlConnection connection = new SqlConnection(sqlConnectionStringClient))
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