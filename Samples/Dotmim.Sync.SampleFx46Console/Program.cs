using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using Dotmim.Sync.Data.Surrogate;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.MySql;
using Dotmim.Sync.SQLite;
//using Dotmim.Sync.MySql;
using Dotmim.Sync.SqlServer;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Dotmim.Sync.SampleFx46Console
{
    class Program
    {
        static void Main(string[] args)
        {
          //  SyncWordPress().Wait();

            SyncWithSchema().Wait();
        }

        public static async Task SyncWithSchema()
        {
            var serverConfig = @"Data Source=(localdb)\MSSQLLocalDB; Initial Catalog=AdventureWorksLT2012;Integrated Security=true;";
            SqlSyncProvider serverProvider = new SqlSyncProvider(serverConfig);

            var clientConfig = @"Data Source=(localdb)\MSSQLLocalDB; Initial Catalog=AdvClientTest;Integrated Security=true;";
            SqlSyncProvider clientProvider = new SqlSyncProvider(clientConfig);

            var tables = new string[] { "SalesLT.ProductCategory", "ProductModel", "SalesLT.Product" };

            SyncAgent agent = new SyncAgent(clientProvider, serverProvider, tables);

            agent.Configuration["ProductModel"].Schema = "SalesLT";

            agent.SyncProgress += SyncProgress;
            agent.ApplyChangedFailed += ApplyChangedFailed;

            do
            {
                Console.Clear();
                Console.WriteLine("Sync Start");
                try
                {
                    var s = await agent.SynchronizeAsync();

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
        public static async Task SyncWordPress()
        {
            var serverConfig = "Server=tlsemysql.mysql.database.azure.com; Port=3306; Database=mysqldatabase165; Uid=spertus@tlsemysql; Pwd=azerty31$; SslMode=Preferred;";
            MySqlSyncProvider serverProvider = new MySqlSyncProvider(serverConfig);

            //var clientConfig = @"Data Source=(localdb)\MSSQLLocalDB; Initial Catalog=WordPress;Integrated Security=true;";
            //SqlSyncProvider clientProvider = new SqlSyncProvider(clientConfig);

            var clientConfig = @"wordpress.sqlite";
            SQLiteSyncProvider clientProvider = new SQLiteSyncProvider(clientConfig);

            // With a config when we are in local mode (no proxy)
            var tables = new string[] { "wp_users", "wp_usermeta", "wp_terms", "wp_termmeta", "wp_term_taxonomy",
                                        "wp_term_relationships", "wp_posts", "wp_postmeta", "wp_options", "wp_links",
                                        "wp_comments", "wp_commentmeta"};


            SyncAgent agent = new SyncAgent(clientProvider, serverProvider, tables);

            // Setting special properties on Configuration tables
            //agent.Configuration["wp_users"].SyncDirection = SyncDirection.DownloadOnly;
            //agent.Configuration["wp_users"].Schema = "SalesLT";



            agent.SyncProgress += SyncProgress;
            agent.ApplyChangedFailed += ApplyChangedFailed;

            do
            {
                Console.Clear();
                Console.WriteLine("Sync Start");
                try
                {
                    CancellationTokenSource cts = new CancellationTokenSource();
                    CancellationToken token = cts.Token;
                    var s = await agent.SynchronizeAsync(token);

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

        public static async Task SyncAdventureWorks()
        {
            // Get SQL Server connection string
            var serverConfig = @"Data Source=(localdb)\MSSQLLocalDB; Initial Catalog=AdventureWorks;Integrated Security=true;";
            SqlSyncProvider serverProvider = new SqlSyncProvider(serverConfig);

            var clientConfig = @"Data Source=(localdb)\MSSQLLocalDB; Initial Catalog=AdvClientTest;Integrated Security=true;";
            SqlSyncProvider clientProvider = new SqlSyncProvider(clientConfig);

            //var clientConfig = @"advworks2012_2.sqlite";
            //SQLiteSyncProvider clientProvider = new SQLiteSyncProvider(clientConfig);

            //var clientConfig = "Server=127.0.0.1; Port=3306; Database=clientdb; Uid=root; Pwd=azerty31*;";
            //MySqlSyncProvider clientProvider = new MySqlSyncProvider(clientConfig);

            // With a config when we are in local mode (no proxy)
            var tables = new string[] {"ErrorLog", "ProductCategory",
                "ProductDescription", "ProductModel",
                "Product", "ProductModelProductDescription",
                "Address", "Customer", "CustomerAddress",
                "SalesOrderHeader", "SalesOrderDetail" };

            SyncAgent agent = new SyncAgent(clientProvider, serverProvider, tables);

            agent.SyncProgress += SyncProgress;
            agent.ApplyChangedFailed += ApplyChangedFailed;

            do
            {
                Console.Clear();
                Console.WriteLine("Sync Start");
                try
                {
                    CancellationTokenSource cts = new CancellationTokenSource();
                    CancellationToken token = cts.Token;
                    var s = await agent.SynchronizeAsync(token);

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



        private static async Task TestMySqlSync()
        {
            // Get SQL Server connection string

            var serverConfig = ConfigurationManager.ConnectionStrings["SqlServerConnectionString"].ConnectionString;
            var clientConfig = ConfigurationManager.ConnectionStrings["MySqlLocalClientConnectionString"].ConnectionString;

            SqlSyncProvider serverProvider = new SqlSyncProvider(serverConfig);
            MySqlSyncProvider clientProvider = new MySqlSyncProvider(clientConfig);

            // With a config when we are in local mode (no proxy)
            SyncConfiguration configuration = new SyncConfiguration(new string[] { "Customers", "ServiceTickets" });
            //configuration.OverwriteConfiguration = true;

            //configuration.DownloadBatchSizeInKB = 500;
            SyncAgent agent = new SyncAgent(clientProvider, serverProvider, configuration);

            agent.SyncProgress += SyncProgress;
            agent.ApplyChangedFailed += ApplyChangedFailed;

            do
            {
                Console.Clear();
                Console.WriteLine("Sync Start");
                try
                {
                    CancellationTokenSource cts = new CancellationTokenSource();
                    CancellationToken token = cts.Token;
                    var s = await agent.SynchronizeAsync(token);

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



        private static void SyncProgress(object sender, SyncProgressEventArgs e)
        {
            SyncProgress(e);
        }

        private static void SyncProgress(SyncProgressEventArgs e, ConsoleColor? consoleColor = null)
        {
            var sessionId = e.Context.SessionId.ToString();

            if (consoleColor.HasValue)
                Console.ForegroundColor = consoleColor.Value;

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
                            var s = new JsonSerializerSettings();
                            s.Formatting = Formatting.Indented;
                            s.StringEscapeHandling = StringEscapeHandling.Default;
                            return s;
                        });
                        JsonConvert.DefaultSettings = settings;
                        var dsString = JsonConvert.SerializeObject(new DmSetSurrogate(ds));

                        //Console.WriteLine(dsString);
                    }
                    break;
                case SyncStage.EnsureDatabase:
                    Console.WriteLine($"Ensure Database");
                    var script = e.DatabaseScript;
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

        static void ApplyChangedFailed(object sender, ApplyChangeFailedEventArgs e)
        {

            e.Action = ApplyAction.Continue;
            return;
            // tables name
            //string serverTableName = e.Conflict.RemoteChanges.TableName;
            //string clientTableName = e.Conflict.LocalChanges.TableName;

            //// server row in conflict
            //var dmRowServer = e.Conflict.RemoteChanges.Rows[0];
            //var dmRowClient = e.Conflict.LocalChanges.Rows[0];

            //// Example 1 : Resolution based on rows values
            //if ((int)dmRowServer["ClientID"] == 100 && (int)dmRowClient["ClientId"] == 0)
            //    e.Action = ApplyAction.Continue;
            //else
            //    e.Action = ApplyAction.RetryWithForceWrite;

            // Example 2 : resolution based on conflict type
            // Line exist on client, not on server, force to create it
            //if (e.Conflict.Type == ConflictType.RemoteInsertLocalNoRow || e.Conflict.Type == ConflictType.RemoteUpdateLocalNoRow)
            //    e.Action = ApplyAction.RetryWithForceWrite;
            //else
            //    e.Action = ApplyAction.RetryWithForceWrite;
        }

    }
}
