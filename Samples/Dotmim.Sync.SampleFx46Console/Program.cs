using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using Dotmim.Sync.Data.Surrogate;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.MySql;
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
            TestMySqlSync().Wait();
        }

        //private static async Task TestMySqlTableBuilder()
        //{
        //    var serverConfig = ConfigurationManager.ConnectionStrings["MySqlServerConnectionString"].ConnectionString;


        //    // Generate the DmSet schema
        //    var set = new DmSet();
        //    DmTable clientsTable = new DmTable("Clients");
        //    DmTable productsTable = new DmTable("Products");

        //    // orders matters !!
        //    set.Tables.Add(clientsTable);
        //    set.Tables.Add(productsTable);

        //    DmColumn id = new DmColumn<Int32>("Id");
        //    id.AllowDBNull = false;
        //    id.AutoIncrement = true;
        //    productsTable.Columns.Add(id);

        //    DmColumn fkClientId = new DmColumn<Guid>("clientId");
        //    fkClientId.AllowDBNull = true;
        //    productsTable.Columns.Add(fkClientId);

        //    DmColumn name = new DmColumn<string>("name");
        //    name.AllowDBNull = false;
        //    name.DbType = DbType.StringFixedLength;
        //    name.MaxLength = 150;
        //    productsTable.Columns.Add(name);

        //    DmColumn salary = new DmColumn<Decimal>("salary");
        //    salary.AllowDBNull = false;
        //    salary.DbType = DbType.VarNumeric;
        //    salary.Precision = 6;
        //    salary.Scale = 2;
        //    productsTable.Columns.Add(salary);

        //    productsTable.PrimaryKey = new DmKey(new DmColumn[] { id, name, salary });

        //    DmColumn clientId = new DmColumn<Guid>("Id");
        //    clientId.AllowDBNull = false;
        //    clientsTable.Columns.Add(clientId);

        //    DmColumn clientName = new DmColumn<string>("Name");
        //    clientsTable.Columns.Add(clientName);

        //    clientsTable.PrimaryKey = new DmKey(clientId);

        //    // ForeignKey
        //    DmRelation fkClientRelation = new DmRelation("FK_Products_Clients", clientId, fkClientId);
        //    productsTable.AddForeignKey(fkClientRelation);

        //    var provider = new MySqlSyncProvider(serverConfig);

        //    using (var connection = provider.CreateConnection())
        //    {
        //        var options = DbBuilderOption.CreateOrUseExistingSchema;
        //        var builderClients = provider.GetDatabaseBuilder(set.Tables["Clients"], options);
        //        var builderProducts = provider.GetDatabaseBuilder(set.Tables["Products"], options);

        //        var scopeBuilder = provider.GetScopeBuilder();

        //        connection.Open();

        //        var scriptClients = builderClients.Script(connection);
        //        var scriptProducts = builderProducts.Script(connection);


        //        connection.Close();
        //    }


        //}

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
            string serverTableName = e.Conflict.RemoteChanges.TableName;
            string clientTableName = e.Conflict.LocalChanges.TableName;

            // server row in conflict
            var dmRowServer = e.Conflict.RemoteChanges.Rows[0];
            var dmRowClient = e.Conflict.LocalChanges.Rows[0];

            // Example 1 : Resolution based on rows values
            if ((int)dmRowServer["ClientID"] == 100 && (int)dmRowClient["ClientId"] == 0)
                e.Action = ApplyAction.Continue;
            else
                e.Action = ApplyAction.RetryWithForceWrite;

            // Example 2 : resolution based on conflict type
            // Line exist on client, not on server, force to create it
            //if (e.Conflict.Type == ConflictType.RemoteInsertLocalNoRow || e.Conflict.Type == ConflictType.RemoteUpdateLocalNoRow)
            //    e.Action = ApplyAction.RetryWithForceWrite;
            //else
            //    e.Action = ApplyAction.RetryWithForceWrite;
        }

    }
}
