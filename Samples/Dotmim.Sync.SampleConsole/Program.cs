using Dotmim.Sync;
using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SampleConsole;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Web.Client;
using Dotmim.Sync.Web.Server;
using Microsoft.AspNetCore.Http;
using Microsoft.Data.Sqlite;
using Newtonsoft.Json;
using System;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;


internal class Program
{
    public static string serverDbName = "AdventureWorks";
    public static string serverProductCategoryDbName = "AdventureWorksProductCategory";
    public static string clientDbName = "Client";
    public static string[] allTables = new string[] {"ProductCategory",
                                                    "ProductModel", "Product",
                                                    "Address", "Customer", "CustomerAddress",
                                                    "SalesOrderHeader", "SalesOrderDetail" };
    private static void Main(string[] args)
    {

        SyncHttpThroughKestellAsync().GetAwaiter().GetResult();


        //TestSyncTable();



    }

    private static async Task TestDeleteWithoutBulkAsync()
    {
        var cs = DbHelper.GetDatabaseConnectionString(serverProductCategoryDbName);
        var cc = DbHelper.GetDatabaseConnectionString(clientDbName);

        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(cs);
        var serverConnection = new SqlConnection(cs);

        //var clientProvider = new SqlSyncProvider(cc);
        //var clientConnection = new SqlConnection(cc);

        var clientProvider = new SqliteSyncProvider("advworks2.db");
        var clientConnection = new SqliteConnection(clientProvider.ConnectionString);

        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, new string[] { "ProductCategory" });

        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"{s.Context.SyncStage}:\t{s.Message}");
            Console.ResetColor();
        });

        var remoteProgress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.Context.SyncStage}:\t{s.Message}");
            Console.ResetColor();
        });
        // agent.AddRemoteProgress(remoteProgress);

        agent.Options.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "sync");
        agent.Options.BatchSize = 0;
        agent.Options.UseBulkOperations = false;
        agent.Options.DisableConstraintsOnApplyChanges = false;
        agent.Options.ConflictResolutionPolicy = ConflictResolutionPolicy.ClientWins;

        Console.WriteLine("Sync Start");
        var s1 = await agent.SynchronizeAsync(progress);
        Console.WriteLine(s1);

        Console.WriteLine("Insert product category on Server");
        var id = InsertOneProductCategoryId(serverConnection, "GLASSES");
        Console.WriteLine("Update Done.");

        Console.WriteLine("Update product category on Server");
        UpdateOneProductCategoryId(serverConnection, id, "OVERGLASSES");
        Console.WriteLine("Update Done.");

        Console.WriteLine("Sync Start");
        s1 = await agent.SynchronizeAsync(progress);
        Console.WriteLine(s1);

        Console.WriteLine("End");
    }


    private static int InsertOneProductCategoryId(IDbConnection c, string updatedName)
    {
        using (var command = c.CreateCommand())
        {
            command.CommandText = "Insert Into ProductCategory (Name) Values (@Name); SELECT SCOPE_IDENTITY();";
            var p = command.CreateParameter();
            p.DbType = DbType.String;
            p.Value = updatedName;
            p.ParameterName = "@Name";
            command.Parameters.Add(p);

            c.Open();
            var id = command.ExecuteScalar();
            c.Close();

            return Convert.ToInt32(id);
        }
    }
    private static void UpdateOneProductCategoryId(IDbConnection c, int productCategoryId, string updatedName)
    {
        using (var command = c.CreateCommand())
        {
            command.CommandText = "Update ProductCategory Set Name = @Name Where ProductCategoryId = @Id";
            var p = command.CreateParameter();
            p.DbType = DbType.String;
            p.Value = updatedName;
            p.ParameterName = "@Name";
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.DbType = DbType.Int32;
            p.Value = productCategoryId;
            p.ParameterName = "@Id";
            command.Parameters.Add(p);

            c.Open();
            command.ExecuteNonQuery();
            c.Close();
        }
    }
    private static void DeleteOneLine(DbConnection c, int productCategoryId)
    {
        using (var command = c.CreateCommand())
        {
            command.CommandText = "Delete from ProductCategory Where ProductCategoryId = @Id";

            var p = command.CreateParameter();
            p.DbType = DbType.Int32;
            p.Value = productCategoryId;
            p.ParameterName = "@Id";
            command.Parameters.Add(p);

            c.Open();
            command.ExecuteNonQuery();
            c.Close();
        }
    }



    //private static void TestSerializers()
    //{
    //    var dslight = new DmSetLight(GetSet());

    //    var serializer1 = new ContractSerializer<DmSetLight>();
    //    var serializer2 = new JsonConverter<DmSetLight>();
    //    var serializer3 = new CustomMessagePackSerializer<DmSetLight>();

    //    var bin1 = serializer1.Serialize(dslight);
    //    var bin2 = serializer2.Serialize(dslight);
    //    var bin3 = serializer3.Serialize(dslight);

    //    var json3 = MessagePack.MessagePackSerializer.ToJson(bin3);
    //    string json2;
    //    using (var ms = new MemoryStream(bin2))
    //    {
    //        using (var sr = new StreamReader(ms))
    //        {
    //            json2 = sr.ReadToEnd();
    //        }
    //    }



    //    DmSetLight newDmSetLight1;
    //    DmSetLight newDmSetLight2;
    //    DmSetLight newDmSetLight3;
    //    DmSet newDmSet1;
    //    DmSet newDmSet2;
    //    DmSet newDmSet3;
    //    using (var ms1 = new MemoryStream(bin1))
    //    {
    //        newDmSetLight1 = serializer1.Deserialize(ms1);
    //        newDmSet1 = CreateDmSet();
    //        newDmSetLight1.WriteToDmSet(newDmSet1);
    //    }
    //    using (var ms2 = new MemoryStream(bin2))
    //    {
    //        newDmSetLight2 = serializer2.Deserialize(ms2);
    //        newDmSet2 = CreateDmSet();
    //        newDmSetLight2.WriteToDmSet(newDmSet2);
    //    }
    //    using (var ms3 = new MemoryStream(bin3))
    //    {
    //        newDmSetLight3 = serializer3.Deserialize(ms3);
    //        newDmSet3 = CreateDmSet();
    //        newDmSetLight3.WriteToDmSet(newDmSet3);
    //    }

    //}


    private static void TestSyncTable()
    {
        var set = GetSet();

        var rows = set.Tables[0].Rows.Select(r => r.ItemArray);

        var schemaSet = new SyncSet("Adv", false);
        var schemaTable = new SyncTable("ServiceTickets");
        schemaTable.Columns.Add(SyncColumn.Create<Guid>("ServiceTicketID"));
        schemaTable.Columns.Add(SyncColumn.Create<string>("Title"));
        schemaTable.Columns.Add(SyncColumn.Create<string>("Description"));
        schemaTable.Columns.Add(SyncColumn.Create<int>("EscalationLevel"));
        schemaTable.Columns.Add(SyncColumn.Create<int>("StatusValue"));
        schemaTable.Columns.Add(SyncColumn.Create<DateTime>("Opened"));
        schemaTable.Columns.Add(SyncColumn.Create<DateTime>("Closed"));
        schemaTable.Columns.Add(SyncColumn.Create<int>("CustomerID"));
        schemaSet.Tables.Add(schemaTable);

        schemaTable.Rows.AddRange(rows);

        var row = schemaTable.NewRow();

        row[0] = Guid.NewGuid();

        schemaTable.Rows.Add(row);

        var schemaJson = JsonConvert.SerializeObject(schemaSet);
        var rowsJson = JsonConvert.SerializeObject(schemaSet.GetContainerSet());

    }

    private static DmSet CreateDmSet()
    {
        var set = new DmSet("ClientDmSet");

        var tbl = new DmTable("ServiceTickets");
        set.Tables.Add(tbl);
        var id = new DmColumn<string>("ServiceTicketID");
        tbl.Columns.Add(id);
        var key = new DmKey(new DmColumn[] { id });
        tbl.PrimaryKey = key;
        tbl.Columns.Add(new DmColumn<string>("Title"));
        tbl.Columns.Add(new DmColumn<string>("Description"));
        tbl.Columns.Add(new DmColumn<int>("StatusValue"));
        tbl.Columns.Add(new DmColumn<int>("EscalationLevel"));
        tbl.Columns.Add(new DmColumn<DateTime>("Opened"));
        tbl.Columns.Add(new DmColumn<DateTime>("Closed"));
        tbl.Columns.Add(new DmColumn<int>("CustomerID"));

        return set;
    }

    private static DmSet GetSet()
    {
        var set = CreateDmSet();
        var tbl = set.Tables[0];

        #region adding rows
        var st = tbl.NewRow();
        st["ServiceTicketID"] = Guid.NewGuid().ToString();
        st["Title"] = "Titre AER";
        st["Description"] = "Description 2";
        st["EscalationLevel"] = 1;
        st["StatusValue"] = 2;
        st["Opened"] = DateTime.Now;
        st["Closed"] = null;
        st["CustomerID"] = 1;
        tbl.Rows.Add(st);

        st = tbl.NewRow();
        st["ServiceTicketID"] = Guid.NewGuid().ToString();
        st["Title"] = "Titre DE";
        st["Description"] = "Description 2";
        st["EscalationLevel"] = 3;
        st["StatusValue"] = 2;
        st["Opened"] = DateTime.Now;
        st["Closed"] = null;
        st["CustomerID"] = 1;
        tbl.Rows.Add(st);

        st = tbl.NewRow();
        st["ServiceTicketID"] = Guid.NewGuid().ToString();
        st["Title"] = "Titre FF";
        st["Description"] = "Description 2";
        st["EscalationLevel"] = 3;
        st["StatusValue"] = 4;
        st["Opened"] = DateTime.Now;
        st["Closed"] = null;
        st["CustomerID"] = 2;
        tbl.Rows.Add(st);

        st = tbl.NewRow();
        st["ServiceTicketID"] = Guid.NewGuid().ToString();
        st["Title"] = "Titre AC";
        st["Description"] = "Description 2";
        st["EscalationLevel"] = 1;
        st["StatusValue"] = 2;
        st["Opened"] = DateTime.Now;
        st["Closed"] = null;
        st["CustomerID"] = 2;
        tbl.Rows.Add(st);

        st = tbl.NewRow();
        st["ServiceTicketID"] = Guid.NewGuid().ToString();
        st["Title"] = "Titre ZDZDZ";
        st["Description"] = "Description 2";
        st["EscalationLevel"] = 0;
        st["StatusValue"] = 1;
        st["Opened"] = DateTime.Now;
        st["Closed"] = null;
        st["CustomerID"] = 2;
        tbl.Rows.Add(st);

        st = tbl.NewRow();
        st["ServiceTicketID"] = Guid.NewGuid().ToString();
        st["Title"] = "Titre VGH";
        st["Description"] = "Description 2";
        st["EscalationLevel"] = 0;
        st["StatusValue"] = 1;
        st["Opened"] = DateTime.Now;
        st["Closed"] = null;
        st["CustomerID"] = 3;
        tbl.Rows.Add(st);

        st = tbl.NewRow();
        st["ServiceTicketID"] = Guid.NewGuid().ToString();
        st["Title"] = "Titre ETTG";
        st["Description"] = "Description 2";
        st["EscalationLevel"] = 2;
        st["StatusValue"] = 1;
        st["Opened"] = DateTime.Now;
        st["Closed"] = null;
        st["CustomerID"] = 3;
        tbl.Rows.Add(st);

        st = tbl.NewRow();
        st["ServiceTicketID"] = Guid.NewGuid().ToString();
        st["Title"] = "Titre SADZD";
        st["Description"] = "Description 2";
        st["EscalationLevel"] = 1;
        st["StatusValue"] = 1;
        st["Opened"] = DateTime.Now;
        st["Closed"] = null;
        st["CustomerID"] = 3;
        tbl.Rows.Add(st);

        st = tbl.NewRow();
        st["ServiceTicketID"] = Guid.NewGuid().ToString();
        st["Title"] = "Titre AEEE";
        st["Description"] = "Description 2";
        st["EscalationLevel"] = 0;
        st["StatusValue"] = 0;
        st["Opened"] = DateTime.Now;
        st["Closed"] = null;
        st["CustomerID"] = 1;
        tbl.Rows.Add(st);

        st = tbl.NewRow();
        st["ServiceTicketID"] = Guid.NewGuid().ToString();
        st["Title"] = "Titre CZDADA";
        st["Description"] = "Description 2";
        st["EscalationLevel"] = 0;
        st["StatusValue"] = 0;
        st["Opened"] = DateTime.Now;
        st["Closed"] = null;
        st["CustomerID"] = 3;
        tbl.Rows.Add(st);

        st = tbl.NewRow();
        st["ServiceTicketID"] = Guid.NewGuid().ToString();
        st["Title"] = "Titre AFBBB";
        st["Description"] = "Description 2";
        st["EscalationLevel"] = 0;
        st["StatusValue"] = 3;
        st["Opened"] = DateTime.Now;
        st["Closed"] = null;
        st["CustomerID"] = 3;
        tbl.Rows.Add(st);

        st = tbl.NewRow();
        st["ServiceTicketID"] = Guid.NewGuid().ToString();
        st["Title"] = "Titre AZDCV";
        st["Description"] = "Description 2";
        st["EscalationLevel"] = 2;
        st["StatusValue"] = 2;
        st["Opened"] = DateTime.Now;
        st["Closed"] = null;
        st["CustomerID"] = 2;
        tbl.Rows.Add(st);

        st = tbl.NewRow();
        st["ServiceTicketID"] = Guid.NewGuid().ToString();
        st["Title"] = "Titre UYTR";
        st["Description"] = "Description 2";
        st["EscalationLevel"] = 0;
        st["StatusValue"] = 1;
        st["Opened"] = DateTime.Now;
        st["Closed"] = null;
        st["CustomerID"] = 3;
        tbl.Rows.Add(st);

        st = tbl.NewRow();
        st["ServiceTicketID"] = Guid.NewGuid().ToString();
        st["Title"] = "Titre NHJK";
        st["Description"] = "Description 2";
        st["EscalationLevel"] = 0;
        st["StatusValue"] = 1;
        st["Opened"] = DateTime.Now;
        st["Closed"] = null;
        st["CustomerID"] = 1;
        tbl.Rows.Add(st);

        st = tbl.NewRow();
        st["ServiceTicketID"] = Guid.NewGuid().ToString();
        st["Title"] = "Titre XCVBN";
        st["Description"] = "Description 2";
        st["EscalationLevel"] = 0;
        st["StatusValue"] = 1;
        st["Opened"] = DateTime.Now;
        st["Closed"] = null;
        st["CustomerID"] = 2;
        tbl.Rows.Add(st);

        st = tbl.NewRow();
        st["ServiceTicketID"] = Guid.NewGuid().ToString();
        st["Title"] = "Titre LKNB";
        st["Description"] = "Description 2";
        st["EscalationLevel"] = 3;
        st["StatusValue"] = 2;
        st["Opened"] = DateTime.Now;
        st["Closed"] = null;
        st["CustomerID"] = 3;
        tbl.Rows.Add(st);

        st = tbl.NewRow();
        st["ServiceTicketID"] = Guid.NewGuid().ToString();
        st["Title"] = "Titre ADFVB";
        st["Description"] = "Description 2";
        st["EscalationLevel"] = 0;
        st["StatusValue"] = 2;
        st["Opened"] = DateTime.Now;
        st["Closed"] = null;
        st["CustomerID"] = 1;
        tbl.Rows.Add(st);
        #endregion

        tbl.AcceptChanges();

        //  st.Delete();


        return set;
    }


    private static async Task SynchronizeWithSyncAgent2Async()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString("AdventureWorks"));
        var clientProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString("Client"));

        // Tables involved in the sync process:
        var tables = new string[] { "ProductCategory", "ProductModel", "Product" };

        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, tables);

        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"{s.Context.SyncStage}:\t{s.Message}");
            Console.ResetColor();
        });

        var remoteProgress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.Context.SyncStage}:\t{s.Message}");
            Console.ResetColor();
        });
        agent.AddRemoteProgress(remoteProgress);

        // Setting configuration options
        agent.Schema.StoredProceduresPrefix = "s";
        agent.Schema.StoredProceduresSuffix = "";
        agent.Schema.TrackingTablesPrefix = "t";
        agent.Schema.TrackingTablesSuffix = "";

        agent.Options.ScopeInfoTableName = "tscopeinfo";
        agent.Options.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "sync");
        agent.Options.BatchSize = 100;
        agent.Options.CleanMetadatas = true;
        agent.Options.UseBulkOperations = true;
        agent.Options.UseVerboseErrors = false;

        agent.LocalOrchestrator.OnTransactionOpen(to =>
        {
            var dt = DateTime.Now;
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"Transaction Opened\t {dt.ToLongTimeString()}.{dt.Millisecond}");
            Console.ResetColor();
        });
        agent.LocalOrchestrator.OnTransactionCommit(to =>
        {
            var dt = DateTime.Now;
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"Transaction Commited\t {dt.ToLongTimeString()}.{dt.Millisecond}");
            Console.ResetColor();
        });


        agent.RemoteOrchestrator.OnTransactionOpen(to =>
        {
            var dt = DateTime.Now;
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"Transaction Opened\t {dt.ToLongTimeString()}.{dt.Millisecond}");
            Console.ResetColor();
        });
        agent.RemoteOrchestrator.OnTransactionCommit(to =>
        {
            var dt = DateTime.Now;
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"Transaction Commited\t {dt.ToLongTimeString()}.{dt.Millisecond}");
            Console.ResetColor();
        });

        do
        {
            Console.Clear();
            Console.WriteLine("Sync Start");
            try
            {
                // Launch the sync process
                var s1 = await agent.SynchronizeAsync(SyncType.Normal, CancellationToken.None, progress);

                // Write results
                Console.WriteLine(s1);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }


            //Console.WriteLine("Sync Ended. Press a key to start again, or Escapte to end");
        } while (Console.ReadKey().Key != ConsoleKey.Escape);

        Console.WriteLine("End");
    }

    private async static Task RunAsync()
    {
        // Create databases 
        await DbHelper.EnsureDatabasesAsync(serverDbName);
        await DbHelper.CreateDatabaseAsync(clientDbName);

        // Launch Sync
        await SyncHttpThroughKestellAsync();
    }


    private static async Task SyncAdvAsync()
    {
        //// Sql Server provider, the master.
        //var serverProvider = new SqlSyncProvider(
        //    @"Data Source=.;Initial Catalog=AdventureWorks;User Id=sa;Password=Password12!;");

        //// Sqlite Client provider for a Sql Server <=> Sqlite sync
        //var clientProvider = new SqliteSyncProvider("advworks2.db");

        //// Tables involved in the sync process:
        //var tables = new string[] {"ProductCategory",
        //        "ProductDescription", "ProductModel",
        //        "Product", "ProductModelProductDescription",
        //        "Address", "Customer", "CustomerAddress",
        //        "SalesOrderHeader", "SalesOrderDetail" };

        //// Sync orchestrator
        //var agent = new SyncAgent(clientProvider, serverProvider, tables);


        //do
        //{
        //    var s = await agent.SynchronizeAsync();
        //    Console.WriteLine($"Total Changes downloaded : {s.TotalChangesDownloaded}");

        //} while (Console.ReadKey().Key != ConsoleKey.Escape);
    }


    /// <summary>
    /// Launch a simple sync, over TCP network, each sql server (client and server are reachable through TCP cp
    /// </summary>
    /// <returns></returns>
    private static async Task SynchronizeAsync()
    {
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString(serverDbName));
        var clientProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString(clientDbName));
        //var clientProvider = new SqliteSyncProvider("advworks.db");

        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, serverProvider, allTables);


        // Using the Progress pattern to handle progession during the synchronization
        var progress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine($"{s.Context.SyncStage}:\t{s.Message}");
            Console.ResetColor();
        });

        var remoteProgress = new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.Context.SyncStage}:\t{s.Message}");
            Console.ResetColor();
        });
        // agent.AddRemoteProgress(remoteProgress);

        // Setting configuration options
        agent.Schema.StoredProceduresPrefix = "s";
        agent.Schema.StoredProceduresSuffix = "";
        agent.Schema.TrackingTablesPrefix = "t";
        agent.Schema.TrackingTablesSuffix = "";

        agent.Options.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "sync");
        agent.Options.BatchSize = 0;
        agent.Options.CleanMetadatas = true;
        agent.Options.UseBulkOperations = true;
        agent.Options.UseVerboseErrors = false;
        agent.Options.ScopeInfoTableName = "tscopeinfo";


        do
        {
            Console.Clear();
            Console.WriteLine("Sync Start");
            try
            {
                //agent.RemoteOrchestrator.OnSchema(args =>
                //{
                //    var serializer = new JsonSerializer();
                //    byte[] bin = null;

                //    using (var ms = new MemoryStream())
                //    {
                //        using (var writer = new StreamWriter(ms))
                //        {
                //            using (var jsonWriter = new JsonTextWriter(writer))
                //            {
                //                serializer.Serialize(jsonWriter, args.Schema);
                //            }
                //        }
                //        bin = ms.ToArray();
                //    }

                //    // for readiness
                //    using (var fs = new FileStream("Json_schema.json", FileMode.Create))
                //    {
                //        fs.Write(bin, 0, bin.Length);
                //    }

                //});

                // Launch the sync process
                var s1 = await agent.SynchronizeAsync(progress);

                // Write results
                Console.WriteLine(s1);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }


            //Console.WriteLine("Sync Ended. Press a key to start again, or Escapte to end");
        } while (Console.ReadKey().Key != ConsoleKey.Escape);

        Console.WriteLine("End");
    }

    /// <summary>
    /// Launch a simple sync, over TCP network, each sql server (client and server are reachable through TCP cp
    /// </summary>
    /// <returns></returns>
    private static async Task SynchronizeExistingTablesAsync()
    {
        //string serverName = "ServerTablesExist";
        //string clientName = "ClientsTablesExist";

        //await DbHelper.EnsureDatabasesAsync(serverName);
        //await DbHelper.EnsureDatabasesAsync(clientName);

        //// Create 2 Sql Sync providers
        //var serverProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString(serverName));
        //var clientProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString(clientName));

        //// Tables involved in the sync process:
        //var tables = allTables;

        //// Creating an agent that will handle all the process
        //var agent = new SyncAgent(clientProvider, serverProvider, tables);

        //// Using the Progress pattern to handle progession during the synchronization
        //var progress = new Progress<ProgressArgs>(s => Console.WriteLine($"[client]: {s.Context.SyncStage}:\t{s.Message}"));




        //// Setting configuration options
        //agent.SetConfiguration(s =>
        //{
        //    s.ScopeInfoTableName = "tscopeinfo";
        //    s.SerializationFormat = Dotmim.Sync.Enumerations.SerializationFormat.Binary;
        //    s.StoredProceduresPrefix = "s";
        //    s.StoredProceduresSuffix = "";
        //    s.TrackingTablesPrefix = "t";
        //    s.TrackingTablesSuffix = "";
        //});

        //agent.SetOptions(opt =>
        //{
        //    opt.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "sync");
        //    opt.BatchSize = 100;
        //    opt.CleanMetadatas = true;
        //    opt.UseBulkOperations = true;
        //    opt.UseVerboseErrors = false;
        //});


        //var remoteProvider = agent.RemoteProvider as CoreProvider;

        //var dpAction = new Action<DatabaseProvisionedArgs>(args =>
        //{
        //    Console.WriteLine($"-- [InterceptDatabaseProvisioned] -- ");

        //    var sql = $"Update tscopeinfo set scope_last_sync_timestamp = 0 where [scope_is_local] = 1";

        //    var cmd = args.Connection.CreateCommand();
        //    cmd.Transaction = args.Transaction;
        //    cmd.CommandText = sql;

        //    cmd.ExecuteNonQuery();

        //});

        //remoteProvider.OnDatabaseProvisioned(dpAction);

        //agent.LocalProvider.OnDatabaseProvisioned(dpAction);

        //do
        //{
        //    Console.Clear();
        //    Console.WriteLine("Sync Start");
        //    try
        //    {
        //        // Launch the sync process
        //        var s1 = await agent.SynchronizeAsync(progress);

        //        // Write results
        //        Console.WriteLine(s1);
        //    }
        //    catch (Exception e)
        //    {
        //        Console.WriteLine(e.Message);
        //    }


        //    //Console.WriteLine("Sync Ended. Press a key to start again, or Escapte to end");
        //} while (Console.ReadKey().Key != ConsoleKey.Escape);

        //Console.WriteLine("End");
    }



    private static async Task SynchronizeOSAsync()
    {
        // Create 2 Sql Sync providers
        //var serverProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString("OptionsServer"));
        //var clientProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString("OptionsClient"));

        //// Tables involved in the sync process:
        //var tables = new string[] { "ObjectSettings", "ObjectSettingValues" };

        //// Creating an agent that will handle all the process
        //var agent = new SyncAgent(clientProvider, serverProvider, tables);

        //// Using the Progress pattern to handle progession during the synchronization
        //var progress = new Progress<ProgressArgs>(s => Console.WriteLine($"[client]: {s.Context.SyncStage}:\t{s.Message}"));

        //do
        //{
        //    Console.Clear();
        //    Console.WriteLine("Sync Start");
        //    try
        //    {
        //        // Launch the sync process
        //        var s1 = await agent.SynchronizeAsync(progress);

        //        // Write results
        //        Console.WriteLine(s1);
        //    }
        //    catch (Exception e)
        //    {
        //        Console.WriteLine(e.Message);
        //    }


        //    //Console.WriteLine("Sync Ended. Press a key to start again, or Escapte to end");
        //} while (Console.ReadKey().Key != ConsoleKey.Escape);

        //Console.WriteLine("End");
    }


    public static async Task SyncHttpThroughKestellAsync()
    {
        // server provider
        // Create 2 Sql Sync providers
        var serverProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString(serverProductCategoryDbName));
        var clientProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString(clientDbName));



        // Tables involved in the sync process:
        //var tables = allTables;
        var tables = new string[] { "ProductCategory" };


        // ----------------------------------
        // Client side
        // ----------------------------------
        var clientOptions = new SyncOptions
        {
            ScopeInfoTableName = "client_scopeinfo",
            BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "sync_client"),
            BatchSize = 0,
            CleanMetadatas = true,
            UseBulkOperations = true,
            UseVerboseErrors = false,
        };

        // ----------------------------------
        // Web Server side
        // ----------------------------------
        var schema = new SyncSet()
        {
            StoredProceduresPrefix = "s",
            StoredProceduresSuffix = "",
            TrackingTablesPrefix = "t",
            TrackingTablesSuffix = ""
        };
        schema.Tables.Add(tables);

        var webServerOptions = new WebServerOptions
        {
            BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "sync_server"),
            CleanMetadatas = true,
            UseBulkOperations = false,
            UseVerboseErrors = false
        };
        webServerOptions.Serializers.Add(new CustomMessagePackSerializerFactory());

        // Create the web proxy client provider with specific options
        var proxyClientProvider = new WebClientOrchestrator
        {
            SerializerFactory = new CustomMessagePackSerializerFactory()
        };


        // Creating an agent that will handle all the process
        var agent = new SyncAgent(clientProvider, proxyClientProvider, null, clientOptions);


        var serverHandler = new RequestDelegate(async context =>
        {
            var proxyServerProvider = WebProxyServerOrchestrator.Create(context, serverProvider, schema, webServerOptions);

            await proxyServerProvider.HandleRequestAsync(context);
        });
        using (var server = new KestrellTestServer())
        {
            var clientHandler = new ResponseDelegate(async (serviceUri) =>
            {
                proxyClientProvider.ServiceUri = serviceUri;
                Console.Clear();
                Console.WriteLine("Sync Start");
                var s1 = await agent.SynchronizeAsync();
                Console.WriteLine(s1);
                Console.WriteLine("--------------------------------------------------");


                Console.WriteLine("Insert product category on Client");
                var id = InsertOneProductCategoryId(new SqlConnection(clientProvider.ConnectionString), "GLASSES");
                Console.WriteLine("Insert Done.");

                Console.WriteLine("Sync Start");
                s1 = await agent.SynchronizeAsync();
                Console.WriteLine(s1);
                Console.WriteLine("--------------------------------------------------");

            });
            await server.Run(serverHandler, clientHandler);
        }

    }

    /// <summary>
    /// Test a client syncing through a web api
    /// </summary>
    private static async Task TestSyncThroughWebApi()
    {
        //var clientProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString(clientDbName));

        //var proxyClientProvider = new WebProxyClientProvider(
        //    new Uri("http://localhost:52288/api/Sync"));

        //var agent = new SyncAgent(clientProvider, proxyClientProvider);

        //Console.WriteLine("Press a key to start (be sure web api is running ...)");
        //Console.ReadKey();
        //do
        //{
        //    Console.Clear();
        //    Console.WriteLine("Web sync start");
        //    try
        //    {
        //        var progress = new Progress<ProgressArgs>(pa => Console.WriteLine($"{pa.Context.SessionId} - {pa.Context.SyncStage}\t {pa.Message}"));

        //        var s = await agent.SynchronizeAsync(progress);

        //        Console.WriteLine(s);

        //    }
        //    catch (SyncException e)
        //    {
        //        Console.WriteLine(e.ToString());
        //    }
        //    catch (Exception e)
        //    {
        //        Console.WriteLine("UNKNOW EXCEPTION : " + e.Message);
        //    }


        //    Console.WriteLine("Sync Ended. Press a key to start again, or Escapte to end");
        //} while (Console.ReadKey().Key != ConsoleKey.Escape);

        //Console.WriteLine("End");

    }



}


[DataContract(Namespace = "http://Microsoft.ServiceModel.Samples")]
internal class Record
{
    private double n1;
    private double n2;
    private string operation;
    private double result;

    internal Record(double n1, double n2, string operation, double result)
    {
        this.n1 = n1;
        this.n2 = n2;
        this.operation = operation;
        this.result = result;
    }

    [DataMember]
    internal double OperandNumberOne
    {
        get { return n1; }
        set { n1 = value; }
    }

    [DataMember]
    internal double OperandNumberTwo
    {
        get { return n2; }
        set { n2 = value; }
    }

    [DataMember]
    internal string Operation
    {
        get { return operation; }
        set { operation = value; }
    }

    [DataMember]
    internal double Result
    {
        get { return result; }
        set { result = value; }
    }

    public override string ToString()
    {
        return $"Record: {n1} {operation} {n2} = {result}";
    }
}