using DmBinaryFormatter;
using Dotmim.Sync.Core;
using Dotmim.Sync.Core.Builders;
using Dotmim.Sync.Core.Enumerations;
using Dotmim.Sync.Core.Manager;
using Dotmim.Sync.Data;
using Dotmim.Sync.Data.Surrogate;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.SqlServer.Builders;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Reflection;
using System.Threading.Tasks;


class Program
{
    static void Main(string[] args)
    {

        TestSync();
        //TestWebPostStream().Wait();

        //TestSerializerSize();

        Console.ReadLine();

    }


    private static void TestSerializerSize()
    {
        DmTable dtClient = new DmTable("Client");
        DmColumn dcId = new DmColumn<int>("Id");
        DmColumn dcName = new DmColumn<string>("Name");
        DmColumn dcBirthdate = new DmColumn<DateTime>("Date");
        DmColumn dcMoney = new DmColumn<Double>("Money");
        DmColumn dcInt = new DmColumn<Int64>("i64");
        DmColumn dcJob = new DmColumn<String>("Job");
        dtClient.Columns.AddRange(new[] { dcId, dcName, dcBirthdate, dcMoney, dcInt, dcJob });

     
        long rowsSize = 0 ;

        foreach (var c in dtClient.Columns)
        {
            DmColumnSurrogate dcs = new DmColumnSurrogate(c);
            Console.WriteLine($"Column {dcs.ColumnName} Surrogate length : " + dcs.GetBytesLength());
            rowsSize += dcs.GetBytesLength();
        }

        var dmTableS = new DmTableSurrogate(dtClient);
        var dmTableSLength = dmTableS.GetEmptyBytesLength();
        Console.WriteLine("DmTableSurrogate length = " + dmTableSLength);

        rowsSize += dmTableSLength;


        for (int i = 0; i < 50000; i++)
        {
            var newRow = dtClient.NewRow();

            Random r = new Random(DateTime.Now.Second);
            var l = r.NextDouble() * 100;
            string name = "";
            for (int j = 0; j < l; j++)
                name += "a";

            newRow["Id"] = 12;
            newRow["Name"] = name;
            newRow["Date"] = DateTime.Now;
            newRow["Money"] = l;

            var l2 = r.NextDouble() * 100;
            string job = "";
            for (int j = 0; j < l2; j++)
                job += "b";
            newRow["Job"] = job;
            newRow["i64"] = l2;

            rowsSize += DmTableSurrogate.GetRowSizeFromDataRow(newRow);

            dtClient.Rows.Add(newRow);

        }


        var serializer = new DmSerializer();

        long dtSize = 0;

        if (File.Exists("dt.bin"))
            File.Delete("dt.bin");


        using (var fs = new FileStream("dt.bin", FileMode.OpenOrCreate, FileAccess.ReadWrite))
        {
            serializer.Serialize(new DmTableSurrogate(dtClient), fs);
            dtSize = fs.Length;

        }

        //using (var fs = new FileStream("dt.bin", FileMode.Open, FileAccess.Read))
        //{
        //    var ds = serializer.Deserialize<DmTableSurrogate>(fs);

        //}

            Console.WriteLine("Rows Size : " + rowsSize);
        Console.WriteLine("Table Size : " + dtSize);

    }


    private static void TestSync()
    {
        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.AddJsonFile("config.json", true);
        IConfiguration Configuration = configurationBuilder.Build();
        var serverConfig = Configuration["AppConfiguration:ServerConnectionString"];
        var clientConfig = Configuration["AppConfiguration:ClientConnectionString"];

        SqlSyncProvider serverProvider = new SqlSyncProvider(serverConfig);
        SqlSyncProvider clientProvider = new SqlSyncProvider(clientConfig);

        // Shortcut 
        //SyncAgent agent = new SyncAgent(clientProvider, serverProvider, new string[] { "ServiceTickets" });

        // With a config when we are in local mode (no proxy)
        ServiceConfiguration configuration = new ServiceConfiguration(new string[] { "ServiceTickets" });
        configuration.DownloadBatchSizeInKB = 500;
        SyncAgent agent = new SyncAgent(clientProvider, serverProvider, configuration);

        agent.SyncProgress += Agent_SyncProgress;

        do
        {
            Console.Clear();
            Console.WriteLine("Sync Start");
            var s = agent.SynchronizeAsync();

            Console.WriteLine("Sync Ended. Press a key to start again, or Escapte to end");
        } while (Console.ReadKey().Key != ConsoleKey.Escape);

        Console.WriteLine("End");
    }
    private async static Task TestWebPostStream()
    {
        var dmTable = GetATestTable();
        var dmSet = new DmSet();
        dmSet.Tables.Add(dmTable);
        var surrgotabeTable = new DmSetSurrogate(dmSet);

        DmSerializer serializer = new DmSerializer();
        var binaryData = serializer.Serialize(surrgotabeTable);

        Uri target = new Uri("http://localhost:5000/api/sync");

        var client = new HttpClient();

        ByteArrayContent arrayContent = new ByteArrayContent(binaryData);
        var response = await client.PostAsync(target, arrayContent);

        if (response.IsSuccessStatusCode)
        {
            using (var stream = await response.Content.ReadAsStreamAsync())
            {
                var ds = serializer.Deserialize<DmSetSurrogate>(stream);

                var newDs = ds.ConvertToDmSet();

            }
        }


    }

    private static DmTable GetATestTable()
    {
        DmTable tbl = null;

        tbl = new DmTable("ServiceTickets");
        var id = new DmColumn<Guid>("ServiceTicketID");
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

        #region adding rows
        var st = tbl.NewRow();
        st["ServiceTicketID"] = Guid.NewGuid();
        st["Title"] = "Titre AER";
        st["Description"] = "Description 2";
        st["EscalationLevel"] = 1;
        st["StatusValue"] = 2;
        st["Opened"] = DateTime.Now;
        st["Closed"] = null;
        st["CustomerID"] = 1;
        tbl.Rows.Add(st);

        st = tbl.NewRow();
        st["ServiceTicketID"] = Guid.NewGuid();
        st["Title"] = "Titre DE";
        st["Description"] = "Description 2";
        st["EscalationLevel"] = 3;
        st["StatusValue"] = 2;
        st["Opened"] = DateTime.Now;
        st["Closed"] = null;
        st["CustomerID"] = 1;
        tbl.Rows.Add(st);

        st = tbl.NewRow();
        st["ServiceTicketID"] = Guid.NewGuid();
        st["Title"] = "Titre FF";
        st["Description"] = "Description 2";
        st["EscalationLevel"] = 3;
        st["StatusValue"] = 4;
        st["Opened"] = DateTime.Now;
        st["Closed"] = null;
        st["CustomerID"] = 2;
        tbl.Rows.Add(st);

        st = tbl.NewRow();
        st["ServiceTicketID"] = Guid.NewGuid();
        st["Title"] = "Titre AC";
        st["Description"] = "Description 2";
        st["EscalationLevel"] = 1;
        st["StatusValue"] = 2;
        st["Opened"] = DateTime.Now;
        st["Closed"] = null;
        st["CustomerID"] = 2;
        tbl.Rows.Add(st);

        st = tbl.NewRow();
        st["ServiceTicketID"] = Guid.NewGuid();
        st["Title"] = "Titre ZDZDZ";
        st["Description"] = "Description 2";
        st["EscalationLevel"] = 0;
        st["StatusValue"] = 1;
        st["Opened"] = DateTime.Now;
        st["Closed"] = null;
        st["CustomerID"] = 2;
        tbl.Rows.Add(st);

        st = tbl.NewRow();
        st["ServiceTicketID"] = Guid.NewGuid();
        st["Title"] = "Titre VGH";
        st["Description"] = "Description 2";
        st["EscalationLevel"] = 0;
        st["StatusValue"] = 1;
        st["Opened"] = DateTime.Now;
        st["Closed"] = null;
        st["CustomerID"] = 3;
        tbl.Rows.Add(st);

        st = tbl.NewRow();
        st["ServiceTicketID"] = Guid.NewGuid();
        st["Title"] = "Titre ETTG";
        st["Description"] = "Description 2";
        st["EscalationLevel"] = 2;
        st["StatusValue"] = 1;
        st["Opened"] = DateTime.Now;
        st["Closed"] = null;
        st["CustomerID"] = 3;
        tbl.Rows.Add(st);

        st = tbl.NewRow();
        st["ServiceTicketID"] = Guid.NewGuid();
        st["Title"] = "Titre SADZD";
        st["Description"] = "Description 2";
        st["EscalationLevel"] = 1;
        st["StatusValue"] = 1;
        st["Opened"] = DateTime.Now;
        st["Closed"] = null;
        st["CustomerID"] = 3;
        tbl.Rows.Add(st);

        st = tbl.NewRow();
        st["ServiceTicketID"] = Guid.NewGuid();
        st["Title"] = "Titre AEEE";
        st["Description"] = "Description 2";
        st["EscalationLevel"] = 0;
        st["StatusValue"] = 0;
        st["Opened"] = DateTime.Now;
        st["Closed"] = null;
        st["CustomerID"] = 1;
        tbl.Rows.Add(st);

        st = tbl.NewRow();
        st["ServiceTicketID"] = Guid.NewGuid();
        st["Title"] = "Titre CZDADA";
        st["Description"] = "Description 2";
        st["EscalationLevel"] = 0;
        st["StatusValue"] = 0;
        st["Opened"] = DateTime.Now;
        st["Closed"] = null;
        st["CustomerID"] = 3;
        tbl.Rows.Add(st);

        st = tbl.NewRow();
        st["ServiceTicketID"] = Guid.NewGuid();
        st["Title"] = "Titre AFBBB";
        st["Description"] = "Description 2";
        st["EscalationLevel"] = 0;
        st["StatusValue"] = 3;
        st["Opened"] = DateTime.Now;
        st["Closed"] = null;
        st["CustomerID"] = 3;
        tbl.Rows.Add(st);

        st = tbl.NewRow();
        st["ServiceTicketID"] = Guid.NewGuid();
        st["Title"] = "Titre AZDCV";
        st["Description"] = "Description 2";
        st["EscalationLevel"] = 2;
        st["StatusValue"] = 2;
        st["Opened"] = DateTime.Now;
        st["Closed"] = null;
        st["CustomerID"] = 2;
        tbl.Rows.Add(st);

        st = tbl.NewRow();
        st["ServiceTicketID"] = Guid.NewGuid();
        st["Title"] = "Titre UYTR";
        st["Description"] = "Description 2";
        st["EscalationLevel"] = 0;
        st["StatusValue"] = 1;
        st["Opened"] = DateTime.Now;
        st["Closed"] = null;
        st["CustomerID"] = 3;
        tbl.Rows.Add(st);

        st = tbl.NewRow();
        st["ServiceTicketID"] = Guid.NewGuid();
        st["Title"] = "Titre NHJK";
        st["Description"] = "Description 2";
        st["EscalationLevel"] = 0;
        st["StatusValue"] = 1;
        st["Opened"] = DateTime.Now;
        st["Closed"] = null;
        st["CustomerID"] = 1;
        tbl.Rows.Add(st);

        st = tbl.NewRow();
        st["ServiceTicketID"] = Guid.NewGuid();
        st["Title"] = "Titre XCVBN";
        st["Description"] = "Description 2";
        st["EscalationLevel"] = 0;
        st["StatusValue"] = 1;
        st["Opened"] = DateTime.Now;
        st["Closed"] = null;
        st["CustomerID"] = 2;
        tbl.Rows.Add(st);

        st = tbl.NewRow();
        st["ServiceTicketID"] = Guid.NewGuid();
        st["Title"] = "Titre LKNB";
        st["Description"] = "Description 2";
        st["EscalationLevel"] = 3;
        st["StatusValue"] = 2;
        st["Opened"] = DateTime.Now;
        st["Closed"] = null;
        st["CustomerID"] = 3;
        tbl.Rows.Add(st);

        st = tbl.NewRow();
        st["ServiceTicketID"] = Guid.NewGuid();
        st["Title"] = "Titre ADFVB";
        st["Description"] = "Description 2";
        st["EscalationLevel"] = 0;
        st["StatusValue"] = 2;
        st["Opened"] = DateTime.Now;
        st["Closed"] = null;
        st["CustomerID"] = 1;
        tbl.Rows.Add(st);
        #endregion

        return tbl;
    }

    private static void TestCreateTrackingTable(SqlSyncProvider provider)
    {

        DmSet set = new DmSet();
        DmTable clientsTable = new DmTable("Clients");
        DmTable productsTable = new DmTable("Products");

        // orders matters !!
        set.Tables.Add(clientsTable);
        set.Tables.Add(productsTable);

        DmColumn id = new DmColumn<Int32>("Id");
        id.AllowDBNull = false;
        id.AutoIncrement = true;
        productsTable.Columns.Add(id);

        DmColumn fkClientId = new DmColumn<Guid>("clientId");
        fkClientId.AllowDBNull = true;
        productsTable.Columns.Add(fkClientId);

        DmColumn name = new DmColumn<string>("name");
        name.AllowDBNull = true;
        name.DbType = System.Data.DbType.StringFixedLength;
        name.MaxLength = 150;
        productsTable.Columns.Add(name);

        DmColumn salary = new DmColumn<Decimal>("salary");
        salary.AllowDBNull = false;
        salary.DbType = System.Data.DbType.VarNumeric;
        salary.Precision = 6;
        salary.Scale = 2;
        productsTable.Columns.Add(salary);

        productsTable.PrimaryKey = new DmKey(new DmColumn[] { id, name, salary });


        DmColumn clientId = new DmColumn<Guid>("Id");
        clientId.AllowDBNull = false;
        clientsTable.Columns.Add(clientId);

        DmColumn clientName = new DmColumn<string>("Name");
        clientsTable.Columns.Add(clientName);

        clientsTable.PrimaryKey = new DmKey(clientId);

        // ForeignKey
        DmRelation fkClientRelation = new DmRelation("FK_Products_Clients", clientId, fkClientId);
        productsTable.AddForeignKey(fkClientRelation);

        DbConnection connection = null;
        try
        {
            using (connection = provider.CreateConnection())
            {
                foreach (var table in set.Tables)
                {
                    var builder = provider.GetDatabaseBuilder(table, DbBuilderOption.CreateOrUseExistingSchema);

                    if (table.TableName == "Clients")
                        builder.AddFilterColumn("Id");

                    if (table.TableName == "Products")
                        builder.AddFilterColumn("clientId");

                    builder.Apply(connection);

                    Console.WriteLine(builder.Script(connection));
                }
            }

        }
        catch (Exception ex)
        {
            Console.WriteLine(ex);
            return;
        }
        finally
        {
            if (connection.State != System.Data.ConnectionState.Closed)
                connection.Close();
        }


    }

    private static void Agent_SyncProgress(object sender, ScopeProgressEventArgs e)
    {
        var scopeName = e.ScopeInfo != null ? e.ScopeInfo.Name : string.Empty;
        scopeName = $"[{scopeName}]";

        switch (e.Stage)
        {
            case SyncStage.BeginSession:
                Console.WriteLine($"Begin Session");
                break;
            case SyncStage.EndSession:
                Console.WriteLine($"End Session");
                break;

            case SyncStage.ReadingScope:
                Console.WriteLine($"{scopeName}. Reading metadata on ScopeName : {e.ScopeInfo.Name} - Last provider timestamp {e.ScopeInfo.LastTimestamp} - Is new : {e.ScopeInfo.IsNewScope} ");

                break;
            case SyncStage.BuildConfiguration:
                Console.WriteLine($"{scopeName}. Configuration read and built");
                var ds = e.Configuration.ScopeSet;
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
                break;
            case SyncStage.EnsureDatabase:
                Console.WriteLine($"{scopeName}. Ensure database is created");
                //Console.WriteLine(e.DatabaseScript);
                break;
            case SyncStage.SelectedChanges:
                Console.WriteLine($"{scopeName}. Selection changes : . Insert  {e.TotalSelectedChangesInserts}. Updates : {e.TotalSelectedChangesUpdates}. Deletes : {e.TotalSelectedChangesDeletes}");
                break;
            case SyncStage.ApplyingInserts:
                Console.WriteLine($"{scopeName}. Applying Inserts : {e.AppliedChanges.Where(ac => ac.State == DmRowState.Added).Sum(ac => ac.ChangesApplied)}");
                break;
            case SyncStage.ApplyingDeletes:
                Console.WriteLine($"{scopeName}. Applying deletes : {e.AppliedChanges.Where(ac => ac.State == DmRowState.Deleted).Sum(ac => ac.ChangesApplied)}");
                break;

            case SyncStage.ApplyingUpdates:
                Console.WriteLine($"{scopeName}. Applying updates : {e.AppliedChanges.Where(ac => ac.State == DmRowState.Modified).Sum(ac => ac.ChangesApplied)}");
                break;
            case SyncStage.WritingScope:
                Console.WriteLine($"{scopeName}. Writing Scopes");
                break;
        }
    }


    static void SqlSyncProvider_ApplyChangedFailed(object sender, ApplyChangeFailedEventArgs e)
    {
        // Note: LocalChange table name may be null if the record does not exist on the server. So use the remote table name.
        string tableName = e.Conflict.RemoteChange.TableName;

        // Line exist on client, not on server, force to create it
        if (e.Conflict.Type == ConflictType.LocalNoRowRemoteUpdate)
        {
            e.Action = ApplyAction.Rollback;
        }
        else
        {
            e.Action = ApplyAction.Continue;
        }

    }



}