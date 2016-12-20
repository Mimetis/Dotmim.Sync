using Dotmim.Sync.Core;
using Dotmim.Sync.Core.Builders;
using Dotmim.Sync.Core.Enumerations;
using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.SqlServer.Builders;
using Microsoft.Extensions.Configuration;
using System;

class Program
{
    static void Main(string[] args)
    {

        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.AddJsonFile("config.json", true);

        IConfiguration Configuration = configurationBuilder.Build();

        var serverConfig = Configuration["AppConfiguration:ServerConnectionString"];
        SqlSyncProvider serverProvider = new SqlSyncProvider(serverConfig);
        var clientConfig = Configuration["AppConfiguration:ClientConnectionString"];
        SqlSyncProvider clientProvider = new SqlSyncProvider(clientConfig);

        TestCreateTrackingTable(clientProvider);

        Console.ReadLine();
        //SyncAgent agent = new SyncAgent(clientProvider, serverProvider);

        //// Today the configuration is hosted in a config file hosted in the db
        //// I want to change that
        //agent.Configuration.SetEnableScope("DefaultScope");

        //agent.SyncProgress += Agent_SyncProgress;

        //agent.SynchronizeAsync();


        //Console.ReadLine();
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

        provider.Connection.Open();

        foreach (var table in set.Tables)
        {
            var builder = provider.CreateDatabaseBuilder(table, DbBuilderOption.Create);

            if (table.TableName == "Clients")
                builder.AddFilterColumn("Id");

            if (table.TableName == "Products")
                builder.AddFilterColumn("clientId");

            builder.Apply();

            Console.WriteLine(builder.Script());
        }

        provider.Connection.Close();
    }

    private static void Agent_SyncProgress(object sender, ScopeProgressEventArgs e)
    {
        var scopeName = e.ScopeInfo != null ? e.ScopeInfo.Name : string.Empty;
        scopeName = $"[{scopeName}]";

        switch (e.Stage)
        {
            case SyncStage.ReadingMetadata:
                Console.WriteLine($"----------------------------------------");
                Console.WriteLine($"{scopeName}. Reading metadata");
                Console.WriteLine($"ScopeName : {e.ScopeInfo.Name} - Last provider timestamp {e.ScopeInfo.LastTimestamp}");
                Console.WriteLine($"----------------------------------------");

                break;
            case SyncStage.ReadingSchema:
                Console.WriteLine($"----------------------------------------");
                Console.WriteLine($"{scopeName}. Reading Schema");
                Console.WriteLine(e.GetSerializedSchema());
                Console.WriteLine($"----------------------------------------");
                break;
            case SyncStage.SelectedChanges:
                Console.WriteLine($"{scopeName}. Selection changes");
                //foreach (var table in e.DmSet.Tables)
                //    foreach (var row in table.Rows)
                //        Console.WriteLine($"[{row["ServiceTicketID"]} : {row["Title"]} ");
                break;
            case SyncStage.ApplyingChanges:
                Console.WriteLine($"{scopeName}. Applying changes");
                //foreach (var table in e.DmSet.Tables)
                //{
                //    foreach (var row in table.Rows)
                //    {
                //        if (row.RowState == DmRowState.Deleted)
                //            Console.WriteLine($"[{row.RowState}] [{row["ServiceTicketID", DmRowVersion.Original]} : {row["Title", DmRowVersion.Original]} ");
                //        else
                //            Console.WriteLine($"[{row.RowState}] [{row["ServiceTicketID"]} : {row["Title"]} ");
                //    }

                //}
                break;
            case SyncStage.ApplyingDeletes:
                Console.WriteLine($"{scopeName}. Applying deletes");
                break;
            case SyncStage.ApplyingInserts:
                Console.WriteLine($"{scopeName}. Applying inserts");
                break;
            case SyncStage.ApplyingUpdates:
                Console.WriteLine($"{scopeName}. Applying updates");
                break;
            case SyncStage.WritingMetadata:
                Console.WriteLine($"{scopeName}. Writing metadatas");
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