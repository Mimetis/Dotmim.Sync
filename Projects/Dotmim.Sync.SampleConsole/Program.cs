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

        // Test DmRelation
        TestDmRelations();


        //TestCreateTrackingTable(serverProvider);

        Console.ReadLine();
        //SyncAgent agent = new SyncAgent(clientProvider, serverProvider);

        //// Today the configuration is hosted in a config file hosted in the db
        //// I want to change that
        //agent.Configuration.SetEnableScope("DefaultScope");

        //agent.SyncProgress += Agent_SyncProgress;

        //agent.SynchronizeAsync();


        //Console.ReadLine();
    }

    private static void TestDmRelations()
    {
        DmSet set = new DmSet();
        DmTable tblClientType = new DmTable("ClientType");

        DmColumn colClientTypeId = new DmColumn<int>("Id");
        colClientTypeId.AutoIncrement = true;
        tblClientType.Columns.Add(colClientTypeId);

        DmColumn colClientTypeName = new DmColumn<String>("Name");
        tblClientType.Columns.Add(colClientTypeName);

        tblClientType.PrimaryKey = new DmKey(colClientTypeId);
        set.Tables.Add(tblClientType);

        DmTable tblClient = new DmTable("Client");

        DmColumn colClientId = new DmColumn<int>("Id");
        colClientId.AutoIncrement = true;
        tblClient.Columns.Add(colClientId);
        tblClient.PrimaryKey = new DmKey(colClientId);

        DmColumn colClientClientTypeId = new DmColumn<int>("ClientTypeId");
        tblClient.Columns.Add(colClientClientTypeId);

        DmColumn colClientName = new DmColumn<String>("Name");
        tblClient.Columns.Add(colClientName);
        set.Tables.Add(tblClient);

        DmRelation fkClientClientType = new DmRelation("Fk_Client_ClientType", colClientTypeId, colClientClientTypeId);
        set.Relations.Add(fkClientClientType);

    }

    private static void TestCreateTrackingTable(SqlSyncProvider provider)
    {

        DmTable table = new DmTable("Products");


        DmColumn id = new DmColumn<Int32>("Id");
        id.AllowDBNull = false;
        id.AutoIncrement = true;
        table.Columns.Add(id);

        DmColumn clientId = new DmColumn<Guid>("clientId");
        clientId.AllowDBNull = true;
        table.Columns.Add(clientId);

        DmColumn name = new DmColumn<string>("name");
        name.AllowDBNull = true;
        name.DbType = System.Data.DbType.StringFixedLength;
        name.MaxLength = 150;
        table.Columns.Add(name);

        DmColumn salary = new DmColumn<Decimal>("salary");
        salary.AllowDBNull = false;
        salary.DbType = System.Data.DbType.VarNumeric;
        salary.Precision = 6;
        salary.Scale = 2;
        table.Columns.Add(salary);

        table.PrimaryKey = new DmKey(new DmColumn[] { id, name, salary });

        provider.Connection.Open();

        using (var transaction = provider.Connection.BeginTransaction())
        {
            var builderTableProducts = provider.CreateDatabaseBuilder(table);

            // this sync is filtered with a client
            builderTableProducts.FilterColumns.Add(clientId);
            builderTableProducts.TrackingTableBuilder.FilterColumns = builderTableProducts.FilterColumns;
            builderTableProducts.ProcBuilder.FilterColumns = builderTableProducts.FilterColumns;
            Console.WriteLine(builderTableProducts.TrackingTableBuilder.CreateTableScriptText());
            Console.WriteLine(builderTableProducts.TrackingTableBuilder.CreatePkScriptText());
            Console.WriteLine(builderTableProducts.TrackingTableBuilder.CreatePopulateFromBaseTableScriptText());
            Console.WriteLine(builderTableProducts.ProcBuilder.CreateDeleteScriptText(transaction, DbBuilderOption.Create));
            Console.WriteLine(builderTableProducts.ProcBuilder.CreateInsertScriptText(transaction, DbBuilderOption.Create));
            Console.WriteLine(builderTableProducts.ProcBuilder.CreateUpdateScriptText(transaction, DbBuilderOption.Create));
            Console.WriteLine(builderTableProducts.ProcBuilder.CreateDeleteMetadataScriptText(transaction, DbBuilderOption.Create));
            Console.WriteLine(builderTableProducts.ProcBuilder.CreateInsertMetadataScriptText(transaction, DbBuilderOption.Create));
            Console.WriteLine(builderTableProducts.ProcBuilder.CreateSelectRowScriptText(transaction, DbBuilderOption.Create));
            Console.WriteLine(builderTableProducts.ProcBuilder.CreateUpdateMetadataScriptText(transaction, DbBuilderOption.Create));
            Console.WriteLine(builderTableProducts.ProcBuilder.CreateTVPTypeScriptText(transaction, DbBuilderOption.Create));
            Console.WriteLine(builderTableProducts.ProcBuilder.CreateBulkDeleteScriptText(transaction, DbBuilderOption.Create));
            Console.WriteLine(builderTableProducts.ProcBuilder.CreateBulkInsertScriptText(transaction, DbBuilderOption.Create));
            Console.WriteLine(builderTableProducts.ProcBuilder.CreateBulkUpdateScriptText(transaction, DbBuilderOption.Create));
            Console.WriteLine(builderTableProducts.ProcBuilder.CreateSelectIncrementalChangesScriptText(transaction, DbBuilderOption.Create));

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