using Dotmim.Sync.Core;
using Dotmim.Sync.Core.Builders;
using Dotmim.Sync.Core.Enumerations;
using Dotmim.Sync.Data;
using Dotmim.Sync.Data.Surrogate;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.SqlServer.Builders;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using System;
using System.Data.Common;
using System.Linq;

class Program
{
    static void Main(string[] args)
    {
        ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
        configurationBuilder.AddJsonFile("config.json", true);
        IConfiguration Configuration = configurationBuilder.Build();
        var serverConfig = Configuration["AppConfiguration:ServerConnectionString"];
        var clientConfig = Configuration["AppConfiguration:ClientConnectionString"];

        SqlSyncProvider serverProvider = new SqlSyncProvider(serverConfig);
        SqlSyncProvider clientProvider = new SqlSyncProvider(clientConfig);

        SyncAgent agent = new SyncAgent(clientProvider, serverProvider,
            new[] { "ServiceTickets", "ProductType", "Products" });

        agent.SyncProgress += Agent_SyncProgress;

        do
        {
            Console.WriteLine("Sync Start");
            var s = agent.SynchronizeAsync();

            Console.WriteLine("Sync Ended. Press a key to start again, or Escapte to end");
        } while (Console.ReadKey().Key != ConsoleKey.Escape);

        Console.WriteLine("End");

        Console.ReadLine();
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
                Console.WriteLine($"----------------------------------------");
                Console.WriteLine($"Begin Session");
                break;
            case SyncStage.EndSession:
                Console.WriteLine($"End Session");
                Console.WriteLine($"----------------------------------------");
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
                Console.WriteLine($"----------------------------------------");
                Console.WriteLine($"{scopeName}. Selection changes");
                foreach (var changes in e.SelectedChanges)
                {
                    Console.WriteLine($"Changes on {changes.TableName}. Insert  {changes.Inserts}. Updates : {changes.Updates}. Deletes : {changes.Deletes}");
                    foreach (var dmRow in changes.View)
                    {
                        if (dmRow.RowState == DmRowState.Deleted)
                            Console.WriteLine($"[{dmRow.RowState}] {dmRow.ToString(DmRowVersion.Original)}");
                        else
                            Console.WriteLine($"[{dmRow.RowState}] {dmRow.ToString()} ");
                    }
                }

                break;
            case SyncStage.ApplyingInserts:
                Console.WriteLine($"----------------------------------------");
                Console.WriteLine($"{scopeName}. Applying Inserts");

                foreach (var apply in e.AppliedChanges.Where(ac => ac.State == DmRowState.Added))
                {
                    Console.WriteLine($"Apply Inserts on table {apply.TableName}. Success  {apply.ChangesApplied}. Failed : {apply.ChangesFailed}");
                    foreach(var dmRow in apply.View)
                    {
                        Console.WriteLine(dmRow.ToString());
                    }
                }

                break;
            case SyncStage.ApplyingDeletes:
                Console.WriteLine($"----------------------------------------");
                Console.WriteLine($"{scopeName}. Applying deletes");
                break;

            case SyncStage.ApplyingUpdates:
                Console.WriteLine($"----------------------------------------");
                Console.WriteLine($"{scopeName}. Applying updates");

                foreach (var apply in e.AppliedChanges.Where(ac => ac.State == DmRowState.Modified))
                {
                    Console.WriteLine($"Apply Updates on table {apply.TableName}. Success  {apply.ChangesApplied}. Failed : {apply.ChangesFailed}");
                    foreach (var dmRow in apply.View)
                    {
                        Console.WriteLine(dmRow.ToString());
                    }
                }
                break;
            case SyncStage.WritingScope:
                Console.WriteLine($"----------------------------------------");
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