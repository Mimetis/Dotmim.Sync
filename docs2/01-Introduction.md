# The simplest way to synchronize two databases

Synchronizing multiples databases can be resumed like this :

1. Choose a server provider : Can be `SqlSyncProvider` or `MySqlSyncProvider` (and more to come ..)
2. Choose a client provider : Can be `SqlSyncProvider`, `SqliteSyncProvider` or `MySqlSyncProvider` (and more to come ..)
3. Choose all tables to be synchronized. A lot of options are available, but keep it simple for now :)  
4. Create a `SyncAgent` orchestrator to handle all the synchronization process
5. Launch the sync process with `SynchronizeAsync()` method.

Here is the simplest code to synchronize two databases :  

```
// Sql Server provider, the master.
SqlSyncProvider serverProvider = new SqlSyncProvider(@"Data Source=(localdb)\MSSQLLocalDB; Initial Catalog=AdventureWorks;Integrated Security=true;");

// Sqlite Client provider for a Sql Server <=> Sqlite sync
SQLiteSyncProvider clientProvider = new SQLiteSyncProvider("advworks.db");

// Tables to be synced
var tables = new string[] {"ErrorLog", "ProductCategory",
    "ProductDescription", "ProductModel",
    "Product", "ProductModelProductDescription",
    "Address", "Customer", "CustomerAddress",
    "SalesOrderHeader", "SalesOrderDetail" };

// Agent
SyncAgent agent = new SyncAgent(clientProvider, serverProvider, tables);

do
{
    var s = await agent.SynchronizeAsync();
    Console.WriteLine($"Total Changes downloaded : {s.TotalChangesDownloaded}");

} while (Console.ReadKey().Key != ConsoleKey.Escape);
```

