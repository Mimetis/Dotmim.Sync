## Introduction

**Dotmim.Sync** is the easiest way to handle synchronization between one master database and multiples slaves databases.  
**Dotmim.Sync** is cross-platforms, multi-databases and based on **.Net Standard 2.0**.   
Choose either **SQL Server**, **SQLite**, **MySQL**, and (I hope soon...) Oracle and PostgreSQL !


## A few lines of codes

TL,DR : Here is the most straightforward way to synchronize two relational databases:

``` cs
// Sql Server provider, the master.
SqlSyncProvider serverProvider = new SqlSyncProvider(
    @"Data Source=.;Initial Catalog=AdventureWorks;Integrated Security=true;");

// Sqlite Client provider for a Sql Server <=> Sqlite sync
SQLiteSyncProvider clientProvider = new SQLiteSyncProvider("advworks.db");

// Tables to be synced
var tables = new string[] {"ProductCategory", "Product", "Address", "Customer", "CustomerAddress" };

// Sync agent, the orchestrator
SyncAgent agent = new SyncAgent(clientProvider, serverProvider, tables);

do
{
    var s = await agent.SynchronizeAsync();
    
    Console.WriteLine($"Total Changes downloaded : {s.TotalChangesDownloaded}");

} while (Console.ReadKey().Key != ConsoleKey.Escape);
```

## Nuget packages

All packages are available through **nuget.org**:

* **DotMim.Sync.Core** : [https://www.nuget.org/packages/Dotmim.Sync.Core/]() : This package is used by all providers. No need to reference it (it will be added by the providers)
* **DotMim.Sync.SqlServer** : [https://www.nuget.org/packages/Dotmim.Sync.SqlServer/]() : This package is the Sql Server package. Use it if you want to synchronize Sql Server databases.
* **DotMim.Sync.Sqlite** : [https://www.nuget.org/packages/Dotmim.Sync.Sqlite/]() : This package is the SQLite package. Be careful, SQLite is allowed only as a client provider (no SQLite Sync Server provider right now )
* **DotMim.Sync.MySql** : [https://www.nuget.org/packages/Dotmim.Sync.MySql/]() : This package is the MySql package. Use it if you want to synchronize MySql databases.
* **DotMim.Sync.Web** : [https://www.nuget.org/packages/Dotmim.Sync.Web/]() : This package allow you to make a sync process using a web server beetween your server and your clients. Use this package with the corresponding Server provider (SQL, MySQL, SQLite).

## Need Help

Feel free to ping me: [@sebpertus](http://www.twitter.com/sebpertus)
