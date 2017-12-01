## Introduction

**Dotmim.Sync** is the easiest way to handle synchronization between one master database and multiples slaves databases.  
**Dotmim.Sync** is cross-platforms, multi-databases and based on **.Net Standard 2.0**.   
Choose either **SQL Server**, **SQLite**, **MySQL**, and (I hope soon...) Oracle and PostgreSQL !


## Documentation

1. [Introduction to Domim.Sync](/01-Introduction)
2. [Get informations during the sync Progression](/docs/02-Sync-progression)
3. [Set some usefuls configuration options](/Mimetis/Dotmim.Sync/docs/03-Configuration-options)
4. [How to handle schemas with SQL Server](/Mimetis/Dotmim.Sync/docs/04-Handle-schemas-on-SQL-Server)
5. [Managing sync direction on each table : Bidirectional / UploadOnly / DownloadOnly](/Mimetis/Dotmim.Sync/docs/05-Set-a-direction-on-each-table)
6. [How to manually handle conflicts](/Mimetis/Dotmim.Sync/docs/06-Sync-conflict)
7. [Implementing a Sync process with ASP.NET Core 2.0](/Mimetis/Dotmim.Sync/docs/07-ASP.NET-Core-2.0-Web-Proxy)
8. [Adding a table filter](/Mimetis/Dotmim.Sync/docs/08-Filtering-tables)
8. [Using the Dotmim.Sync CLI](/Mimetis/Dotmim.Sync/docs/09-Using-Dotmim.Sync-CLI)

## A few lines of codes

TL,DR : Here is the most straightforward way to synchronize two relational databases:

```
// Sql Server provider, the master.
SqlSyncProvider serverProvider = new SqlSyncProvider(@"Data Source=(localdb)\MSSQLLocalDB; Initial Catalog=AdventureWorks;Integrated Security=true;");

// Sqlite Client provider for a Sql Server <=> Sqlite sync
SQLiteSyncProvider clientProvider = new SQLiteSyncProvider("advworks.db");

// Tables to be synced
var tables = new string[] {"ProductCategory", "ProductDescription", "ProductModel", "Product", "Address", "Customer", "CustomerAddress" };

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

Feel free to ping me: [@sebastienpertus](http://www.twitter.com/sebastienpertus)
