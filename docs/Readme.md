# Warning : This is a work in progress !!!

If you want to contribute or test :  
* Code is a work in progress, no available Nuget packages at this time. 
* Code is a work in progress, I found bugs every days. No doubt you'll find a lot, too. Keep calm and open an issue :)
* Code is a work in progress, if you want to test / code / you need to install **Visual Studio 2017 Preview** to be able to target **.net standard 2.0**.

![](Assets/VS2017.png)

Go download a free version here : [Visual Studio 2017 Preview](https://www.visualstudio.com/vs/preview/)
# TL;DR

**DotMim.Sync** is a straightforward SDK for syncing relational databases.  
It's **.Net Standard 2.0**, available and ready for **IOT**, **Xamarin**, **.NET**, and so on :)  

Multi Databases | Cross Plaform |  .Net Standard 2.0 
-------------|---------------------|--------------------
![](Assets/CrossPlatform.png) | ![](Assets/MultiOS.png) | ![](Assets/NetCore.png) 

It's based on a master slaves architecture :  
* One provider, as the master, for the server side.
* One or more provider(s) for the client(s) as slave(s).
* One sync agent object `SyncAgent` to handle the sync process.

Here are the nuget packages :

* **DotMim.Sync.Core** : [https://www.nuget.org/packages/Dotmim.Sync.Core/]() : This package is used by all providers. No need to reference it (it will be added by the providers)
* **DotMim.Sync.SqlServer** : [https://www.nuget.org/packages/Dotmim.Sync.SqlServer/]() : This package is the Sql Server package. Use it if you want to synchronize Sql Server databases.
* **DotMim.Sync.Sqlite** : [https://www.nuget.org/packages/Dotmim.Sync.Sqlite/]() : This package is the SQLite package. Be careful, SQLite is allowed only as a client provider (no SQLite Sync Server provider right now )
* **DotMim.Sync.MySql** : [https://www.nuget.org/packages/Dotmim.Sync.MySql/]() : This package is the MySql package. Use it if you want to synchronize MySql databases.
* **DotMim.Sync.Web** : [https://www.nuget.org/packages/Dotmim.Sync.Web/]() : This package allow you to make a sync process using a web server beetween your server and your clients. Use this package with the corresponding Server provider (SQL, MySQL, SQLite).


![](Assets/Schema01.PNG)

## TL;DR: I Want to test !

If you don't have any databases ready for testing, use this one : [AdventureWorks lightweight script for SQL Server](/CreateAdventureWorks.sql)  

The script is ready to execute in SQL Server. It contains :
* A lightweight AdvenureWorks database, acting as the Server database (called **AdventureWorks**)
* An empty database, acting as the Client database (called **Client**)

Here are the simplest steps to be able to make a simple sync : 

* Create a **.Net Core 2.0** or **.Net Fx 4.6** console application.  
* Add the nugets packages [DotMim.Sync.SqlServer](https://www.nuget.org/packages/Dotmim.Sync.SqlServer/) and [DotMim.Sync.Sqlite](https://www.nuget.org/packages/Dotmim.Sync.Sqlite/)  
* Add this code :   

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


# I want more !

All options and detailed can be found in the [Wiki](https://github.com/Mimetis/Dotmim.Sync/wiki/01-Introduction) !

Seb
