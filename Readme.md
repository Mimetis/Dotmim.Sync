[![Build Status](https://dev.azure.com/dotmim/Dotmim.Sync/_apis/build/status/Dotmim.Sync%20Full%20Tests)](https://dev.azure.com/dotmim/Dotmim.Sync/_build/latest?definitionId=5)

See the Azure Devops CI : [https://dev.azure.com/dotmim/Dotmim.Sync](https://dev.azure.com/dotmim/Dotmim.Sync)

## Sources
**Release version 0.2.1 and pre-release 0.3.0** are hosted on [nuget.org](https://www.nuget.org) : [https://www.nuget.org/packages?q=dotmim.sync](https://www.nuget.org/packages?q=dotmim.sync)

## Dotmim.Sync

**DotMim.Sync** is a straightforward SDK for syncing relational databases, developed on top of **.Net Standard 2.0**, available and ready to use within  **IOT**, **Xamarin**, **.NET**, **UWP** and so on :)  

**The full documentation** is available here : [https://mimetis.github.io/Dotmim.Sync/](https://mimetis.github.io/Dotmim.Sync/)  

Multi Databases | Cross Plaform |  .Net Standard 2.0 
-------------|---------------------|--------------------
![](Assets/CrossPlatform.png) | ![](Assets/MultiOS.png) | ![](Assets/NetCore.png) 


## How it works

Here are the easiest way to be able to make a simple sync : 

* Create a **.Net Core 2.0** or **.Net Fx 4.6** console application.  
* Add the nugets packages [DotMim.Sync.SqlServer](https://www.nuget.org/packages/Dotmim.Sync.SqlServer/) and [DotMim.Sync.Sqlite](https://www.nuget.org/packages/Dotmim.Sync.Sqlite/)  
* Add this code :   

``` csharp
// Sql Server provider, the master.
SqlSyncProvider serverProvider = new SqlSyncProvider(
    @"Data Source=.;Initial Catalog=AdventureWorks;Integrated Security=true;");

// Sqlite Client provider for a Sql Server <=> Sqlite sync
SQLiteSyncProvider clientProvider = new SQLiteSyncProvider("advworks.db");

// Tables involved in the sync process:
var tables = new string[] {"ProductCategory",
    "ProductDescription", "ProductModel",
    "Product", "ProductModelProductDescription",
    "Address", "Customer", "CustomerAddress",
    "SalesOrderHeader", "SalesOrderDetail" };

// Sync orchestrator
SyncAgent agent = new SyncAgent(clientProvider, serverProvider, tables);

do
{
    var s = await agent.SynchronizeAsync();
    Console.WriteLine($"Total Changes downloaded : {s.TotalChangesDownloaded}");

} while (Console.ReadKey().Key != ConsoleKey.Escape);
```

If you don't have any databases ready for testing, use this one : [AdventureWorks lightweight script for SQL Server](/CreateAdventureWorks.sql)  

The script is ready to execute in SQL Server. It contains :
* A lightweight AdvenureWorks database, acting as the Server database (called **AdventureWorks**)
* An empty database, acting as the Client database (called **Client**)

## Building from source

1) install VS 2017
2) install Microsoft SQL Server (localdb is sufficient)
3) install MySQL (run installMySql.ps1 which automates this using chocolatey)
4) open "SQLUtils.HelperDB" and modify the connection strings to suit your configuration

## Need Help

* Check the full documentation, available here : [https://mimetis.github.io/Dotmim.Sync/](https://mimetis.github.io/Dotmim.Sync/)
* Feel free to ping me: [@sebpertus](http://www.twitter.com/sebpertus)
