![DMS](Assets/Icon.png)

[![Build Status](https://dev.azure.com/dotmim/Dotmim.Sync/_apis/build/status/Dotmim.Sync%20Full%20Tests)](https://dev.azure.com/dotmim/Dotmim.Sync/_build/latest?definitionId=5)

See the Azure Devops CI : [https://dev.azure.com/dotmim/Dotmim.Sync](https://dev.azure.com/dotmim/Dotmim.Sync)

## Sources
**Release and pre-release** are hosted on [nuget.org](https://www.nuget.org) : [https://www.nuget.org/packages?q=dotmim.sync](https://www.nuget.org/packages?q=dotmim.sync)

## Dotmim.Sync

**DotMim.Sync** (**DMS**) is a straightforward framework for syncing relational databases, developed on top of **.Net Standard 2.0**, available and ready to use within  **IOT**, **Xamarin**, **.NET**, **UWP** and so on :)  

**The full documentation** is available here : [https://mimetis.github.io/Dotmim.Sync/](https://mimetis.github.io/Dotmim.Sync/)  

Multi Databases | Cross Plaform |  .Net Standard 2.0 
-------------|---------------------|--------------------
![](Assets/CrossPlatform.png) | ![](Assets/MultiOS.png) | ![](Assets/NetCore.png) 


## TL;DR;

Here is the easiest way to create a first sync, from scratch : 

* Create a **.Net Standard 2.0** compatible project, like a **.Net Core 2.0 / 3.1** or **.Net Fx 4.8** console application.  
* Add the nugets packages [DotMim.Sync.SqlServer](https://www.nuget.org/packages/Dotmim.Sync.SqlServer/) and [DotMim.Sync.Sqlite](https://www.nuget.org/packages/Dotmim.Sync.Sqlite/) 
* If you don't have any hub database for testing purpose, use this one : [AdventureWorks lightweight script for SQL Server](/CreateAdventureWorks.sql)  
* If you want to test **MySql**, use this script :  [AdventureWorks lightweight script for MySQL Server](/CreateMySqlAdventureWorks.sql)  
* Add this code :   

``` csharp
// Sql Server provider, the "server" or "hub".
SqlSyncProvider serverProvider = new SqlSyncProvider(
    @"Data Source=.;Initial Catalog=AdventureWorks;Integrated Security=true;");

// Sqlite Client provider acting as the "client"
SqliteSyncProvider clientProvider = new SqliteSyncProvider("advworks.db");

// Tables involved in the sync process:
var tables = new string[] {"ProductCategory", "ProductDescription", "ProductModel", "Product", "ProductModelProductDescription",
                           "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail" };

// Sync agent
SyncAgent agent = new SyncAgent(clientProvider, serverProvider, tables);

do
{
    var result = await agent.SynchronizeAsync();
    Console.WriteLine(result);

} while (Console.ReadKey().Key != ConsoleKey.Escape);
```

And here is the result you should have, after a few seconds:

``` cmd
Synchronization done.
        Total changes  uploaded: 0
        Total changes  downloaded: 2752
        Total changes  applied: 2752
        Total resolved conflicts: 0
        Total duration :0:0:3.776
```

You're done !

Now try to update a row in your client or server database, then hit enter again.   
You should see something like that:

``` cmd
Synchronization done.
        Total changes  uploaded: 0
        Total changes  downloaded: 1
        Total changes  applied: 1
        Total resolved conflicts: 0
        Total duration :0:0:0.045
```

Yes it's blazing fast !

## Need Help

* Check the full documentation, available here : [https://mimetis.github.io/Dotmim.Sync/](https://mimetis.github.io/Dotmim.Sync/)
* Feel free to ping me: [@sebpertus](http://www.twitter.com/sebpertus)
