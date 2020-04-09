## Dotmim.Sync

![DMS](assets/Smallicon.png)

**Dotmim.Sync** (**DMS**) is the easiest way to handle a full **synchronization** between one server database and multiples clients databases.  
**Dotmim.Sync** (**DMS**) is cross-platforms, multi-databases and based on **.Net Standard 2.0**.   
Choose either **SQL Server**, **SQLite**, **MySQL**, and (hopefully, I hope soon...) Oracle and PostgreSQL !

For simplicity, we can say **Dotmim.Sync** or **DMS** framework.

No need to handle any configuration file, or any generation code or whatever. Just code the list of tables you need to synchronize, call `SynchronizeAsync()` and that's all !


## A few lines of codes

**TL,DR** : Here is the most straightforward way to synchronize two relational databases:

> If you don't have any databases ready for testing, use this one, for SQL Server : [AdventureWorks lightweight script for SQL Server](https://github.com/Mimetis/Dotmim.Sync/blob/master/CreateAdventureWorks.sql)   
> This one, if you want to try MySQL : [AdventureWorks lightweight script for MySQL](https://github.com/Mimetis/Dotmim.Sync/blob/master/CreateMySqlAdventureWorks.sql)
> The script is ready to execute in SQL Server (or MySQL Workbench). It contains :
> - A lightweight AdvenureWorks database, acting as the Server database (called AdventureWorks)
> - An empty database, acting as the Client database (called Client)



``` cs
// Create 2 Sql Sync providers
// First provider is using the Sql change tracking feature. Don't forget to enable it on your database until running this code !
// For instance, use this SQL statement on your server database : ALTER DATABASE AdventureWorks  SET CHANGE_TRACKING = ON  (CHANGE_RETENTION = 10 DAYS, AUTO_CLEANUP = ON)  
// Otherwise, if you don't want to use Change Tracking feature, just change [SqlSyncChangeTrackingProvider] to [SqlSyncProvider]
var serverProvider = new SqlSyncChangeTrackingProvider(serverConnectionString);

// IF you want to try with a MySql Database, use the [MySqlSyncProvider] instead
// var serverProvider = new MySqlSyncProvider(serverConnectionString);

// Second provider on the client side, is the [SqliteSyncProvider] used for SQLite databases, relying on triggers and tracking tables to create the sync environment
var clientProvider = new SqliteSyncProvider(clientConnectionString);


// Tables involved in the sync process:
var tables = new string[] {"ProductCategory", "ProductModel", "Product",
    "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail" };

// Creating an agent that will handle all the process
var agent = new SyncAgent(clientProvider, serverProvider, tables);

do
{
    // Launch the sync process
    var s1 = await agent.SynchronizeAsync();
    // Write results
    Console.WriteLine(s1);

} while (Console.ReadKey().Key != ConsoleKey.Escape);

Console.WriteLine("End");
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

It took almost **4 seconds** on my machine to make a full synchronization between the **Server** and the **Client**.  

## Nuget packages

You can add your database type provider, through nuget, from **Visual Studio**:  
![Nuget packages](/assets/Packages.png)


or through your command line, assuming you are developing with **Visual Studio Code**, **Rider** or **Notepad** :)
``` bash
# Adding the package required to synchronize a SQL Server database:
dotnet add package Dotmim.Sync.SqlServer
# Adding the package required to synchronize a SQL Server database, using the Change Tracking feature:
dotnet add package Dotmim.Sync.SqlServer.ChangeTracking
# Adding the package required to synchronize a MySQL database:
dotnet add package Dotmim.Sync.MySql
# Adding the package required to synchronize a SQLite database:
dotnet add package Dotmim.Sync.Sqlite
```

For instance, if you need to synchronize two MySql databases, the only package you need to install, on both Server and Client side, is `Dotmim.Sync.MySql`
On the other side, if you need to synchronize a SQL server database, with multiple SQLite client databases, install `Dotmim.Sync.SqlServer` (or `Dotmim.Sync.SqlServer.ChangeTracking`) on the server side, and install `Dotmim.Sync.Sqlite` on the client side.

> Note: the package `Dotmim.Sync.Core` is the core framework, and is used by all the providers. You don't have to explicitely add it to your projects, since it's always part of the provider you've just installed.

The last two packages available, `Dotmim.Sync.Web.Client` and `Dotmim.Sync.Web.Server` are packages used on a specific scenario, where you server database is not accessible directly, and is exposed with a web api.


## First overview

This first sample took almost **4 seconds** to make a *full* sync between a **Server** and a **Client**.

It's a little bit long, because, under the hood, the `Dotmim.Sync` framework, on the **first sync only**, will have to:
- Get the schema from the **Server** side and create all the tables on the **Client** side, if needed. (yes, you don't need a client database with an existing schema)
- Create on both side all the required stuff to be able to manage a full sync process, creating *tracking* tables, stored procedures, triggers and so on ... be careful, `Dotmim.Sync` is a little bit intrusive if you're not using the `SqlSyncChangeTrackingProvider` provider :)
- Then eventually launch the first sync, and get the **2752** items from the **Server**, and apply them on the **Client**.

Now everything is configured and the first sync is successfull.  

We can add **101** items in the `ProductCategory` table (on the server side, `Adventureworks`):

``` sql
Insert into ProductCategory (Name)
Select SUBSTRING(CONVERT(varchar(255), NEWID()), 0, 7)
Go 100
```
From the same console application (indeed, we have a `do while` loop), same code, just hit `enter` to relaunch the synchronization and see the results:

``` cmd
Synchronization done.
        Total changes  uploaded: 0
        Total changes  downloaded: 100
        Total changes  applied: 100
        Total resolved conflicts: 0
        Total duration :0:0:0.145
```

Boom, less than **150** milliseconds. 

## Hello Sync Sample

You will find the sample used for this demonstration, here : [HelloSync sample](https://github.com/Mimetis/Dotmim.Sync/tree/master/Samples/HelloSync)

You can see this sample as well, live, hosted on [dotnetfiddle](https://dotnetfiddle.net) : [Hello Sync On dotnetfiddle](https://dotnetfiddle.net/CZgNDm)

## Nuget packages

All packages are available through **nuget.org**:

* **Dotmim.Sync.Core** : [https://www.nuget.org/packages/Dotmim.Sync.Core/](https://www.nuget.org/packages/Dotmim.Sync.Core/) : This package is used by all providers. No need to reference it (it will be added by the providers)
* **Dotmim.Sync.SqlServer** : [https://www.nuget.org/packages/Dotmim.Sync.SqlServer/](https://www.nuget.org/packages/Dotmim.Sync.SqlServer/) : This package is the Sql Server package. Use it if you want to synchronize Sql Server databases.
* **Dotmim.Sync.SqlSyncChangeTrackingProvider** : [https://www.nuget.org/packages/Dotmim.Sync.SqlServer.ChangeTracking/](https://www.nuget.org/packages/Dotmim.Sync.SqlServer.ChangeTracking/) : This package is based on the Sql Server package, but will use the **Change Tracking** feature from SQL server, instead of classic tracking tables/ triggers/ stored procedures.
* **Dotmim.Sync.Sqlite** : [https://www.nuget.org/packages/Dotmim.Sync.Sqlite/](https://www.nuget.org/packages/Dotmim.Sync.Sqlite/) : This package is the SQLite package. Be careful, SQLite is allowed only as a client provider (no SQLite Sync Server provider right now )
* **Dotmim.Sync.MySql** : [https://www.nuget.org/packages/Dotmim.Sync.MySql/](https://www.nuget.org/packages/Dotmim.Sync.MySql/) : This package is the MySql package. Use it if you want to synchronize MySql databases.
* **Dotmim.Sync.Web.Server** : [https://www.nuget.org/packages/Dotmim.Sync.Web.Server/](https://www.nuget.org/packages/Dotmim.Sync.Web.Server/) : This package allow you to make a sync process over **HTTP** using a web server beetween your server and your clients. Use this package with the corresponding Server provider (SQL, MySQL, SQLite) on your server side. Since we are **.Net Standard 2.0** you can use it from an **ASP.NET Core** application or a classic **ASP.NET** application (Framework 4.7 +)
* **Dotmim.Sync.Web.Client** : [https://www.nuget.org/packages/Dotmim.Sync.Web.Client/](ttps://www.nuget.org/packages/Dotmim.Sync.Web.Client/) : This package has to be referenced on your client application, if you want to make a synchronization over **HTTP**.

## Need Help

Feel free to ping me: [@sebpertus](http://www.twitter.com/sebpertus)
