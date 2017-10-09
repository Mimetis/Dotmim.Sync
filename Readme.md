# Warning : This is a work in progress !!!

If you want to contribute or test :  
* Code is a work in progress, no available Nuget packages at this time. 
* Code is a work in progress, I found bugs every days. No doubt you'll find a lot, too. Keep calm and open an issue :)
* Code is a work in progress, if you want to test / code / you need to install **Visual Studio 2017 Preview** to be able to target **.net standard 2.0**.

![](Assets/VS2017.png)

Go download a free version here : [Visual Studio 2017 Preview](https://www.visualstudio.com/vs/preview/)
# TL;DR

**DotMim.Sync** is a straightforward SDK for syncing relational databases. It's **.Net Standard 2.0**, available and ready for **IOT**, **Xamarin**, **.NET**, and so on :)  

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

## TL;DR: I Want to test !

If you don't have any databases ready for testing, use this one : [AdventureWorks leightweight script for SQL Server](/CreateAdventureWorks.sql)  

The script is ready to execute in SQL Server. It contains :
* A leightweight AdvenureWorks database, acting as the Server database (called **AdventureWorks**)
* An empty database, acting as the Client database (called **Client**)

Here are the simpliest steps to be able to make a simple sync : 

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


# Introduction

DotMim.Sync is a new API, based on .Net Standard 2.0, allowing you to synchronize any kind of relational datasources.

Multi Databases | Cross Plaform |  .Net Standard 2.0 
-------------|---------------------|--------------------
![](Assets/CrossPlatform.png) | ![](Assets/MultiOS.png) | ![](Assets/NetCore.png) 


Today supported databases are  :
* SQL Server
* SQLite
* MySql

Next availables providers (**NEED HELP**) :
* PostgreSQL
* Oracle

**I'm currently looking for a .net developer with skills on Oracle or PostgreSQL to create the corresponding providers**

The sync process is a **Master** - **Slave** model (and not a peer to peer model).

It could be represented like this :

![](Assets/Schema01.PNG)

**This version is not compatible with any others versions already existing**.

# Availabe features

## Adding configuration

You can configure your synchronization with some parameters, available through the `SyncConfiguration` object

    SqlSyncProvider serverProvider = new SqlSyncProvider(serverConfig);
    SqlSyncProvider clientProvider = new SqlSyncProvider(clientConfig);

    SyncConfiguration configuration = new SyncConfiguration(new string[] { "ServiceTickets" });

    // With a config when we are in local mode (no proxy)
    SyncConfiguration configuration = new SyncConfiguration(new string[] { "ServiceTickets" });

    // Configure the default resolution priority
    // Default : Server wins
    configuration.ConflictResolutionPolicy = ConflictResolutionPolicy.ServerWins;
    // Configure the batch size when memory is limited.
    // Default : 0. Batch is disabled
    configuration.DownloadBatchSizeInKB = 1000;
    // Configure the batch directory if batch size is specified
    // Default : Windows tmp folder
    configuration.BatchDirectory = "D://tmp";
    // configuration is stored in memory, you can disable this behavior
    // Default : false
    configuration.OverwriteConfiguration = true;
    // Configure the default serialization mode (Json or Binary)
    // Default : Json
    configuration.SerializationFormat = SerializationFormat.Json;
    // Configure the default model to Insert / Update / Delete rows (SQL Server use TVP to bulk insert)
    // Default true if SQL Server
    configuration.UseBulkOperations = true;


    // Sync agent, running on client side
    SyncAgent agent = new SyncAgent(clientProvider, serverProvider, configuration);
    
    var session = await agent.SynchronizeAsync();

## Adding progress

You can follow the sync progression through the `SyncPogress` event :

    agent.SyncProgress += SyncProgress;

    private static void SyncProgress(object sender, SyncProgressEventArgs e)
    {
        switch (e.Context.SyncStage)
        {
            case SyncStage.BeginSession:
                Console.WriteLine($"Begin Session.");
                break;
            case SyncStage.EndSession:
                Console.WriteLine($"End Session.");
                break;
            case SyncStage.EnsureMetadata:
                if (e.Configuration != null)
                    Console.WriteLine($"Configuration readed. {e.Configuration.ScopeSet.Tables.Count} table(s) involved.");
                if (e.DatabaseScript != null)
                    Console.WriteLine($"Database is created");
                break;
            case SyncStage.SelectedChanges:
                Console.WriteLine($"Selected changes : {e.ChangesStatistics.TotalSelectedChanges}");
                break;
            case SyncStage.AppliedChanges:
                Console.WriteLine($"Applied changes : {e.ChangesStatistics.TotalAppliedChanges}");
                break;
            case SyncStage.WriteMetadata:
                if (e.Scopes != null)
                    e.Scopes.ForEach(sc => Console.WriteLine($"\t{sc.Id} synced at {sc.LastSync}. "));
                break;
            case SyncStage.CleanupMetadata:
                Console.WriteLine($"CleanupMetadata");
                break;
        }
    }

## Handling a conflict

On the server side, you can handle a conflict. Just subscribe on the `ApplyChangedFailed` event and choose the correct version.  

You will determinate the correct version through the `ApplyAction` object :

* `ApplyAction.RetryWithForce` : The client row will be applied on server, even if there is a conflict, so the client row wins.
* `ApplyAction.Continue` : The client row won't be applied on the server, so the server row wins.


        agent.ApplyChangedFailed += ApplyChangedFailed;

        static void ApplyChangedFailed(object sender, ApplyChangeFailedEventArgs e)
        {
            // tables name
            string serverTableName = e.Conflict.RemoteChanges.TableName;
            string clientTableName = e.Conflict.LocalChanges.TableName;

            // server row in conflict
            var dmRowServer = e.Conflict.RemoteChanges.Rows[0];
            var dmRowClient = e.Conflict.LocalChanges.Rows[0];

            // Example 1 : Resolution based on rows values
            if ((int)dmRowServer["ClientID"] == 100 && (int)dmRowClient["ClientId"] == 0)
                e.Action = ApplyAction.Continue;
            else
                e.Action = ApplyAction.RetryWithForceWrite;

            // Example 2 : resolution based on conflict type
            // Line exist on client, not on server, force to create it
            //if (e.Conflict.Type == ConflictType.RemoteInsertLocalNoRow || e.Conflict.Type == ConflictType.RemoteUpdateLocalNoRow)
            //    e.Action = ApplyAction.RetryWithForceWrite;
            //else
            //    e.Action = ApplyAction.RetryWithForceWrite;
        }

## Using Asp.net Core 

Obviously, you won't have a direct TCP link to your local and remote servers.  
That's why we should use a proxy, through an exposed webapi.  
Create a simple **.Net core 2.0 Web API** and add both **Dotmim.Sync.Core** and **Dotmim.Sync.SqlServerProvider**.  

Then, register the Sync provider in the `Startup` class, thanks to Dependency Injection :

        public void ConfigureServices(IServiceCollection services)
        {
            services.AddMvc();

            var connectionString = Configuration["Data:ConnectionString"];
            services.AddSyncServer<SqlSyncProvider>(connectionString, configuration =>
            {
                configuration.Tables = new string[] { "ServiceTickets" };
            });
        }

Open you controller, inject a ``. Use it in the Post method, and that's all 

    [Route("api/[controller]")]
    public class ValuesController : Controller
    {
        // GET api/values
        [HttpGet]
        public IEnumerable<string> Get()
        {
            return new string[] { "value1", "value2" };
        }
      
        // proxy to handle requests and send them to SqlSyncProvider
        private WebProxyServerProvider webProxyServer;

        // Injected thanks to Dependency Injection
        public ValuesController(WebProxyServerProvider proxy)
        {
            webProxyServer = proxy;
        }

        // Handle all requests :)
        [HttpPost]
        public async Task Post()
        {
            await webProxyServer.HandleRequestAsync(this.HttpContext);
        }
    }

Your Client side is pretty similar, except we will use a proxy as well to be able to send all the requests to our Web API :


        var clientProvider = new SqlSyncProvider("[ConnectionString]");
        var proxyClientProvider = new WebProxyClientProvider(new Uri("http://localhost:56782/api/values"));

        var agent = new SyncAgent(clientProvider, proxyClientProvider);

        Console.WriteLine("Press a key to start...(Wait for you Web API is ready) ");
        Console.ReadKey();
   
        Console.Clear();
        Console.WriteLine("Sync Start");
            var s = await agent.SynchronizeAsync();



# TO DO 

* Adding Oracle, PostgreSQL and MySql providers
* Finding issues

Seb
