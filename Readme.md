## Warning : This is a work in progress !!!

If you want to contribute or test :  
* Code is a work in progress, no available Nuget packages at this time. 
* Code is a work in progress, I found bugs evey days. No doubt you'll find a lot, too. Keep calm and open an issue :)
* Code is a work in progress, if you want to test / code / you need to install **Visual Studio 2017 Preview** to be able to target **.net standard 2.0**.

Go download a free version here : [Visual Studio 2017 Preview](https://www.visualstudio.com/vs/preview/)

## Introduction

DotMim.Sync is a new API, based on .Net Standard 2.0, allowing you to synchronize any kind of relational datasources.

Multi Databases | Cross Plaform |  .Net Standard 2.0 
-------------|---------------------|--------------------
![](Assets/CrossPlatform.png) | ![](Assets/MultiOS.png) | ![](Assets/NetCore.png) 


Today supported databases are  :
* SQL Server
* Azure SQL Database
* SQL Server on Linux
* SQLite

Next availables providers :
* MySql
* PostgreSQL
* Oracle

**I currently looking for a .net developer with skills on Oracle or MySql or PostgreSQL to create the corresponding providers**

The sync process is a Master - Slave model (and not a peer to peer model).

It could be represented like this :

![](Assets/Schema01.PNG)

**This version is not compatible with any others versions already existing**.


## How it works, in a nutshell

Keep it simple!  

* One provider for the server side
* One provider for the client side
* One sync agent object to handle the sync process

### Console sample


    SqlSyncProvider serverProvider = new SqlSyncProvider(serverConfig);
    SqlSyncProvider clientProvider = new SqlSyncProvider(clientConfig);

    // Sync agent, running on client side
    SyncAgent agent = new SyncAgent(clientProvider, serverProvider, new string[] { "ServiceTickets" });
    
    var session = await agent.SynchronizeAsync();

### Adding configuration

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

### Handling a conflict

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

### Using Asp.net Core 

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


### Tests / Samples

You will find the unit tests and samples in /Tests and /Samples directories


## TO DO 


* Testing the SQLite provider
* Adding Oracle, PostgreSQL and MySql providers
* Finding issues

Seb