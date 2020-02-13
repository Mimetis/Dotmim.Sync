# ASP.NET Core 2.0 Web Proxy

In a more realistic world, you will not have *always* a direct TCP link to your local and remote servers.  
That's why we should use a web proxy, and expose our server through a web api.   

To be able to *proxyfy* everything, you will have to
* Create a new **ASP.NET Core Web application**. (Don't forget to add the Web API stuff)
* Add the `Dotmim.Sync.Web.Server` nuget package on your ASP.NET project: [https://www.nuget.org/packages/Dotmim.Sync.Web.Server]()
    * Add your server provider, like `Dotmim.Sync.SqlServerProvider` for example.
* Add the `Dotmim.Sync.Web.Client` nuget package on you client application: [https://www.nuget.org/packages/Dotmim.Sync.Web.Client]() 

You will find a sample in `/Samples/Dotmim.Sync.SampleWebServer` folder, in the Github repository


## Server side

Once your **ASP.NET** application is created and you have added the `DotMim.Sync` packages to you project, register the Sync provider in the `Startup` class, thanks to Dependency Injection.

> *Note*: We are using `SyncOptions` and `SyncSetup` in this example, but these objects are optional. Use them only if needed 

``` cs
public void ConfigureServices(IServiceCollection services)
{
    services.AddMvc().SetCompatibilityVersion(CompatibilityVersion.Version_2_2);

    // [Required]: Handling multiple sessions
    services.AddMemoryCache();

    // [Required]: Get a connection string to your server data source
    var connectionString = Configuration.GetSection("ConnectionStrings")["DefaultConnection"];

    // [Optional]: Web server Options. Batching directory and Snapshots directory
    var options = new WebServerOptions()
    {
        BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "server"),
        SnapshotsDirectory = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), "Snapshots")
    };

    // [Required]: Tables list involved in the sync process
    var tables = new string[] {"ProductCategory",
                     "ProductDescription", "ProductModel",
                     "Product", "ProductModelProductDescription",
                     "Address", "Customer", "CustomerAddress",
                     "SalesOrderHeader", "SalesOrderDetail" };

    // [Optional]: database setup, for objects naming conventions
    var setup = new SyncSetup(tables)
    {
        // optional :
        StoredProceduresPrefix = "s",
        StoredProceduresSuffix = "",
        TrackingTablesPrefix = "s",
        TrackingTablesSuffix = ""
    };

    // [Required]: Add a SqlSyncProvider acting as the server hub.
    // If you don't specify a SyncSetup object, just add the tables array.
    services.AddSyncServer<SqlSyncProvider>(connectionString, setup, options);
}
```

> We have added a memory cache, through `services.AddMemoryCache();`. Having a cache is mandatory to be able to serve multiple requests. 

Once you have correctly configured your service, you can create your controller:

* Create a new controller (in my sample, called `SyncController`)
* In your newly created controller, inject a `WebProxyServerProvider`.   
* Use it in the Post method, call the `HandleRequestAsync` method and ... **that's all** !

``` cs
[Route("api/[controller]")]
[ApiController]
public class SyncController : ControllerBase
{
    private WebProxyServerOrchestrator webProxyServer;

    // Injected thanks to Dependency Injection
    public SyncController(WebProxyServerOrchestrator proxy) => this.webProxyServer = proxy;

    [HttpPost]
    public async Task Post()
    {
        // [Optional]: handling conflicts for table Region
        webProxyServer.WebServerOrchestrator.OnApplyChangesFailed(e =>
        {
            if (e.Conflict.RemoteRow.Table.TableName == "Region")
            {
                e.Resolution = ConflictResolution.MergeRow;
                e.FinalRow["RegionDescription"] = "Eastern alone !";
            }
            else
            {
                e.Resolution = ConflictResolution.ServerWins;
            }
        });

        //[Required]: Handling everything from an incoming request
        await webProxyServer.HandleRequestAsync(this.HttpContext);
    }
}

```

## Client side

Your Client side is pretty similar, except you will have to use a proxy as well to be able to send all the requests to your Web API :

``` cs
// [Required]: Defininf the local provider
var clientProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString(clientDbName));

// [Required]: Replacing a classic remote orchestrator with a web proxy orchestrator that point on the web api
var proxyClientProvider = new WebClientOrchestrator("http://localhost:52288/api/Sync");

// [Optional]: Specifying some useful client options
var clientOptions = new SyncOptions
{
    ScopeInfoTableName = "client_scopeinfo",
    BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "sync_client"),
    BatchSize = 50,
    CleanMetadatas = true,
    UseBulkOperations = true,
    UseVerboseErrors = false,
};

// [Optional]: Specifying some useful databases options
var clientSetup = new SyncSetup
{
    StoredProceduresPrefix = "s",
    StoredProceduresSuffix = "",
    TrackingTablesPrefix = "t",
    TrackingTablesSuffix = "",
    TriggersPrefix = "",
    TriggersSuffix = "",
};

// [Required]: Create an agent to launch the sync process
var agent = new SyncAgent(clientProvider, proxyClientProvider, clientSetup, clientOptions);

// [Optional]: Get some progress event during the sync process
var progress = new SynchronousProgress<ProgressArgs>(pa => Console.WriteLine($"{pa.Context.SessionId} - {pa.Context.SyncStage}\t {pa.Message}"));

// [Required]: Launch the sync.
var s = await agent.SynchronizeAsync(progress);

Console.WriteLine(s);

```
