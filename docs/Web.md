# ASP.NET Core 2.0 Web Proxy

In a more realistic world, you will not have *always* a direct TCP link to your local and remote servers.  
That's why we should use a web proxy, and expose our server through a web api.   

To be able to *proxyfy* everything, you will have to
* Create a new **ASP.NET Core Web application**. (Don't forget to add the Web API stuff)
* Add the `Dotmim.Sync.Web.Server` nuget package on your ASP.NET project: [https://www.nuget.org/packages/Dotmim.Sync.Web.Server]()
    * Add your server provider, like `Dotmim.Sync.SqlServerProvider` for example.
* Add the `Dotmim.Sync.Web.Client` nuget package on you client application: [https://www.nuget.org/packages/Dotmim.Sync.Web.Client]() 


## Server side

Once your **ASP.NET** application is created and you have added the `DotMim.Sync` packages to you project, register the Sync provider in the `Startup` class, thanks to Dependency Injection :

``` cs
public void ConfigureServices(IServiceCollection services)
{
    services.AddMvc().SetCompatibilityVersion(CompatibilityVersion.Version_2_2);

    // Mandatory to be able to handle multiple sessions
    services.AddMemoryCache();

    // Get a connection string for your server data source
    var connectionString = Configuration.GetSection("ConnectionStrings")["DefaultConnection"];

    // Set the web server Options
    var options = new WebServerOptions()
    {
        BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "server"),
    };

    // Create the setup used for your sync process
    var tables = new string[] {"ProductCategory",
                    "ProductDescription", "ProductModel",
                    "Product", "ProductModelProductDescription",
                    "Address", "Customer", "CustomerAddress",
                    "SalesOrderHeader", "SalesOrderDetail" };

    var setup = new SyncSetup(tables)
    {
        // optional :
        StoredProceduresPrefix = "s",
        StoredProceduresSuffix = "",
        TrackingTablesPrefix = "t",
        TrackingTablesSuffix = ""
    };

    // add a SqlSyncProvider acting as the server hub
    services.AddSyncServer<SqlSyncProvider>(connectionString, setup, options);
}
```

> We have added a memory cache, through `services.AddMemoryCache();`. having a cache is mandatory to be able to serve multiple requests. Don't forget to provider a memory cache system.

Once you have correctly configured your service, you can create your controller:

* Create a new controller (in my sample, called `SyncController`)
* In your newly created controller, inject a `WebProxyServerProvider`.   
* Use it in the Post method, call the `HandleRequestAsync` method and ... **that's all** !

``` cs
[Route("api/[controller]")]
[ApiController]
public class SyncController : ControllerBase
{
    private WebProxyServerProvider webProxyServer;

    // Injected thanks to Dependency Injection
    public SyncController(WebProxyServerProvider proxy)
    {
        webProxyServer = proxy;
    }

    [HttpPost]
    public async Task Post()
    {
        await webProxyServer.HandleRequestAsync(this.HttpContext);
    }
}
```

## Client side

Your Client side is pretty similar, except you will have to use a proxy as well to be able to send all the requests to your Web API :

``` cs
var clientProvider = new SqlSyncProvider(ConnectionString);
var proxyClientProvider = new WebProxyClientProvider(new Uri("http://localhost:56782/api/sync"));

var agent = new SyncAgent(clientProvider, proxyClientProvider);

Console.WriteLine("Press a key to start...(Wait for you Web API is ready) ");
Console.ReadKey();

Console.Clear();
Console.WriteLine("Sync Start");
var s = await agent.SynchronizeAsync();
```
