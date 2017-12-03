# ASP.NET Core 2.0 Web Proxy

Obviously, you won't have a direct TCP link to your local and remote servers.  
That's why we should use a proxy, through an exposed webapi.  

**Don't forget to add the Dotmim.Sync Web Proxy nuget package** : [https://www.nuget.org/packages/Dotmim.Sync.Web]()

Then create a simple **.Net core 2.0 Web API** and add both **Dotmim.Sync.Core** and **Dotmim.Sync.SqlServerProvider**.  

Register the Sync provider in the `Startup` class, thanks to Dependency Injection :

``` cs
public void ConfigureServices(IServiceCollection services)
{
    services.AddMvc();

    var connectionString = Configuration["Data:ConnectionString"];
    services.AddSyncServer<SqlSyncProvider>(connectionString, configuration =>
    {
        configuration.Tables = new string[] { "ServiceTickets" };
    });
}
```

Open you controller, inject a `WebProxyServerProvider `.   
Use it in the Post method, call the `HandleRequestAsync` method and ... **that's all** !

``` cs
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
```

Your Client side is pretty similar, except we will use a proxy as well to be able to send all the requests to our Web API :

``` cs
var clientProvider = new SqlSyncProvider(ConnectionString);
var proxyClientProvider = new WebProxyClientProvider(new Uri("http://localhost:56782/api/values"));

var agent = new SyncAgent(clientProvider, proxyClientProvider);

Console.WriteLine("Press a key to start...(Wait for you Web API is ready) ");
Console.ReadKey();

Console.Clear();
Console.WriteLine("Sync Start");
var s = await agent.SynchronizeAsync();
```
