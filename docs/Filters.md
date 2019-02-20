# Filters

You can filter datas from any tables.  
In a nutshell, enabling filter is two steps :
1. Configuring tables and columns on the server side in the `SyncConfiguration` object.
2. Adding the correct paramater on each client in the `SyncAgent` orchestrator.

## Server side configuration

First of all, you have to add all the filtered **tables** and the corresponding **columns** in the `SyncConfiguration` object.
This step is required on the **server** side, to be able to generate all required **stored procédures**.  

In the `Filters` property, add a new filter, composed with **table** name and **column** name :  
### TCP mode

``` cs
// Add a filter
agent.SetConfiguration(c =>c.Filters.Add("Customer", "CustomerId"));

```

### HTTP mode

``` cs
private WebProxyServerProvider webProxyServer;

// Injected thanks to Dependency Injection
public SyncController(WebProxyServerProvider proxy)
{
    webProxyServer = proxy;
}

[HttpPost]
public async Task Post()
{
    // Get the underline local provider
    var provider = webProxyServer.GetLocalProvider(this.HttpContext);
    provider.SetConfiguration(c =>c.Filters.Add("Customer", "CustomerId"));
    await webProxyServer.HandleRequestAsync(this.HttpContext);
}

```

## Client side configuration

On each client, you will have to specify the **value** to provider fo filtering.  
On the `SyncAgent` just add the corresponding parameter, with the correct value :  

``` cs
SyncAgent agent = new SyncAgent(clientProvider, serverProvider, configuration);
agent.Parameters.Add("ServiceTickets", "CustomerID", 10);

var session = await agent.SynchronizeAsync();
```

On this particular client, only tickets when CustomerId=10 are synchronized. 
