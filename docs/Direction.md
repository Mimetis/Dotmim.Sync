# Direction : Bidirectional, DownloadOnly, UploadOnly

You can set a synchronization direction to each table.  
Use the `SyncDirection` enumeration for each table in the `SyncConfiguration` object.

`SyncDirection.Bidirectional` is the default value for all tables added in the `SyncConfiguration` object

``` cs
public enum SyncDirection
{
    Bidirectional = 1,
    DownloadOnly = 2,
    UploadOnly = 3
}
```

In this example, `Customer` `CustomerAddress` and `Address` are defined as `DownloadOnly` :

``` cs
var serverConfig = @"Data Source=.;Initial Catalog=DB;Integrated Security=true;";
SqlSyncProvider serverProvider = new SqlSyncProvider(serverConfig);

var clientConfig = @"Data Source=.;Initial Catalog=DB1;Integrated Security=true;";
SqlSyncProvider clientProvider = new SqlSyncProvider(clientConfig);

var tables = new string[] { "SalesLT.ProductCategory", "SalesLT.ProductModel", "SalesLT.Product",
                                "SalesLT.Address", "SalesLT.Customer", "SalesLT.CustomerAddress"};

SyncAgent agent = new SyncAgent(clientProvider, serverProvider, tables);

agent.Configuration["Address"].SyncDirection = SyncDirection.DownloadOnly;
agent.Configuration["Customer"].SyncDirection = SyncDirection.DownloadOnly;
agent.Configuration["CustomerAddress"].SyncDirection = SyncDirection.DownloadOnly;

var s = await agent.SynchronizeAsync();

```
 