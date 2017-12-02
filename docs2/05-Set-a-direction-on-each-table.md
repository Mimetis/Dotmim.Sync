# SyncDirection : Bidirectional, DownloadOnly, UploadOnly

You can set a synchronization direction to each table.  
Use the `SyncDirection` enumeration for each table in the `SyncConfiguration` object.

`SyncDirection.Bidirectional` is the default value for all tables added in the `SyncConfiguration` object

```
public enum SyncDirection
{
    /// <summary>
    /// Table will be sync from server to client and from client to server
    /// </summary>
    Bidirectional = 1,

    /// <summary>
    /// Table will be sync from server to client only.
    /// All changes occured client won't be uploaded to server
    /// </summary>
    DownloadOnly = 2,

    /// <summary>
    /// Table will be sync from client to server only
    /// All changes from server won't be downloaded to client
    /// </summary>
    UploadOnly = 3
}
```

In this example, `Customer` `CustomerAddress` and `Address` are defined as `DownloadOnly` :

```
var serverConfig = @"Data Source=(localdb)\MSSQLLocalDB; Initial Catalog=AdventureWorksLT2012;Integrated Security=true;";
SqlSyncProvider serverProvider = new SqlSyncProvider(serverConfig);

var clientConfig = @"Data Source=(localdb)\MSSQLLocalDB; Initial Catalog=AdvClientTest;Integrated Security=true;";
SqlSyncProvider clientProvider = new SqlSyncProvider(clientConfig);

var tables = new string[] { "SalesLT.ProductCategory", "SalesLT.ProductModel", "SalesLT.Product",
                                "SalesLT.Address", "SalesLT.Customer", "SalesLT.CustomerAddress"};

SyncAgent agent = new SyncAgent(clientProvider, serverProvider, tables);

agent.Configuration["Address"].SyncDirection = SyncDirection.DownloadOnly;
agent.Configuration["Customer"].SyncDirection = SyncDirection.DownloadOnly;
agent.Configuration["CustomerAddress"].SyncDirection = SyncDirection.DownloadOnly;

var s = await agent.SynchronizeAsync();

```
 