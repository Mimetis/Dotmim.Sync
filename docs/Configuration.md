# Configuration & Options

You can configure your synchronization model with some parameters, available through the `SyncConfiguration` and `SyncOptions` objects :

What's the differences between `SyncConfiguration` and `SyncOptions` ?

- `SyncConfiguration` contains all the parameters **shared** between the server and all the clients.
  -- These parameters are set by the **Server** and will override all **Clients** configuration.
- `SyncOptions` contains all the parameters **not shared** between the server and all the clients.

## SyncConfiguration

If we look at the `SyncConfiguration` object, we have some parameters that **must** be identical between server and client.  
For instance, the `SerializationFormat` property has to be the same. Obviously we can not have a different format between the server and the client!

```csharp
public class SyncConfiguration
    {
        public ConflictResolutionPolicy ConflictResolutionPolicy { get; set; }
        public DmSet Schema{ get; set; }
        public SerializationFormat SerializationFormat { get; set; }
        public string ScopeName { get; set; }
        public ICollection<FilterClause> Filters { get; set; }
        public string StoredProceduresPrefix { get; set; }
        public string StoredProceduresSuffix { get; set; }
        public string TriggersPrefix { get; set; }
        public string TriggersSuffix { get; set; }
        public string TrackingTablesPrefix { get; set; }
        public string TrackingTablesSuffix { get; set; }
        public string ScopeInfoTableName { get; set; }
}
```

Because the `SyncConfiguration` object is shared, and will be modified by some of the `Dotmim.Sync` core objects, the best way to set values is through an `Action<SyncConfiguration>` method:

```csharp
public void SetConfiguration(Action<SyncConfiguration> configuration)
```

### TCP mode

In a simple world, assuming both **Client** and **Server** are on the same network, we can provide the `SyncConfiguration` action through the `SyncAgent` instance:

```csharp
// Setting configuration options
agent.SetConfiguration(s =>
{
    s.ScopeInfoTableName = "tscopeinfo";
    s.SerializationFormat = Dotmim.Sync.Enumerations.SerializationFormat.Binary;
    s.StoredProceduresPrefix = "s";
    s.StoredProceduresSuffix = "";
    s.TrackingTablesPrefix = "t";
    s.TrackingTablesSuffix = "";
});
```

In this straightforward sample, we are setting some values to be able to modify the schema structure.     
Once your first sync is done, you should have something like that:

![SyncConfiguration 01](./assets/SyncConfiguration01.png)

### HTTP mode

In a more realistic scenario, you will probably have a web proxy in front of your **Server** database.  
You must provide your configuration values on the server side, not on the client side, since the server side will always override the values from the client.

As we saw in the [Web](./Web) chapter, we are using the **ASP.NET Dependency injection** system to create our **Server** remote provider.  
It's the best place to setup your sync configuration:

```csharp
public void ConfigureServices(IServiceCollection services)
{
    services.AddMvc().SetCompatibilityVersion(CompatibilityVersion.Version_2_2);

    services.AddMemoryCache();

    var connectionString = Configuration.GetSection("ConnectionStrings")["DefaultConnection"];

    services.AddSyncServer<SqlSyncProvider>(connectionString,
        c =>
        {
            var tables = new string[] {"ProductCategory",
                    "ProductDescription", "ProductModel",
                    "Product", "ProductModelProductDescription",
                    "Address", "Customer", "CustomerAddress",
                    "SalesOrderHeader", "SalesOrderDetail" };
            c.Add(tables);
            c.ScopeInfoTableName = "tscopeinfo";
            c.SerializationFormat = Dotmim.Sync.Enumerations.SerializationFormat.Binary;
            c.StoredProceduresPrefix = "s";
            c.StoredProceduresSuffix = "";
            c.TrackingTablesPrefix = "t";
            c.TrackingTablesSuffix = "";


        });
}
```

## SyncOptions

On the other side, `SyncOptions` can be customized on server and on client, with their own different values.  
For instance, we can have a different value for the the `BatchDirectory` (representing the tmp directory when batch is enabled) on server and on client.

```csharp
public class SyncOptions
{
    public string BatchDirectory { get; set; }
    public int BatchSize { get; set; }
    public bool UseVerboseErrors { get; set; }
    public bool UseBulkOperations { get; set; } = true;
    public bool CleanMetadatas { get; set; } = true;
}
```

> If nothing is supplied, a default `SyncOptions` is created with default values.

`SyncOptions` has some useful methods, you can rely on:

``` csharp
/// <summary>
/// Get the default Batch directory full path ([User Temp Path]/[DotmimSync])
/// </summary>
public static string GetDefaultUserBatchDiretory()

/// <summary>
/// Get the default user tmp folder
/// </summary>
public static string GetDefaultUserTempPath()

/// <summary>
/// Get the default sync tmp folder name (usually 'DotmimSync')
/// </summary>
public static string GetDefaultUserBatchDirectoryName()

```


### TCP mode
Like the `SyncConfiguration` values, we can supply options values, through an `Action<SyncOptions>`: 

```csharp
agent.SetOptions(opt =>
{
    opt.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "sync");
    opt.BatchSize = 100;
    opt.CleanMetadatas = true;
    opt.UseBulkOperations = true;
    opt.UseVerboseErrors = false;
});

```

### HTTP mode.

Since the options values are not shared, you can set `SyncOptions` values on both client and server.   
The **Client** side still remains the same as the *TCP* mode, since the `SyncAgent` is always bind to the **Client** provider:

```csharp
agent.SetOptions(opt =>
{
    opt.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "client");
    opt.BatchSize = 100;
});

```

Once again, on the **Server** side, use the `ConfigureServices` method to inject your options values:
``` csharp
public void ConfigureServices(IServiceCollection services)
{
services.AddMvc().SetCompatibilityVersion(CompatibilityVersion.Version_2_2);

services.AddMemoryCache();

var connectionString = Configuration.GetSection("ConnectionStrings")["DefaultConnection"];

services.AddSyncServer<SqlSyncProvider>(connectionString,
    c =>
    {
        // ....

    }, options =>
    {
        options.BatchDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "server");
        options.BatchSize = 1000;
    });
}
```







