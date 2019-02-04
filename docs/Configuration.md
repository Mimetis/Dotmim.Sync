# Configuration & Options

You can configure your synchronization model with some parameters, available through the `SyncConfiguration` and `SyncOptions` objects :  

What's the differences between `SyncConfiguration` and `SyncOptions` ?
- `SyncConfiguration` contains all the parameters **shared** between the server and all the clients.
-- These parameters are set by the **Server** and will override all **Clients** configuration.
- `SyncOptions` contains all the parameters **not shared** between the server and all the clients.

## SyncConfiguration

If we look at the `SyncConfiguration` object, we have some parameters that **must** be identical between server and client.   
For instance, the `SerializationFormat` property has to be the same. Obviously we can not have a different format between the server and the client! 

``` csharp
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

So how to set the `SyncConfiguration` object ?  

You can use your `SyncAgent`, if you're not in a *web proxy* mode.  




## SyncOptions
On the other side, `SyncOptions` can be customized on server and on client, with their own different values.   
For instance, we can have a different value for the the `BatchDirectory` (representing the tmp directory when batch is enabled) on server and on client.

``` csharp
  public class SyncOptions
    {
        public string BatchDirectory { get; set; }
        public int BatchSize { get; set; }
        public bool UseVerboseErrors { get; set; }
        public bool UseBulkOperations { get; set; } = true;
        public bool CleanMetadatas { get; set; } = true;
    }
```

If nothing is supplied, a default `SyncOptions` is created with default values.

Here is a simple `SyncConfiguration` object, defining tables to be synchronized (if you're not using the `SyncAgent` constructor) :
``` cs
    SyncConfiguration configuration = new SyncConfiguration(new string[] { "Customer", "Address", "CustomerAddress" });
```

## How to set Sync
