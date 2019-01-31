# SyncConfiguration

You can configure your synchronization with some parameters, available through the `SyncConfiguration` and `SyncOptions` objects :  

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

The main difference between these two objects are:
* `SyncConfiguration` is shared between server and client. Each property **must be exactly the same** on client and server 
    * For instance `SerializationFormat` can't be `Json` on server and `Binary` on client
* `SyncOptions` can be customized on server and on client, with their own different values.
    * For instance, we can have a different value for the the `BatchDirectory` (representing the tmp directory when batch is enabled) on server and on client.

If nothing is supplied, a default `SyncOptions` is created with default values.


Here is a simple `SyncConfiguration` object, defining tables to be synchronized (if you're not using the `SyncAgent` constructor) :
``` cs
    SyncConfiguration configuration = new SyncConfiguration(new string[] { "Customer", "Address", "CustomerAddress" });
```

Let's see some options :  

- `ConflictResolutionPolicy` handling the way **conflicts** are resolved. by default server is allways winning conflicts.
- `SerializationFormat` is defining the serialization format. Default is `Json`

``` cs
    configuration.ConflictResolutionPolicy = ConflictResolutionPolicy.ServerWins; 
    configuration.SerializationFormat = SerializationFormat.Json;
```

