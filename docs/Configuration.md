# SyncConfiguration

You can configure your synchronization with some parameters, available through the `SyncConfiguration` object :  

Here is a simple `SyncConfiguration` object :
``` cs
    SyncConfiguration configuration = new SyncConfiguration(new string[] { "Customer", "Address", "CustomerAddress" });
```

Let's see some options :  

`ConflictResolutionPolicy` handling the way **conflicts** are resolved. by default server is allways winning conflicts.
``` cs
    configuration.ConflictResolutionPolicy = ConflictResolutionPolicy.ServerWins; 
```
  
`DownloadBatchSizeInKB` is defining the memory limits. if you set batch size to 0, only one batch is sended and received.  
If you define a batch size, the sync process will be batched to limit memory consumption.  
``` cs
    configuration.DownloadBatchSizeInKB = 1000;
```
  
`BatchDirectory` is configuring the batch directory if batch size is specified. Default is set to Windows tmp folder.
``` cs
    configuration.BatchDirectory = "D://tmp";
```
  
`SerializationFormat` is defining the serialization format. Default is `Json`
``` cs
    configuration.SerializationFormat = SerializationFormat.Json;
```
  
If you're using the `SqlSyncProvider` you can use a great feature called **Table Value Parameter** and make bulk operations ! 
``` cs
    configuration.UseBulkOperations = true;
```
