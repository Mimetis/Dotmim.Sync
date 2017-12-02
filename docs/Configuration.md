# SyncConfiguration

You can configure your synchronization with some parameters, available through the `SyncConfiguration` object :  

Here is a simple `SyncConfiguration` object :
```
    SyncConfiguration configuration = new SyncConfiguration(new string[] { "Customer", "Address", "CustomerAddress" });
```

Let's see some options :  

`ConflictResolutionPolicy` handling the way **conflicts** are resolved. by default server is allways winning conflicts.
```
    configuration.ConflictResolutionPolicy = ConflictResolutionPolicy.ServerWins; 
```
  
`DownloadBatchSizeInKB` is defining the memory limits. if you set batch size to 0, only one batch is sended and received.  
If you define a batch size, the sync process will be batched to limit memory consumption.  
```
    configuration.DownloadBatchSizeInKB = 1000;
```
  
`BatchDirectory` is configuring the batch directory if batch size is specified. Default is set to Windows tmp folder.
```
    configuration.BatchDirectory = "D://tmp";
```
  
`SerializationFormat` is defining the serialization format. Default is `Json`
```
    configuration.SerializationFormat = SerializationFormat.Json;
```
  
If you're using the `SqlSyncProvider` you can use a great feature called **Table Value Parameter** and make bulk operations ! 
```
    configuration.UseBulkOperations = true;
```
