# How to Synchronize

You have one main method to launch a synchronization, with several optional parameters:

```csharp
SynchronizeAsync();
SynchronizeAsync(IProgress<ProgressArgs> progress);
SynchronizeAsync(CancellationToken cancellationToken);
SynchronizeAsync(SyncType syncType);
SynchronizeAsync(SyncType syncType, CancellationToken cancellationToken);
```

Obviously, you can use the `CancellationToken` object whenever you want to rollback an "*in progress*" synchronization.
And since we have an async synchronization, you can pass an `IProgress<ProgressArgs>` object to have feedback during the sync process

let's see now a straightforward sample illustrating the use of the `SyncType` argument.
You will find the sample used for this demonstration, here : [SyncType sample](/samples/SyncType)

```csharp
SqlSyncProvider serverProvider = new SqlSyncProvider(GetDatabaseConnectionString("AdventureWorks"));
SqlSyncProvider clientProvider = new SqlSyncProvider(GetDatabaseConnectionString("Client"));

SyncAgent agent = new SyncAgent(clientProvider, serverProvider, new string[] {
"ProductCategory", "ProductModel", "Product", "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail"});

var syncContext = await agent.SynchronizeAsync();

Console.WriteLine(syncContext);

```

Here is the result, after the **first initial** synchronization:

```
Synchronization done.
        Total changes  uploaded: 0
        Total changes  downloaded: 2752
        Total changes  applied: 2752
        Total resolved conflicts: 0
        Total duration :0:0:4.720
```

As you can see, the client has downloaded 2752 lines from the server.   
Obviously if we made a new sync, without making any changes neither on the server nor the client, the result will be :

```
Synchronization done.
        Total changes  uploaded: 0
        Total changes  downloaded: 0
        Total changes  applied: 0
        Total resolved conflicts: 0
        Total duration :0:0:0.382
```

Ok make sense !

## Using SyncType

The `SyncType` enumeration allows you to reinitialize a client database (already synchronized or not).  
For any reason, you could want to re-download the whole database from the server (bug, out of sync, and so on ...)

Here is the `SyncType` definition:
```csharp
public enum SyncType
{
    /// <summary>
    /// Normal synchronization
    /// </summary>
    Normal,
    /// <summary>
    /// Reinitialize the whole sync database, applying all rows from the server to the client
    /// </summary>
    Reinitialize,
    /// <summary>
    /// Reinitialize the whole sync database, applying all rows from the server to the client, 
    /// after tried a client upload
    /// </summary>
    ReinitializeWithUpload
}
```

* `SyncType.Normal`: Default value, represents a normal sync process
* `SyncType.Reinitialize`: Launches the sync process, marking the client to be resynchronized. Be careful, any changes on the client will be overwritten by this value !
* `SyncType.ReinitializeWithUpload`: Like *Reinitialize* this value will launch a process to resynchronize the whole client database, except that the client will *try* to send its local changes before making the resync process.

From the sample we saw before, here is the different behaviors with each `SyncType` enumeration value:  
First of all, for demo purpose, we are updating a row on the **client**:

``` sql
-- initial value is 'The Bike Store'
UPDATE Client.dbo.Customer SET CompanyName='The New Bike Store' WHERE CustomerId = 1 
```

### SyncType.Normal

```csharp
SqlSyncProvider serverProvider = new SqlSyncProvider(GetDatabaseConnectionString("AdventureWorks"));
SqlSyncProvider clientProvider = new SqlSyncProvider(GetDatabaseConnectionString("Client"));

SyncAgent agent = new SyncAgent(clientProvider, serverProvider, new string[] {
"ProductCategory", "ProductModel", "Product", "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail"});

var syncContext = await agent.SynchronizeAsync();

Console.WriteLine(syncContext);

```

```
Synchronization done.
        Total changes  uploaded: 1
        Total changes  downloaded: 0
        Total changes  applied: 0
        Total resolved conflicts: 0
        Total duration :0:0:1.382
```

The default behavior is what we could expect : Uploading the modified row to the server.

### SyncType.Reinitialize

The `SyncType.Reinitialize` mode will reinitialize the whole client database.
Every rows on the client will be deleted and downloaded againg from server, even if some of them are not synced correctly.
Use this mode with caution, since you could lost some rows.


```csharp
SqlSyncProvider serverProvider = new SqlSyncProvider(GetDatabaseConnectionString("AdventureWorks"));
SqlSyncProvider clientProvider = new SqlSyncProvider(GetDatabaseConnectionString("Client"));

SyncAgent agent = new SyncAgent(clientProvider, serverProvider, new string[] {
"ProductCategory", "ProductModel", "Product", "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail"});

var syncContext = await agent.SynchronizeAsync(SyncType.Reinitialize);

Console.WriteLine(syncContext);

```

```
Synchronization done.
        Total changes  uploaded: 0
        Total changes  downloaded: 2752
        Total changes  applied: 2752
        Total resolved conflicts: 0
        Total duration :0:0:1.872
```
As you can see, the `SyncType.Reinitialize` value has marked the client database to be fully resynchronized.  
The modified row is overwritten by the server value.


### SyncType.ReinitializeWithUpload

`ReinitializeWithUpload` will do the same job as `Reinitialize` except it will send any changes available from the client, before making the reinitialize phase.


```csharp
SqlSyncProvider serverProvider = new SqlSyncProvider(GetDatabaseConnectionString("AdventureWorks"));
SqlSyncProvider clientProvider = new SqlSyncProvider(GetDatabaseConnectionString("Client"));

SyncAgent agent = new SyncAgent(clientProvider, serverProvider, new string[] {
"ProductCategory", "ProductModel", "Product", "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail"});

var syncResult = await agent.SynchronizeAsync(SyncType.ReinitializeWithUpload);

Console.WriteLine(syncResult);

```

```
Synchronization done.
        Total changes  uploaded: 1
        Total changes  downloaded: 2752
        Total changes  applied: 2752
        Total resolved conflicts: 0
        Total duration :0:0:1.923
```
In this case, as you can see, the `SyncType.ReinitializeWithUpload` value has marked the client database to be fully resynchronized, but the edited row has been sent correctly to the server.  




