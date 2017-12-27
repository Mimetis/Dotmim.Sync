# How to Synchronize

You have one main method to launch a synchronization, with several optional parameters:

```csharp
SynchronizeAsync();
SynchronizeAsync(CancellationToken cancellationToken);
SynchronizeAsync(SyncType syncType);
SynchronizeAsync(SyncType syncType, CancellationToken cancellationToken);
```

Obviously, you can use the `CancellationToken` object whenever you want to rollback an "*in progress*" synchronization. 

let's see now a straightforward sample :

```csharp
SqlSyncProvider serverProvider = new SqlSyncProvider(GetDatabaseConnectionString("Northwind"));
SqlSyncProvider clientProvider = new SqlSyncProvider(GetDatabaseConnectionString("NW1"));

SyncAgent agent = new SyncAgent(clientProvider, serverProvider, new string[] {
"Customers", "Region"});

var syncContext = await agent.SynchronizeAsync();

Console.WriteLine(syncContext);

```

Here is the result, after the **first initial** synchronization:

```
Sync Start
Synchronization done.
        Total changes downloaded: 99
        Total changes uploaded: 0
        Total conflicts: 0
        Total duration :0:0:3.334
```

As you can see, the client has downloaded 99 lines from the server into the `Customers` and `Region` tables.  
Obviously if we made a new sync, without making changes neither on the server nor the client, the result will be :

```
Sync Start
Synchronization done.
        Total changes downloaded: 0
        Total changes uploaded: 0
        Total conflicts: 0
        Total duration :0:0:3.334
```

Ok make sense !

## Using SyncType

The `SyncType` enumeration allows you to reinitialize a client synchronization tables.  
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
    /// Reinitialize the whole sync database, applying all rows from the server to the client, after trying a client upload
    /// </summary>
    ReinitializeWithUpload
}
```

* `SyncType.Normal`: Default value, represents a normal sync process
* `SyncType.Reinitialize`: Launches the sync process, marking the client to be resynchronized. Be careful, any changes on the client will be overwritten by this value !
* `SyncType.ReinitializeWithUpload`: Like *Reinitialize* this value will launch a process to resynchronize the whole client database, except that the client will *try* to send its local changes before making the resync process.

From the sample we saw before, here is the different behaviors with each `SyncType` enumeration value:  
First of all, for demo purpose, we are updating a row on the client:

``` sql
-- initial value is 'Berlin'
UPDATE Customers SET City='Paris' WHERE CustomerID = 'ALFKI'
```

### SyncType.Normal

```csharp
SqlSyncProvider serverProvider = new SqlSyncProvider(GetDatabaseConnectionString("Northwind"));
SqlSyncProvider clientProvider = new SqlSyncProvider(GetDatabaseConnectionString("NW1"));

SyncAgent agent = new SyncAgent(clientProvider, serverProvider, new string[] {
"Customers", "Region"});

var syncContext = await agent.SynchronizeAsync(SyncType.Normal);

Console.WriteLine(syncContext);

```

```
Sync Start
Synchronization done.
        Total changes downloaded: 0
        Total changes uploaded: 1
        Total conflicts: 0
        Total duration :0:0:3.334
```

The default behavior is what we could expect : Uploading the modified row to the server.

### SyncType.Reinitialize

```csharp
SqlSyncProvider serverProvider = new SqlSyncProvider(GetDatabaseConnectionString("Northwind"));
SqlSyncProvider clientProvider = new SqlSyncProvider(GetDatabaseConnectionString("NW1"));

SyncAgent agent = new SyncAgent(clientProvider, serverProvider, new string[] {
"Customers", "Region"});

var syncContext = await agent.SynchronizeAsync(SyncType.Reinitialize);

Console.WriteLine(syncContext);

```

```
Sync Start
Synchronization done.
        Total changes downloaded: 99
        Total changes uploaded: 0
        Total conflicts: 0
        Total duration :0:0:3.334
```
In this case, as you can see, the `SyncType.Reinitialize` value has marked the client database to be fully resynchronized.  
The modified row is overwritten and the value is restored to *Berlin* (instead of *Paris*)


### SyncType.ReinitializeWithUpload

```csharp
SqlSyncProvider serverProvider = new SqlSyncProvider(GetDatabaseConnectionString("Northwind"));
SqlSyncProvider clientProvider = new SqlSyncProvider(GetDatabaseConnectionString("NW1"));

SyncAgent agent = new SyncAgent(clientProvider, serverProvider, new string[] {
"Customers", "Region"});

var syncContext = await agent.SynchronizeAsync(SyncType.ReinitializeWithUpload);

Console.WriteLine(syncContext);

```

```
Sync Start
Synchronization done.
        Total changes downloaded: 99
        Total changes uploaded: 1
        Total conflicts: 0
        Total duration :0:0:3.334
```
In this case, as you can see, the `SyncType.ReinitializeWithUpload` value has marked the client database to be fully resynchronized, but the edited row will be sent to the server.  
The row value on the server and on the client are both of them equal to *Paris*




