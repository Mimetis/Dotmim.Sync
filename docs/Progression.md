# Progression

Progression in a sync process could be complex.   

First of all, during a full synchronization, we have **two distincts** progression:
* A first **Progression** from the client side
* A second **Progression** from the server side.

We have a lot of events raised by both the **server** and the **client** side.
* Each event is raised from the assiocated **Orchestrator** instance.
* Each event in a sync process is called a *stage*, represented by a **SyncStage** enumeration:
``` cs
public enum SyncStage
{
    None,
    // Sync start and ends
    BeginSession,
    EndSession,

    // Loading metadatas
    ScopeLoading,
    ScopeLoaded,

    // Creating a snapshot on the server side
    SnapshotCreating,
    SnapshotCreated,

    // Applying a snapshot on the client side
    SnapshotApplying,
    SnapshotApplied,

    // Reading a schema
    SchemaReading,
    SchemaRead,

    // Provisioning a schema (tables / stored proc / triggers / tracking tables)
    Provisioning,
    Provisioned,

    // Deprovisioning a schema
    Deprovisioning,
    Deprovisioned,

    // Selecting changes to apply to the server or client
    ChangesSelecting,
    ChangesSelected,

    // Applying changes to the server or client
    ChangesApplying,
    ChangesApplied,

    // Cleaning tracking information from any tracking table
    MetadataCleaning,
    MetadataCleaned,
}

```
Now, imagine we have a really straightforward sync process, using the sample from [Hello sync sample](/Samples/HelloSync) :

``` csharp
var serverProvider = new SqlSyncChangeTrackingProvider(serverConnectionString);
var clientProvider = new SqlSyncProvider(clientConnectionString);

var tables = new string[] {"ProductCategory", "ProductModel", "Product",
            "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail" };

var agent = new SyncAgent(clientProvider, serverProvider, tables);
do
{
    // Launch the sync process
    var s1 = await agent.SynchronizeAsync();
    // Write results
    Console.WriteLine(s1);

} while (Console.ReadKey().Key != ConsoleKey.Escape);

Console.WriteLine("End");


```
We are going to see how to get useful information, from each stage involved during the sync processus, thanks to `IProgress<T>` and then we will go deeper with the notion of `Interceptor<T>`.

## How progression is handled 

The progress values are raised from both side : **Server** side and **Client** side, ordered.  

In our sample, we can say that : 
- The `RemoteOrchestrator` instance using the `serverProvider` provider instance will report all the progress from the server side.   
- The `LocalOrchestrator` instance using the `clientProvider` provider instance will report all the progress from the client side.  

> The `syncAgent` instance will report progress **only** from the **Client** side.
> Why? Because the `syncAgent` instance always run **locally** on the client local machine, and the **server** may be is behind an **HTTP** endpoint. Then `syncAgent` has no idea what's going on the server side.

Just remember this true fact: The `syncAgent` object is **always** on the client side of any architecture.  

## Progress\<ProgressArgs\>

Since version **v0.4** the `Dotmim.Sync` does not use any more the `EventHandler` events mechanism.   

Since our main method `SynchronizeAsync()` is marked `async` method, we will use the [Progress\<T\>](https://docs.microsoft.com/en-us/dotnet/api/system.progress-1?view=netcore-2.2) to be able to report progress value.

So far, the most straightforward way to get feedback from a current sync, is to pass an instance of `Progress<T>` when calling the method `SynchronizeAsync()`

You will find the sample used for this demonstration, here : [Progression sample](/Samples/Progression)


Here is a quick example, often used to provide some feedback to the users:   

``` cs
var serverProvider = new SqlSyncChangeTrackingProvider(serverConnectionString);
var clientProvider = new SqlSyncProvider(clientConnectionString);

// Tables involved in the sync process:
var tables = new string[] {"ProductCategory", "ProductModel", "Product",
            "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail" };

// Creating an agent that will handle all the process
var agent = new SyncAgent(clientProvider, serverProvider, tables);

// Using the IProgress<T> pattern to handle progession dring the synchronization
// Be careful, Progress<T> is not synchronous. We are using instead a custom made SynchronousProgress<T>
var progress = new SynchronousProgress<ProgressArgs>(args => Console.WriteLine($"{args.Context.SyncStage}:\t{args.Message}"));

do
{
    // Launch the sync process
    var s1 = await agent.SynchronizeAsync(progress);
    // Write results
    Console.WriteLine(s1);

} while (Console.ReadKey().Key != ConsoleKey.Escape);

Console.WriteLine("End");

```
Here is the result, after the first synchronization, assuming the **Client** database is empty:

``` cmd
BeginSession:   22:27:06.811
ScopeLoaded:    22:27:07.215     [Client] [DefaultScope] [Version ] Last sync: Last sync duration:0:0:0.0
Provisioned:    22:27:09.140     [Client] tables count:8 provision:Table, TrackingTable, StoredProcedures, Triggers
ChangesSelected:        22:27:09.207     [Client] upserts:0 deletes:0 total:0
ChangesApplying:        22:27:09.786     [Client] [ProductCategory] Modified applied:41 resolved conflicts:0
ChangesApplying:        22:27:09.819     [Client] [ProductModel] Modified applied:128 resolved conflicts:0
ChangesApplying:        22:27:09.897     [Client] [Product] Modified applied:295 resolved conflicts:0
ChangesApplying:        22:27:09.940     [Client] [Address] Modified applied:450 resolved conflicts:0
ChangesApplying:        22:27:10.83      [Client] [Customer] Modified applied:847 resolved conflicts:0
ChangesApplying:        22:27:10.124     [Client] [CustomerAddress] Modified applied:417 resolved conflicts:0
ChangesApplying:        22:27:10.164     [Client] [SalesOrderHeader] Modified applied:32 resolved conflicts:0
ChangesApplying:        22:27:10.218     [Client] [SalesOrderDetail] Modified applied:542 resolved conflicts:0
ChangesApplied: 22:27:10.268     [Client] applied:2752 resolved conflicts:0
EndSession:     22:27:10.269
Synchronization done.
        Total changes  uploaded: 0
        Total changes  downloaded: 2752
        Total changes  applied: 2752
        Total resolved conflicts: 0
        Total duration :0:0:3.463
```

As you can see, it's a first synchronization, so:
* Session begins 
* Client apply databases schema for all tables
* Client select changes to send (nothing, obviously, because the tables have just been created on the client)
* Client applies changes from server 
* Session ends


If you want both informations from server and from client, we can do a little trick here :

``` csharp
// I want the server side progress as well
var remoteProgress = new SynchronousProgress<ProgressArgs>(s =>
{
    Console.ForegroundColor = ConsoleColor.Yellow;
    Console.WriteLine($"{s.Context.SyncStage}:\t{s.Message}");
    Console.ResetColor();
});
agent.AddRemoteProgress(remoteProgress);
```

Since the agent is executing on the client, as we said, the `progress` instance reference passed to the `agent.SynchronizeAsync(progress)` will trigger all the progress fromt the client.   

On the other side, to be able to get progress from the server side (if you are not in a web proxy mode), you can call the `AddRemoteProgress()` method with your `remoteProgress` instance.

The result is really verbose, but you have ALL the informations  from both **Client** side and **Server** side !

![Verbose progression](assets/ProgressionVerbose.png)


## Go further : Interceptor\<T\>

The `Progress<T>` stuff is great, but as we said, it's mainly read only, and the progress is always reported **at the end of the current sync stage**.   

So, if you need a more granular control on all the events, you can subscribe to an `Interceptor<T>`.   
On each **orchestrator**, you will find a lot of relevant methods to intercept the sync process:

![Interceptor](assets/interceptor01.png)


Imagine you have a table that should **never** be synchronized. You're able to use an interceptor like this:

``` csharp
// We are using a cancellation token that will passed as an argument to the SynchronizeAsync() method !
var cts = new CancellationTokenSource();

agent.LocalOrchestrator.OnTableChangesApplying((args) =>
{
    if (args.SchemaTable.TableName == "Table_That_Should_Not_Be_Sync")
        cts.Cancel();
});

```
Be careful, your `CancellationTokenSource` instance will rollback the whole sync session and you will get a `SyncException` error ! 

Other useful example, you can use the **interceptors** `OnTableChangesSelecting` and `OnTableChangesSelected` to have more details on what changes are selected for each table.

> The changes you get from the interceptor `OnTableChangesSelected` are "*copy of*" the changes that will be sent. Making any modification in these rows won't affect the current sync session !

``` csharp
agent.LocalOrchestrator.OnTableChangesSelecting(args =>
{
    Console.WriteLine($"-------- Getting changes from table {args.TableName} ...");
});

agent.LocalOrchestrator.OnTableChangesSelected(args =>
{
    if (args.Changes == null || args.Changes.Rows.Count == 0)
        return;

    foreach (var row in args.Changes.Rows)
        Console.WriteLine(row);
});
```

``` bash
BeginSession:   00:06:54.838
ScopeLoaded:    00:06:55.181     [Client] [DefaultScope] [Version 1] Last sync:08/04/2020 22:04:51 Last sync duration:0:0:0.905
ScopeLoaded:    00:06:55.260     [AdventureWorks] [DefaultScope] [Version 1]
-------- Getting changes from table ProductCategory ...
[Sync state]:Modified, [ProductCategoryID]:6, [ParentProductCategoryID]:1, [Name]:Road Bikes Bis, [rowguid]:000310c0-bcc8-42c4-b0c3-45ae611af06b, [ModifiedDate]:01/06/2002 00:00:00
[Sync state]:Modified, [ProductCategoryID]:7, [ParentProductCategoryID]:1, [Name]:Touring Bikesde , [rowguid]:02c5061d-ecdc-4274-b5f1-e91d76bc3f37, [ModifiedDate]:01/06/2002 00:00:00
[Sync state]:Modified, [ProductCategoryID]:15, [ParentProductCategoryID]:2, [Name]:Headsets II, [rowguid]:7c782bbe-5a16-495a-aa50-10afe5a84af2, [ModifiedDate]:01/06/2002 00:00:00
ChangesSelecting:       00:06:55.624     [Client] [ProductCategory] upserts:3 deletes:0 total:3
-------- Getting changes from table ProductModel ...
-------- Getting changes from table Product ...
-------- Getting changes from table Address ...
-------- Getting changes from table Customer ...
-------- Getting changes from table CustomerAddress ...
-------- Getting changes from table SalesOrderHeader ...
-------- Getting changes from table SalesOrderDetail ...
[Sync state]:Deleted, [SalesOrderID]:71784, [SalesOrderDetailID]:110775
[Sync state]:Deleted, [SalesOrderID]:71784, [SalesOrderDetailID]:110776
[Sync state]:Deleted, [SalesOrderID]:71784, [SalesOrderDetailID]:110777
[Sync state]:Deleted, [SalesOrderID]:71784, [SalesOrderDetailID]:110778
[Sync state]:Deleted, [SalesOrderID]:71784, [SalesOrderDetailID]:110779
[Sync state]:Deleted, [SalesOrderID]:71784, [SalesOrderDetailID]:110780
[Sync state]:Deleted, [SalesOrderID]:71784, [SalesOrderDetailID]:110781
ChangesSelecting:       00:06:55.655     [Client] [SalesOrderDetail] upserts:0 deletes:7 total:7
ChangesSelected:        00:06:55.657     [Client] upserts:3 deletes:7 total:10
ChangesApplying:        00:06:55.988     [AdventureWorks] [SalesOrderDetail] Deleted applied:7 resolved conflicts:0
ChangesApplying:        00:06:56.27      [AdventureWorks] [ProductCategory] Modified applied:3 resolved conflicts:0
ChangesApplied: 00:06:56.42      [AdventureWorks] applied:10 resolved conflicts:0
ChangesSelected:        00:06:56.102     [AdventureWorks] upserts:0 deletes:0 total:0
ChangesApplied: 00:06:56.147     [Client] applied:0 resolved conflicts:0
EndSession:     00:06:56.149
Synchronization done.
        Total changes  uploaded: 10
        Total changes  downloaded: 0
        Total changes  applied: 0
        Total resolved conflicts: 0
        Total duration :0:0:1.313
```

