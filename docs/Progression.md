# Progression

Progression in a sync process could be complex.   

Firs of all, you have two kind of progression:
* **Progression** from the client side
* **Progression** from the server side.

Progression is divided in several ordered parts:
* **Server** begins a new session.
* **Client** begins a new session.
* **Client** ensures that its scope exists.
* **Server** ensures thats server and client scopes exists.
* **Server** reads the configuration, updating the tables metadatas.
* **Client** gets the configuration object from server.
* **Server** ensures database is ready (creating stored procedures, triggers and so on, if needed)
* **Client** ensures database is ready (creating tables, stored procedudes and so on, if needed)
* **Client** get local changes to be applied on server.
* **Server** applies changes from client.
* **Server** resolves conflicts if needed.
* **Server** get server changes to be applied on client.
* **Client** applies changes from server.
* **Server** get local timestamp and commit sync.
* **Client** get local timestamp and commit sync.
* **Server** ends the session
* **Client** ends the session

As you can see, you have a lot events raised by both server and client side.  
Each event in a sync process is called a *stage*, represented by a **SyncStage** enumeration:
``` cs
public enum SyncStage
{
    None,
    BeginSession,
    ScopeLoading,
    ScopeSaved,
    ConfigurationApplying,
    ConfigurationApplied,
    DatabaseApplying,
    DatabaseApplied,
    DatabaseTableApplying,
    DatabaseTableApplied,
    TableChangesSelecting,
    TableChangesSelected,
    TableChangesApplying,
    TableChangesApplied,
    EndSession,
    CleanupMetadata
}

```
Possibles values are:
* **BeginSession**: Begin a new sync session.
* **ScopeLoading** and **ScopeSaved**: Check if the scope table is created and check last sync.
* **ConfigurationApplying** and **ConfigurationApplied**: Raised before and after configuration is readed.
* **DatabaseApplying** and **DatabaseApplying**: Raised before and after the database is updated will all tables, stored procedures and triggers.
* **DatabaseTableApplying** and **DatabaseTableApplied** : Raised before and after each table has been created (with its stored procedure and triggers)
* **ChangesSelecting** and **ChangesSelecting**: Raised before and after changes have been selected from the server / client
* **ChangesApplying** and **ChangesApplying**: Raised before and after changes have been selected from server / client.
* **EndSession**: End the current sync session.
* **CleanupMetadata**: Cleanup metadata from tracking tables.
    
Now, imagine you have a really straightforward sync process :

``` csharp
SqlSyncProvider serverProvider = new SqlSyncProvider(GetDatabaseConnectionString("Northwind"));
SqlSyncProvider clientProvider = new SqlSyncProvider(GetDatabaseConnectionString("NW1"));

SyncAgent syncAgent = new SyncAgent(clientProvider, serverProvider, new string[] { "ProductCategory", "Product" });

var context = await syncAgent.SynchronizeAsync();
Console.WriteLine(context);

```
We are going to see how to get informations through the stages, thanks to `IProgress<T>` and then go deeper with `Interceptor<T>`.

## How progression is handled 

The progress values are raised from both side : **Server** side and **Client** side, ordered.  

In our sample, we can say that : 
- The `serverProvider` instance will report each progress from the server provider.   
- The `clientProvider` instance will report each progress from the client provider.  

> The `syncAgent` instance will report progress **only** from the `clientProvider` side.
> Why? Because the `syncAgent` instance will always run on the client local machine, and maybe the server provider is behind an **HTTP** endpoint. The `syncAgent` has no idea what's going on the server side.

Just remember this : The `syncAgent` object is **always** on the client side of any architecture.  

## Progress\<ProgressArgs\>

Since version **0.3** the `Dotmim.Sync` does not use any more the `EventHandler` events mechanism.   

Since our main method `SynchronizeAsync()` is marked `async` method, we will use the (https://docs.microsoft.com/en-us/dotnet/api/system.progress-1?view=netcore-2.2)[Progress<T>] to be able to report progress value.

The informations you will get from the `SyncProgress` event are **read only** (If you want more options, see section below *Go further*)

Here is a quick example, often used to provide some feedback to the users:   

``` cs

// Using the IProgress<T> pattern to handle progession dring the synchronization
var progress = new Progress<ProgressArgs>(s => Console.WriteLine($"{s.Context.SyncStage}:\t{s.Message}"));

// Dont forget to add this instance to the SynchronizeAsync method call
...
...
        // Launch the sync process
        var context = await agent.SynchronizeAsync(progress);
...
...

```
Here is the result, after the first synchronization, assuming the **Client** database is empty:

``` cmd
Sync Start
BeginSession:
ScopeLoading:   Id:0f5ac5e0-7987-4054-93bc-078d300a2cd2 LastSync: LastSyncDuration:0 SyncState:Successful
SchemaApplied:  Tables count:2
DatabaseTableApplied:   TableName: Product Provision:All
DatabaseApplied:        TableName: ProductCategory Provision:All
DatabaseApplied:        Tables count:22 Provision:All
TableChangesSelected:   ProductCategory Inserts:0 Updates:0 Deletes:0 TotalChanges:0
TableChangesSelected:   Product Inserts:0 Updates:0 Deletes:0 TotalChanges:0
TableChangesApplied:    ProductCategory State:Added Applied:11 Failed:0
TableChangesApplied:    Product State:Added Applied:14 Failed:0
ScopeSaved:     Id:0f5ac5e0-7987-4054-93bc-078d300a2cd2 LastSync:01/02/2019 16:36:10 LastSyncDuration:32585196 SyncState:Successful
Synchronization done.
        Total changes downloaded: 25
        Total changes uploaded: 0
        Total conflicts: 0
        Total duration :0:0:3.258
EndSession:
```

As you can see, it's a first synchronization, so:
* Session begins 
* Client apply databases schema for **Product** and **ProductCategory**
* Client select changes to send (nothing, obviously is selected since the tables are just created)
* Client applies changes from server 
* Session ends


If you want both informations from server and from client, we can do a little trick like this :

``` csharp
// I want the server side progress as well
agent.RemoteProvider.SetProgress(progress);
// Launch the sync process
var s1 = await agent.SynchronizeAsync(progress);
```

Since the agent is executing on the client, as we said, the `progress` instance reference passed to the `agent.SynchronizeAsync()` will trigger all the progress fromt the client.   

On the other side, to be able to get progress from server side (if you are not in a web proxy mode), you can call the `SetProgress()` method with your `progress` instance on the `RemoteProvider` property.

The result is really verbose, but you have ALL the informations  from both **Client** side and **Server** side as well !

## Go further : Interceptor\<T\>

The `Progress<T>` stuff is great, but as we said, it's read only, and the progress is always reported **at the end of the current sync stage**.   
For instance, the `SyncStage` step called `DatabaseTableApplying` is never reported through `Progress<T>` (in opposite to `DatabaseTableApplied` that is called).   

So, if you need to kind of *intercept* stages, you can subscribe to an `Interceptor<T>`.   
On any provider, you will find a lot of relevant methods to intercept the sync process:

![Interceptor](assets/interceptor01.png)


Imagine you have a table that should **never** be synchronized. You're able to use an interceptor like this:

``` csharp
agent.LocalProvider.InterceptTableChangesApplying((args) =>
{
    if (args.TableName == "Table_That_Should_Not_Be_Sync")
        args.Action = ChangeApplicationAction.Rollback;
});
```
Be careful, returning a `ChangeApplicationAction.Rollback` will rollback the whole sync session ! 

Other useful example, you can use interceptors to have more detailed logs. For instance :

``` csharp
agent.LocalProvider.InterceptTableChangesSelecting(args =>
{
    Console.WriteLine($"Get changes for table {args.TableName}");
});


agent.LocalProvider.InterceptTableChangesSelected(args =>
{
    Console.WriteLine($"Changes selected from table {args.TableChangesSelected.TableName}: ");
    Console.WriteLine($"\tInserts:{args.TableChangesSelected.Inserts}.");
    Console.WriteLine($"\tUpdates:{args.TableChangesSelected.Updates}.");
    Console.WriteLine($"\tDeletes:{args.TableChangesSelected.Deletes}.");
});
```