# Setup Migration

It's quite complicated, once you have created your sync configuration, and once it's running, to add or remove tables to a named scope...

For example going from this:
``` csharp
var setup = new SyncSetup(new string[] { "ProductCategory", "Product" })
```

to this:
``` csharp
var setup = new SyncSetup(new string[] { "ProductCategory", "Product", "ProductDescription" })
```
Is really complicated...


Since the last version (from `v0.5.1`)  **DMS** will automaticaly migrate your whole **old** Setup configuration to match your **new** Setup.

This migration is handled for you automatically, once you've called the method `await agent.SynchronizeAsync();`

Basically, **DMS** will make a comparison between the **last valid** Setup:
- Stored in the `scope_info` table on the local database 
- Stored in the `scope_info_server` for the server side database

And then will merge the databases, adding *tracking tables*, *stored procedures*, *triggers* and *tables* if needed

## Example 1

Going from this:
``` csharp
var setup = new SyncSetup(new string[] { "ProductCategory", "Product" })
```
to this:
``` csharp
var setup = new SyncSetup(new string[] { "ProductCategory", "Product", "ProductDescription" })
```
Will generate:
- A new table `ProductDescription` on the client
- A new tracking table `ProductDescription_tracking` on the client and the server
- New **stored procedures** on both databases
- New **triggers** on both databases

## Example 2

Going from this:
``` csharp
var setup = new SyncSetup(new string[] { "ProductCategory", "Product" })
```
to this:
``` csharp
var setup = new SyncSetup(new string[] { "ProductCategory", "Product" })
{
    TrackingTablesPrefix = "t",
    TrackingTablesSuffix = "",
};
```
Will generate:
- A renaming of the trackings tables on both databases

AND because renaming the trackings tables will have an impact on triggers and stored proc ..

- A drop / create of all stored procedures
- A drop / create of all triggers

## LocalOrchestrator and RemoteOrchestrator methods

First of all, if you are just using `agent.SynchronizeAsync()`, everything will be handled automatically.  

But you can use the orchestrators to do the job. It will allow you to migrate your setup, without having to make a synchronization.

You have 2 new methods on both orchestrators:

On `LocalOrchestrator`:

``` csharp
public virtual async Task MigrationAsync(SyncSetup oldSetup, SyncSet schema, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
```
Basically, you need the old setup to migrate `oldSetup`, and the new `schema`. 
You don't need the new `Setup` because you have already add it during the **ctor** of the `LocalOrchestrator`

Why do you need the `schema` ? If you are adding a new table, that is not present locally, we need the schema from the server side, to get the new table structure.

Here is an example, using this method on your local database:

``` csharp
// adding 2 new tables
var newSetup = new SyncSetup(new string[] { "ProductCategory", "Product", "ProdutModel", "ProductDescription" });

// local orchestrator
var localOrchestrator = new LocalOrchestrator(clientProvider, options, setup);

// remote orchestrator to get the schema for the 2 new tables to add
var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options, setup);
// If you are on a web sync architecture, you can use the WebClientOrchestrator as well:
// var remoteOrchestrator = new WebClientOrchestrator(serviceUri)

// get the old setup
var scopeInfo = await localOrchestrator.GetClientScopeAsync();
var oldSetup = scopeInfo.Setup;

// get the schema
var schema = await remoteOrchestrator.GetSchemaAsync();

// Migrating the old setup to the new one, using the schema if needed
await localOrchestrator.MigrationAsync(oldSetup, schema);
```
On `RemoteOrchestrator`:

``` csharp
public virtual async Task MigrationAsync(SyncSetup oldSetup, CancellationToken cancellationToken = default, IProgress<ProgressArgs> progress = null)
```
Basically, it's the same method as on `LocalOrchestrator` but we don't need to pass a schema, since we are on the server side, and we know how to get the schema :)

The same example will become:

``` csharp
// adding 2 new tables
var newSetup = new SyncSetup(new string[] { "ProductCategory", "Product", "ProdutModel", "ProductDescription" });

// remote orchestrator to get the schema for the 2 new tables to add
var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options, setup);

// get the old setup
var serverScopeInfo = await remoteOrchestrator.GetServerScopeAsync();
var oldServerSetup = serverScopeInfo.Setup;

// Migrating the old setup to the new one, using the schema if needed
await remoteOrchestrator.MigrationAsync(oldServerSetup);
```

For instance, the `RemoteOrchestrator` `MigrationAsync` could be really useful if you want to migrate your server database, when configuring as **HTTP** mode.

Once migrated, all new clients, will get the new setup from the server, and will apply locally the migration, automatically.

## What Setup migration doesn't do !

Be careful, the migration stuff will **only** allows you to migrate your setup (adding or removing tables from your sync, renaming stored proc and so on ...)

**You can't use it to migrate your own schema database !!**

Well, it could work if:
- You are adding a new table : Quite easy, just add this table to your `SyncSetup` and you're done.
- You are removing a table: Once again, easy, remove it from your `SyncSetup`, and you're good to go
- You are removing physically a column from a table on your server: You **can't** use this technic to migrate your clients database.

The **DMS** framework won't be able to make an `Alter table` to add / remove columns. Too complicated to handle, too much possibilities and scenario.

If you have to deal with this kind of situation, the best solution is to
- Call `DeprovisionAsync`
- Apply your alter 
- Call `ProvisionAsync`
It's a manual step, not automatically handled by the **DMS** framework, but it's working

## Last timestamp sync

Oh wait, last thing....
Let's talk about our first example, where we have added a new table `ProductDescription`.   

What's happen on the next sync ?  
Well as we said the table `ProductDescription` will be provisioned (stored proc, triggers and tracking table) on both databases (server / client) and if the table does not exists on the client, it will be created.   

Then the **DMS** framework will make a sync ....

And this sync will get all the rows from the server side **that have changed since the last sucessful sync**

And you look the `ProductDescription` table on the client database : **NO ROWS** !!!

As you understand, you will have an empty `ProductDescription` table on the client !! : Because no rows have been marked as **changed** in the server tracking table.
Indeed, we've just created this tracking table on the server !!

So, if you're adding a new table, you **MUST** do a full sync, call the `SynchronizeAsync()` method with a `SyncType.Reinitialize` or  `SyncType.ReinitializeWithUpload` parameter.

Adding a new table is not trivial.   
Hopefully if you are using `snapshots` it should not be too heavy for your server database :) 

## Pro Tip: Forcing Reinitialize sync type from server side.

As we saw, it could be useful to force a reinitialize from a client, to get all the needed data.   
Unfortunatelly, you should have a *special* routine from the client side, to launch the synchronization with a `SynchronizeAsync(SyntType.Reinitialize)`, like an admin button or whatever.   

Fortunatelly, using an interceptor, from the **server side**, you are able to *force* the reinitialization from the client.

On the server side, from your controller, just modify the request `SyncContext` with the correct value, like this:

``` csharp
[HttpPost]
public async Task Post()
{

    // Get Orchestrator regarding the incoming scope name (from http context)
    var orchestrator = webServerManager.GetOrchestrator(this.HttpContext);

    // override sync type to force a reinitialization from a particular client
    orchestrator.OnServerScopeLoaded(sla =>
    {
        // ClientId represents one client. If you want to reinitialize ALL clients, just remove this condition
        if (sla.Context.ClientScopeId == clientId)
        {
            sla.Context.SyncType = SyncType.Reinitialize;
        }
    });

    // handle request
    await webServerManager.HandleRequestAsync(this.HttpContext);
}

```







