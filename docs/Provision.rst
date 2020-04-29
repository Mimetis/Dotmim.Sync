Provision and Deprovision
===================================

Overview
^^^^^^^^^^^

During the first sync, **DMS** will provision everything, on the server side and on the client side.

When you launch for the first time a sync process, **DMS** will:

- **[Server Side]**: Get the database schema from the server database.
- **[Server Side]**: Create **Stored procedures**, **triggers** and **tracking tables**.
- **[Client Side]**: Fetch the server schema.
- **[Client Side]**: Create **tables** on the client database, if needed.
- **[Client Side]**: Create **Stored procedures**, **triggers** and **tracking tables**

.. note:: If you're using the ``SqlSyncChangeTrackingProvider``, **DMS** will skip the creation of triggers and tracking tables, relying on the *Change Tracking* feature from SQL Server.

| Basically, all these steps are managed by the ``RemoteOrchestrator`` on the server side, and by the ``LocalOrchestrator`` on the client side. 
| All the methods used to provision and deprovision tables are available from bot the ``LocalOrchestrator`` and ``RemoteOrchestrator`` instances.





### Automatic provisionning sample

Basically, we have a simple database containing two tables *Customers* and *Region*:

![Norhwind database](assets/Provision_Northwind01.png)

And here the most straightforward code to be able to sync a client db :

```csharp
SqlSyncProvider serverProvider = new SqlSyncProvider(GetDatabaseConnectionString("Northwind"));
SqlSyncProvider clientProvider = new SqlSyncProvider(GetDatabaseConnectionString("NW1"));

SyncAgent agent = new SyncAgent(clientProvider, serverProvider, new string[] {
"Customers", "Region"});

var syncContext = await agent.SynchronizeAsync();

Console.WriteLine(syncContext);

```

Once your sync process is finished, you will have a full configured database :

![Norhwind database](assets/Provision_Northwind02.png)

## Manually provisionning

In some circumstances, you may want to provision manually your database (server or client as well).  
* If you have a really big database, the provision step could be really long, so it could be better to provision the server side before any sync process.
* If you have to modify your schema. You will have to *deprovision* then *edit* your schema then *provision* again your database.

That's why the **DMS** framework contains several methods to let you control how, and when, you want to provision and deprovision your database.

Each orchestrator has two main methods, basically:
* `ProvisionAsync(SyncSet schema, SyncProvision provision)`
* `DeprovisionAsync(SyncSet schema, SyncProvision provision)`

the `SyncProvision` enum parameter lets you decide which kind of objects (tables, stored proc, triggers or tracking tables) you will provision on your target database.

```csharp
[Flags]
public enum SyncProvision
{
    Table = 1,
    TrackingTable = 2,
    StoredProcedures = 4,
    Triggers = 8,
    ClientScope = 16,
    ServerScope = 32,
    ServerHistoryScope = 64,
}

```
For instance, here is the code you need to implement to be able to provision a database :

```csharp
var serverProvider = new SqlSyncProvider(serverConnectionString);
var clientProvider = new SqlSyncProvider(clientConnectionString);

// Create a local orchestrator, to manage the local database
var localOrchestrator = new LocalOrchestrator(clientProvider, Config.GetClientOptions(), Config.GetSetup());

// Create a remote orchestrator, to manage hub server database
var remoteOrchestrator = new RemoteOrchestrator(serverProvider, Config.GetClientOptions(), Config.GetSetup());

// Get schema from server side
var schema = await remoteOrchestrator.GetSchemaAsync();

var provision = SyncProvision.ClientScope | SyncProvision.StoredProcedures | SyncProvision.Table | SyncProvision.TrackingTable | SyncProvision.Triggers;

// provision the local database
await localOrchestrator.ProvisionAsync(schema, provision);
```

## Manually Deprovision

Like provisioning, deprovisioning uses basically the same method.
We don't need the full schema to be able to deprovision a table, so far, a `Setup` instance is enough

```csharp
var serverProvider = new SqlSyncProvider(serverConnectionString);
var clientProvider = new SqlSyncProvider(clientConnectionString);

var setup = new SyncSetup(new string[]{"Product", "ProductCategory"});

// Create a local orchestrator, to manage the local database
var localOrchestrator = new LocalOrchestrator(clientProvider, new SyncOptions(), setup);

var provision = SyncProvision.ClientScope | SyncProvision.StoredProcedures | SyncProvision.Table | SyncProvision.TrackingTable | SyncProvision.Triggers;

// provision the local database
await localOrchestrator.DeprovisionAsync(schema, provision);
```

## Managing a database migration

During any dev cycle, you will probably have to make some evolutions on your server database.  
Adding or deleting columns will break the sync process.  
Manually, without the `Provision()` and `Deprovision()` methods, you will have to edit all the stored procedures, triggers and so on to be able to recreate a full sync processus.  

We are going to handle, with a little example, how we could add a new column on an existing sync typo:
Now imagine you want to add this new column, called `Comments` on the `Customers` table.   
Here is how you could handle it:

* Create the providers.
* Delete the stored procedures and the triggers.
* We don't want to loose the `Customers_tracking` tracking table, to keep the sync historic, so we don't delete it. Here we have to be sure the primary keys from `Customers` is still the same (So don't touch primary keys or this technic won't work)
* Edit the client and server schema (you can use here the EF migration)
* Re apply the triggers and the stored procédures. They will be re-generated with the new column !
* Re launch a new sync process (don't use the same `SyncAgent` as before, recreate a new one).

``` csharp
private static async Task AlterSchemasAsync()
{
    SqlSyncProvider serverProvider = new SqlSyncProvider(GetDatabaseConnectionString("Northwind"));
    SqlSyncProvider clientProvider = new SqlSyncProvider(GetDatabaseConnectionString("NW1"));

    var options = new SyncOptions();

    // tables to deprovision
    var setup new SyncSetup(new string[]{"Customers"});

    var localOrchestrator = new LocalOrchestrator(clientProvider, options, setup);
    var remoteOrchestrator = new RemoteOrchestrator(clientProvider, options, setup);


    // delete triggers and sp
    await localOrchestrator.DeprovisionAsync(SyncProvision.StoredProcedures | SyncProvision.Triggers);
    await remoteOrchestrator.DeprovisionAsync(SyncProvision.StoredProcedures | SyncProvision.Triggers);

    // use whatever you want to edit your schema
    // add column on server
    using (SqlConnection cs = serverProvider.CreateConnection() as SqlConnection)
    {
        cs.Open();
        SqlCommand cmd = new SqlCommand("ALTER TABLE dbo.Customers ADD Comments nvarchar(50) NULL", cs);
        cmd.ExecuteNonQuery();
        cs.Close();
    }
    // add column on client
    using (SqlConnection cs = clientProvider.CreateConnection() as SqlConnection)
    {
        cs.Open();
        SqlCommand cmd = new SqlCommand("ALTER TABLE dbo.Customers ADD Comments nvarchar(50) NULL", cs);
        cmd.ExecuteNonQuery();
        cs.Close();
    }

    // Get schema from server side
    var schema = await remoteOrchestrator.GetSchemaAsync();

    // Provision again
    await serverProvider.ProvisionAsync(schema, SyncProvision.StoredProcedures | SyncProvision.Triggers);
    await clientProvider.ProvisionAsync(schema, SyncProvision.StoredProcedures | SyncProvision.Triggers);

    // sync !
    await this.SynchronizeAsync();
}
```







