Provision, Deprovision & Migration
===================================

Overview
^^^^^^^^^^^

| Since your sync architecture will evolve over the time, you may need to update the sync generated code as well. 
| Regarding the **DMS** architecture, we have two situations, the first one is automatically handled by **DMS** and the other one, not. 
| Fortunately, **DMS** provides some useful methods for all these scenario.

We have to distinguish 2 mains reasons to make an *update* of your databases schemas:

* First, you are modifying your sync setup, represented by a ```SyncSetup`` instance.
    
  * Adding or removing a table, modifying prefix or suffix used in stored procedure or triggers..

* Second, you are modifying your schema, your own tables, by adding or removing a column.

| In the first case, **DMS** will be able to compare the existing ``SyncSetup`` saved in one **DMS** table, with the new one you are providing.
| Once **DMS** concludes there is a difference between the old and the new setup, it will run an automatic **migration**. 

| On the other hand, the second case is more tricky, since there is no way for **DMS** to see the difference between the old table schema and the new one.
| It will be your responsability to **deprovision** and then **provision** again all the **DMS** infrastructure relative to this table.

.. note:: * Editing a ``SyncSetup`` setup: Automatic **migration** handled by **DMS**
          * Editing a table schema: Your responsability to **deprovision** then **provision** again the **DMS** infrastructure.


Migration
^^^^^^^^^^^^^^^

| Firstly, let's see a common scenario that are handled automatically by **DMS**.
| As we said in the overview, the **migration** occurs when you are modifying your setup instance.

For instance going from this:

.. code-block:: csharp

    var setup = new SyncSetup(new string[] { "ProductCategory", "Product" })


to this:

.. code-block:: csharp

    var setup = new SyncSetup(new string[] { "ProductCategory", "Product", "ProductDescription" })


**DMS** will automaticaly migrate your whole **old** setup to match your **new** setup.

This migration is handled for you automatically, once you've called the method ``await agent.SynchronizeAsync();``

Basically, **DMS** will make a comparison between the **last valid** Setup:

* Stored in the ``scope_info`` table on the local database 
* Stored in the ``scope_info_server`` for the server side database

And then will merge the databases, adding (or removing) *tracking tables*, *stored procedures*, *triggers* and *tables* if needed


Adding a table
---------------------

Going from this:

.. code-block:: csharp

    var setup = new SyncSetup(new string[] { "ProductCategory", "Product" })

to this:

.. code-block:: csharp

    var setup = new SyncSetup(new string[] { "ProductCategory", "Product", "ProductDescription" })


Will generate:

* A new table `ProductDescription` on the client
* A new tracking table `ProductDescription_tracking` on the client and the server
* New **stored procedures** on both databases
* New **triggers** on both databases

Editing the prefix or suffix
----------------------------


Going from this:

.. code-block:: csharp

    var setup = new SyncSetup(new string[] { "ProductCategory", "Product" })


to this:

.. code-block:: csharp

    var setup = new SyncSetup(new string[] { "ProductCategory", "Product" })
    {
        TrackingTablesPrefix = "t",
        TrackingTablesSuffix = "",
    };

Will generate:

* A renaming of the trackings tables on both databases

**AND** because renaming the trackings tables will have an impact on triggers and stored proc ..

* A drop / create of all stored procedures
* A drop / create of all triggers


Orchestrators methods
--------------------------

First of all, if you are just using ``agent.SynchronizeAsync()``, everything will be handled automatically.  

But you can use the **orchestrators** to do the job. It will allow you to migrate your setup, without having to make a synchronization.

You have one new method on both orchestrators:

On ``LocalOrchestrator``:

.. code-block:: csharp

    public virtual async Task MigrationAsync(SyncSetup oldSetup, SyncSet schema)


| Basically, you need the old setup to migrate ``oldSetup``, and the new ``schema``. 
| You don't need the new ``Setup`` because you have already add it when you have initiliaed your ``LocalOrchestrator`` instance (it's a mandatory argument in the constructor).

.. hint:: Why do you need the ``schema`` ? If you are adding a new table, which is potentially not present locally, we need the schema from the server side, to get the new table structure.

Here is an example, using this method on your local database:

.. code-block:: csharp

    // adding 2 new tables
    var newSetup = new SyncSetup(new string[] { "ProductCategory", "Product", 
             "ProdutModel", "ProductDescription" });

    // creaete a local orchestrator
    var localOrchestrator = new LocalOrchestrator(clientProvider, options, setup);

    // create remote orchestrator to get the schema for the 2 new tables to add
    var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options, setup);
    
    // If you are on a web sync architecture, you can use the WebRemoteOrchestrator as well:
    // var remoteOrchestrator = new WebRemoteOrchestrator(serviceUri)

    // get the old setup
    var scopeInfo = await localOrchestrator.GetClientScopeAsync();
    var oldSetup = scopeInfo.Setup;

    // get the schema from server side
    var schema = await remoteOrchestrator.GetSchemaAsync();

    // Migrating the old setup to the new one, using the schema if needed
    await localOrchestrator.MigrationAsync(oldSetup, schema);


On ``RemoteOrchestrator``:

.. code-block:: csharp

    public virtual async Task MigrationAsync(SyncSetup oldSetup)

Basically, it's the same method as on `LocalOrchestrator` but we don't need to pass a schema, since we are on the server side, and we know how to get the schema :)

The same example will become:

.. code-block:: csharp

    // adding 2 new tables
    var newSetup = new SyncSetup(new string[] { "ProductCategory", "Product", 
            "ProdutModel", "ProductDescription" });

    // remote orchestrator to get the schema for the 2 new tables to add
    var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options, setup);

    // get the old setup
    var serverScopeInfo = await remoteOrchestrator.GetServerScopeAsync();
    var oldServerSetup = serverScopeInfo.Setup;

    // Migrating the old setup to the new one, using the schema if needed
    await remoteOrchestrator.MigrationAsync(oldServerSetup);


For instance, the ``RemoteOrchestrator`` ``MigrationAsync`` could be really useful if you want to migrate your server database, when configuring as **HTTP** mode.

Once migrated, all new clients, will get the new setup from the server, and will apply locally the migration, automatically.

What Setup migration does not do !
-----------------------------------

Be careful, the migration stuff will **only** allows you to migrate your setup (adding or removing tables from your sync, renaming stored proc and so on ...)

**You can't use it to migrate your own schema database !!**

Well, it could work if:

* You are **adding** a new table : Quite easy, just add this table to your ``SyncSetup`` and you're done.
* You are **removing** a table: Once again, easy, remove it from your ``SyncSetup``, and you're good to go.


But, it won't work if:

* You are **removing** or **adding** a column from a table on your server: You **can't** use this technic to migrate your clients database.

| **DMS** won't be able to make an ``Alter table`` to add / remove columns. 
| Too complicated to handle, too much possibilities and scenario.

If you have to deal with this kind of situation, the best solution is to handle this migration by yourself using ``ProvisionAsync`` and ``DeprovisionAsync`` methods.


Last timestamp sync
-----------------------

| What happens if you're adding a new table ?
| What will happen on the next sync ?  
| Well as we said the new table will be provisioned (stored proc, triggers and tracking table) on both databases (server / client) and the table will be created on the client.

Then the **DMS** framework will make a sync ....

And this sync will get all the rows from the server side **that have changed since the last sucessful sync**

And your new table on the client database has ... **NO ROWS** !!!

| Because no rows have been marked as **changed** in the server tracking table since the last sync process.
| Indeed, we've just created this tracking table on the server !!

So, if you're adding a new table, you **MUST** do a full sync, calling the ``SynchronizeAsync()`` method with a ``SyncType.Reinitialize`` or  ``SyncType.ReinitializeWithUpload`` parameter.

| Adding a new table is not trivial.   
| Hopefully if you are using ``snapshots`` it should not be too heavy for your server database :) 

Forcing Reinitialize sync type from server side.
-------------------------------------------------

| As we saw, it could be useful to force a reinitialize from a client, to get all the needed data.   
| Unfortunatelly, you should have a *special* routine from the client side, to launch the synchronization with a ``SynchronizeAsync(SyntType.Reinitialize)``, like an admin button or whatever.   

Fortunatelly, using an interceptor, from the **server side**, you are able to *force* the reinitialization from the client.

On the server side, from your controller, just modify the request ``SyncContext`` with the correct value, like this:

.. code-block:: csharp

    [HttpPost]
    public async Task Post()
    {
        // override sync type to force a reinitialization from a particular client
        orchestrator.OnServerScopeLoaded(sla =>
        {
            // ClientId represents one client. If you want to reinitialize ALL clients, 
            // just remove this condition
            if (sla.Context.ClientScopeId == clientId)
            {
                sla.Context.SyncType = SyncType.Reinitialize;
            }
        });

        // handle request
        await orchestrator.HandleRequestAsync(this.HttpContext);
    }


Provision / Deprovision
^^^^^^^^^^^^^^^^^^^^^^^^

The ``ProvisionAsync`` and ``DeprovisionAsync`` methods are used internally by **DMS**

For instance, during the first sync, **DMS** will provision everything, on the server side and on the client side.

When you launch for the first time a sync process, **DMS** will:

- **[Server Side]**: Get the database schema from the server database.
- **[Server Side]**: Create **Stored procedures**, **triggers** and **tracking tables**.
- **[Client Side]**: Fetch the server schema.
- **[Client Side]**: Create **tables** on the client database, if needed.
- **[Client Side]**: Create **Stored procedures**, **triggers** and **tracking tables**

.. note:: If you're using the ``SqlSyncChangeTrackingProvider``, **DMS** will skip the creation of triggers and tracking tables, relying on the *Change Tracking* feature from SQL Server.

| Basically, all these steps are managed by the ``RemoteOrchestrator`` on the server side, and by the ``LocalOrchestrator`` on the client side. 
| All the methods used to provision and deprovision tables are available from both the ``LocalOrchestrator`` and ``RemoteOrchestrator`` instances.


.. code-block:: csharp

    public async Task<SyncSet> ProvisionAsync(SyncProvision provision)
    public async Task<SyncSet> ProvisionAsync(SyncSet schema, SyncProvision provision)
 
    public async Task DeprovisionAsync(SyncProvision provision)
    public virtual async Task DeprovisionAsync(SyncSet schema, SyncProvision provision)


Let's start with a basic example, where you have a simple database containing two tables *Customers* and *Region*:

.. image:: assets/Provision_Northwind01.png
    :alt: provision


And here the most straightforward code to be able to sync a client db :

.. code-block:: csharp

    SqlSyncProvider serverProvider = new SqlSyncProvider(GetDatabaseConnectionString("Northwind"));
    SqlSyncProvider clientProvider = new SqlSyncProvider(GetDatabaseConnectionString("NW1"));

    SyncAgent agent = new SyncAgent(clientProvider, serverProvider, new string[] {
    "Customers", "Region"});

    var syncContext = await agent.SynchronizeAsync();

    Console.WriteLine(syncContext);


Once your sync process is finished, you will have a full configured database :

.. image:: assets/Provision_Northwind02.png
    :alt: provision

**DMS** has provisioned:

* One tracking table per table from your setup.
* Three triggers on each table.
* Several stored procedures for each table.


Provision
-------------

In some circumstances, you may want to provision manually your database, on the server using a remote orchestrator, or on the client side using a local orchestrator.

* If you have a really big database, the provision step could be really long, so it could be better to provision the server side before any sync process happens.
* If you have to modify your schema, you will have to **deprovision**, **edit** your schema and finally **provision** again your database.

That's why **DMS** exposes several methods to let you control how, and when, you want to provision and deprovision your database.

Each orchestrator has two main methods, basically:

.. code-block:: csharp

    ProvisionAsync(SyncSet schema, SyncProvision provision)
    DeprovisionAsync(SyncSet schema, SyncProvision provision)

The ``SyncProvision`` enum parameter lets you decide which kind of objects (tables, stored proc, triggers or tracking tables) you will provision on your target database.

.. code-block:: csharp

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


.. warning:: Each time you are provisioning or deprovisioning your local / server database, do not forget to update the scope tables:

             * **scope_info** table from local orchestrator using the ``WriteClientScopeAsync`` method.
             * **scope_info_server** table from remote orchestrator using the ``WriteServerScopeAsync`` method.

             It's important to stay synchronized between your actual database schema, and the metadata contained in the scope tables.
             

The remote (server side) provisioning is quite simple, since the schema is already there. 

| But the local (client side) provisioning could a little bit more tricky since we may miss tables. 
| In that particular case, we will rely on the schema returned by the remote orchestrator.


.. hint:: You will find this complete sample here : `Provision & Deprovision sample <https://github.com/Mimetis/Dotmim.Sync/tree/master/Samples/ProvisionDeprovision>`_ 


Provisioning from server side, using a remote orchestrator:

.. code-block:: csharp

    var serverProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString(serverDbName));

    // Create standard Setup and Options
    var setup = new SyncSetup(new string[] { "Address", "Customer", "CustomerAddress" });
    var options = new SyncOptions();

    // -----------------------------------------------------------------
    // Server side
    // -----------------------------------------------------------------

    // This method is useful if you want to provision by yourself the server database
    // You will need to :
    // - Create a remote orchestrator with the correct setup to create
    // - Provision everything

    // Create a server orchestrator used to Deprovision and Provision only table Address
    var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options, setup);

    // Provision everything needed (sp, triggers, tracking tables)
    // Internally provision will fectch the schema a will return it to the caller. 
    var newSchema = await remoteOrchestrator.ProvisionAsync();


Provision on the client side is quite similar, despite the fact we will rely on the server schema to create any missing table.

.. code-block:: csharp

    // Create 2 Sql Sync providers
    var serverProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString(serverDbName));
    var clientProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString(clientDbName));

    // Create standard Setup and Options
    var setup = new SyncSetup(new string[] { "Address", "Customer", "CustomerAddress" });
    var options = new SyncOptions();

    // -----------------------------------------------------------------
    // Client side
    // -----------------------------------------------------------------

    // This method is useful if you want to provision by yourself the client database
    // You will need to :
    // - Create a local orchestrator with the correct setup to provision
    // - Get the schema from the server side using a RemoteOrchestrator or a WebRemoteOrchestrator
    // - Provision everything locally

    // Create a local orchestrator used to provision everything locally
    var localOrchestrator = new LocalOrchestrator(clientProvider, options, setup);

    // Because we need the schema from remote side, create a remote orchestrator
    var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options, setup);

    // Getting the schema from server side
    var serverSchema = await remoteOrchestrator.GetSchemaAsync();

    // At this point, if you need the schema and you are not able to create a RemoteOrchestrator,
    // You can create a WebRemoteOrchestrator and get the schema as well
    // var proxyClientProvider = new WebRemoteOrchestrator("https://localhost:44369/api/Sync");
    // var serverSchema = proxyClientProvider.GetSchemaAsync();

    // Provision everything needed (sp, triggers, tracking tables, AND TABLES)
    await localOrchestrator.ProvisionAsync(serverSchema);


Deprovision
-------------

Like provisioning, deprovisioning uses basically the same kind of algorithm.

.. hint:: We don't need the full schema to be able to deprovision a table, so far, a ``SyncSetup`` instance is enough to be able to deprovision a database.

.. warning:: Once again, do not forget to save the metadatas in the scope tables, if needed.

Deprovisioning from server side, using a remote orchestrator:

.. code-block:: csharp

    // Create server provider
    var serverProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString(serverDbName));

    // Create standard Setup and Options
    var setup = new SyncSetup(new string[] { "Address", "Customer", "CustomerAddress" });
    var options = new SyncOptions();

    // Create a server orchestrator used to Deprovision everything on the server side
    var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options, setup);

    // Deprovision everything
    await remoteOrchestrator.DeprovisionAsync();


Deprovisioning from client side, using a local orchestrator:

.. code-block:: csharp

    // Create client provider
    var clientProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString(clientDbName));

    // Create standard Setup and Options
    var setup = new SyncSetup(new string[] { "Address", "Customer", "CustomerAddress" });
    var options = new SyncOptions();

    // Create a local orchestrator used to Deprovision everything
    var localOrchestrator = new LocalOrchestrator(clientProvider, options, setup);

    // Deprovision everything
    await localOrchestrator.DeprovisionAsync();


Migrating a database schema
-----------------------------

| During any dev cycle, you will probably have to make some evolutions on your server database.  
| Adding or deleting columns will break the sync process.  
| Manually, without the ``ProvisionAsync()`` and ``DeprovisionAsync()`` methods, you will have to edit all the stored procedures, triggers and so on to be able to recreate a full sync processus.  

We are going to handle, with a little example, how we could add a new column on an already existing sync architecture:

.. hint:: You will find this complete sample here : `Migration sample <https://github.com/Mimetis/Dotmim.Sync/tree/master/Samples/Migration>`_ 


Basically, we can imagine having a sync process already in place:

.. code-block:: csharp

    // Create 2 Sql Sync providers
    var serverProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString(serverDbName));
    var clientProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString(clientDbName));

    // Create standard Setup and Options
    var setup = new SyncSetup(new string[] { "Address", "Customer", "CustomerAddress" });
    var options = new SyncOptions();

    // Creating an agent that will handle all the process
    var agent = new SyncAgent(clientProvider, serverProvider, options, setup);

    // First sync to have a starting point
    var s1 = await agent.SynchronizeAsync(progress);

    Console.WriteLine(s1);

Now, we are adding a new column on both side, in the **Address** table:

.. hint:: Here, using a tool like EF Migrations could be really useful.

.. code-block:: csharp

    // -----------------------------------------------------------------
    // Migrating a table by adding a new column
    // -----------------------------------------------------------------

    // Adding a new column called CreatedDate to Address table, on the server, and on the client.
    await AddNewColumnToAddressAsync(serverProvider.CreateConnection());
    await AddNewColumnToAddressAsync(clientProvider.CreateConnection());

Then, using ``ProvisionAsync`` and ``DeprovisionAsync`` we can handle the server side:

.. code-block:: csharp

    // -----------------------------------------------------------------
    // Server side
    // -----------------------------------------------------------------

    // Creating a setup regarding only the table Address
    var setupAddress = new SyncSetup(new string[] { "Address" });

    // Create a server orchestrator used to Deprovision and Provision only table Address
    var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options, setupAddress);

    // Deprovision the old Address triggers / stored proc. 
    // We can keep the Address tracking table, since we just add a column, 
    // that is not a primary key used in the tracking table
    // That way, we are preserving historical data
    await remoteOrchestrator.DeprovisionAsync(SyncProvision.StoredProcedures 
                    | SyncProvision.Triggers);

    // Provision the new Address triggers / stored proc again, 
    // This provision method will fetch the address schema from the database, 
    // so it will contains all the columns, including the new Address column added
    await remoteOrchestrator.ProvisionAsync(SyncProvision.StoredProcedures 
                    | SyncProvision.Triggers);


Then, on the client side, using the schema already in place:

.. code-block:: csharp

    // -----------------------------------------------------------------
    // Client side
    // -----------------------------------------------------------------
    
    // Creating a setup regarding only the table Address
    var setupAddress = new SyncSetup(new string[] { "Address" });

    // Now go for local orchestrator
    var localOrchestrator = new LocalOrchestrator(clientProvider, options, setupAddress);

    // Deprovision the Address triggers / stored proc. 
    // We can kepp the tracking table, since we just add a column, 
    // that is not a primary key used in the tracking table
    await localOrchestrator.DeprovisionAsync(SyncProvision.StoredProcedures 
                        | SyncProvision.Triggers);

    // Provision the Address triggers / stored proc again, 
    // This provision method will fetch the address schema from the database, 
    // so it will contains all the columns, including the new one added
    await localOrchestrator.ProvisionAsync(SyncProvision.StoredProcedures 
                        | SyncProvision.Triggers);


