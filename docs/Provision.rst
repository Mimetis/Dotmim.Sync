Provision,Deprovision and Migration
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

You have 2 new methods on both orchestrators:

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
    
    // If you are on a web sync architecture, you can use the WebClientOrchestrator as well:
    // var remoteOrchestrator = new WebClientOrchestrator(serviceUri)

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

What Setup migration doesn't do !
-----------------------------------

Be careful, the migration stuff will **only** allows you to migrate your setup (adding or removing tables from your sync, renaming stored proc and so on ...)

**You can't use it to migrate your own schema database !!**

Well, it could work if:

* You are **adding** a new table : Quite easy, just add this table to your `SyncSetup` and you're done.
* You are **removing** a table: Once again, easy, remove it from your `SyncSetup`, and you're good to go

But, it won't work if:

* You are **removing** or **adding** a column from a table on your server: You **can't** use this technic to migrate your clients database.

| **DMS** won't be able to make an ``Alter table`` to add / remove columns. 
| Too complicated to handle, too much possibilities and scenario.

If you have to deal with this kind of situation, the best solution is to handle this migration by yourself using ``ProvisionAsync`` and ``DeprovisionAsync`` methods.


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

**DMS** has provisioned:

* One tracking table per table from your setup.
* Three triggers on each table.
* Several stored procedures for each table.


Provision
^^^^^^^^^^^

In some circumstances, you may want to provision manually your database (server or client as well):

* If you have a really big database, the provision step could be really long, so it could be better to provision the server side before any sync process happens.
* If you have to modify your schema. You will have to **deprovision** then **edit** your schema and finally **provision** again your database.

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


For instance, here is the code you need to implement to be able to provision a database :

.. code-block:: csharp

    var serverProvider = new SqlSyncProvider(serverConnectionString);
    var clientProvider = new SqlSyncProvider(clientConnectionString);

    // Create a local orchestrator, to manage the local database
    var localOrchestrator = new LocalOrchestrator(clientProvider, options, setup);

    // Create a remote orchestrator, to manage hub server database
    var remoteOrchestrator = new RemoteOrchestrator(serverProvider, options, setup);

    // Get schema from server side
    var schema = await remoteOrchestrator.GetSchemaAsync();

    var provision = SyncProvision.ClientScope | SyncProvision.StoredProcedures 
        | SyncProvision.Table | SyncProvision.TrackingTable | SyncProvision.Triggers;

    // provision the local database
    await localOrchestrator.ProvisionAsync(schema, provision);



Deprovision
^^^^^^^^^^^^^^

Like provisioning, deprovisioning uses basically the same method.
We don't need the full schema to be able to deprovision a table, so far, a ``SyncSetup`` instance is enough:

.. code-block:: csharp

    var serverProvider = new SqlSyncProvider(serverConnectionString);
    var clientProvider = new SqlSyncProvider(clientConnectionString);

    var setup = new SyncSetup(new string[]{"Product", "ProductCategory"});

    // Create a local orchestrator, to manage the local database
    var localOrchestrator = new LocalOrchestrator(clientProvider, new SyncOptions(), setup);

    var provision = SyncProvision.ClientScope | SyncProvision.StoredProcedures 
        | SyncProvision.Table | SyncProvision.TrackingTable | SyncProvision.Triggers;

    // provision the local database
    await localOrchestrator.DeprovisionAsync(schema, provision);


Migrating a database schema
^^^^^^^^^^^^^^^^^^^^^^^^^^^^


| During any dev cycle, you will probably have to make some evolutions on your server database.  
| Adding or deleting columns will break the sync process.  
| Manually, without the ``ProvisionAsync()`` and ``DeprovisionAsync()`` methods, you will have to edit all the stored procedures, triggers and so on to be able to recreate a full sync processus.  

| We are going to handle, with a little example, how we could add a new column on an existing sync typo:
| Now imagine you want to add this new column, called ``Comments`` on the ``Customers`` table.   
| Here is how you could handle it:

* Create the providers.
* Delete the stored procedures and the triggers.
* We don't want to loose the ``Customers_tracking`` tracking table rows, to keep the sync historic, so we don't delete it. Here we have to be sure the primary key from ``Customers`` is still the same (So don't touch primary keys or this technic won't work)
* Edit the client and server schema (you can use here the EF migration)
* Re apply the triggers and the stored procédures. They will be re-generated with the new column !
* Re launch a new sync process (don't use the same ``SyncAgent`` as before, recreate a new one).

.. code-block:: csharp

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








