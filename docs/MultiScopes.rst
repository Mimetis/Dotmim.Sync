MultiScopes
================================

Overview
^^^^^^^^^^

In some scenario, you may want to sync some tables at one time, and some others tables at another time.

For example, let's imagine we want to:

- Sync all the **Product** during a certain amount of time.
- Sync all the **Customer** and related **SalesOrderHeader**, once we sure all products are on the client database.

Or you can think about syncing a tables with a filter, but with different parameters:

- Sync all the **ProductCategories** and **Products** when **ProductCategoryID** is 'ROADFR'
- Sync all the **ProductCategories** and **Products** when **ProductCategoryID** is 'MOUNTB'

These kind of scenarios are possible using the **multi scopes** sync architecture.

How does it work ?
^^^^^^^^^^^^^^^^^^^^^^

On the client side, we store metadatas in the **scope_info** table.  

Imagine we have a really straightforward sync, using the default scope name (``DefaultScope``):

.. code-block:: csharp

    var serverProvider = new SqlSyncChangeTrackingProvider(serverConnectionString);
    var clientProvider = new SqliteSyncProvider(clientConnectionString);
    var setup = new SyncSetup("ProductCategory", "ProductModel", "Product",
        "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail" );

    var agent = new SyncAgent(clientProvider, serverProvider);
    var s1 = await agent.SynchronizeAsync(setup);
    
    // Equivalent to
    // var s1 = await agent.SynchronizeAsync("DefaultScope", setup);

Once the sync is complete, you will have in the client database, a table scope_info containing metadatas:

- A **scope Id**: Defines the Id of your client database (unique).
- A **scope name**: Defines a user friendly name (unique) for your scope. Default name is ``DefaultScope``.
- A **setup**, serialized: Contains all the tables and options you defined from your ``SyncSetup`` instance.
- A **schema**, serialized: Contains all the tables, filters, parameters and so on, for this scope.
- A local last timestamp: Defines the last time this scope was successfully synced with the server.
- A server last timestamp: Defines the last time this scope was successfully synced, but from a server point of view. 
- A duration: Amount of times for the last sync.

.. code-block:: sql

    SELECT TOP [sync_scope_id] ,[sync_scope_name] ,[sync_scope_schema] ,[sync_scope_setup]
      ,[sync_scope_version] ,[scope_last_server_sync_timestamp] ,[scope_last_sync_timestamp]
      ,[scope_last_sync_duration] ,[scope_last_sync]
  FROM [scope_info]


=============   ===============   =========================   =======================  ===================================
sync_scope_id   sync_scope_name   sync_scope_schema           sync_scope_setup         scope_last_server_sync_timestamp
-------------   ---------------   -------------------------   -----------------------  -----------------------------------
9E9722CD-...    DefaultScope      { "t" : [{......}] }        { "t" : [{......}] }     2589   
=============   ===============   =========================   =======================  ===================================

In a multi scopes scenario, we will have one line per scope, containing for each line a different setup / schema / timestamp & so on ...


Multi Scopes
^^^^^^^^^^^^^^^^^^^^^^

To be able to create a multi scopes scenario, you just have to:

- Create two ``SyncSetup`` instances with your tables / filters and options.
- Sync your databases calling ``SynchronizeAsync`` with a different scope name for each setup
- Or call ``ProvisionAsync`` with your scope name.

Here is a full example, where we sync separately the **Product** table, then the **Customer** table:

.. code-block:: csharp

    // Create 2 Sql Sync providers
    var serverProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString(serverDbName));
    var clientProvider = new SqlSyncProvider(DbHelper.GetDatabaseConnectionString(clientDbName));

    // Create 2 setup 
    var setupProducts = new SyncSetup("ProductCategory", "ProductModel", "Product");
    var setupCustomers = new SyncSetup("Address", "Customer", "CustomerAddress", 
                "SalesOrderHeader", "SalesOrderDetail");

    // Create an agent
    var agent = new SyncAgent(clientProvider, serverProvider);

    // Using the Progress pattern to handle progession during the synchronization
    var progress = new SynchronousProgress<ProgressArgs>(s =>
        Console.WriteLine($"{s.Context.SyncStage}:\t{s.Message}");
    );

    Console.WriteLine("Hit 1 for sync Products. Hit 2 for sync customers and sales");
    var k = Console.ReadKey().Key;

    if (k == ConsoleKey.D1)
    {
        Console.WriteLine("Sync Products:");
        var s1 = await agent.SynchronizeAsync("products", setupProducts, progress);
        Console.WriteLine(s1);
    }
    else
    {
        Console.WriteLine("Sync Customers and Sales:");
        var s1 = await agent.SynchronizeAsync("customers", setupCustomers, progress);
        Console.WriteLine(s1);
    }

Once you have made the 2 syncs, your local syns_scope table should looks like that:

=============   ===============   =========================   =======================  ===================================
sync_scope_id   sync_scope_name   sync_scope_schema           sync_scope_setup         scope_last_server_sync_timestamp
-------------   ---------------   -------------------------   -----------------------  -----------------------------------
9E9722CD-...    products          { "t" : [{......}] }        { "t" : [{......}] }     2589   
9E9722CD-...    customers         { "t" : [{......}] }        { "t" : [{......}] }     2592   
=============   ===============   =========================   =======================  ===================================

Here is another example, if you want to sync the same tables but with differents filters:

.. code-block:: csharp

    // create client orchestrator that is the same as server
    var clientDatabaseName = HelperDatabase.GetRandomName("tcpfilt_cli_");
    var clientProvider = this.CreateProvider(this.ServerType, clientDatabaseName);

    // create 1 setup only
    var setup = new SyncSetup("ProductCategory", "Product");

    // Customize columns
    setup.Tables[productCategoryTableName].Columns.AddRange(
        new string[] { "ProductCategoryId", "Name", "rowguid", "ModifiedDate" });

    // Add filters
    var productFilter = new SetupFilter("Product");
    productFilter.AddParameter("ProductCategoryID", "Product");
    productFilter.AddWhere("ProductCategoryID", "Product", "ProductCategoryID");

    var productCategoryFilter = new SetupFilter("ProductCategory");
    productCategoryFilter.AddParameter("ProductCategoryID", "ProductCategory");
    productCategoryFilter.AddWhere("ProductCategoryID", "ProductCategory", "ProductCategoryID");

    setup.Filters.Add(productCategoryFilter);
    setup.Filters.Add(productFilter);

    // ------------------------------------------------
    var paramMountb = new SyncParameters(("ProductCategoryID", "MOUNTB"));
    var paramRoadfr = new SyncParameters(("ProductCategoryID", "ROADFR"));

    // create agent with filtered tables and parameter
    var agent = new SyncAgent(clientProvider, Server.Provider, options);

    var rTourb = await agent.SynchronizeAsync("Mountb", setup, paramMountb);
    var rRoadfr = await agent.SynchronizeAsync("Roadfr", setup, paramRoadfr);
