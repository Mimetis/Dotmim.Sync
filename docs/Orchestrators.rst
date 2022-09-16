Orchestrators
================================

Overview
^^^^^^^^^^

| An **Orchestrator** is agnostic to the underlying database. 
| it communicates with the database through a provider. A provider is always required when you’re creating a new orchestrator. 

We have two kind of orchestrators: 

* The **Local Orchestrator** (or let’s say client side orchestrator) : ``LocalOrchestrator``.
* The **Remote Orchestrator** (or let’s say server side orchestrator) : ``RemoteOrchestrator``.

We have other orchestrators, that will handle, under the hood, the web sync process:

* The ``WebRemoteOrchestrator``: This orchestrator will run locally, and will act "*as*" an orchestrator from the sync agent, but under the hood will generate an http request with a payload containing all the required information, and will send it to the server side.
* The ``WebServerAgent``: On the opposite side, this agent is hosted with an ASP.NET WebApi an is exposed by a web api, and will get the incoming request from the ``WebRemoteOrchestrator`` and will then call the underline ``RemoteOrchestrator`` correctly.

A set of methods are accessible from both ``LocalOrchestrator`` or ``RemoteOrchestrator`` (and for some of them from ``WebRemoteOrchestrator``).

* Database builder methods: Create tables, metadatas, tracking tables ...
* Sync changes methods: (Get changes, get estimated changes count ...)
* Tracking Tables
* Tables
* Schemas
* Scopes


Builder Methods
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You need a ``ScopeInfo`` instance to be able to create any metadatas (stored proc, tables, triggers or tracking tables) in your data source.


This method runs on any ``Orchestrator``, but we are using here a ``RemoteOrchestrator`` because the client database is empty and getting a table schema from an empty database... well.. :)

.. code-block:: csharp

    var provider = new SqlSyncProvider(serverConnectionString);
    var options = new SyncOptions();
    var setup = new SyncSetup("ProductCategory", "ProductModel", "Product");
    var orchestrator = new RemoteOrchestrator(provider, options);

    var serverSchema = await orchestrator.GetSchemaAsync(setup);

    foreach (var column in serverSchema.Tables["Product"].Columns)
        Console.WriteLine(column);


.. code-block:: bash

    ProductID - Int32
    Name - String
    ProductNumber - String
    Color - String
    StandardCost - Decimal
    ListPrice - Decimal
    Size - String
    Weight - Decimal
    ProductCategoryID - Int32
    ProductModelID - Int32
    SellStartDate - DateTime
    SellEndDate - DateTime
    DiscontinuedDate - DateTime
    ThumbNailPhoto - Byte[]
    ThumbnailPhotoFileName - String
    rowguid - Guid
    ModifiedDate - DateTime


Managing stored procedures
----------------------------------

Managing **Stored Procedures** could be done using:

* ``LocalOrchestrator.CreateStoredProcedureAsync()`` : Create a stored procedure using the ``DbStoredProcedureType`` enumeration, for one ``SetupTable`` argument.
* ``LocalOrchestrator.ExistStoredProcedureAsync()``: Check if a stored procedure already exists, using the ``DbStoredProcedureType`` enumeration, for one ``SetupTable`` argument.
* ``LocalOrchestrator.DropStoredProcedureAsync()`` : Drop a stored procedure using the ``DbStoredProcedureType`` enumeration, for one ``SetupTable`` argument.
* ``LocalOrchestrator.CreateStoredProceduresAsync()`` : Create all stored procedures needed for one ``SetupTable`` argument.
* ``LocalOrchestrator.DropStoredProceduresAsync()`` : Drop all stored procedures created for one ``SetupTable`` argument.

Creating a stored procedure could be done like this:

.. code-block:: csharp

    var provider = new SqlSyncProvider(serverConnectionString);
    var options = new SyncOptions();
    var setup = new SyncSetup(new string[] { "ProductCategory", "ProductModel", "Product" });
    var orchestrator = new RemoteOrchestrator(provider, options, setup);

    // working on the product Table
    var productSetupTable = setup.Tables["Product"];

    var spExists = await orchestrator.ExistStoredProcedureAsync(productSetupTable, 
                    DbStoredProcedureType.SelectChanges);
    if (!spExists)
        await orchestrator.CreateStoredProcedureAsync(productSetupTable, 
                    DbStoredProcedureType.SelectChanges);

.. image:: https://user-images.githubusercontent.com/4592555/103882421-11683000-50dc-11eb-8805-d2fe79342f12.png


Be careful, this stored procedure relies on a tracking table for table ``Product``, but we did not create it, yet.

Creating a tracking table
--------------------------------

Continuing on the last sample, we can create in the same way, the tracking table for table `Product`:

.. code-block:: csharp

    var provider = new SqlSyncProvider(serverConnectionString);
    var options = new SyncOptions();
    var setup = new SyncSetup(new string[] { "ProductCategory", "ProductModel", "Product" });
    var orchestrator = new RemoteOrchestrator(provider, options, setup);

    // working on the product Table
    var productSetupTable = setup.Tables["Product"];

    var spExists = await orchestrator.ExistTrackingTableAsync(productSetupTable);
    if (!spExists)
        await orchestrator.CreateTrackingTableAsync(productSetupTable);

.. image:: https://user-images.githubusercontent.com/4592555/103882789-99e6d080-50dc-11eb-824d-47e564a91fa7.png


Droping a tracking table and a stored procedure
--------------------------------------------------

Now we can drop this newly created stored procedure and tracking table:

.. code-block:: csharp

    var trExists = await orchestrator.ExistTrackingTableAsync(productSetupTable);
    if (trExists)
        await orchestrator.DropTrackingTableAsync(productSetupTable);

    var spExists = await orchestrator.ExistStoredProcedureAsync(productSetupTable, 
                            DbStoredProcedureType.SelectChanges);
    if (spExists)
        await orchestrator.DropStoredProcedureAsync(productSetupTable, 
                            DbStoredProcedureType.SelectChanges);



LocalOrchestrator
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The local orchestrator runs only on the client side. You have access to several useful methods to get the changes to send on the next sync, or even an estimation of these changes.


GetChangesAsync
-------------------

Get the changes from local datasource, to be sent to the server.

| You need to provide a ``ScopeInfoClient`` instance to be able to get the changes.
| Returns an instance of ``ClientSyncChanges`` containing a reference to the changes serialized on disk.

.. code-block:: csharp

    var localOrchestrator = new LocalOrchestrator(clientProvider);
    var cScopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync(scopeName, parameters);
    var changes = await localOrchestrator.GetChangesAsync(cScopeInfoClient);


If you need to load all changes in memory, you can use ``LoadTableFromBatchInfoAsync`` method:

GetEstimatedChangesCountAsync
--------------------------------

Get the estimated changes count from local datasource, to be sent to the server.

| You need to provide a ``ScopeInfoClient`` instance to be able to get the changes.
| Returns an instance of ``ClientSyncChanges`` containing a reference to the changes serialized on disk.
| The propery ``ClientChangesSelected`` (of type ``DatabaseChangesSelected``) from the returned ``ClientSyncChanges`` value, contains the estimated changes count.

.. warning:: No changes are downloaded, so far the ``ClientBatchInfo`` property is always **null**.

.. code-block:: csharp

    var localOrchestrator = new LocalOrchestrator(clientProvider);
    var cScopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync(scopeName, parameters);
    var estimatedChanges = await localOrchestrator.GetEstimatedChangesCountAsync(cScopeInfoClient);

    Console.WriteLine(estimatedChanges.ClientChangesSelected.TotalChangesSelected);

    foreach (var table in changes.ClientChangesSelected.TableChangesSelected)
        Console.WriteLine($"Table: {table.TableName} - Total changes:{table.TotalChanges}");



LoadTableFromBatchInfoAsync
-----------------------------------

Load a table from a batch info. This method is used to load all rows contains in a ``BatchInfo`` instance in memory.

You can specify a ``SyncRowState`` parameter to get rows with a specific state.

.. code-block:: csharp

    var localOrchestrator = new LocalOrchestrator(clientProvider);
    // Loading all rows for table SalesLT.SalesOrderDetail, with a state fo Deleted:
    var sodTable = await localOrchestrator.LoadTableFromBatchInfoAsync(
                scopeName, batchInfo, "SalesOrderDetail", "SalesLT", SyncRowState.Deleted);

    foreach (var orderDetail in sodTable.Rows)
        Console.WriteLine(orderDetail["TotalLine"]);


LoadBatchInfosAsync
-------------------------

Load all batch infos for a given scope name. The batch infos are loaded from the tmp directory set from ``SyncOptions.BatchDirectory``.

.. code-block:: csharp

    var localOrchestrator = new LocalOrchestrator(clientProvider);
    var batchInfos = await localOrchestrator.LoadBatchInfosAsync();
        
    foreach (var batchInfo in batchInfos)
        Console.WriteLine(batchInfo.RowsCount);


LoadTablesFromBatchInfoAsync
-----------------------------------

Load all tables from a batch info. This method is used to load all tables contains in a ``BatchInfo`` instance in memory.

Each file contained in the BatchInfo instance is loaded in memory, and returned as a ``SyncTable`` instance.

.. warning:: this method returns an ``IAsyncEnumerable<SyncTable>``. You need to iterate on it using the ``async`` keyword to get all tables.

.. code-block:: csharp

    var localOrchestrator = new LocalOrchestrator(clientProvider);
    var batchInfos = await localOrchestrator.LoadBatchInfosAsync();

    foreach (var batchInfo in batchInfos)
    {
        var allTables = localOrchestrator.LoadTablesFromBatchInfoAsync(batchInfo);

        // Enumerate all rows from each table
        await foreach (var table in allTables)
            foreach (var row in table.Rows)
                Console.WriteLine(row);
    }


GetSchemaAsync
------------------

Get the schema from the local datasource.

Be careful:

- ``GetScopeInfo()`` returns a ScopeInfo object, which contains the schema of the local database, saved in the :guilabel:`scope_info` table.
- ``GetSchema()`` returns a SyncSet object, which contains the schema of the local database, read on the fly.

Internally **DMS** is using GetSchema whenever it's needed, and eventually saved the schema in the :guilabel:`scope_info` table.

Using ``GetSchema()`` will not save the schema anywhere.

.. code-block:: csharp

    var localOrchestrator = new LocalOrchestrator(clientProvider);
    var setup = new SyncSetup("ProductCategory", "Product");
    var schema = await localOrchestrator.GetSchemaAsync(setup);


ProvisionAsync
------------------

Provision the local datasource with the tracking tables, stored procedures, triggers and even tables needed for the sync process.

| You need a ``ScopeInfo`` instance to be able to provision the local database.
| If you do not specify the ``provision`` argument, a default value ``SyncProvision.Table | SyncProvision.StoredProcedures | SyncProvision.Triggers | SyncProvision.TrackingTable`` is used.

Usually, the ScopeInfo instance is retrieved from your server database, using a ``RemoteOrchestrator`` or a ``WebRemoteOrchestrator`` instance.

.. code-block:: csharp

    var remoteOrchestrator = new RemoteOrchestrator(serverProvider);
    var sScopeInfo = await remoteOrchestrator.GetScopeInfoAsync();
    var cScopeInfo = await localOrchestrator.ProvisionAsync(sScopeInfo);

| If you have already done a first sync (or a first provision) of your client database, you can use the ``GetScopeInfoAsync`` method to get the ScopeInfo instance from your client database instead of your server database.
| Provision an already provisioned local database can be useful if you want to overwrite / recreate everything.

.. WARNING:: Be careful, the client database may not contains a ScopeInfo instance if you have not done a first sync.

.. code-block:: csharp

    var localOrchestrator = new LocalOrchestrator(clientProvider);
    var cScopeInfo = await localOrchestrator.GetScopeInfoAsync();
    if (cScopeInfo != null)
        cScopeInfo = await localOrchestrator.ProvisionAsync(cScopeInfo, overwrite:true);

.. admonition:: More ...

   Check the `Provision & Deprovision </Provision.html>`_ section for more details about the provision process.


DeprovisionAsync
----------------------

Deprovision the local datasource. This will drop tracking tables, stored procedures or triggers created by the sync process.

.. note:: By default, **DMS** will never deprovision a table, if not explicitly set with the **provision** argument. 
    
    Same behavior applies to the :guilabel:`scope_info` and :guilabel:`scope_info_client`  tables.


.. code-block:: csharp
    
    var localOrchestrator = new LocalOrchestrator(clientProvider);
    await localOrchestrator.DeprovisionAsync(SyncProvision.StoredProcedures | SyncProvision.Triggers);


If you do not have any scope info locally (the :guilabel:`scope_info` table does not exists anymore, or is empty), you still can try to deprovision your local database using a simple ``SyncSetup`` instance:

.. code-block:: csharp

    var localOrchestrator = new LocalOrchestrator(clientProvider);
    var setup = new SyncSetup("ProductCategory", "Product");
    await localOrchestrator.DeprovisionAsync(setup, 
                SyncProvision.StoredProcedures | SyncProvision.Triggers);

.. admonition:: More ...

   Check the `Provision & Deprovision </Provision.html>`_ section for more details about the provision process.


DropAllAsync
----------------

Drop all DMS metadatas from your local database, except tables. Everythin is dropped: **tracking tables**, **stored procedures**, **triggers**, **scope info tables**, etc.

.. code-block:: csharp
    
    var localOrchestrator = new LocalOrchestrator(clientProvider);
    await localOrchestrator.DropAllAsync();

DeleteMetadatasAsync
---------------------------

| Delete all DMS metadatas from the tracking tables, in your local database.
| This operation is automatically managed by DMS on the client side. You should not have to use it manually, except on specific scenario.

.. code-block:: csharp

    var localOrchestrator = new LocalOrchestrator(clientProvider);
    await localOrchestrator.DeleteMetadatasAsync();

.. admonition:: More ...

   Check the `Metadatas </Metadatas.html>`_ section for more details about the metadatas deletion process.

ResetTableAsync
---------------------

Delete all rows from a **table** and the corresponding **tracking table**.

This method is used internall

.. code-block:: csharp

    var localOrchestrator = new LocalOrchestrator(clientProvider);
    var scopeInfo = await localOrchestrator.GetScopeInfoAsync();
    await localOrchestrator.ResetTableAsync(scopeInfo, "ProductCategory");

.. warning:: Be careful, this method will delete all rows from your table !!


EnableConstraintsAsync & DisableConstraintsAsync
------------------------------------------------------------

**Enable** or **Disable** all constraints on your local database.

Useful if you want to apply rows without having to check any constraints.

This method is used internally by **DMS** when you are using the ``SyncOptions.DisableConstraintsOnApplyChanges`` option.

.. code-block:: csharp

    var localOrchestrator = new LocalOrchestrator(clientProvider);

    using var sqlConnection = new SqlConnection(clientProvider.ConnectionString);

    sqlConnection.Open();
    using var sqlTransaction = sqlConnection.BeginTransaction();

    var scopeInfo = await localOrchestrator.GetScopeInfoAsync(sqlConnection, sqlTransaction);
    await localOrchestrator.DisableConstraintsAsync(scopeInfo, "ProductCategory", default,
        sqlConnection, sqlTransaction);

    // .. Do some random insert in the ProductCategory table
    await DoSomeRandomInsertInProductCategoryTableAsync(sqlConnection, sqlTransaction);

    await localOrchestrator.EnableConstraintsAsync(scopeInfo, "ProductCategory", default,
        sqlConnection, sqlTransaction);

    sqlTransaction.Commit();
    sqlConnection.Close();

GetLocalTimestampAsync
------------------------------

Get the local timestamp from the local database.

.. code-block:: csharp

    var localOrchestrator = new LocalOrchestrator(clientProvider);
    var ts = await localOrchestrator.GetLocalTimestampAsync();
    