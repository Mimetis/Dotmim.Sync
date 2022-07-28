Orchestrators
================================

Overview
^^^^^^^^^^

| An **Orchestrator** is agnostic to the underlying database. 
| it communicates with the database through a provider. A provider is always required when you’re creating a new orchestrator. 

We have two kind of orchestrators: 

* Local Orchestrator (or let’s say client side orchestrator) : ``LocalOrchestrator``.
* Remote Orchestrator or let’s say server side orchestrator) : ``RemoteOrchestrator``.

We have to more kind of orchestrators, that will handle under the hood the web sync process:

* The ``WebRemoteOrchestrator``: This orchestrator will run locally, and will act "*as*" an orchestrator from the sync agent, but under the hood will generate an http request with a payload containing all the required information
* The ``WebServerAgent``: On the opposite side, this agent is hosted through an exposed web api, and will get the incoming request from the ``WebRemoteOrchestrator`` and will then call the underline ``RemoteOrchestrator`` correctly.


Orchestrators public methods
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A set of methods are accessible from both ``LocalOrchestrator`` or ``RemoteOrchestrator`` (and for some of them from ``WebRemoteOrchestrator``).

Generaly, you have access to three methods (``Create_XXX``, ``Drop_XXX``, ``Exists_XXX``) for all the core components :

* Stored Procedures
* Triggers
* Tracking Tables
* Tables
* Schemas
* Scopes

Here is some examples using these methods:

Get a table schema
----------------------

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

