Interceptors
=====================

``Ìnterceptor<T>`` : A more advanced technic to handle a lot of more events from within **DMS**

Overview
^^^^^^^^^^^^

The ``Progress<T>`` stuff is great, but as we said, it's mainly read only, and the progress is always reported **at the end of a current sync stage**.   

| So, if you need a more granular control on all the progress values, you can subscribe to an ``Interceptor<T>``.   
| On each **orchestrator**, you will find a lot of relevant methods to intercept the sync process, encapsulate in a fancy ``OnMethodAsync()`` method:

.. image:: assets/interceptor01.png


Imagine you have a table that should **never** be synchronized on one particular client (and is part of your ``SyncSetup``). You're able to use an interceptor like this:

.. code-block:: csharp

    // We are using a cancellation token that will be passed as an argument 
    // to the SynchronizeAsync() method !
    var cts = new CancellationTokenSource();

    agent.LocalOrchestrator.OnTableChangesApplying((args) =>
    {
        if (args.SchemaTable.TableName == "Table_That_Should_Not_Be_Sync")
            args.Cancel = true;
    });

Be careful, your table will never be synced !

Intercepting rows
^^^^^^^^^^^^^^^^^^

| You may want to intercept all the rows that have just been selected from the source (client or server), and are about to be sent to their destination (server or client).   
| Or even intercept all the rows that are going to be applied on a destination database.   
| That way, you may be able to modify these rows, to meet your business / requirements rules.  

.. hint:: You will find the sample used for this chapter, here : `Spy sample <https://github.com/Mimetis/Dotmim.Sync/tree/master/Samples/Spy>`_. 

``DMS`` workload allows you to intecept different kinds of events on different levels:

- Database level
- Table level
- Row level 

On each side (client and server), you will have:

- Interceptors during the "_Select_" phase : Getting changes from the database.
- Interceptors during the "_Apply_" phase : Applying Insert / Delete or Update to the database.
- Interceptors for extra workloads like conflict resolution, serialization, converters & so on ...

On each level you will have:

- A before event: Generally ending by "_ing_" like ``OnDatabaseChangesApplying``.
- An after event: Generally ending by "_ied_" like ``OnDatabaseChangesApplied``.

Selecting changes
-------------------

Regarding the rows selection from your client or server:

- ``OnDatabaseChangesSelecting`` : Raised before selecting rows. You have info about the tmp folder and batch size that will be used.
- ``OnTableChangesSelecting`` : Raised before selecting rows for a particular table : You have info about the current table and the ``DbCommand`` used to fetch data.

On the other side, once rows are selected, you still can:

- ``OnRowsChangesSelected`` : Raised once a row is read from the databse, but not yet serialized to disk.
- ``OnTableChangesSelected`` : Raised once a table changes as been fully read. Changes are serialized to disk.
- ``OnDatabaseChangesSelected`` : Raised once all changes are grabbed from the local database. Changes are serialized to disk.

Applying changes
---------------------

Regarding the rows to apply on your client (or server) database, you can intercept different kind of events:

- ``OnDatabaseChangesApplying``: Rows are serialized locally in a batch info folder BUT they are not yet read internally and are not in memory. You can iterate over all the files and see if you have rows to apply.
- ``OnTableChangesApplying``: Rows are still on disk and not in memory. This interceptor is called for each table that has rows to apply.
- ``OnRowsChangesApplying`` : Rows ARE now in memory, in a batch (depending on batch size and provider max batch), and are going to be applied.

On the other side, once rows are applied, you can iterate through different interceptors:

- ``OnTableChangesApplied``: Contains a summary of all rows applied on a table for a particular state (DataRowState.Modified or Deleted).
- ``OnDatabaseChangesApplied`` : Contains a summary of all changes applied on the database level.

Here are some useful information about some of these interceptors:


OnDatabaseChangesSelecting
-------------------------------

The ``OnDatabaseChangesSelecting`` occurs before the database will get changes from the database.

.. code-block:: csharp

    localOrchestrator.OnDatabaseChangesSelecting(args =>
    {
        Console.WriteLine($"--------------------------------------------");
        Console.WriteLine($"Getting changes from local database:");
        Console.WriteLine($"--------------------------------------------");

        Console.WriteLine($"BatchDirectory: {args.BatchDirectory}. BatchSize: {args.BatchSize}.");
    });

.. code-block:: bash
    
    --------------------------------------------
    Getting changes from local database:
    --------------------------------------------
    BatchDirectory: C:\Users\spertus\AppData\Local\Temp\DotmimSync\2022_07_18_36tygabvdj2bw. 
    BatchSize: 2000.


OnDatabaseChangesApplying
-------------------------------

| The ``OnDatabaseChangesApplying`` interceptor is happening when changes are going to be applied on the client or server.
| The changes are not yet loaded in memory. They are all stored locally in a temporary folder.

To be able to load batches from the temporary folder, or save rows, you can use the ``LoadTableFromBatchInfoAsync`` and ``SaveTableToBatchPartInfoAsync`` methods 

.. code-block:: csharp

    localOrchestrator.OnDatabaseChangesApplying(async args =>
    {
        Console.WriteLine($"--------------------------------------------");
        Console.WriteLine($"Changes to be applied on the local database:");
        Console.WriteLine($"--------------------------------------------");

        foreach (var table in args.ApplyChanges.Schema.Tables)
        {
            // loading in memory all batches containing rows for the current table
            var syncTable = await localOrchestrator.LoadTableFromBatchInfoAsync(
                args.ApplyChanges.BatchInfo, table.TableName, table.SchemaName);

            Console.WriteLine($"Changes for table {table.TableName}. Rows:{syncTable.Rows.Count}");
            foreach (var row in syncTable.Rows)
                Console.WriteLine(row);

            Console.WriteLine();

        }
    });

.. code-block:: bash

    --------------------------------------------
    Changes to be applied on the local database:
    --------------------------------------------
    Changes for table ProductCategory. Rows:1
    [Sync state]:Modified, [ProductCategoryID]:e7224bd1-192d-4237-8dc6-a3c21a017745, 
    [ParentProductCategoryID]:<NULL />

    Changes for table ProductModel. Rows:0

    Changes for table Product. Rows:0

    Changes for table Address. Rows:0

    Changes for table Customer. Rows:1
    [Sync state]:Modified, [CustomerID]:30125, [NameStyle]:False, [Title]:<NULL />, 
    [FirstName]:John, [MiddleName]:<NULL />

    Changes for table CustomerAddress. Rows:0

    Changes for table SalesOrderHeader. Rows:0

    Changes for table SalesOrderDetail. Rows:0

OnTableChangesApplying
----------------------------

| The ``OnTableChangesApplying`` is happening right before rows are applied on the client or server.
| Like ``OnDatabaseChangesApplying`` the changes are not yet loaded in memory. They are all stored locally in a temporary folder.
| Be careful, this interceptor is called for each state (Modified / Deleted), so be sure to check the state of the rows:
| Note that this interceptor is not called if the current tables has no rows to applied.

.. code-block:: csharp

    // Just before applying changes locally, at the table level
    localOrchestrator.OnTableChangesApplying(async args =>
    {
        if (args.BatchPartInfos != null)
        {
            var syncTable = await localOrchestrator.LoadTableFromBatchInfoAsync(
                args.BatchInfo, args.SchemaTable.TableName, args.SchemaTable.SchemaName, args.State);

            if (syncTable != null && syncTable.HasRows)
            {
                Console.WriteLine($"- --------------------------------------------");
                Console.WriteLine($"- Applying [{args.State}] 
                        changes to Table {args.SchemaTable.GetFullName()}");
                Console.WriteLine($"Changes for table 
                        {args.SchemaTable.TableName}. Rows:{syncTable.Rows.Count}");
                foreach (var row in syncTable.Rows)
                    Console.WriteLine(row);
            }

        }
    });


.. code-block:: bash

    - --------------------------------------------
    - Applying [Modified] changes to Table ProductCategory
    Changes for table ProductCategory. Rows:1
    [Sync state]:Modified, [ProductCategoryID]:e7224bd1-192d-4237-8dc6-a3c21a017745, 
    [ParentProductCategoryID]:<NULL />
    - --------------------------------------------
    - Applying [Modified] changes to Table Customer
    Changes for table Customer. Rows:1
    [Sync state]:Modified, [CustomerID]:30125, [NameStyle]:False, [Title]:<NULL />, [FirstName]:John, 
    [MiddleName]:<NULL />, [LastName]:Doe, [Suffix]:<NULL />, [CompanyName]:<NULL />, [SalesPerson]:<NULL />,
    


OnRowsChangesApplying
-----------------------------------

The ``OnRowsChangesApplying`` interceptor is happening just before applying a batch of rows to the local (client or server) database.

The number of rows to be applied here is depending on:

- The batch size you have set in your SyncOptions instance : ``SyncOptions.BatchSize`` (Default is 2 Mo)
- The max number of rows to applied in one single instruction : ``Provider.BulkBatchMaxLinesCount`` (Default is 10 000 rows per instruction)

.. code-block:: csharp

    localOrchestrator.OnRowsChangesApplying(async args =>
    {
        Console.WriteLine($"- --------------------------------------------");
        Console.WriteLine($"- In memory rows that are going to be Applied");
        foreach (var row in args.SyncRows)
            Console.WriteLine(row);

        Console.WriteLine();
    });


.. code-block:: bash

    - --------------------------------------------
    - In memory rows that are going to be Applied
    [Sync state]:Modified, [ProductCategoryID]:275c44e0-cfc7-.., [ParentProductCategoryID]:<NULL />

    - --------------------------------------------
    - In memory rows that are going to be Applied
    [Sync state]:Modified, [CustomerID]:30130, [NameStyle]:False, [Title]:<NULL />, [FirstName]:John


Interceptors DbCommand execution
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Interceptors on ``DbCommand`` will let you change the command used, depending on your requirements:

* ``Interceptors`` on creating the architecture.
* ``Interceptors`` when executing sync queries.

Let see a straightforward sample : *Customizing a tracking table*.

Adding a new column in a tracking table
-------------------------------------------

The idea here is to add a new column ``internal_id`` in the tracking table:

.. code-block:: csharp

    var provider = new SqlSyncProvider(serverConnectionString);
    var options = new SyncOptions();
    var setup = new SyncSetup(new string[] { "ProductCategory", "ProductModel", "Product" });
    var orchestrator = new RemoteOrchestrator(provider, options, setup);

    // working on the product Table
    var productSetupTable = setup.Tables["Product"];

    orchestrator.OnTrackingTableCreating(ttca =>
    {
        var addingID = '$'" ALTER TABLE {ttca.TrackingTableName.Schema().Quoted()} " +
                       '$'" ADD internal_id varchar(10) null";
        ttca.Command.CommandText += addingID;
    });

    var trExists = await orchestrator.ExistTrackingTableAsync(productSetupTable);
    if (!trExists)
        await orchestrator.CreateTrackingTableAsync(productSetupTable);

.. image:: https://user-images.githubusercontent.com/4592555/103886481-e08af980-50e1-11eb-97cf-b54af5a44e8c.png

Ok, now we need to customize the triggers to insert a correct value in the ``internal_id`` column:

.. code-block:: csharp

    orchestrator.OnTriggerCreating(tca =>
    {
        string val;
        if (tca.TriggerType == DbTriggerType.Insert)
            val = "INS";
        else if (tca.TriggerType == DbTriggerType.Delete)
            val = "DEL";
        else
            val = "UPD";

        var cmdText = '$'"UPDATE Product_tracking " +
                    '$'"SET Product_tracking.internal_id='{val}' " +
                    '$'"FROM Product_tracking JOIN Inserted ON " + 
                    '$'"Product_tracking.ProductID = Inserted.ProductID;";

        tca.Command.CommandText += Environment.NewLine + cmdText;
    });

    var trgExists = await orchestrator.ExistTriggerAsync(productSetupTable, 
                            DbTriggerType.Insert);
    if (!trgExists)
        await orchestrator.CreateTriggerAsync(productSetupTable, 
                            DbTriggerType.Insert);

    trgExists = await orchestrator.ExistTriggerAsync(productSetupTable, 
                            DbTriggerType.Update);
    if (!trgExists)
        await orchestrator.CreateTriggerAsync(productSetupTable, 
                            DbTriggerType.Update);

    trgExists = await orchestrator.ExistTriggerAsync(productSetupTable, 
                            DbTriggerType.Delete);
    if (!trgExists)
        await orchestrator.CreateTriggerAsync(productSetupTable, 
                            DbTriggerType.Delete);

    orchestrator.OnTriggerCreating(null);


Here is the `Sql` script executed for trigger ``Insert``:

.. code-block:: sql

    CREATE TRIGGER [dbo].[Product_insert_trigger] ON [dbo].[Product] FOR INSERT AS

    SET NOCOUNT ON;

    -- If row was deleted before, it already exists, so just make an update
    UPDATE [side] 
    SET  [sync_row_is_tombstone] = 0
        ,[update_scope_id] = NULL -- scope id is always NULL when update is made locally
        ,[last_change_datetime] = GetUtcDate()
    FROM [Product_tracking] [side]
    JOIN INSERTED AS [i] ON [side].[ProductID] = [i].[ProductID]

    INSERT INTO [Product_tracking] (
        [ProductID]
        ,[update_scope_id]
        ,[sync_row_is_tombstone]
        ,[last_change_datetime]
    ) 
    SELECT
        [i].[ProductID]
        ,NULL
        ,0
        ,GetUtcDate()
    FROM INSERTED [i]
    LEFT JOIN [Product_tracking] [side] ON [i].[ProductID] = [side].[ProductID]
    WHERE [side].[ProductID] IS NULL


    UPDATE Product_tracking SET Product_tracking.internal_id='INS' 
    FROM Product_tracking 
    JOIN Inserted ON Product_tracking.ProductID = Inserted.ProductID;


Intercepting web events
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Some interceptors are specific to web orchestrators ``WebRemoteOrchestrator`` & ``WebServerAgent``.

These orchestrators will let you intercept all the ``Requests`` and ``Responses`` that will be generated by ``DMS`` during a web call.

WebServerAgent
------------------------

The two first interceptors will intercept basically all requests and responses coming in and out:

* ``webServerAgent.OnHttpGettingRequest(args => {})``
* ``webServerAgent.OnHttpSendingResponse(args => {})``

Each of them will let you access the `HttpContext`, `SyncContext` and `SessionCache` instances:

.. code-block:: csharp

    webServerAgent.OnHttpGettingRequest(args =>
    {
        var httpContext = args.HttpContext;
        var syncContext = args.Context;
        var session = args.SessionCache;
    });


The two last new web server http interceptors will let you intercept all the calls made when server *receives* client changes and when server *sends back* server changes.

* ``webServerAgent.OnHttpGettingChanges(args => {});``
* ``webServerAgent.OnHttpSendingChanges(args => {});``

Here is a quick example using all of them:

.. code-block:: csharp

    webServerAgent.OnHttpGettingRequest(req =>
        Console.WriteLine("Receiving Client Request:" + req.Context.SyncStage + 
        ". " + req.HttpContext.Request.Host.Host + "."));

    webServerAgent.OnHttpSendingResponse(res =>
        Console.WriteLine("Sending Client Response:" + res.Context.SyncStage + 
        ". " + res.HttpContext.Request.Host.Host));

    webServerAgent.OnHttpGettingChanges(args 
        => Console.WriteLine("Getting Client Changes" + args));
    webServerAgent.OnHttpSendingChanges(args 
        => Console.WriteLine("Sending Server Changes" + args));

    await webServerManager.HandleRequestAsync(context);

.. code-block:: bash


    Receiving Client Request:ScopeLoading. localhost.
    Sending Client Response:Provisioning. localhost
    Receiving Client Request:ChangesSelecting. localhost.
    Sending Server Changes[localhost] Sending All Snapshot Changes. Rows:0
    Sending Client Response:ChangesSelecting. localhost
    Receiving Client Request:ChangesSelecting. localhost.
    Getting Client Changes[localhost] Getting All Changes. Rows:0
    Sending Server Changes[localhost] Sending Batch  Changes. (1/11). Rows:658
    Sending Client Response:ChangesSelecting. localhost
    Receiving Client Request:ChangesSelecting. localhost.
    Sending Server Changes[localhost] Sending Batch  Changes. (2/11). Rows:321
    Sending Client Response:ChangesSelecting. localhost
    Receiving Client Request:ChangesSelecting. localhost.
    Sending Server Changes[localhost] Sending Batch  Changes. (3/11). Rows:29
    Sending Client Response:ChangesSelecting. localhost
    Receiving Client Request:ChangesSelecting. localhost.
    Sending Server Changes[localhost] Sending Batch  Changes. (4/11). Rows:33
    Sending Client Response:ChangesSelecting. localhost
    Receiving Client Request:ChangesSelecting. localhost.
    Sending Server Changes[localhost] Sending Batch  Changes. (5/11). Rows:39
    Sending Client Response:ChangesSelecting. localhost
    Receiving Client Request:ChangesSelecting. localhost.
    Sending Server Changes[localhost] Sending Batch  Changes. (6/11). Rows:55
    Sending Client Response:ChangesSelecting. localhost
    Receiving Client Request:ChangesSelecting. localhost.
    Sending Server Changes[localhost] Sending Batch  Changes. (7/11). Rows:49
    Sending Client Response:ChangesSelecting. localhost
    Receiving Client Request:ChangesSelecting. localhost.
    Sending Server Changes[localhost] Sending Batch  Changes. (8/11). Rows:32
    Sending Client Response:ChangesSelecting. localhost
    Receiving Client Request:ChangesSelecting. localhost.
    Sending Server Changes[localhost] Sending Batch  Changes. (9/11). Rows:758
    Sending Client Response:ChangesSelecting. localhost
    Receiving Client Request:ChangesSelecting. localhost.
    Sending Server Changes[localhost] Sending Batch  Changes. (10/11). Rows:298
    Sending Client Response:ChangesSelecting. localhost
    Receiving Client Request:ChangesSelecting. localhost.
    Sending Server Changes[localhost] Sending Batch  Changes. (11/11). Rows:1242
    Sending Client Response:ChangesSelecting. localhost
    Synchronization done.


The main differences are that the two first ones will intercept **ALL** requests coming from the client and the two last one will intercept **Only** requests where data are exchanged (but you have more detailed)

WebRemoteOrchestrator
-------------------------

You have pretty much the same ``Http`` interceptors on the client side. ``OnHttpGettingRequest`` becomes ``OnHttpSendingRequest`` and ``OnHttpSendingResponse`` becomes ``OnHttpGettingResponse``:

.. code-block:: csharp

    localOrchestrator.OnHttpGettingResponse(req => Console.WriteLine("Receiving Server Response"));
    localOrchestrator.OnHttpSendingRequest(res =>Console.WriteLine("Sending Client Request."));
    localOrchestrator.OnHttpGettingChanges(args => Console.WriteLine("Getting Server Changes" + args));
    localOrchestrator.OnHttpSendingChanges(args => Console.WriteLine("Sending Client Changes" + args));


.. code-block:: bash

    Sending Client Request.
    Receiving Server Response
    Sending Client Request.
    Receiving Server Response
    Sending Client Changes[localhost] Sending All Changes. Rows:0
    Sending Client Request.
    Receiving Server Response
    Getting Server Changes[localhost] Getting Batch Changes. (1/11). Rows:658
    Sending Client Request.
    Receiving Server Response
    Getting Server Changes[localhost] Getting Batch Changes. (2/11). Rows:321
    Sending Client Request.
    Receiving Server Response
    Getting Server Changes[localhost] Getting Batch Changes. (3/11). Rows:29
    Sending Client Request.
    Receiving Server Response
    Getting Server Changes[localhost] Getting Batch Changes. (4/11). Rows:33
    Sending Client Request.
    Receiving Server Response
    Getting Server Changes[localhost] Getting Batch Changes. (5/11). Rows:39
    Sending Client Request.
    Receiving Server Response
    Getting Server Changes[localhost] Getting Batch Changes. (6/11). Rows:55
    Sending Client Request.
    Receiving Server Response
    Getting Server Changes[localhost] Getting Batch Changes. (7/11). Rows:49
    Sending Client Request.
    Receiving Server Response
    Getting Server Changes[localhost] Getting Batch Changes. (8/11). Rows:32
    Sending Client Request.
    Receiving Server Response
    Getting Server Changes[localhost] Getting Batch Changes. (9/11). Rows:758
    Sending Client Request.
    Receiving Server Response
    Getting Server Changes[localhost] Getting Batch Changes. (10/11). Rows:298
    Sending Client Request.
    Receiving Server Response
    Getting Server Changes[localhost] Getting Batch Changes. (11/11). Rows:1242
    Synchronization done.


Example: Hook Bearer token
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The idea is to inject the user identifier ``UserId`` in the ``SyncParameters`` collection on the server, after having extract this value from a ``Bearer`` token.

That way the ``UserId`` is not hard coded or store somewhere on the client application, since this value is generated during the authentication part.

As you can see:

* My ``SyncController`` is marked with the `[Authorize]` attribute.
* The orchestrator is only called when we know that the user is authenticated.
* We are injecting the ``UserId`` value coming from the bearer into the ``SyncContext.Parameters``.
* Optionally, because we don't want to send back this value to the client, we are removing it when sending the response.

.. code-block:: csharp

    [Authorize]
    [ApiController]
    [Route("api/[controller]")]
    public class SyncController : ControllerBase
    {
        private WebServerAgent webServerAgent;

        // Injected thanks to Dependency Injection
        public SyncController(WebServerAgent webServerAgent) 
            => this.webServerAgent = webServerAgent;

        /// <summary>
        /// This POST handler is mandatory to handle all the sync process
        [HttpPost]
        public async Task Post()
        {
            // If you are using the [Authorize] attribute you don't need to check
            // the User.Identity.IsAuthenticated value
            if (HttpContext.User.Identity.IsAuthenticated)
            {
                // OPTIONAL: -------------------------------------------
                // OPTIONAL: Playing with user coming from bearer token
                // OPTIONAL: -------------------------------------------

                // on each request coming from the client, just inject the User Id parameter
                webServerAgent.OnHttpGettingRequest(args =>
                {
                    var pUserId = args.Context.Parameters["UserId"];

                    if (pUserId == null)
                    {
                        var userId = this.HttpContext.User.Claims.FirstOrDefault(
                            x => x.Type == ClaimTypes.NameIdentifier);
                        args.Context.Parameters.Add("UserId", userId);
                    }

                });

                // Because we don't want to send back this value, remove it from the response 
                webServerAgent.OnHttpSendingResponse(args =>
                {
                    if (args.Context.Parameters.Contains("UserId"))
                        args.Context.Parameters.Remove("UserId");
                });

                await webServerAgent.HandleRequestAsync(this.HttpContext);
            }
            else
            {
                this.HttpContext.Response.StatusCode = StatusCodes.Status401Unauthorized;
            }
        }

        /// <summary>
        /// This GET handler is optional. It allows you to see the configuration hosted on the server
        /// The configuration is shown only if Environmenent == Development
        /// </summary>
        [HttpGet]
        [AllowAnonymous]
        public Task Get() => this.HttpContext.WriteHelloAsync(webServerAgent);
    }




