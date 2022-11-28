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
-----------------------

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


Datasource level
^^^^^^^^^^^^^^^^^^^^^^

| We have some interceptors that are not related to a specific table, but to the whole datasource.
| They are tight to the connection, the transaction or the command used to get the changes, apply changes or even handle conflicts and errors.


OnConnectionOpen
-------------------------

The ``OnConnectionOpen`` event is raised when a connection is opened, through the underline provider.

TODO

OnReConnect
-------------------------

The ``OnReConnect`` event is raised when a connection is re-opened, through the underline provider.

DMS is using a custom retry policy, inspired from `Polly <http://www.thepollyproject.org/>`_  to manage a connection retry policy.


.. code-block:: csharp

    localOrchestrator.OnReConnect(args => {
        Console.WriteLine($"[Retry] Can't connect to database {args.Connection?.Database}. " +
        $"Retry N°{args.Retry}. " +
        $"Waiting {args.WaitingTimeSpan.Milliseconds}. Exception:{args.HandledException.Message}.");
    });    

You can customize the retry policy, only on http mode, when using a ``WebRemoteOrchestrator`` instance.

.. code-block:: csharp

    var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri);

    // limit to 2 retries only
    webRemoteOrchestrator.SyncPolicy.RetryCount = 2;

.. code-block:: csharp

    var webRemoteOrchestrator = new WebRemoteOrchestrator(serviceUri);

    // retry for ever (not sure it's a good idea, that being said)
    webRemoteOrchestrator.SyncPolicy = SyncPolicy.WaitAndRetryForever(TimeSpan.FromSeconds(1));


OnTransactionOpen
-------------------------

The ``OnTransactionOpen`` event is raised when a transaction is opened, through the underline provider.

TODO

OnConnectionClose
-------------------------

The ``OnConnectionClose`` event is raised when a connection is closed, through the underline provider.

TODO

OnTransactionCommit
-------------------------

The ``OnTransactionCommit`` event is raised when a transaction is committed, through the underline provider.

TODO

OnGetCommand
-----------------

The OnGetCommand interceptor is happening when a command is retrieved from the underline provider (``SqlSyncProvider``, ``MySqlSyncProvider``, etc..)

.. code-block:: csharp

    agent.RemoteOrchestrator.OnGetCommand(args =>
    {
        if (args.Command.CommandType == CommandType.StoredProcedure)
        {
            args.Command.CommandText = args.Command.CommandText.Replace("_filterproducts_", "_default_");
        }
    });



OnExecuteCommand
--------------------

The ``OnExecuteCommand`` interceptor is happening when a command is about to be executed on the client or server.

.. code-block:: csharp

    agent.RemoteOrchestrator.OnExecuteCommand(args =>
    {
        Console.WriteLine(args.Command.CommandText);
    });



Selecting changes
^^^^^^^^^^^^^^^^^^^^

Regarding the rows selection from your client or server:

- ``OnDatabaseChangesSelecting`` : Raised before selecting rows. You have info about the tmp folder and batch size that will be used.
- ``OnTableChangesSelecting`` : Raised before selecting rows for a particular table : You have info about the current table and the ``DbCommand`` used to fetch data.

On the other side, once rows are selected, you still can:

- ``OnRowsChangesSelected`` : Raised once a row is read from the databse, but not yet serialized to disk. Row is still in memory, and connection / reader still opened.
- ``OnTableChangesSelected`` : Raised once a table changes as been fully read. Changes (all batches for this table) are serialized to disk. Connection / reader are closed.
- ``OnDatabaseChangesSelected`` : Raised once all changes are grabbed from the local database. Changes are serialized to disk.

OnDatabaseChangesSelecting
-------------------------------

Occurs when changes are going to be queried from the underline database.

.. code-block:: csharp

    var localOrchestrator = new LocalOrchestrator(clientProvider);
    localOrchestrator.OnDatabaseChangesSelecting(args => {
        Console.WriteLine($"Getting changes from local database:");
        Console.WriteLine($"Batch directory: {args.BatchDirectory}. Batch size: {args.BatchSize}. 
                            Is first sync: {args.IsNew}");
        Console.WriteLine($"From: {args.FromTimestamp}. To: {args.ToTimestamp}.");
    }


OnTableChangesSelecting
---------------------------

| Occurs when changes are going to be queried from the underline database for a particular table. 
| You have access to the command / connection / transaction that going to be used to query the database.

.. note:: The ``Command`` property can be changed here, depending on your needs.

.. code-block:: csharp

    var localOrchestrator = new LocalOrchestrator(clientProvider);
    localOrchestrator.OnTableChangesSelecting(args =>
    {
        Console.WriteLine($"Getting changes from local database " +
                          $"for table:{args.SchemaTable.GetFullName()}");

        Console.WriteLine($"{args.Command.CommandText}");
    });


OnRowsChangesSelected
-------------------------

| Occurs when a row is selected from the underline database.
| You have access to the ``SyncRow`` row property, the table schema and the state of the row (Modified, Deleted).
| You can change any value from the ``SyncRow`` property on the fly if needed.

.. code-block:: csharp

    var localOrchestrator = new LocalOrchestrator(clientProvider);
    localOrchestrator.OnRowsChangesSelected(args =>
    {
        Console.WriteLine($"Row read from local database for table:{args.SchemaTable.GetFullName()}");
        Console.WriteLine($"{args.SyncRow}");
    });

.. warning:: This event is raised for each row, so be careful with the number of rows you have in your database.

    Plus, this event is raised during the reading phase of the database, that means that the connection is still opened.

    If you have a lot of rows, you may want to use the ``OnTableChangesSelected`` event instead, that occurs once the table is fully read, and results are serialized on disk.

OnTableChangesSelected
-------------------------

| Occurs when a table is fully selected from the underline database.

.. code-block:: csharp

    localOrchestrator.OnTableChangesSelected(args =>
    {
        Console.WriteLine($"Table: {args.SchemaTable.GetFullName()} read. " +
                          $"Rows count:{args.BatchInfo.RowsCount}.");" +

        Console.WriteLine($"Directory: {args.BatchInfo.DirectoryName}. " +
                          $"Number of files: {args.BatchPartInfos?.Count()} ");
        
        Console.WriteLine($"Changes: {args.TableChangesSelected.TotalChanges} " +
                          $"({args.TableChangesSelected.Upserts}/{args.TableChangesSelected.Deletes})");
    });    

.. hint:: You have access to the serialized rows on disk, in the ``BatchInfo`` property. 

    You can iterate through all the files, and read the rows from the files, using the `LoadTableFromBatchInfoAsync <Orchestrators.html#loadtablefrombatchinfoasync>`_ 


OnDatabaseChangesSelected
-----------------------------

| Occurs when all changes are selected from the underline database.
| The ``BatchInfo`` property is fully filled with all batch files.


.. code-block:: csharp

    localOrchestrator.OnDatabaseChangesSelected(args =>
    {
        Console.WriteLine($"Directory: {args.BatchInfo.DirectoryName}. "
                          $"Number of files: {args.BatchInfo.BatchPartsInfo?.Count()} ");
        
        Console.WriteLine($"Total: {args.ChangesSelected.TotalChangesSelected} " +
                            $"({args.ChangesSelected.TotalChangesSelectedUpdates}" +
                            $"/{args.ChangesSelected.TotalChangesSelectedDeletes})");
        
        foreach (var table in args.ChangesSelected.TableChangesSelected)
            Console.WriteLine($"Table: {table.TableName}. "
                              $"Total: {table.TotalChanges} ({table.Upserts / table.Deletes}");
    });        

.. hint:: You have access to the serialized rows on disk, in the ``BatchInfo`` property. 

    You can iterate through all the files, and read the rows from the files, using the `LoadTablesFromBatchInfoAsync <Orchestrators.html#loadtablesfrombatchinfoasync>`_



Applying changes
^^^^^^^^^^^^^^^^^^^^

Regarding the rows to apply on your client (or server) database, you can intercept different kind of events:

- ``OnDatabaseChangesApplying``: Rows are serialized locally in a batch info folder BUT they are not yet read internally and are not in memory. You can iterate over all the files and see if you have rows to apply.
- ``OnTableChangesApplying``: Rows are still on disk and not in memory. This interceptor is called for each table that has rows to apply.
- ``OnRowsChangesApplying`` : Rows ARE now in memory, in a batch (depending on batch size and provider max batch), and are going to be applied.

On the other side, once rows are applied, you can iterate through different interceptors:

- ``OnTableChangesApplied``: Contains a summary of all rows applied on a table for a particular state (DataRowState.Modified or Deleted).
- ``OnDatabaseChangesApplied`` : Contains a summary of all changes applied on the database level.


OnDatabaseChangesApplying
-------------------------------

| The ``OnDatabaseChangesApplying`` interceptor is happening when changes are going to be applied on the client or server.
| The changes are not yet loaded in memory. They are all stored locally in a temporary folder.

To be able to load batches from the temporary folder, or save rows, you can use the `LoadTablesFromBatchInfoAsync <Orchestrators.html#loadtablesfrombatchinfoasync>`_ and `SaveTableToBatchPartInfoAsync <Orchestrators.html#savetabletobatchpartinfoasync>`_ methods 

.. code-block:: csharp

    localOrchestrator.OnDatabaseChangesApplying(async args =>
    {
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

                foreach (var row in syncTable.Rows)
                    Console.WriteLine(row);
            }

        }
    });


OnBatchChangesApplying
-------------------------------

| The ``OnBatchChangesApplying`` interceptor is happening when a batch for a particular table is about to be applied on the local data source.
| The number of rows contained in each batch file is depending on the value you have set in your SyncOptions instance : ``SyncOptions.BatchSize`` (Default is 2 Mo)
| This interceptor is called for each batch file, and for each state (``Modified`` / ``Deleted``).
| That means that if you have **1000** batches, and **2** calls of this interceptor (one for ``Modified``, one for ``Deleted``), you will fire **2000** times this interceptor.

.. code-block:: csharp

    agent.LocalOrchestrator.OnBatchChangesApplying(async args =>
    {
        if (args.BatchPartInfo != null)
        {
            Console.WriteLine($"FileName:{args.BatchPartInfo.FileName}. RowsCount:{args.BatchPartInfo.RowsCount} ");
            Console.WriteLine($"Applying rows from this batch part info:");

            var table = await agent.LocalOrchestrator.LoadTableFromBatchPartInfoAsync(args.BatchInfo,
                            args.BatchPartInfo, args.State, args.Connection, args.Transaction);

            foreach (var row in table.Rows)
                Console.WriteLine(row);

        }
    });



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


OnTableChangesApplied
----------------------------

The ``OnTableChangesApplied`` interceptor is happening when all rows, for a specific table, are applied on the local (client or server) database.

TODO


OnBatchChangesApplying
-------------------------------

| The ``OnBatchChangesApplied`` interceptor is happening when a batch for a particular table has been applied.

.. code-block:: csharp

    agent.LocalOrchestrator.OnBatchChangesApplied(async args =>
    {
        if (args.BatchPartInfo != null)
        {
            Console.WriteLine($"FileName:{args.BatchPartInfo.FileName}. RowsCount:{args.BatchPartInfo.RowsCount} ");
            Console.WriteLine($"Applied rows from this batch part info:");

            var table = await agent.LocalOrchestrator.LoadTableFromBatchPartInfoAsync(args.BatchInfo,
                            args.BatchPartInfo, args.State, args.Connection, args.Transaction);

            foreach (var row in table.Rows)
                Console.WriteLine(row);

        }
    });




OnDatabaseChangesApplied
-------------------------------

The ``OnDatabaseChangesApplied`` interceptor is happening when all changes are applied on the client or server.

TODO


Snapshots
^^^^^^^^^^^^^^

See how snapshots work in the `Snapshots <Snapshot.html>`_ section.

OnSnapshotCreating
-------------------------

The ``OnSnapshotCreating`` interceptor is happening when a snapshot is going to be created from the server side

TODO

OnSnapshotCreated
-------------------------

The ``OnSnapshotCreated`` interceptor is happening when a snapshot is created from the server side.

TODO

OnSnapshotApplying
-------------------------

The ``OnSnapshotApplying`` interceptor is happening when a snapshot is going to be applied on the client side.

TODO

OnSnapshotApplied
-------------------------

The ``OnSnapshotApplied`` interceptor is happening when a snapshot is applied on the client side.

TODO


Specific
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

OnProvisioning
------------------

The ``OnProvisioning`` interceptor is happening when the database is being provisioned.

TODO


OnProvisioned
------------------

The ``OnProvisioned`` interceptor is happening when the database is provisioned.

TODO


OnDeprovisioning
------------------

The ``OnDeprovisioning`` interceptor is happening when the database is being deprovisioned.

TODO

OnDeprovisioned
------------------

The ``OnDeprovisioned`` interceptor is happening when the database is deprovisioned.

TODO


OnLocalTimestampLoading
------------------------------

OnLocalTimestampLoaded
------------------------------

OnSchemaLoading
--------------------

OnSchemaLoaded
--------------------

OnMetadataCleaning
-------------------------

OnMetadataCleaned
-------------------------

OnApplyChangesConflictOccured
---------------------------------

See `Conflicts <Conflict.html>`_ 

OnApplyChangesErrorOccured
---------------------------------

See `Errors <Errors.html>`_ 

OnSerializingSyncRow
------------------------------

OnDeserializingSyncRow
------------------------------



OnSessionBegin
-------------------------


OnSessionEnd
-------------------------



OnConflictingSetup
-------------------------

OnGettingOperation
-------------------------

The ``OnGettingOperation`` interceptor is happening when a server receive a request from a client for initiate a synchronization.

From here, you have the option to **override** the operation, using the ``SyncOperation`` enumeration:

.. code-block:: csharp

    public enum SyncOperation
    {
        /// <summary>
        /// Normal synchronization
        /// </summary>
        Normal = 0,

        /// <summary>
        /// Reinitialize the whole sync database, 
        /// applying all rows from the server to the client
        /// </summary>
        Reinitialize = 1,
        
        /// <summary>
        /// Reinitialize the whole sync database, 
        /// applying all rows from the server to the client, after trying a client upload
        /// </summary>
        ReinitializeWithUpload = 2,

        /// <summary>
        /// Drop all the sync metadatas even tracking tables and 
        /// scope infos and make a full sync again
        /// </summary>
        DropAllAndSync = 4,

        /// <summary>
        /// Drop all the sync metadatas even tracking tables and 
        /// scope infos and exit
        /// </summary>
        DropAllAndExit = 8,

        /// <summary>
        /// Deprovision stored procedures and triggers and sync again
        /// </summary>
        DeprovisionAndSync = 16,

        /// <summary>
        /// Exit a Sync session without syncing
        /// </summary>
        AbortSync = 32,
    }

Useful for example to force a ReinitializeWithUpload operation, when you have a conflict on the client side, and you want to force the client to upload all his changes to the server, then reinitialize everything.

.. hint:: This method is usefull most of the time, from the server side, when using a proxy ASP.NET Core Web API. 


.. code-block:: csharp

    [HttpPost]
    public async Task Post()
    {

        var scopeName = context.GetScopeName();
        var clientScopeId = context.GetClientScopeId();

        var webServerAgent = webServerAgents.First(wsa => wsa.ScopeName == scopeName);

        webServerAgent.RemoteOrchestrator.OnGettingOperation(operationArgs =>
        {
            if (scopeName == "all" && clientScopeId == A_PARTICULAR_CLIENT_ID_TO_CHECK)
                operationArgs.SyncOperation = SyncOperation.ReinitializeWithUpload;

        });

        await webServerAgent.HandleRequestAsync(context);
    }

OnOutdated
-------------------------

The ``OnOutdated`` interceptor is happening when a client is outdated. You can use this interceptor to force the client to reinitialize its database if it is outdated.

By default, an error is raised, and sync is stopped. This event is raised only on the client side.

.. code-block:: csharp

    agent.LocalOrchestrator.OnOutdated(oa =>
    {
        Console.ForegroundColor = ConsoleColor.Red;
        Console.WriteLine("local database is too old to synchronize with the server.");
        Console.ResetColor();
        Console.WriteLine("Do you want to synchronize anyway, and potentially lost data ? ");
        Console.Write("Enter a value ('r' for reinitialize or 'ru' for reinitialize with upload): ");
        var answer = Console.ReadLine();

        if (answer.ToLowerInvariant() == "r")
            oa.Action = OutdatedAction.Reinitialize;
        else if (answer.ToLowerInvariant() == "ru")
            oa.Action = OutdatedAction.ReinitializeWithUpload;

    });



Web
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
------------------------------

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




