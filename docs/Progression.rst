Progression
=====================

| Getting useful information during a sync process could be complex.
| You can have a lot of information from an in-going sync, through two kinds of things:

* ``IProgress<ProgressArgs>`` : A best practice using `IProgress<T> <https://docs.microsoft.com/en-us/dotnet/api/system.progress-1>`_ to handle progress from within an *awaitable* method.
* ``Ìnterceptor<T>`` : A more advanced technic to handle a lot of more events from within **DMS**

Overview
^^^^^^^^^^^^

During a full synchronization, we have **two distincts** type of progression:

* The **Progression** from the client side.
* The **Progression** from the server side.

We have a lot of progress values raised from both the **server** and the **client** side:

* Each progress value is catched at the end of a method called by the **Orchestrator** instance.
* Each progress value in a sync process corresponds to a specific *stage*, represented by a ``SyncStage`` enumeration.

.. code-block:: csharp

    public enum SyncStage
    {
        None = 0,

        BeginSession,
        EndSession,

        ScopeLoading,
        ScopeWriting,

        SnapshotCreating,
        SnapshotApplying,

        SchemaReading,

        Provisioning,
        Deprovisioning,

        ChangesSelecting,
        ChangesApplying,

        Migrating,

        MetadataCleaning,
    }

To explain how things work, we are starting from a really straightforward sync process example, using the sample from `Hello sync sample <https://github.com/Mimetis/Dotmim.Sync/blob/master/Samples/HelloSync>`_:

.. code-block:: csharp

    var serverProvider = new SqlSyncChangeTrackingProvider(serverConnectionString);
    var clientProvider = new SqlSyncProvider(clientConnectionString);

    var setup = new SyncSetup("ProductCategory", "ProductModel", "Product",
                "Address", "Customer", "CustomerAddress", "SalesOrderHeader", 
                "SalesOrderDetail" );

    var agent = new SyncAgent(clientProvider, serverProvider);
    do
    {
        // Launch the sync process
        var s1 = await agent.SynchronizeAsync(setup);
        // Write results
        Console.WriteLine(s1);

    } while (Console.ReadKey().Key != ConsoleKey.Escape);

    Console.WriteLine("End");

We are going to see how to get useful information, from each stage involved during the sync processus, thanks to ``IProgress<T>`` and then we will go deeper with the notion of ``Interceptor<T>``.

.. note:: You will find this complete sample here : `Progression sample <https://github.com/Mimetis/Dotmim.Sync/tree/master/Samples/Progression>`_ 


IProgress\<T\>
^^^^^^^^^^^^^^^^

As we said, the progress values are triggered from both side : **Server** side and **Client** side, ordered.  

In our sample, we can say that : 

* The ``RemoteOrchestrator`` instance, using the server provider instance, will report all the progress from the server side.   
* The ``LocalOrchestrator`` instance using the client provider instance, will report all the progress from the client side.  


.. note:: A ``syncAgent`` object is **always** running on the client side of **any** architecture.  

Since our main method ``SynchronizeAsync()`` is marked ``async`` method, we will use the `Progress\<T\> <https://docs.microsoft.com/en-us/dotnet/api/system.progress-1?view=netcore-2.2>`_ to be able to report progress value.

So far, the most straightforward way to get feedback from a current sync, is to pass an instance of ``IProgress<T>`` when calling the method ``SynchronizeAsync()``.

.. note:: ``Progress<T>`` is **not** synchronous. So far, no guarantee that the progress callbacks will be raised in an ordered way.   
          
          That's why you can use a **DMS** progess class called ``SynchronousProgress<T>`` which is synchronous, using the correct synchronization context.

Here is a quick example used to provide some feedback to the user:   

.. code-block:: csharp

    var serverProvider = new SqlSyncChangeTrackingProvider(serverConnectionString);
    var clientProvider = new SqlSyncProvider(clientConnectionString);

    // Tables involved in the sync process:
    var setup = new SyncSetup ("ProductCategory", "ProductModel", "Product",
        "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail" );

    // Creating an agent that will handle all the process
    var agent = new SyncAgent(clientProvider, serverProvider);

    // Using the IProgress<T> pattern to handle progession dring the synchronization
    var progress = new SynchronousProgress<ProgressArgs>(args => 
        Console.WriteLine($"{s.ProgressPercentage:p}:  \t[{s.Source[..Math.Min(4, s.Source.Length)]}] {s.TypeName}: {s.Message}"));

    do
    {
        // Launch the sync process
        var s1 = await agent.SynchronizeAsync(setup, progress);
        // Write results
        Console.WriteLine(s1);

    } while (Console.ReadKey().Key != ConsoleKey.Escape);

    Console.WriteLine("End");


Here is the result, after the first synchronization, assuming the **Client** database is empty:


.. code-block:: bash

    0,00 %:         [Clie] ProvisionedArgs: Provisioned 9 Tables. Provision:Table, TrackingTable, StoredProcedures, Triggers.
    55,00 %:        [Adve] TableChangesSelectedArgs: [SalesOrderHeader] [Total] Upserts:32. Deletes:0. Total:32.
    75,00 %:        [Adve] TableChangesSelectedArgs: [Address] [Total] Upserts:450. Deletes:0. Total:450.
    75,00 %:        [Adve] TableChangesSelectedArgs: [SalesOrderDetail] [Total] Upserts:542. Deletes:0. Total:542.
    75,00 %:        [Adve] TableChangesSelectedArgs: [ProductCategory] [Total] Upserts:41. Deletes:0. Total:41.
    75,00 %:        [Adve] TableChangesSelectedArgs: [ProductModel] [Total] Upserts:128. Deletes:0. Total:128.
    75,00 %:        [Adve] TableChangesSelectedArgs: [CustomerAddress] [Total] Upserts:417. Deletes:0. Total:417.
    75,00 %:        [Adve] TableChangesSelectedArgs: [ProductDescription] [Total] Upserts:762. Deletes:0. Total:762.
    75,00 %:        [Adve] TableChangesSelectedArgs: [Product] [Total] Upserts:295. Deletes:0. Total:295.
    75,00 %:        [Adve] TableChangesSelectedArgs: [Customer] [Total] Upserts:847. Deletes:0. Total:847.
    75,00 %:        [Adve] DatabaseChangesSelectedArgs: [Total] Upserts:3514. Deletes:0. Total:3514. [C:\Temp\DotmimSync\2022_07_17_12iks12xfjrzx]
    80,42 %:        [Clie] TableChangesAppliedArgs: [ProductDescription] Changes Modified Applied:762. Resolved Conflicts:0.
    80,71 %:        [Clie] TableChangesAppliedArgs: [ProductCategory] Changes Modified Applied:41. Resolved Conflicts:0.
    81,62 %:        [Clie] TableChangesAppliedArgs: [ProductModel] Changes Modified Applied:128. Resolved Conflicts:0.
    83,72 %:        [Clie] TableChangesAppliedArgs: [Product] Changes Modified Applied:295. Resolved Conflicts:0.
    86,92 %:        [Clie] TableChangesAppliedArgs: [Address] Changes Modified Applied:450. Resolved Conflicts:0.
    92,95 %:        [Clie] TableChangesAppliedArgs: [Customer] Changes Modified Applied:847. Resolved Conflicts:0.
    95,92 %:        [Clie] TableChangesAppliedArgs: [CustomerAddress] Changes Modified Applied:417. Resolved Conflicts:0.
    96,14 %:        [Clie] TableChangesAppliedArgs: [SalesOrderHeader] Changes Modified Applied:32. Resolved Conflicts:0.
    100,00 %:       [Clie] TableChangesAppliedArgs: [SalesOrderDetail] Changes Modified Applied:542. Resolved Conflicts:0.
    100,00 %:       [Clie] DatabaseChangesAppliedArgs: [Total] Applied:3514. Conflicts:0.
    100,00 %:       [Clie] SessionEndArgs: [Client] Session Ends. Id:3b69c8ab-cce8-4b94-bf75-db22ea43169d. Scope name:DefaultScope.
    Synchronization done.
            Total changes  uploaded: 0
            Total changes  downloaded: 3514
            Total changes  applied: 3514
            Total resolved conflicts: 0
            Total duration :00.00:02.042
    Sync Ended. Press a key to start again, or Escapte to end


As you can see, it's a first synchronization, so:

* Session begins 
* Server creates all metadatas needed for AdventureWorks database
* Client creates all metadatas needed for Client database
* Server selects all changes to upserts
* Client applies all changes sent from ths server
* Client selects changes to send (nothing, obviously, because the tables have just been created on the client)
* Session ends

You can have more information, depending on your need, and still based on ``IProgress<T>``.

Using a ``SyncProgressLevel`` enumeration affected to the ProgressLevel property of your SyncOptions instance:

.. code-block:: csharp

    public enum SyncProgressLevel
    {
        /// <summary>
        /// Progress that contain the most detailed messages and the Sql statement executed
        /// </summary>
        Sql,

        /// <summary>
        /// Progress that contain the most detailed messages. These messages may contain sensitive application data
        /// </summary>
        Trace,

        /// <summary>
        /// Progress that are used for interactive investigation during development
        /// </summary>
        Debug,

        /// <summary>
        /// Progress that track the general flow of the application. 
        /// </summary>
        Information,

        /// <summary>
        /// Specifies that a progress output should not write any messages.
        /// </summary>
        None
    }

.. warning:: Be careful: The Sql level may contains sensitive data !


.. code-block:: csharp

    var syncOptions = new SyncOptions
    {
        ProgressLevel = SyncProgressLevel.Debug
    };

    // Creating an agent that will handle all the process
    var agent = new SyncAgent(clientProvider, serverProvider, syncOptions);

    var progress = new SynchronousProgress<ProgressArgs>(s =>
    {
        Console.WriteLine($"{s.ProgressPercentage:p}:  \t[{s.Source[..Math.Min(4, s.Source.Length)]}] {s.TypeName}: {s.Message}");
    });

    var s = await agent.SynchronizeAsync(setup, SyncType.Reinitialize, progress);
    Console.WriteLine(s);


And the details result with a ``SyncProgressLevel.Debug`` flag:

.. code-block:: bash

    0,00 %:         [Clie] SessionBeginArgs: [Client] Session Begins. Id:f62adec4-21a7-4a35-b86e-d3d7d52bc590. Scope name:DefaultScope.
    0,00 %:         [Clie] ClientScopeInfoLoadingArgs: [Client] Client Scope Table Loading.
    0,00 %:         [Clie] ClientScopeInfoLoadedArgs: [Client] [DefaultScope] [Version 0.9.5] Last sync:17/07/2022 20:06:57 Last sync duration:0:0:2.172.
    0,00 %:         [Adve] ServerScopeInfoLoadingArgs: [AdventureWorks] Server Scope Table Loading.
    0,00 %:         [Adve] ServerScopeInfoLoadedArgs: [AdventureWorks] [DefaultScope] [Version 0.9.5] Last cleanup timestamp:0.
    0,00 %:         [Adve] OperationArgs: Client Operation returned by server.
    10,00 %:        [Clie] LocalTimestampLoadingArgs: [Client] Getting Local Timestamp.
    10,00 %:        [Clie] LocalTimestampLoadedArgs: [Client] Local Timestamp Loaded:17055.
    30,00 %:        [Adve] ServerScopeInfoLoadingArgs: [AdventureWorks] Server Scope Table Loading.
    30,00 %:        [Adve] ServerScopeInfoLoadedArgs: [AdventureWorks] [DefaultScope] [Version 0.9.5] Last cleanup timestamp:0.
    30,00 %:        [Adve] DatabaseChangesApplyingArgs: Applying Changes. Total Changes To Apply: 0
    30,00 %:        [Adve] DatabaseChangesAppliedArgs: [Total] Applied:0. Conflicts:0.
    55,00 %:        [Adve] LocalTimestampLoadingArgs: [AdventureWorks] Getting Local Timestamp.
    55,00 %:        [Adve] LocalTimestampLoadedArgs: [AdventureWorks] Local Timestamp Loaded:2000.
    55,00 %:        [Adve] DatabaseChangesSelectingArgs: [AdventureWorks] Getting Changes. [C:\Users\spertus\AppData\Local\Temp\DotmimSync]. Batch size:5000. IsNew:True.
    55,00 %:        [Adve] TableChangesSelectingArgs: [Customer] Getting Changes.
    55,00 %:        [Adve] TableChangesSelectingArgs: [Address] Getting Changes.
    55,00 %:        [Adve] TableChangesSelectingArgs: [SalesOrderDetail] Getting Changes.
    55,00 %:        [Adve] TableChangesSelectingArgs: [Product] Getting Changes.
    55,00 %:        [Adve] TableChangesSelectingArgs: [ProductCategory] Getting Changes.
    55,00 %:        [Adve] TableChangesSelectingArgs: [ProductModel] Getting Changes.
    55,00 %:        [Adve] TableChangesSelectingArgs: [SalesOrderHeader] Getting Changes.
    55,00 %:        [Adve] TableChangesSelectingArgs: [CustomerAddress] Getting Changes.
    55,00 %:        [Adve] TableChangesSelectingArgs: [ProductDescription] Getting Changes.
    55,00 %:        [Adve] TableChangesSelectedArgs: [ProductCategory] [Total] Upserts:41. Deletes:0. Total:41.
    75,00 %:        [Adve] TableChangesSelectedArgs: [SalesOrderHeader] [Total] Upserts:32. Deletes:0. Total:32.
    75,00 %:        [Adve] TableChangesSelectedArgs: [ProductModel] [Total] Upserts:128. Deletes:0. Total:128.
    75,00 %:        [Adve] TableChangesSelectedArgs: [Address] [Total] Upserts:450. Deletes:0. Total:450.
    75,00 %:        [Adve] TableChangesSelectedArgs: [CustomerAddress] [Total] Upserts:417. Deletes:0. Total:417.
    75,00 %:        [Adve] TableChangesSelectedArgs: [SalesOrderDetail] [Total] Upserts:542. Deletes:0. Total:542.
    75,00 %:        [Adve] TableChangesSelectedArgs: [ProductDescription] [Total] Upserts:762. Deletes:0. Total:762.
    75,00 %:        [Adve] TableChangesSelectedArgs: [Product] [Total] Upserts:295. Deletes:0. Total:295.
    75,00 %:        [Adve] TableChangesSelectedArgs: [Customer] [Total] Upserts:847. Deletes:0. Total:847.
    75,00 %:        [Adve] DatabaseChangesSelectedArgs: [Total] Upserts:3514. Deletes:0. Total:3514. [C:\Users\spertus\AppData\Local\Temp\DotmimSync\2022_07_17_00fbihwicdj11]
    75,00 %:        [Adve] ScopeSavingArgs: [AdventureWorks] Scope Table [ServerHistory] Saving.
    75,00 %:        [Adve] ScopeSavedArgs: [AdventureWorks] Scope Table [ServerHistory] Saved.
    75,00 %:        [Clie] DatabaseChangesApplyingArgs: Applying Changes. Total Changes To Apply: 3514
    75,00 %:        [Clie] TableChangesApplyingArgs: Applying Changes To ProductDescription.
    75,00 %:        [Clie] TableChangesApplyingSyncRowsArgs: Applying [ProductDescription] batch rows. State:Modified. Count:762
    80,42 %:        [Clie] TableChangesBatchAppliedArgs: [ProductDescription] [Modified] Applied:(762) Total:(762/3514).
    80,42 %:        [Clie] TableChangesAppliedArgs: [ProductDescription] Changes Modified Applied:762. Resolved Conflicts:0.
    80,42 %:        [Clie] TableChangesApplyingArgs: Applying Changes To ProductCategory.
    80,42 %:        [Clie] TableChangesApplyingSyncRowsArgs: Applying [ProductCategory] batch rows. State:Modified. Count:41
    80,71 %:        [Clie] TableChangesBatchAppliedArgs: [ProductCategory] [Modified] Applied:(41) Total:(803/3514).
    80,71 %:        [Clie] TableChangesAppliedArgs: [ProductCategory] Changes Modified Applied:41. Resolved Conflicts:0.
    80,71 %:        [Clie] TableChangesApplyingArgs: Applying Changes To ProductModel.
    80,71 %:        [Clie] TableChangesApplyingSyncRowsArgs: Applying [ProductModel] batch rows. State:Modified. Count:128
    81,62 %:        [Clie] TableChangesBatchAppliedArgs: [ProductModel] [Modified] Applied:(128) Total:(931/3514).
    81,62 %:        [Clie] TableChangesAppliedArgs: [ProductModel] Changes Modified Applied:128. Resolved Conflicts:0.
    81,62 %:        [Clie] TableChangesApplyingArgs: Applying Changes To Product.
    81,62 %:        [Clie] TableChangesApplyingSyncRowsArgs: Applying [Product] batch rows. State:Modified. Count:295
    83,72 %:        [Clie] TableChangesBatchAppliedArgs: [Product] [Modified] Applied:(295) Total:(1226/3514).
    83,72 %:        [Clie] TableChangesAppliedArgs: [Product] Changes Modified Applied:295. Resolved Conflicts:0.
    83,72 %:        [Clie] TableChangesApplyingArgs: Applying Changes To Address.
    83,72 %:        [Clie] TableChangesApplyingSyncRowsArgs: Applying [Address] batch rows. State:Modified. Count:450
    86,92 %:        [Clie] TableChangesBatchAppliedArgs: [Address] [Modified] Applied:(450) Total:(1676/3514).
    86,92 %:        [Clie] TableChangesAppliedArgs: [Address] Changes Modified Applied:450. Resolved Conflicts:0.
    86,92 %:        [Clie] TableChangesApplyingArgs: Applying Changes To Customer.
    86,92 %:        [Clie] TableChangesApplyingSyncRowsArgs: Applying [Customer] batch rows. State:Modified. Count:847
    92,95 %:        [Clie] TableChangesBatchAppliedArgs: [Customer] [Modified] Applied:(847) Total:(2523/3514).
    92,95 %:        [Clie] TableChangesAppliedArgs: [Customer] Changes Modified Applied:847. Resolved Conflicts:0.
    92,95 %:        [Clie] TableChangesApplyingArgs: Applying Changes To CustomerAddress.
    92,95 %:        [Clie] TableChangesApplyingSyncRowsArgs: Applying [CustomerAddress] batch rows. State:Modified. Count:417
    95,92 %:        [Clie] TableChangesBatchAppliedArgs: [CustomerAddress] [Modified] Applied:(417) Total:(2940/3514).
    95,92 %:        [Clie] TableChangesAppliedArgs: [CustomerAddress] Changes Modified Applied:417. Resolved Conflicts:0.
    95,92 %:        [Clie] TableChangesApplyingArgs: Applying Changes To SalesOrderHeader.
    95,92 %:        [Clie] TableChangesApplyingSyncRowsArgs: Applying [SalesOrderHeader] batch rows. State:Modified. Count:32
    96,14 %:        [Clie] TableChangesBatchAppliedArgs: [SalesOrderHeader] [Modified] Applied:(32) Total:(2972/3514).
    96,14 %:        [Clie] TableChangesAppliedArgs: [SalesOrderHeader] Changes Modified Applied:32. Resolved Conflicts:0.
    96,14 %:        [Clie] TableChangesApplyingArgs: Applying Changes To SalesOrderDetail.
    96,14 %:        [Clie] TableChangesApplyingSyncRowsArgs: Applying [SalesOrderDetail] batch rows. State:Modified. Count:542
    100,00 %:       [Clie] TableChangesBatchAppliedArgs: [SalesOrderDetail] [Modified] Applied:(542) Total:(3514/3514).
    100,00 %:       [Clie] TableChangesAppliedArgs: [SalesOrderDetail] Changes Modified Applied:542. Resolved Conflicts:0.
    100,00 %:       [Clie] TableChangesApplyingArgs: Applying Changes To SalesOrderDetail.
    100,00 %:       [Clie] TableChangesApplyingSyncRowsArgs: Applying [SalesOrderDetail] batch rows. State:Deleted. Count:0
    100,00 %:       [Clie] TableChangesApplyingArgs: Applying Changes To SalesOrderHeader.
    100,00 %:       [Clie] TableChangesApplyingSyncRowsArgs: Applying [SalesOrderHeader] batch rows. State:Deleted. Count:0
    100,00 %:       [Clie] TableChangesApplyingArgs: Applying Changes To CustomerAddress.
    100,00 %:       [Clie] TableChangesApplyingSyncRowsArgs: Applying [CustomerAddress] batch rows. State:Deleted. Count:0
    100,00 %:       [Clie] TableChangesApplyingArgs: Applying Changes To Customer.
    100,00 %:       [Clie] TableChangesApplyingSyncRowsArgs: Applying [Customer] batch rows. State:Deleted. Count:0
    100,00 %:       [Clie] TableChangesApplyingArgs: Applying Changes To Address.
    100,00 %:       [Clie] TableChangesApplyingSyncRowsArgs: Applying [Address] batch rows. State:Deleted. Count:0
    100,00 %:       [Clie] TableChangesApplyingArgs: Applying Changes To Product.
    100,00 %:       [Clie] TableChangesApplyingSyncRowsArgs: Applying [Product] batch rows. State:Deleted. Count:0
    100,00 %:       [Clie] TableChangesApplyingArgs: Applying Changes To ProductModel.
    100,00 %:       [Clie] TableChangesApplyingSyncRowsArgs: Applying [ProductModel] batch rows. State:Deleted. Count:0
    100,00 %:       [Clie] TableChangesApplyingArgs: Applying Changes To ProductCategory.
    100,00 %:       [Clie] TableChangesApplyingSyncRowsArgs: Applying [ProductCategory] batch rows. State:Deleted. Count:0
    100,00 %:       [Clie] TableChangesApplyingArgs: Applying Changes To ProductDescription.
    100,00 %:       [Clie] TableChangesApplyingSyncRowsArgs: Applying [ProductDescription] batch rows. State:Deleted. Count:0
    100,00 %:       [Clie] DatabaseChangesAppliedArgs: [Total] Applied:3514. Conflicts:0.
    100,00 %:       [Clie] ClientScopeInfoLoadingArgs: [Client] Client Scope Table Loading.
    100,00 %:       [Clie] ClientScopeInfoLoadedArgs: [Client] [DefaultScope] [Version 0.9.5] Last sync:17/07/2022 20:06:57 Last sync duration:0:0:2.172.
    100,00 %:       [Clie] MetadataCleaningArgs: Cleaning Metadatas.
    100,00 %:       [Clie] MetadataCleanedArgs: Tables Cleaned:0. Rows Cleaned:0.
    100,00 %:       [Clie] ScopeSavingArgs: [Client] Scope Table [Client] Saving.
    100,00 %:       [Clie] ScopeSavedArgs: [Client] Scope Table [Client] Saved.
    100,00 %:       [Clie] SessionEndArgs: [Client] Session Ends. Id:f62adec4-21a7-4a35-b86e-d3d7d52bc590. Scope name:DefaultScope.
    Synchronization done.
            Total changes  uploaded: 0
            Total changes  downloaded: 3514
            Total changes  applied: 3514
            Total resolved conflicts: 0
            Total duration :00.00:00.509
    Sync Ended. Press a key to start again, or Escapte to end