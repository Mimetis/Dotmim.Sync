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

    var tables = new string[] {"ProductCategory", "ProductModel", "Product",
                "Address", "Customer", "CustomerAddress", "SalesOrderHeader", 
                "SalesOrderDetail" };

    var agent = new SyncAgent(clientProvider, serverProvider, tables);
    do
    {
        // Launch the sync process
        var s1 = await agent.SynchronizeAsync();
        // Write results
        Console.WriteLine(s1);

    } while (Console.ReadKey().Key != ConsoleKey.Escape);

    Console.WriteLine("End");

We are going to see how to get useful information, from each stage involved during the sync processus, thanks to ``IProgress<T>`` and then we will go deeper with the notion of ``Interceptor<T>``.

.. hint:: You will find this complete sample here : `Progression sample <https://github.com/Mimetis/Dotmim.Sync/tree/master/Samples/Progression>`_ 

IProgress\<T\>
^^^^^^^^^^^^^^^^

As we said, the progress values are triggered from both side : **Server** side and **Client** side, ordered.  

In our sample, we can say that : 

* The ``RemoteOrchestrator`` instance, using the server provider instance, will report all the progress from the server side.   
* The ``LocalOrchestrator`` instance using the client provider instance, will report all the progress from the client side.  


.. note:: A ``syncAgent`` object is **always** running on the client side of **any** architecture.  

Since our main method ``SynchronizeAsync()`` is marked ``async`` method, we will use the `Progress\<T\> <https://docs.microsoft.com/en-us/dotnet/api/system.progress-1?view=netcore-2.2>`_ to be able to report progress value.

So far, the most straightforward way to get feedback from a current sync, is to pass an instance of ``Progress<T>`` when calling the method ``SynchronizeAsync()``.

.. note:: ``Progress<T>`` is **not** synchronous. So far, no guarantee that the progress callbacks will be raised in an ordered way.   
          
          That's why you can use a **DMS** progess class called ``SynchronousProgress<T>`` which is synchronous, using the correct synchronization context.

Here is a quick example used to provide some feedback to the user:   

.. code-block:: csharp

    var serverProvider = new SqlSyncChangeTrackingProvider(serverConnectionString);
    var clientProvider = new SqlSyncProvider(clientConnectionString);

    // Tables involved in the sync process:
    var tables = new string[] {"ProductCategory", "ProductModel", "Product",
        "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail" };

    // Creating an agent that will handle all the process
    var agent = new SyncAgent(clientProvider, serverProvider, tables);

    // Using the IProgress<T> pattern to handle progession dring the synchronization
    var progress = new SynchronousProgress<ProgressArgs>(args => 
        Console.WriteLine($"{s.PogressPercentageString}:\t{s.Source}:\t{s.Message}"));

    do
    {
        // Launch the sync process
        var s1 = await agent.SynchronizeAsync(progress);
        // Write results
        Console.WriteLine(s1);

    } while (Console.ReadKey().Key != ConsoleKey.Escape);

    Console.WriteLine("End");


Here is the result, after the first synchronization, assuming the **Client** database is empty:


.. code-block:: bash

    0%:     625b4be7-54a5-4fe7-8a47-cd2bf46f15b9:   Session Begins.
    0%:     AdventureWorks: Schema Loaded For 9 Tables.
    0%:     AdventureWorks: Provisioned 9 Tables. Provision:TrackingTable, StoredProcedures, Triggers.
    0%:     Client: Provisioned 9 Tables. Provision:Table, TrackingTable, StoredProcedures, Triggers.
    30%:    AdventureWorks: [Total] Applied:0. Conflicts:0.
    57%:    AdventureWorks: [ProductDescription] [Total] Upserts:762. Deletes:0. Total:762.
    59%:    AdventureWorks: [ProductCategory] [Total] Upserts:41. Deletes:0. Total:41.
    61%:    AdventureWorks: [ProductModel] [Total] Upserts:128. Deletes:0. Total:128.
    63%:    AdventureWorks: [Product] [Total] Upserts:295. Deletes:0. Total:295.
    66%:    AdventureWorks: [Address] [Total] Upserts:450. Deletes:0. Total:450.
    68%:    AdventureWorks: [Customer] [Total] Upserts:847. Deletes:0. Total:847.
    70%:    AdventureWorks: [CustomerAddress] [Total] Upserts:417. Deletes:0. Total:417.
    72%:    AdventureWorks: [SalesOrderHeader] [Total] Upserts:32. Deletes:0. Total:32.
    75%:    AdventureWorks: [SalesOrderDetail] [Total] Upserts:542. Deletes:0. Total:542.
    75%:    AdventureWorks: [Total] Upserts:3514. Deletes:0. Total:3514
    80%:    Client: [ProductDescription] [Modified] Applied:(762) Total:(762/3514).
    80%:    Client: [ProductCategory] [Modified] Applied:(41) Total:(803/3514).
    81%:    Client: [ProductModel] [Modified] Applied:(128) Total:(931/3514).
    83%:    Client: [Product] [Modified] Applied:(295) Total:(1226/3514).
    86%:    Client: [Address] [Modified] Applied:(450) Total:(1676/3514).
    92%:    Client: [Customer] [Modified] Applied:(847) Total:(2523/3514).
    95%:    Client: [CustomerAddress] [Modified] Applied:(417) Total:(2940/3514).
    96%:    Client: [SalesOrderHeader] [Modified] Applied:(32) Total:(2972/3514).
    100%:   Client: [SalesOrderDetail] [Modified] Applied:(542) Total:(3514/3514).
    100%:   Client: [Total] Applied:3514. Conflicts:0.
    100%:   625b4be7-54a5-4fe7-8a47-cd2bf46f15b9:   Session Ended.
    Synchronization done.
            Total changes  uploaded: 0
            Total changes  downloaded: 3514
            Total changes  applied: 3514
            Total resolved conflicts: 0
            Total duration :0:0:7.440


As you can see, it's a first synchronization, so:

* Session begins 
* Server creates all metadatas needed for AdventureWorks database
* Client creates all metadatas needed for Client database
* Server selects all changes to upserts
* Client applies all changes sent from ths server
* Client selects changes to send (nothing, obviously, because the tables have just been created on the client)
* Session ends
