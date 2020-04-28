Synchronization types
=================================

You have one main method to launch a synchronization, with several optional parameters:

.. code-block:: csharp

	SynchronizeAsync();
	SynchronizeAsync(IProgress<ProgressArgs> progress);
	SynchronizeAsync(CancellationToken cancellationToken);
	SynchronizeAsync(SyncType syncType);
	SynchronizeAsync(SyncType syncType, CancellationToken cancellationToken);


| You can use the ``CancellationToken`` object whenever you want to rollback an "*in progress*" synchronization.
| And since we have an async synchronization, you can pass an ``IProgress<ProgressArgs>`` object to have feedback during the sync process.

.. note:: The progression system is explained in the next chapter `Progress <Progression.html>`_ 


let's see now a straightforward sample illustrating the use of the ``SyncType`` argument.

.. hint:: You will find the sample used for this chapter, here : `SyncType sample <https://github.com/Mimetis/Dotmim.Sync/tree/master/Samples/SyncType>`_ 

.. code-block:: csharp

	SqlSyncProvider serverProvider = new SqlSyncProvider(GetDatabaseConnectionString("AdventureWorks"));
	SqlSyncProvider clientProvider = new SqlSyncProvider(GetDatabaseConnectionString("Client"));

	SyncAgent agent = new SyncAgent(clientProvider, serverProvider, new string[] {
	"ProductCategory", "ProductModel", "Product", "Address", "Customer", 
	"CustomerAddress", "SalesOrderHeader", "SalesOrderDetail"});

	var syncContext = await agent.SynchronizeAsync();

	Console.WriteLine(syncContext);

Here is the result, after the **first initial** synchronization:

.. code-block:: bash

	Synchronization done.
		Total changes  uploaded: 0
		Total changes  downloaded: 2752
		Total changes  applied: 2752
		Total resolved conflicts: 0
		Total duration :0:0:4.720

As you can see, the client has downloaded 2752 lines from the server.   
Obviously if we made a new sync, without making any changes neither on the server nor the client, the result will be :

.. code-block:: bash

	Synchronization done.
		Total changes  uploaded: 0
		Total changes  downloaded: 0
		Total changes  applied: 0
		Total resolved conflicts: 0
		Total duration :0:0:0.382

Ok make sense !

SyncType
^^^^^^^^^^^^

| The ``SyncType`` enumeration allows you to **reinitialize** a client database (already synchronized or not).  
| For various reason, you may want to re-download the whole database schema and rows from the server (bug, out of sync, and so on ...)

``SyncType`` is mainly an enumeration used when calling the ``SynchronizeAsync()`` method:

.. code-block:: csharp

	public enum SyncType
	{
		/// <summary>
		/// Normal synchronization
		/// </summary>
		Normal,
		/// <summary>
		/// Reinitialize the whole sync database, applying all rows from the server to the client
		/// </summary>
		Reinitialize,
		/// <summary>
		/// Reinitialize the whole sync database, applying all rows from the server to the client, 
		/// after tried a client upload
		/// </summary>
		ReinitializeWithUpload
	}


* ``SyncType.Normal``: Default value, represents a normal sync process.
* ``SyncType.Reinitialize``: Marks the client to be resynchronized. Be careful, any changes on the client will be overwritten by this value.
* ``SyncType.ReinitializeWithUpload``: Like *Reinitialize* this value will launch a process to resynchronize the whole client database, except that the client will *try* to send its local changes before making the resync process.

From the sample we saw before, here is the different behaviors with each ``SyncType`` enumeration value:  

First of all, for demo purpose, we are updating a row on the **client**:

.. code-block:: sql

	-- initial value is 'The Bike Store'
	UPDATE Client.dbo.Customer SET CompanyName='The New Bike Store' WHERE CustomerId = 1 


SyncType.Normal
--------------------

Let's see what happens, now that we have updated a row on the client side, with a *normal* sync:

.. code-block:: csharp

	SqlSyncProvider serverProvider = new SqlSyncProvider(GetDatabaseConnectionString("AdventureWorks"));
	SqlSyncProvider clientProvider = new SqlSyncProvider(GetDatabaseConnectionString("Client"));

	SyncAgent agent = new SyncAgent(clientProvider, serverProvider, new string[] {
	"ProductCategory", "ProductModel", "Product", "Address", "Customer", 
	"CustomerAddress", "SalesOrderHeader", "SalesOrderDetail"});

	var syncContext = await agent.SynchronizeAsync();

	Console.WriteLine(syncContext);

.. code-block:: bash

	Synchronization done.
			Total changes  uploaded: 1
			Total changes  downloaded: 0
			Total changes  applied: 0
			Total resolved conflicts: 0
			Total duration :0:0:1.382

The default behavior is what we were waiting for: Uploading the modified row to the server.

SyncType.Reinitialize
-------------------------

The ``SyncType.Reinitialize`` mode will **reinitialize** the whole client database.

Every rows on the client will be deleted and downloaded again from the server, even if some of them are not synced correctly.

Use this mode with caution, since you could lost some "*out of sync client*" rows.


.. code-block:: csharp

	SqlSyncProvider serverProvider = new SqlSyncProvider(GetDatabaseConnectionString("AdventureWorks"));
	SqlSyncProvider clientProvider = new SqlSyncProvider(GetDatabaseConnectionString("Client"));

	SyncAgent agent = new SyncAgent(clientProvider, serverProvider, new string[] {
	"ProductCategory", "ProductModel", "Product", "Address", "Customer", 
	"CustomerAddress", "SalesOrderHeader", "SalesOrderDetail"});

	var syncContext = await agent.SynchronizeAsync(SyncType.Reinitialize);

	Console.WriteLine(syncContext);

.. code-block:: bash

	Synchronization done.
			Total changes  uploaded: 0
			Total changes  downloaded: 2752
			Total changes  applied: 2752
			Total resolved conflicts: 0
			Total duration :0:0:1.872

As you can see, the ``SyncType.Reinitialize`` value has marked the client database to be fully resynchronized.  

The modified row on the client has not been sent to the server and then has been restored to the initial value sent by the server row.


SyncType.ReinitializeWithUpload
-----------------------------------

``ReinitializeWithUpload`` will do the same job as ``Reinitialize`` except it will send any changes available from the client, before making the reinitialize phase.


.. code-block:: csharp

	SqlSyncProvider serverProvider = new SqlSyncProvider(GetDatabaseConnectionString("AdventureWorks"));
	SqlSyncProvider clientProvider = new SqlSyncProvider(GetDatabaseConnectionString("Client"));

	SyncAgent agent = new SyncAgent(clientProvider, serverProvider, new string[] {
	"ProductCategory", "ProductModel", "Product", "Address", "Customer", 
	"CustomerAddress", "SalesOrderHeader", "SalesOrderDetail"});

	var syncResult = await agent.SynchronizeAsync(SyncType.ReinitializeWithUpload);

	Console.WriteLine(syncResult);

.. code-block:: bash

	Synchronization done.
			Total changes  uploaded: 1
			Total changes  downloaded: 2752
			Total changes  applied: 2752
			Total resolved conflicts: 0
			Total duration :0:0:1.923

In this case, as you can see, the ``SyncType.ReinitializeWithUpload`` value has marked the client database to be fully resynchronized, but the edited row has been sent correctly to the server.  


Forcing Reinitialize 
^^^^^^^^^^^^^^^^^^^^^

.. warning:: This part covers some concept explained later in the next chapters:

			* Progression : `Using interceptors <Progression.html#interceptor-t>`_.
			* HTTP architecture : `Using ASP.Net Web API <Web.html>`_ 


| This technic applies if you do not have access to the client machine, allowing you to *force* the reinitialization of the client.
| It could be useful to *override* a normal synchronization with a reinitialization for a particular client, from the server side.

.. note:: Forcing a reinitialization from the server is a good practice if you have an **HTTP** architecture.

Using an `interceptor <Progression.html#interceptor-t>`_, from the **server side**, you are able to *force* the reinitialization from the client.


On the server side, from your controller, just modify the request ``SyncContext`` with the correct value, like this:

.. code-block:: csharp

	[HttpPost]
	public async Task Post()
	{

		// Get Orchestrator regarding the incoming scope name (from http context)
		var orchestrator = webServerManager.GetOrchestrator(this.HttpContext);

		// override sync type to force a reinitialization from a particular client
		orchestrator.OnServerScopeLoaded(sla =>
		{
			// ClientId represents one client. If you want to reinitialize ALL clients, just remove this condition
			if (sla.Context.ClientScopeId == clientId)
			{
				sla.Context.SyncType = SyncType.Reinitialize;
			}
		});

		// handle request
		await webServerManager.HandleRequestAsync(this.HttpContext);
	}

