ScopeInfoClients
================================

What is a scope client ?
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We saw that a **scope** is a set of tables and is stored in the :guilabel:`scope_info` table.

A **scope client** is a the association of one scope with a filter, and is stored in the :guilabel:`scope_info_client`  table.

A scope client record contains:

- A scope (think "**FROM**" in a database) : Set of tables defined in the scope_info table
- A list of filter parameters (think "**WHERE**" in a database) : The filter definition is stored in the scope. We are talking here about the values of theses filter.

Let's imagine you are synchronizing some **Products** and **ProductCategories**, where you want only the products of the category **"Books"**. You will have to define a scope client with the following parameters:

- **Scope** : :guilabel:`Product`, :guilabel:`ProductCategory` tables.
- **Filter parameters values** : ``ProductCategoryID = "Books"``

**DMS** will automatically create:

- The scope in **scope_info** with the 2 tables :guilabel:`Product`, :guilabel:`ProductCategory`.
- The filter parameter value ``ProductCategoryID = 'Books'`` in the **scope_info_client** table.


Methods & Properties
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can access the scope client information, as a ``ScopeInfoClient`` instance, using a ``LocalOrchestrator`` or ``RemoteOrchestrator`` instance (directly from a ``SyncAgent`` instance or by creating a new instance directly)


Properties
---------------------

Once a first scope sync has been done, you will have, on both sides, a :guilabel:`scope_info_client`  table, containing:

- A **scope name**: Defines a user friendly name (unique) for your scope. Default name is ``DefaultScope``. References the :guilabel:`scope_info` table.
- A **scope info client id**: Defines a unique id for the scope client. Think this Id as the uniqure representation of the client database.
- A **scope info hash**: Defines the hash of the JSON property ``scope_parameters``.
- A **scope info parameters**: Defines the parameters for this scope info client. This is a JSON property, containing the list of filter parameters values, and is, combined with **scope_name**, unique.
- A **scope info timestamp**: Defines the last time the scope info client has been updated.
- A **scope info server timestamp**: Defines the last time the scope info client has been updated on the server side.
- A **scope last sync date**: Defines the last time the scope has been synchronized, as a datetime.
- A **scope last sync duration**: Defines the last time the scope has been synchronized, as a duration.
- A **scope errors**: Defines the last errors happened during last sync. Point directly to a BatchInfo directory containing the errors (as JSON files).
- A **scope properties**: Defines additionnal properties.

Here is a small example to see how scope client infos are created:

.. code-block:: csharp

  var setup = new SyncSetup("ProductCategory", "Product", "Employee");
  
  setup.Tables[productCategoryTableName].Columns
          .AddRange("ProductCategoryId", "Name", "rowguid", "ModifiedDate");

  setup.Filters.Add("ProductCategory", "ProductCategoryId");
  setup.Filters.Add("Product", "ProductCategoryId");

  var pMount = new SyncParameters(("ProductCategoryId", "MOUNTB"));
  var pRoad = new SyncParameters(("ProductCategoryId", "ROADFR"));

  var agent = new SyncAgent(client.Provider, Server.Provider);
  var r1 = await agent.SynchronizeAsync("v1", setup, pMount);
  var r2 = await agent.SynchronizeAsync("v1", setup, pRoad);

Once the sync is done, you will have 2 scope clients created:


===============   ============================================== ================================================== ==================================================
sync_scope_id     sync_scope_name                                sync_scope_parameters                              scope_last_sync_timestamp  
---------------   ---------------------------------------------- -------------------------------------------------- --------------------------------------------------
F02BC17-A478-..   v1                                             [{pn:ProductCategoryId, v:MOUNTB}]                 2000
---------------   ---------------------------------------------- -------------------------------------------------- --------------------------------------------------
F02BC17-A478-..   v1                                             [{pn:ProductCategoryId, v:ROADFR}]                 20022
===============   ============================================== ================================================== ==================================================

Each scope client is independant, and can be synchronized separately, since they have their own **timestamp** associated with their combo **scope name / scope parameters**.

.. note:: We have the same scope for both sync, with the same tables / scope name. You'll see that the :guilabel:`scope_info`  will contains only one record for the scope v1

The corresponding .NET objet is the ``ScopeInfoClient`` class:

.. code-block:: csharp

 public class ScopeInfoClient
    {
        public Guid Id { get; set; }
        public string Name { get; set; }
        public string Hash { get; set; }
        public long? LastSyncTimestamp { get; set; }
        public long? LastServerSyncTimestamp { get; set; }
        public bool IsNewScope { get; set; }
        public SyncParameters Parameters { get; set; }
        public DateTime? LastSync { get; set; }
        public long LastSyncDuration { get; set; }
        public string Properties { get; set; }
        public string Errors { get; set; }
        public string LastSyncDurationString { get; }
    }


GetScopeInfoClientAsync
------------------------

This method allows to get a scope client information, from a scope name and a list of filter parameters values.

.. code-block:: csharp

  var parameters = new SyncParameters(("ProductCategoryId", "MOUNTB"));
  var scopeInfoClient = await orchestrator.GetScopeInfoClientAsync("v1", parameters);

.. note:: If the :guilabel:`scope_info_client`  does not exists, it will be created, and the a new record is added.

.. warning:: If you call this method using a ``RemoteOrchestrator``, you'll need to pass the clientId parameter 

GetAllScopeInfosAsync
----------------------

Returns all scope clients information, from a scope name.

.. code-block:: csharp

    var cAllScopeInfoClients = await agent.LocalOrchestrator.GetAllScopeInfoClientsAsync();

    var minServerTimeStamp = cAllScopeInfoClients.Min(sic => sic.LastServerSyncTimestamp);
    var minClientTimeStamp = cAllScopeInfoClients.Min(sic => sic.LastSyncTimestamp);
    var minLastSync = cAllScopeInfoClients.Min(sic => sic.LastSync);



SaveScopeInfoAsync
------------------

This method allows you to save and override a scope client information. You should not have to do it, but some scenarios can be done with this method.

.. code-block:: csharp

  var cScopeInfoClient = await localOrchestrator.GetScopeInfoClientAsync();

  if (cScopeInfoClient.IsNewScope)
  {
    cScopeInfoClient.IsNewScope = false;
    cScopeInfoClient.LastSync = DateTime.Now;
    cScopeInfoClient.LastSyncTimestamp = 0;
    cScopeInfoClient.LastServerSyncTimestamp = 0;

    await agent.LocalOrchestrator.SaveScopeInfoClientAsync(cScopeInfoClient);
  }




