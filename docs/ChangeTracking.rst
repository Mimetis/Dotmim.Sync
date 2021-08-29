Change Tracking
============================
| **SQL Server** provides a great feature that track changes to data in a database: **Change tracking**. 
| This features enables applications to determine the DML changes (insert, update, and delete operations) that were made to user tables in a database. 

| Change tracking is supported since **SQL Server 2008** and is available from within **Azure Sql Database**.
| If you need, for some reasons, to run your sync from an older version, you will have to fallback on the ``SqlSyncProvider``.

.. note:: If you need more information on this feature, the best place to start is here : `Track data changes with SQL Server <https://docs.microsoft.com/en-us/sql/relational-databases/track-changes/track-data-changes-sql-server?view=sql-server-ver15#Tracking>`_ 

| A new **Sql** sync provider which uses this **Change Tracking** feature is available with **DMS**:
| This provider is called ``SqlSyncChangeTrackingProvider``.

The ``SqlSyncChangeTrackingProvider`` is compatible with all others sync providers: You can have a server database using the ``SqlSyncChangeTrackingProvider`` and some clients databases using any of the others providers.

What does it mean to use Change Tracking from within your database ?

* No more tracking tables in your database
* No more triggers on your tables in your database
* Metadatas retention managed by SQL Server itself 
* Changes tracked by the SQL Engine, way better performances than using triggers and tracking tables

To be able to use ``SqlSyncChangeTrackingProvider`` on your database, do not forget to activate the **Change Tracking** on your database :

.. code-block:: sql

    ALTER DATABASE AdventureWorks  
    SET CHANGE_TRACKING = ON  
    (CHANGE_RETENTION = 14 DAYS, AUTO_CLEANUP = ON)  

You don't have to activate **Change Tracking** on each table. It will be enabled by **DMS** on each table part of the sync process.

Once it's done, the code is almost the same:

.. code-block:: csharp

    var serverProvider = new SqlSyncChangeTrackingProvider("Data Source=....");
    var clientProvider = new SqlSyncChangeTrackingProvider("Data Source=....");

