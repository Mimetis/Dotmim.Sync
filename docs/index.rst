.. Dotmim.Sync documentation master file, created by
   sphinx-quickstart on Tue Apr 21 15:27:02 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Dotmim.Sync
=============================================

.. image:: assets/Smallicon.png
   :align: center
   :alt: icon

   
**DotMim.Sync** (**DMS**) is a straightforward framework for syncing relational databases, developed on top of **.Net Standard 2.0**, available and ready to use within  **Xamarin**, **MAUI**, **.NET Core 3.1**, **.NET 6 & 7** and so on :)  

Available for syncing **SQL Server**, **MySQL**, **MariaDB** and **Sqlite** databases.

.. note:: The source code is available on `Github <https://www.github.com/mimetis/dotmim.sync>`_. 
   
   This framework is still in beta. There is no support other than me and the time I can put on it. Don't be afraid to reach me out, but expect delay sometimes :)

.. image:: assets/allinone.png
   :align: center
   :alt: all in one

.. image:: assets/Architecture01.png
   :alt: Architecture

Starting from scratch
=============================================

Here is the easiest way to create a first sync, from scratch : 

* Create a **.NET Core 3.1** or **.NET 6** / **.NET 7** console application.  
* Add the nugets packages `DotMim.Sync.SqlServer <https://www.nuget.org/packages/Dotmim.Sync.SqlServer>`_  and `DotMim.Sync.Sqlite <https://www.nuget.org/packages/Dotmim.Sync.Sqlite>`_  
* If you don't have any hub database for testing purpose, use this one : `AdventureWorks lightweight script for SQL Server </CreateAdventureWorks.sql>`_ 
* If you want to test **MySql**, use this script : `AdventureWorks lightweight script for MySQL Server </CreateMySqlAdventureWorks.sql>`_   

Add this code:

.. code-block:: csharp

   // Sql Server provider, the "server" or "hub".
   SqlSyncProvider serverProvider = new SqlSyncProvider(
      @"Data Source=.;Initial Catalog=AdventureWorks;Integrated Security=true;");

   // Sqlite Client provider acting as the "client"
   SqliteSyncProvider clientProvider = new SqliteSyncProvider("advworks.db");

   // Tables involved in the sync process:
   var setup = new SyncSetup("ProductCategory", "ProductDescription", "ProductModel", 
    "Product", "ProductModelProductDescription", "Address", "Customer", 
    "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail" );

   // Sync agent
   SyncAgent agent = new SyncAgent(clientProvider, serverProvider);

   do
   {
      var result = await agent.SynchronizeAsync(setup);
      Console.WriteLine(result);

   } while (Console.ReadKey().Key != ConsoleKey.Escape);


And here is the result you should have, after a few seconds:

.. code-block:: csharp

   Synchronization done.
         Total changes  uploaded: 0
         Total changes  downloaded: 3514
         Total changes  applied on client: 3514
         Total changes  applied on server: 0
         Total changes  failed to apply on client: 0
         Total changes  failed to apply on server: 0
         Total resolved conflicts: 0
         Total duration :00.00:02.125

You're done !

Now try to update a row in your client or server database, then hit enter again.   
You should see something like that:

.. code-block:: csharp

   Synchronization done.
         Total changes  uploaded: 0
         Total changes  downloaded: 1
         Total changes  applied on client: 1
         Total changes  applied on server: 0
         Total changes  failed to apply on client: 0
         Total changes  failed to apply on server: 0
         Total resolved conflicts: 0
         Total duration :00.00:00.030

Yes it's blazing fast !

Need Help
=============================================

Feel free to ping me: `@sebpertus <http://www.twitter.com/sebpertus>`_ 



.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: DMS

   Overview
   HowDoesItWorks
   Synchronize
   Scopes
   ScopeClients
   Orchestrators
   Progression
   Interceptors
   ChangeTracking
   Web
   WebSecurity
   SerializerConverter
   Timeout
   Snapshot
   Configuration
   Provision
   Metadatas
   Conflict
   Errors
   Filters
   SqliteEncryption
   AlreadyExisting
   Debugging
