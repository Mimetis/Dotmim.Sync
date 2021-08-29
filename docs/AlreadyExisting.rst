Tables & Rows already existing
==================================

How to handle existing **clients** databases, with **existing** rows...

Default behavior
^^^^^^^^^^^^^^^^^^^^^^^^

Before going further let's see the default behavior of ``DMS``, regarding this particular scenario where you have existing rows in your client databases:

Basically, ``DMS`` will not take care of any existing client rows. 
On the first sync, these rows will stay on the client and will not be uploaded to the server (On the other part, of course the server rows will be downloaded to the client)

(Obviously, after this first sync, if you are updating locally any of these existing rows, they will be handled on the next sync)

The reason behind this behavior is to fit the scenario where you want to use a client database with some pre-existing rows (for example a server backup downloaded to the client ?) and where you don't wan't to upload them to the server (because they are already existing on the server)

Now, we can have a second scenario where you actually want to upload these pre-existing rows.

For this scenario, you have a special method, available on the ``LocalOrchestrator`` only, called ``UpdateUntrackedRowsAsync`` that will mark all non tracked rows for the next sync.

UpdateUntrackedRowsAsync
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. note:: You will find a complete sample here : `Already Existing rows <https://github.com/Mimetis/Dotmim.Sync/tree/master/Samples/AlreadyExistingDatabases>`_ 


Assuming you have a client database with some pre-existing rows and before going further, be sure that your server and client table has the same schema.

The workflow to handle these lines is:

* Make a first sync, to be sure we have all the required metadata locally (tracking tables, triggers, stored proc ...)
    * During this first sync, you will download the server rows as well.
* Call the ``UpdateUntrackedRowsAsync`` method to mark all non tracked client rows.
* Make a second sync to upload these rows to server.

Here is a small sample, following this workflow:

.. code-block:: csharp

    // Tables involved in the sync process:
    var tables = new string[] { "ServiceTickets" };

    // Creating an agent that will handle all the process
    var agent = new SyncAgent(clientProvider, serverProvider, tables);

    // Launch the sync process
    // This first sync will create all the sync architecture
    // and will get the server rows
    var s1 = await agent.SynchronizeAsync();

    // This first sync did not upload the client rows.
    // We only have rows from server that have been downloaded
    // The important step here, done by 1st Sync,
    // is to have setup everything locally (triggers / tracking tables ...)
    Console.WriteLine(s1);

    // Now we can "mark" original clients rows as "to be uploaded"
    await agent.LocalOrchestrator.UpdateUntrackedRowsAsync();

    // Then we can make a new synchronize to upload these rows to server
    // Launch the sync process
    var s2 = await agent.SynchronizeAsync();
    Console.WriteLine(s2);
