using Dotmim.Sync;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using Microsoft.Data.SqlClient;
using System;
using System.IO;
using System.Threading.Tasks;

namespace Migration
{
    class Program
    {
        private static string serverConnectionString = $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog=AdventureWorks;Integrated Security=true;";
        private static string clientConnectionString = $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog=Client;Integrated Security=true;";

        static async Task Main(string[] args)
        {
            /* 
             
            The idea here is to see how 2 clients will handle a migration
            We need to add a column [CreatedDate] in the [Address] table
            This column has default null value to allows clients to still sync even if they
            are not migrated

            The first client (Sql Server) will migrate immediately
            The second client (Sqlite) will not migrate and will stay on the old schema without the new column

            */
            await MigrateClientsUsingMultiScopesAsync();

        }

        private static async Task MigrateClientsUsingMultiScopesAsync()
        {
            // Create the server Sync provider
            var serverProvider = new SqlSyncProvider(serverConnectionString);

            // Create 2 clients. First will migrate, 2nd will stay without new column
            var client1Provider = new SqlSyncProvider(clientConnectionString);
            var databaseName = $"{Path.GetRandomFileName().Replace(".", "").ToLowerInvariant()}.db";
            var client2Provider = new SqliteSyncProvider(databaseName);

            // Create standard Setup
            var setup = new SyncSetup("Address", "Customer", "CustomerAddress");

            // Creating agents that will handle all the process
            var agent1 = new SyncAgent(client1Provider, serverProvider);
            var agent2 = new SyncAgent(client2Provider, serverProvider);

            // Using the Progress pattern to handle progession during the synchronization
            var progress = new SynchronousProgress<ProgressArgs>(args => Console.WriteLine($"{args.ProgressPercentage:p}:\t{args.Message}"));

            // First sync to have a starting point
            // To make a full example, we are going to use differente scope name (v0, v1)
            // v0 is the initial database
            // v1 will contains the new column in the Address table
            var s1 = await agent1.SynchronizeAsync("v0", setup, progress);
            Console.WriteLine("Initial Sync on Sql Server Client 1");
            Console.WriteLine(s1);

            var s2 = await agent2.SynchronizeAsync("v0", setup, progress);
            Console.WriteLine("Initial Sync on Sqlite Client 2");
            Console.WriteLine(s2);


            // -----------------------------------------------------------------
            // Migrating a table by adding a new column
            // -----------------------------------------------------------------

            // Adding a new column called CreatedDate to Address table, on the server
            await Helper.AddNewColumnToAddressAsync(new SqlConnection(serverConnectionString));
            Console.WriteLine("Column added on server");

            // -----------------------------------------------------------------
            // Server side
            // -----------------------------------------------------------------

            // Creating a new setup with the same tables
            // We are going to provision a new scope (v1)
            // Since this scope is not existing yet, it will force DMS to refresh the schema and
            // get the new column
            var setupAddress = new SyncSetup("Address", "Customer", "CustomerAddress");

            // Create a server orchestrator used to Deprovision and Provision only table Address
            var remoteOrchestrator = new RemoteOrchestrator(serverProvider);

            // Provision everything again for this new scope v1, 
            // This provision method will fetch the address schema from the database, 
            // since the new scope name is not existing yet
            // so it will contains all the columns, including the new Address column added
            await remoteOrchestrator.ProvisionAsync("v1", setupAddress, progress:progress);
            Console.WriteLine("Server migration with new column CreatedDate done.");


            // At this point, server database has two scopes:
            // v0   : first scope with Address table without the new column
            // v1   : second scope with Address table with the new column CreatedDate

            // Take a look at the database in SQL management studio and see differences in stored proc

            // Now add a row on the server (with the new column)
            var addressId = await Helper.InsertOneAddressWithNewColumnAsync(new SqlConnection(serverConnectionString));
            Console.WriteLine($"New address row added with pk {addressId}");

            // -----------------------------------------------------------------
            // SQlite Client will stay on old schema (without the new CreatedDate column)
            // -----------------------------------------------------------------

            // First of all, we are still able to sync the local database without having to migrate the client
            // allows old clients that do not have the new column, to continue sync normally
            // these old clients will continue to sync on the v0 scope

            var s3 = await agent2.SynchronizeAsync("v0", setup, progress: progress);
            Console.WriteLine($"Sqlite not migrated, doing a sync on first scope v0:");
            Console.WriteLine(s3);

            // If we get the row from the client, we have the new row inserted on server,
            // but without the new column
            var client2row = await Helper.GetLastAddressRowAsync(client2Provider.CreateConnection(), addressId);
            Console.WriteLine(client2row);

            // -----------------------------------------------------------------
            // SQL Server Client will add the column and will sync on the new scope (with the new CreatedDate column)
            // -----------------------------------------------------------------

            // Now we are going to upgrade the client 1

            // adding the column to the client
            await Helper.AddNewColumnToAddressAsync(new SqlConnection(clientConnectionString));
            Console.WriteLine("Sql Server client1 migration with new column CreatedDate done.");

            // Provision client with the new the V1 scope
            // Getting the scope from server and apply it locally
            var serverScope = await agent1.RemoteOrchestrator.GetServerScopeInfoAsync("v1", progress: progress);
            var v1clientScope = await agent1.LocalOrchestrator.ProvisionAsync(serverScope, progress:progress);
            Console.WriteLine("Sql Server client1 Provision done.");

            // if you look the stored procs on your local sql database
            // you will that you have the two scopes (v0 and v1)

            // TRICKY PART
            /* 
                The scope v1 is new.
                If we sync now, since v1 is new, we are going to sync all the rows from start
                What we want is to sync from the last point we sync the old v0 scope
                That's why we are shadowing the metadata info from v0 into v1  
            */
            var v0clientScope = await agent1.LocalOrchestrator.GetClientScopeInfoAsync("v0");
            v1clientScope.ShadowScope(v0clientScope);
            v1clientScope = await agent1.LocalOrchestrator.SaveClientScopeInfoAsync(v1clientScope);

            // Now test a new sync, on this new scope v1
            var s4 = await agent1.SynchronizeAsync("v1", progress: progress);
            Console.WriteLine($"Sql Server client1 migrated, doing a sync on second scope v1:");
            Console.WriteLine(s4);

            // If we get the client row from the client database, it should contains the value
            var client1row = await Helper.GetLastAddressRowAsync(new SqlConnection(clientConnectionString), addressId);

            Console.WriteLine(client1row);

            // OPTIONAL
            // -----------------------------------------------------------------

            // On this new client, migrated, we no longer need the v0 scope
            // we can deprovision it
            await agent1.LocalOrchestrator.DeprovisionAsync("v0", SyncProvision.StoredProcedures, progress:progress);
            await agent1.LocalOrchestrator.DeleteClientScopeInfoAsync(v0clientScope, progress: progress);
            Console.WriteLine($"Deprovision of old scope v0 done on Sql Server client1");

            // -----------------------------------------------------------------
            // SQLite Client will eventually migrate to v1
            // -----------------------------------------------------------------

            // It's time to migrate the sqlite client
            // Adding the column to the SQLite client
            await Helper.AddNewColumnToAddressAsync(client2Provider.CreateConnection());
            Console.WriteLine($"Column eventually added to Sqlite client2");

            // Provision SQLite client with the new the V1 scope
            var v1client2Scope= await agent2.LocalOrchestrator.ProvisionAsync(serverScope, progress: progress);
            Console.WriteLine($"Provision v1 done on SQLite client2");

            // ShadowScope old scope to new scope
            var v0client2Scope = await agent2.LocalOrchestrator.GetClientScopeInfoAsync("v0");
            v1client2Scope.ShadowScope(v0client2Scope);
            v1client2Scope = await agent2.LocalOrchestrator.SaveClientScopeInfoAsync(v1client2Scope);

            // let's try to sync firstly
            // Now test a new sync, on this new scope v1
            // Obviously, we don't have anything from the server
            var s5 = await agent2.SynchronizeAsync("v1", progress: progress);
            Console.WriteLine(s5);

            // If we get the row from client, we have the new column, but value remains null
            // since this row was synced before client migration
            client2row = await Helper.GetLastAddressRowAsync(client2Provider.CreateConnection(), addressId);
            Console.WriteLine(client2row);

            // What we can do here, is just make a sync with Renit
            var s6 = await agent2.SynchronizeAsync("v1", SyncType.Reinitialize, progress: progress);
            Console.WriteLine($"Making a full Reinitialize sync on SQLite client2");
            Console.WriteLine(s6);

            // And now the row is correct
            // If we get the row from client, we have the new column, but value remains null
            // since this row was synced before client migration
            client2row = await Helper.GetLastAddressRowAsync(client2Provider.CreateConnection(), addressId);
            Console.WriteLine(client2row);


            // Migration done 

            Console.WriteLine("End");

        }
    }
}
