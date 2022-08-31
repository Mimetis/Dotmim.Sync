using Dotmim.Sync;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using System;
using System.Threading.Tasks;

namespace Conflict
{
    class Program
    {

        private static string serverConnectionString = $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog=AdventureWorks;Integrated Security=true;";
        private static string clientConnectionString = $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog=Client;Integrated Security=true;";

        static async Task Main() => await ConflictAsync();

        private static async Task ConflictAsync()
        {
            // Database script used for this sample : https://github.com/Mimetis/Dotmim.Sync/blob/master/CreateAdventureWorks.sql 

            // Create 2 Sql Sync providers
            var serverProvider = new SqlSyncProvider(serverConnectionString);

            // Second provider is using plain old Sql Server provider, relying on triggers and tracking tables to create the sync environment
            var clientProvider = new SqlSyncProvider(clientConnectionString);

            // Tables involved in the sync process:
            var tables = new string[] {"Customer" };

            var setup = new SyncSetup(tables);
            setup.Tables["Customer"].Columns.AddRange(new string[] { "CustomerID", "FirstName", "LastName" });

            // Creating an agent that will handle all the process
            var agent = new SyncAgent(clientProvider, serverProvider);

            Console.WriteLine("- Initialize the databases with initial data");
            // Make a first sync to have everything in place
            Console.WriteLine(await agent.SynchronizeAsync(setup));


            do
            {
                try
                {
                    Console.WriteLine("-----------------------------------");
                    Console.WriteLine("- Insert data in client and server databases to generate a conflict Insert Client - Insert Server");

                    var id = new Random((int)DateTime.Now.Ticks).Next();

                    // Insert a value on client
                    await Helper.InsertOneConflictCustomerAsync(
                        clientProvider.CreateConnection(), id, "John", "Clientdoe");

                    // Insert a value on server with same key, to generate a conflict
                    await Helper.InsertOneConflictCustomerAsync(
                        serverProvider.CreateConnection(), id, "John", "Serverdoe");

                    agent.OnApplyChangesConflictOccured(async acfa =>
                    {
                        var conflict = await acfa.GetSyncConflictAsync();

                        Console.WriteLine("______________________________");
                        Console.WriteLine("Handling conflict:");
                        Console.WriteLine($"Server row : {conflict.RemoteRow}");
                        Console.WriteLine($"Client row : {conflict.LocalRow}");
                        Console.WriteLine("Please use which one is the winner of the conflict:");
                        Console.WriteLine("* 1: Server Wins");
                        Console.WriteLine("* 2: Client Wins");
                        Console.WriteLine("* 3: Merge Row");
                        var choose = Console.ReadLine();

                        if (choose == "1")
                            acfa.Resolution = ConflictResolution.ServerWins;
                        else if (choose == "2")
                            acfa.Resolution = ConflictResolution.ClientWins;
                        else
                        {
                            acfa.Resolution = ConflictResolution.MergeRow;
                            acfa.FinalRow["LastName"] = "MergedDoe";
                        }
                    });

                    Console.WriteLine("- Launch synchronization");
                    var res = await agent.SynchronizeAsync();
                    Console.WriteLine(res);

                    var clientRow = await Helper.GetCustomerAsync(clientProvider.CreateConnection(), id);
                    Console.WriteLine("Client row:");
                    Console.WriteLine(clientRow);

                    var serverRow = await Helper.GetCustomerAsync(serverProvider.CreateConnection(), id);
                    Console.WriteLine("Server row:");
                    Console.WriteLine(serverRow);

                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }

            } while (Console.ReadKey().Key != ConsoleKey.Escape);

            Console.WriteLine("End");
        }

    }
}
