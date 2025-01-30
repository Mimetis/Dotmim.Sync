﻿using Dotmim.Sync;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Web.Client;
using System;
using System.Threading.Tasks;

namespace MutliOrchestratorsWebSyncClient
{
    internal class Program
    {
        // private static string clientConnectionString = $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog=Client;Integrated Security=true;";
        private static string clientConnectionString = $"Data Source=.\\SQLEXPRESS; Initial Catalog=Client;Integrated Security=true;TrustServerCertificate=True;";

        private static async Task Main(string[] args)
        {
            Console.WriteLine("Be sure the web api has started. Then click enter..");
            Console.ReadLine();
            await SynchronizeAsync().ConfigureAwait(false);
        }

        private static async Task SynchronizeAsync()
        {
            // Database script used for this sample : https://github.com/Mimetis/Dotmim.Sync/blob/master/CreateAdventureWorks.sql
            var serverOrchestrator = new WebRemoteOrchestrator("https://localhost:44342/api/sync", identifier: "adventureworks");

            // Second provider is using plain old Sql Server provider, relying on triggers and tracking tables to create the sync environment
            var clientProvider = new SqlSyncProvider(clientConnectionString);

            var options = new SyncOptions
            {
                BatchSize = 1000,
            };

            // Creating an agent that will handle all the process
            var agent = new SyncAgent(clientProvider, serverOrchestrator, options);

            do
            {
                try
                {
                    var progress = new SynchronousProgress<ProgressArgs>(args => Console.WriteLine($"{args.ProgressPercentage:p}:\t{args.Message}"));

                    // Launch the sync process
                    var s1 = await agent.SynchronizeAsync(progress).ConfigureAwait(false);

                    // Write results
                    Console.WriteLine(s1);
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
            }
            while (Console.ReadKey().Key != ConsoleKey.Escape);

            Console.WriteLine("End");
        }
    }
}