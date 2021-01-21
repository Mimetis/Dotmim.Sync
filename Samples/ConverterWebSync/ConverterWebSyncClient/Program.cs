using ConverterWebSyncClient;
using Dotmim.Sync;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Web.Client;
using System;
using System.IO;
using System.Threading.Tasks;

namespace HelloWebSyncClient
{
    class Program
    {
        private static string clientConnectionString = $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog=Client;Integrated Security=true;";

        static async Task Main(string[] args)
        {
            Console.WriteLine("[Converter sample] Be sure the web api has started. Then click enter..");
            Console.ReadLine();
            await SynchronizeAsync();
        }

        private static async Task SynchronizeAsync()
        {
            // Database script used for this sample : https://github.com/Mimetis/Dotmim.Sync/blob/master/CreateAdventureWorks.sql 

            // Create a web proxy Orchesrtrator with a custom serializer
            var serverProxyOrchestrator = new WebClientOrchestrator("https://localhost:44342/api/sync")
            {
                SerializerFactory = new CustomMessagePackSerializerFactory(),
                Converter = new CustomConverter()
            };


            // Second provider is using plain old Sql Server provider, relying on triggers and tracking tables to create the sync environment
            var clientProvider = new SqlSyncProvider(clientConnectionString);

            var progress = new SynchronousProgress<ProgressArgs>(args => Console.WriteLine($"{args.Context.SyncStage}:\t{args.Message}"));


            // Creating an agent that will handle all the process
            var agent = new SyncAgent(clientProvider, serverProxyOrchestrator);

            do
            {
                // Launch the sync process
                var s1 = await agent.SynchronizeAsync(progress);
                // Write results
                Console.WriteLine(s1);

            } while (Console.ReadKey().Key != ConsoleKey.Escape);

            Console.WriteLine("End");
        }
    }
}
