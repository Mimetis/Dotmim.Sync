using Dotmim.Sync;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Microsoft.Extensions.Configuration;
using System;
using System.IO;
using System.Threading.Tasks;

namespace XamAppServerConsole
{
    class Program
    {
        private static IConfiguration configuration;
        static async Task Main(string[] args)
        {

            configuration = new ConfigurationBuilder()
              .AddJsonFile("appsettings.json", false, true)
              .AddJsonFile("appsettings.local.json", true, true)
              .Build();

            await CreateSnapshotAsync();
        }

        private static async Task CreateSnapshotAsync()
        {
            // [Required]: Get a connection string to your server data source
            var connectionString = configuration.GetSection("ConnectionStrings")["SqlConnection"];

            // [Required] Tables involved in the sync process:
            var tables = new string[] {"ProductCategory", "ProductModel", "Product",
            "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail" };

            //var tables = new string[] { "Customer" };

            var syncOptions = new SyncOptions
            {
                SnapshotsDirectory = Path.Combine(SyncOptions.GetDefaultUserBatchDiretory(), "Snapshots"),
                BatchSize = 2000,
            };

            var syncSetup = new SyncSetup(tables);

            // Using the Progress pattern to handle progession during the synchronization
            var progress = new SynchronousProgress<ProgressArgs>(s => Console.WriteLine($"{s.Source}:\t{s.Message}"));

            var provider = new SqlSyncProvider(connectionString);

            var orchestrator = new RemoteOrchestrator(provider, syncOptions);

            var snap = await orchestrator.CreateSnapshotAsync(syncSetup, progress: progress);

        }
    }
}
