using Dotmim.Sync;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Web.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WebSyncClient
{
    public class SyncServices
    {
        public static async Task SynchronizeDefaultAsync(WebClientOrchestrator serverOrchestrator, SqlSyncProvider clientProvider, SyncOptions options, bool reinitialize = false)
        {
            var agent = new SyncAgent(clientProvider, serverOrchestrator, options);
            var parameters = new SyncParameters
            {
                { "City", "Toronto" },
                { "postal", DBNull.Value }
            };
            var syncType = reinitialize ? SyncType.Reinitialize : SyncType.Normal;

            var progress = new SynchronousProgress<ProgressArgs>(
               pa => Console.WriteLine($"{pa.ProgressPercentage:p}\t {pa.Message}"));

            try
            {
                // Launch the sync process
                var s1 = await agent.SynchronizeAsync(syncType, progress);
                // Write results
                Console.WriteLine(s1);

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

        }

        public static async Task SynchronizeLogsAsync(WebClientOrchestrator serverOrchestrator, SqlSyncProvider clientProvider, SyncOptions options, bool reinitialize = false)
        {
            // Creating an agent that will handle all the process
            var agent = new SyncAgent(clientProvider, serverOrchestrator, options);

            var progress = new SynchronousProgress<ProgressArgs>(
               pa => Console.WriteLine($"{pa.ProgressPercentage:p}\t {pa.Message}"));

            var syncType = reinitialize ? SyncType.Reinitialize : SyncType.Normal;

            try
            {
                // Launch the sync process
                var s1 = await agent.SynchronizeAsync("logs", syncType, progress);
                // Write results
                Console.WriteLine(s1);

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            Console.WriteLine("End");
        }
    }
}
