using Dotmim.Sync;
using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Text;

namespace Orchestrators
{
    public static class Config
    {

        private static SyncOptions syncOptions = new SyncOptions { BatchSize = 100 };
        private static SyncSetup setup = new SyncSetup(new string[] { "ProductCategory", "ProductModel", "Product", "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail" });

        /// <summary>
        /// Get Setup, containing tables
        /// </summary>
        public static SyncSetup GetSetup() => setup;

        /// <summary>
        /// Get Options
        /// </summary>
        public static SyncOptions GetClientOptions() => syncOptions;

        /// <summary>
        /// Get sync provision 
        /// </summary>
        public static SyncProvision GetClientProvision() => SyncProvision.ClientScope | SyncProvision.StoredProcedures | SyncProvision.Table | SyncProvision.TrackingTable | SyncProvision.Triggers;

        /// <summary>
        /// Get sync provision 
        /// </summary>
        public static SyncProvision GetServerProvision() => SyncProvision.ServerHistoryScope | SyncProvision.ServerScope | SyncProvision.StoredProcedures | SyncProvision.Table | SyncProvision.TrackingTable | SyncProvision.Triggers;

        /// <summary>
        /// Get a synchronous progress object
        /// </summary>
        public static SynchronousProgress<ProgressArgs> GetProgress() => new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.Context.SyncStage}:\t{s.Message}");
            Console.ResetColor();
        });
    }
}
