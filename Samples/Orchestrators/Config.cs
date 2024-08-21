using Dotmim.Sync;
using Dotmim.Sync.Enumerations;
using System;

namespace Orchestrators
{
    public static class Config
    {

        private static SyncOptions syncOptions = new SyncOptions { BatchSize = 100 };
        private static SyncSetup setup = new SyncSetup("ProductCategory", "ProductModel", "Product", "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail");

        /// <summary>
        /// Get Setup, containing tables.
        /// </summary>
        public static SyncSetup GetSetup() => setup;

        /// <summary>
        /// Get Options.
        /// </summary>
        public static SyncOptions GetClientOptions() => syncOptions;

        /// <summary>
        /// Get sync provision.
        /// </summary>
        public static SyncProvision GetClientProvision() => SyncProvision.ScopeInfo | SyncProvision.ScopeInfoClient | SyncProvision.StoredProcedures | SyncProvision.Table | SyncProvision.TrackingTable | SyncProvision.Triggers;

        /// <summary>
        /// Get sync provision.
        /// </summary>
        public static SyncProvision GetServerProvision() => SyncProvision.ScopeInfo | SyncProvision.ScopeInfoClient | SyncProvision.StoredProcedures | SyncProvision.Table | SyncProvision.TrackingTable | SyncProvision.Triggers;

        /// <summary>
        /// Get a synchronous progress object.
        /// </summary>
        public static SynchronousProgress<ProgressArgs> GetProgress() => new SynchronousProgress<ProgressArgs>(s =>
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine($"{s.ProgressPercentage:p}:\t{s.Message}");
            Console.ResetColor();
        });
    }
}