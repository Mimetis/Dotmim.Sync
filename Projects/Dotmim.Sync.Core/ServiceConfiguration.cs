using Dotmim.Sync.Core.Enumerations;
using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Core
{
    public sealed class ServiceConfiguration
    {
        internal int? DownloadBatchSizeInKB;
        internal string BatchSpoolDirectory;

        /// <summary>
        /// Gets or Sets the default conflict resolution policy.
        /// </summary>
        public ConflictResolutionPolicy ConflictResolutionPolicy { get; set; } = ConflictResolutionPolicy.ServerWins;

        /// <summary>
        /// Gets or Sets the DmSet Schema used for synchronization
        /// </summary>
        public DmSet ScopeSet { get; set; }
        
        /// <summary>
        /// Gets or Sets the scope Name
        /// </summary>
        public string ScopeName { get; set; }

        /// <summary>
        /// Gets or Sets the list that contains the filter parameters that the service is configured to operate on.
        /// </summary>
        public List<SyncParameter> FilterParameters { get; set; }

        /// <summary>
        /// Gets Or Sets a boolean indicating if we care about the database configuration, if exists
        /// </summary>
        public Boolean OverWriteInBaseConfiguration { get; set; } = false;


        public static ServiceConfiguration CreateDefaultConfiguration()
        {
            ServiceConfiguration configuration = new ServiceConfiguration();
            configuration.ScopeSet = new DmSet("NewDmSet");
            configuration.EnableDiagnosticPage = false;
            configuration.ScopeName = "DefaultScope";
            configuration.ConflictResolutionPolicy = ConflictResolutionPolicy.ServerWins;
            configuration.DownloadBatchSizeInKB = 2000;
            configuration.UseVerboseErrors = false;
            return configuration;

        }

    
     
        /// <summary>
        /// Set the path where batches will be spooled. The directory must already exist. Default directory is %TEMP%.
        /// </summary>
        /// <param name="directoryPath">Path to the batch spooling directory.</param>
        public void SetBatchSpoolDirectory(string directoryPath)
        {
            BatchSpoolDirectory = directoryPath;
        }

        /// <summary>
        /// Set a download batch size. Batching is disabled by default.
        /// </summary>
        /// <param name="batchSizeInKB">Download batch size in KB</param>
        public void SetDownloadBatchSize(uint batchSizeInKB)
        {
            DownloadBatchSizeInKB = (int?)(batchSizeInKB);
        }

        /// <summary>
        /// Gets/Sets the server connection string.
        /// </summary>
        public string ServerConnectionString { get; set; }

        /// <summary>
        /// Gets/Sets the log level for sync operations. Default value is None.
        /// </summary>
        public bool UseVerboseErrors { get; set; }

        /// <summary>
        /// Indicates if batching is enabled on the provider service.
        /// </summary>
        public bool IsBatchingEnabled => (null != DownloadBatchSizeInKB);

        /// <summary>Enable or disable the diagnostic page served by the $diag URL.</summary>
        public bool EnableDiagnosticPage { get; set; }

        /// <summary>
        /// Gets or Sets if we should use the bulk operations 
        /// </summary>
        public bool UseBulkOperations { get; set; } = true;



    }
}
