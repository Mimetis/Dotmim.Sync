using Dotmim.Sync.Core.Enumerations;
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

        private static readonly object _lockObject = new object();
        private List<SyncParameter> _filterParameters = new List<SyncParameter>();

        /// <summary>
        /// Scope Name
        /// </summary>
        public string ScopeName { get; private set; }

     
        internal void ClearFilterParameters()
        {
            _filterParameters.Clear();
        }

        internal int? DownloadBatchSizeInKB;

        internal string BatchSpoolDirectory;

        /// <summary>
        /// Contains the SQL Schema that was used to provision the sync objects in the database.
        /// </summary>
        internal string SyncObjectSchema { get; private set; }


        // default policies
        internal ConflictResolutionPolicy ConflictResolutionPolicy = ConflictResolutionPolicy.ServerWins;

     
        /// <summary>
        /// Indicates if the configuration is initialized. 
        /// We ideally don't want to allow rediscovery of types (mainly for performance),
        /// so this flag is checked before the type discovery is attempted.
        /// </summary>
        internal bool IsInitialized { get; private set; }

        /// <summary>
        /// Readonly list that contains the filter parameters that the service is configured to operate on.
        /// </summary>
        internal List<SyncParameter> FilterParameters => _filterParameters;
     
        /// <summary>
        /// Change the default conflict resolution policy. The default value is ClientWins.
        /// </summary>
        /// <param name="policy">The new conflict resolution policy</param>
        public void SetConflictResolutionPolicy(ConflictResolutionPolicy policy)
        {
            ConflictResolutionPolicy = policy;
        }
     

        /// <summary>
        /// Enable scopes.
        /// </summary>
        /// <param name="scopeName">Scope name to enable for sync.</param>
        /// <exception cref="ArgumentNullException">Throws when scopeName is null</exception>
        public void SetEnableScope(string scopeName)
        {
            this.ScopeName = scopeName;
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
        /// Set the schema name under which sync related objects were generated in the SQL database when the database was provisioned.
        /// </summary>
        /// <param name="schemaName">Name of the schema under which sync related objects are created.</param>
        public void SetSyncObjectSchema(string schemaName)
        {
            SyncObjectSchema = schemaName;
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

 
        


    }
}
