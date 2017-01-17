using Dotmim.Sync.Core.Enumerations;
using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Core
{
    public sealed class ServiceConfiguration
    {
        /// <summary>
        /// Gets or Sets the default conflict resolution policy.
        /// </summary>
        public ConflictResolutionPolicy ConflictResolutionPolicy { get; set; } = ConflictResolutionPolicy.ServerWins;

        /// <summary>
        /// Tables involved. Once we have completed the ScopeSet property, this property become obsolete
        /// </summary>
        public string[] Tables { get; set; }

        /// <summary>
        /// Gets or Sets the DmSet Schema used for synchronization
        /// </summary>
        public DmSet ScopeSet { get; set; }

        /// <summary>
        /// Gets or Sets the directory used for batch mode
        /// </summary>
        public String BatchDirectory { get; set; }

        /// <summary>
        /// Gets or Sets the size used for downloading in batch mode
        /// </summary>
        public int DownloadBatchSizeInKB { get; set; }

        /// <summary>
        /// Gets or Sets the list that contains the filter parameters that the service is configured to operate on.
        /// </summary>
        public List<SyncParameter> FilterParameters { get; set; }

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
        public bool IsBatchingEnabled => (DownloadBatchSizeInKB > 0);

        /// <summary>
        /// Enable or disable the diagnostic page served by the $diag URL.
        /// </summary>
        public bool EnableDiagnosticPage { get; set; }

        /// <summary>
        /// Gets or Sets if we should use the bulk operations 
        /// </summary>
        public bool UseBulkOperations { get; set; } = true;


        public ServiceConfiguration()
        {
            this.ScopeSet = new DmSet("DotmimSync");
            this.EnableDiagnosticPage = false;
            this.ConflictResolutionPolicy = ConflictResolutionPolicy.ServerWins;
            this.DownloadBatchSizeInKB = 0;
            this.UseVerboseErrors = false;
            this.BatchDirectory = Path.Combine(Path.GetTempPath(), "/DotmimSync");
        }

        public ServiceConfiguration(string[] tables) : this()
        {
            this.Tables = tables;
        }

        internal ServiceConfiguration Clone()
        {
            ServiceConfiguration serviceConfiguration = new ServiceConfiguration();
            serviceConfiguration.BatchDirectory = this.BatchDirectory;
            serviceConfiguration.ConflictResolutionPolicy = this.ConflictResolutionPolicy;
            serviceConfiguration.DownloadBatchSizeInKB = this.DownloadBatchSizeInKB;
            serviceConfiguration.EnableDiagnosticPage = this.EnableDiagnosticPage;
            serviceConfiguration.ScopeSet = this.ScopeSet.Clone();
            serviceConfiguration.ServerConnectionString = this.ServerConnectionString;
            serviceConfiguration.Tables = this.Tables;
            serviceConfiguration.UseBulkOperations = this.UseBulkOperations;
            serviceConfiguration.UseVerboseErrors = this.UseVerboseErrors;

            if (this.FilterParameters != null)
            {
                serviceConfiguration.FilterParameters = new List<SyncParameter>();
                foreach (var p in this.FilterParameters)
                {
                    SyncParameter p1 = new SyncParameter();
                    p1.Name = p.Name;
                    p1.Value = p.Value;
                    serviceConfiguration.FilterParameters.Add(p1);
                }
            }

            return serviceConfiguration;

        }
    }
}
