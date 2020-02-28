using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using Microsoft.Extensions.Caching.Memory;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Text;

namespace Dotmim.Sync.Web.Server
{
    public class WebServerOptions
    {

        /// <summary>
        /// Gets or Sets the scope_info table name. Default is scope_info
        /// </summary>
        public string ScopeInfoTableName { get; set; }

        /// <summary>
        /// Gets or Sets the directory used for batch mode.
        /// Default value is [User Temp Path]/[DotmimSync]
        /// </summary>
        public string BatchDirectory { get; set; }


        /// <summary>
        /// Gets or Sets the directory where snapshots are stored.
        /// This value could be overwritten by server is used in an http mode
        /// </summary>
        public string SnapshotsDirectory { get; set; }


        /// <summary>
        /// Get the default Batch directory full path ([User Temp Path]/[DotmimSync])
        /// </summary>
        public static string GetDefaultUserBatchDiretory() => Path.Combine(GetDefaultUserTempPath(), GetDefaultUserBatchDirectoryName());

        /// <summary>
        /// Get the default user tmp folder
        /// </summary>
        public static string GetDefaultUserTempPath() => Path.GetTempPath();

        /// <summary>
        /// Get the default sync tmp folder name
        /// </summary>
        public static string GetDefaultUserBatchDirectoryName() => "DotmimSync";


        public MemoryCacheEntryOptions GetServerCacheOptions()
        {
            var sessionCacheEntryOptions = new MemoryCacheEntryOptions();
            sessionCacheEntryOptions.SetSlidingExpiration(this.ServerCacheSlidingExpiration);
            return sessionCacheEntryOptions;
        }

        public MemoryCacheEntryOptions GetClientCacheOptions()
        {
            var sessionCacheEntryOptions = new MemoryCacheEntryOptions();
            sessionCacheEntryOptions.SetSlidingExpiration(this.ClientCacheSlidingExpiration);
            return sessionCacheEntryOptions;
        }

        /// <summary>
        /// Gets/Sets the log level for sync operations. Default value is false.
        /// </summary>
        public bool UseVerboseErrors { get; set; }

        /// <summary>
        /// Gets or Sets if we should use the bulk operations. Default is true.
        /// If provider does not support bulk operations, this option is overrided to false.
        /// </summary>
        public bool UseBulkOperations { get; set; } = true;

        /// <summary>
        /// Gets or Sets if we should cleaning tmp dir files after sync.
        /// </summary>
        public bool CleanFolder { get; set; } = true;

        /// <summary>
        /// Gets or Sets if we should disable constraints before making apply changes 
        /// Default value is true
        /// </summary>
        public bool DisableConstraintsOnApplyChanges { get; set; } = true;


        /// <summary>
        /// Serializers that could be used by each client
        /// </summary>
        public SerializersCollection Serializers { get; set; }

        /// <summary>
        /// Gets or Sets the default conflict resolution policy
        /// </summary>
        public ConflictResolutionPolicy ConflictResolutionPolicy { get; set; }

        /// <summary>
        /// Gets or Sets Converters used by different clients
        /// </summary>
        public Collection<IConverter> Converters { get; set; }

        /// <summary>
        /// Gets or Sets how long the server cache entry can be inactive(e.g.not accessed) before it will be removed. Default is 1h
        /// </summary>
        public TimeSpan ServerCacheSlidingExpiration { get; set; }

        /// <summary>
        /// Gets or Sets how long the client session cache entry can be inactive(e.g.not accessed) before it will be removed. Default is 10 min
        /// </summary>
        public TimeSpan ClientCacheSlidingExpiration { get; set; }

        /// <summary>
        /// Create a new instance of options with default values
        /// </summary>
        public WebServerOptions()
        {
            this.BatchDirectory = GetDefaultUserBatchDiretory();
            this.CleanFolder = true;
            this.UseBulkOperations = true;
            this.UseVerboseErrors = false;
            this.DisableConstraintsOnApplyChanges = true;
            this.Serializers = new SerializersCollection();
            this.Converters = new Collection<IConverter>();
            this.ConflictResolutionPolicy = ConflictResolutionPolicy.ServerWins;
            this.ServerCacheSlidingExpiration = TimeSpan.FromHours(1);
            this.ClientCacheSlidingExpiration = TimeSpan.FromMinutes(10);
            this.ScopeInfoTableName = SyncOptions.DefaultScopeInfoTableName;

        }

    }
}
