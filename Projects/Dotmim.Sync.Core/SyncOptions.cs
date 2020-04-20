using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Serialization;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;

namespace Dotmim.Sync
{
    /// <summary>
    /// This class determines all the options you can set on Client & Server, that could potentially be different
    /// For instance, the batch directory path could not be the same on the server and client
    /// </summary>
    public class SyncOptions
    {

        /// <summary>
        /// Default name if nothing is specified for the scope inf table, stored on the client db
        /// </summary>
        public const string DefaultScopeInfoTableName = "scope_info";

        /// <summary>
        /// Default scope name if not specified
        /// </summary>
        public const string DefaultScopeName = "DefaultScope";

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
        /// Gets or Sets the size used (approximatively in kb, depending on the serializer) for each batch file, in batch mode. 
        /// Default is 0 (no batch mode)
        /// </summary>
        public int BatchSize { get; set; }

        /// <summary>
        /// Gets or Sets the log level for sync operations. Default value is false.
        /// </summary>
        public bool UseVerboseErrors { get; set; }

        /// <summary>
        /// Gets or Sets if we should use the bulk operations. Default is true.
        /// If provider does not support bulk operations, this option is overrided to false.
        /// </summary>
        public bool UseBulkOperations { get; set; } = true;

        /// <summary>
        /// Gets or Sets if we should clean tracking table metadatas.
        /// </summary>
        public bool CleanMetadatas { get; set; } = true;

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
        /// Gets or Sets the scope_info table name. Default is scope_info
        /// </summary>
        public string ScopeInfoTableName { get; set; }

        /// <summary>
        /// Gets or Sets the default conflict resolution policy. This value could potentially be ovewritten and replaced by the server
        /// </summary>
        public ConflictResolutionPolicy ConflictResolutionPolicy { get; set; }

        /// <summary>
        /// Gets or Sets the default logger used for logging purpose
        /// </summary>
        public ILogger Logger { get; set; }

        /// <summary>
        /// Create a new instance of options with default values
        /// </summary>
        public SyncOptions()
        {
            this.BatchDirectory = GetDefaultUserBatchDiretory();
            this.BatchSize = 0;
            this.CleanMetadatas = true;
            this.UseBulkOperations = true;
            this.UseVerboseErrors = false;
            this.DisableConstraintsOnApplyChanges = true;
            this.ScopeInfoTableName = DefaultScopeInfoTableName;
            this.ConflictResolutionPolicy = ConflictResolutionPolicy.ServerWins;
            this.Logger = new SyncLogger().AddDebug();
        }


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



    }
}
