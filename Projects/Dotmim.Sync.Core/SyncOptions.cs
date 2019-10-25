using System;
using System.IO;
using System.Runtime.Serialization;

namespace Dotmim.Sync
{
    /// <summary>
    /// This class determines all the options you can set on Client & Server, that could potentially be different
    /// For instance, the batch directory path could not be the same on the server and client
    /// </summary>
    [Serializable]
    public class SyncOptions
    {

        /// <summary>
        /// Gets or Sets the directory used for batch mode.
        /// Default value is [User Temp Path]/[DotmimSync]
        /// </summary>
        [DataMember(Name = "bd")]
        public string BatchDirectory { get; set; }

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

        /// <summary>
        /// Gets or Sets the size used (approximatively in kb) for each batch file, in batch mode. 
        /// Default is 0 (no batch mode)
        /// </summary>
        [DataMember(Name = "bs")]
        public int BatchSize { get; set; }

        /// <summary>
        /// Gets/Sets the log level for sync operations. Default value is false.
        /// </summary>
        [DataMember(Name = "uve")]
        public bool UseVerboseErrors { get; set; }

        /// <summary>
        /// Gets or Sets if we should use the bulk operations. Default is true.
        /// If provider does not support bulk operations, this option is overrided to false.
        /// </summary>
        [DataMember(Name = "ubo")]
        public bool UseBulkOperations { get; set; } = true;

        /// <summary>
        /// Gets or Sets if we should cleaning tmp dir files after sync.
        /// </summary>
        [DataMember(Name = "cm")]
        public bool CleanMetadatas { get; set; } = true;

        /// <summary>
        /// Gets or Sets if we should disable constraints before making apply changes 
        /// Default value is true
        /// </summary>
        [DataMember(Name = "dcoa")]
        public bool DisableConstraintsOnApplyChanges { get; set; } = true;

        /// <summary>
        /// Gets or Sets the scope_info table name. Default is scope_info
        /// </summary>
        [DataMember(Name = "sitn")]
        public string ScopeInfoTableName { get; set; }

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
            this.ScopeInfoTableName = "scope_info";

        }

    }
}
