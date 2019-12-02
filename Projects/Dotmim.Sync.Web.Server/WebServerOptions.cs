using Dotmim.Sync.Serialization;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Dotmim.Sync.Web.Server
{
    public class WebServerOptions
    {

        /// <summary>
        /// Gets or Sets the directory used for batch mode.
        /// Default value is [User Temp Path]/[DotmimSync]
        /// </summary>
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
        public bool CleanMetadatas { get; set; } = true;

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
        /// Create a new instance of options with default values
        /// </summary>
        public WebServerOptions()
        {
            this.BatchDirectory = GetDefaultUserBatchDiretory();
            this.CleanMetadatas = true;
            this.UseBulkOperations = true;
            this.UseVerboseErrors = false;
            this.DisableConstraintsOnApplyChanges = true;
            this.Serializers = new SerializersCollection();
        }

    }
}
