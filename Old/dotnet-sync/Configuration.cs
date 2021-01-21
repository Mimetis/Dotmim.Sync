using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Tools
{
    public class Configuration
    {

        /// <summary>
        /// Gets or Sets the default conflict resolution policy.
        /// </summary>
        public ConflictResolutionPolicy ConflictResolutionPolicy { get; set; } = ConflictResolutionPolicy.ServerWins;

        /// <summary>
        /// Gets or Sets the directory used for batch mode.
        /// Default value is [Windows Temp Path]/[DotmimSync]
        /// </summary>
        public String BatchDirectory { get; set; }

        /// <summary>
        /// Gets or Sets the size used for downloading in batch mode. 
        /// Default is 0 (no batch mode)
        /// </summary>
        public Int64 DownloadBatchSizeInKB { get; set; }

        /// <summary>
        /// Gets/Sets the serialization converter object. Default is Json
        /// </summary>
        public SerializationFormat SerializationFormat { get; set; }


        /// <summary>
        /// Gets or Sets if we should use the bulk operations. Default is true.
        /// If provider doe not support bulk operations, this option is overrided to false.
        /// </summary>
        public bool UseBulkOperations { get; set; } = true;

    }
}
