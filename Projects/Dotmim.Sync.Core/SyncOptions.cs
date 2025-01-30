﻿using Dotmim.Sync.Enumerations;
using Microsoft.Extensions.Logging;
using System;
using System.IO;

namespace Dotmim.Sync
{
    /// <summary>
    /// This class determines all the options you can set on Client and Server, that could potentially be different.
    /// </summary>
    public class SyncOptions
    {

        /// <summary>
        /// Default name if nothing is specified for the scope info table and scope info client table, stored on both side.
        /// </summary>
        public const string DefaultScopeInfoTableName = "scope_info";

        /// <summary>
        /// Default scope name if not specified.
        /// </summary>
        public const string DefaultScopeName = "DefaultScope";

        private int batchSize;

        /// <summary>
        /// Gets or Sets the directory used for batch mode.
        /// Default value is [User Temp Path]/[DotmimSync].
        /// </summary>
        public string BatchDirectory { get; set; }

        /// <summary>
        /// Gets or Sets the directory where snapshots are stored.
        /// This value could be overwritten by server is used in an http mode.
        /// </summary>
        public string SnapshotsDirectory { get; set; }

        /// <summary>
        /// Gets or Sets the size used (approximatively in kb, depending on the serializer) for each batch file, in batch mode.
        /// Default is 5000
        /// Min value is 100.
        /// </summary>
        public int BatchSize
        {
            get => this.batchSize;
            set => this.batchSize = Math.Max(value, 100);
        }

        /// <summary>
        /// Gets or sets a value indicating whether gets or Sets the log level for sync operations. Default value is false.
        /// </summary>
        public bool UseVerboseErrors { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether gets or Sets if we should clean tracking table metadatas.
        /// </summary>
        public bool CleanMetadatas { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether gets or Sets if we should cleaning tmp dir files after sync.
        /// </summary>
        public bool CleanFolder { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether gets or Sets if we should disable constraints before making apply changes
        /// Default value is false.
        /// </summary>
        // trying false by default : https://github.com/Mimetis/Dotmim.Sync/discussions/453#discussioncomment-380530
        public bool DisableConstraintsOnApplyChanges { get; set; }

        /// <summary>
        /// Gets or Sets the scope_info table name. Default is scope_info.
        /// </summary>
        public string ScopeInfoTableName { get; set; }

        /// <summary>
        /// Gets or Sets the default conflict resolution policy. This value could potentially be ovewritten and replaced by the server.
        /// </summary>
        public ConflictResolutionPolicy ConflictResolutionPolicy { get; set; }

        /// <summary>
        /// Gets or Sets the default error resolution policy when an error occurs locally.
        /// The error policy can be different on server / clients.
        /// </summary>
        public ErrorResolution ErrorResolutionPolicy { get; set; }

        /// <summary>
        /// Gets or Sets the default logger used for logging purpose.
        /// </summary>
        public ILogger Logger { get; set; }

        /// <summary>
        /// Gets or sets the Progress Level.
        /// </summary>
        public SyncProgressLevel ProgressLevel { get; set; }

        /// <summary>
        /// Gets or Sets the sql commands timeout in the sync. 30 sec by default.
        /// </summary>
        public int? DbCommandTimeout { get; set; }

        /// <summary>
        /// Gets or Sets the transaction mode for applying changes.
        /// </summary>
        public TransactionMode TransactionMode { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncOptions"/> class.
        /// Create a new instance of options with default values.
        /// </summary>
        public SyncOptions()
        {
            this.BatchDirectory = GetDefaultUserBatchDirectory();
            this.BatchSize = 2000;
            this.CleanMetadatas = true;
            this.CleanFolder = true;
            this.UseVerboseErrors = true;
            this.DisableConstraintsOnApplyChanges = false;
            this.ScopeInfoTableName = DefaultScopeInfoTableName;
            this.ConflictResolutionPolicy = ConflictResolutionPolicy.ServerWins;
            this.Logger = new SyncLogger().AddDebug();
            this.ProgressLevel = SyncProgressLevel.Information;
            this.TransactionMode = TransactionMode.AllOrNothing;
        }

        /// <summary>
        /// Get the default Batch directory full path ([User Temp Path]/[DotmimSync]).
        /// </summary>
        public static string GetDefaultUserBatchDirectory() => Path.Combine(GetDefaultUserTempPath(), GetDefaultUserBatchDirectoryName());

        /// <summary>
        /// Get the default user tmp folder.
        /// </summary>
        public static string GetDefaultUserTempPath() => Path.GetTempPath();

        /// <summary>
        /// Get the default sync tmp folder name.
        /// </summary>
        public static string GetDefaultUserBatchDirectoryName() => "DotmimSync";
    }
}