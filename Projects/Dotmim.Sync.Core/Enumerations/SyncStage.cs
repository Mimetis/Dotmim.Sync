using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Enumerations
{
    /// <summary>
    /// Sync progress step. Used for the user feedback
    /// </summary>
    public enum SyncStage
    {
        BeginSession,

        EndSession, 

        /// <summary>
        /// Reading local table metadata.
        /// </summary>
        ReadingScope,

        /// <summary>
        /// Reading the table schema from the store.
        /// </summary>
        BuildConfiguration,


        /// <summary>
        /// Ensure database is created, and all tables / tracking tables / proc stock and so on
        /// </summary>
        EnsureDatabase,

        /// <summary>
        /// Updating local metadata.
        /// </summary>
        WritingScope,

        /// <summary>Sending changes to the remote.</summary>
        UploadingChanges,
        /// <summary>Receiving changes from the remote.</summary>
        DownloadingChanges,

        /// <summary>Applying inserts to the local datasource.</summary>
        ApplyingInserts,
        /// <summary>Applying updates to the local datasource.</summary>
        ApplyingUpdates,
        /// <summary>Applying deletes to the local datasource.</summary>
        ApplyingDeletes,
        
 
        /// <summary>Enumerating changes from the store.</summary>
        SelectedChanges,

        /// <summary>Cleanup metadata from tracking tables.</summary>
        CleanupMetadata
    }
}