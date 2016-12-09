using Dotmim.Sync.Core.Batch;
using Dotmim.Sync.Core.Scope;
using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Core
{
    public class SyncSetProgress : IDisposable
    {
        //DmSet dmSet;
        internal SyncBatchSerializer serializer;
        //SyncBatchInfo syncBatchInfo;

        /// <summary>
        /// Reference if the sync process is batched
        /// </summary>
        public bool IsDataBatched { get; set; }

        /// <summary>
        /// Reference to the batchfilename
        /// </summary>
        public string BatchFileName { get; set; }

        /// <summary>
        /// Scope from local provider
        /// </summary>
        public ScopeInfo ScopeInfo { get; set; }

        /// <summary>
        /// Schema used for sync
        /// </summary>
        public ScopeConfigData ScopeConfigData { get; internal set; }

        /// <summary>
        /// Gets or sets whether the current batch is the last batch of changes.
        /// </summary>
        public bool IsLastBatch { get; internal set; }

        /// <summary>
        /// Gets or sets whether a remote is outdated, which means that the peer does not have sufficient metadata to correctly synchronize.
        /// </summary>
        public bool IsOutdated { get; internal set; }

        /// <summary>
        /// Gets a collection of SyncTableProgress objects.
        /// </summary>
        public List<SyncTableProgress> TablesProgress { get; } = new List<SyncTableProgress>();

        /// <summary>
        /// Returns a SyncTableProgress object that contains synchronization progress statistics for a table.
        /// </summary>
        public SyncTableProgress FindTableProgress(string tableName) => this.TablesProgress.FirstOrDefault(tb => string.Equals(tb.Changes.TableName, tableName, StringComparison.CurrentCultureIgnoreCase));

        /// <summary>
        /// Add a progress table
        /// </summary>
        public SyncTableProgress AddTableProgress(DmTable dmtable)
        {
            SyncTableProgress tableProgress = new SyncTableProgress(dmtable, serializer);
            this.TablesProgress.Add(tableProgress);
            return tableProgress;
        }


        public SyncSetProgress(SyncBatchSerializer serializer = null)
        {
            this.serializer = serializer;
        }
        public void Dispose()
        {
            
        }

       
    }
}
