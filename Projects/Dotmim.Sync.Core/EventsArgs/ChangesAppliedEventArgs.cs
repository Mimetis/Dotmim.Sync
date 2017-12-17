using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync
{

    /// <summary>
    /// Event args raised to get Changes applied on a provider
    /// </summary>
    public class TableChangesAppliedEventArgs : BaseProgressEventArgs
    {
        public TableChangesAppliedEventArgs(string providerTypeName, SyncStage stage, TableChangesApplied tableChangesApplied) : base(providerTypeName, stage)
        {
            this.TableChangesApplied = tableChangesApplied;
        }

        public TableChangesApplied TableChangesApplied { get; set; }
    }

    /// <summary>
    /// Event args before Changes are applied on a provider
    /// </summary>
    public class TableChangesApplyingEventArgs : BaseProgressEventArgs
    {
        public TableChangesApplyingEventArgs(string providerTypeName, SyncStage stage, string tableName, DmRowState state) : base(providerTypeName, stage)
        {
            this.TableName = tableName;
            this.State = state;
        }

        /// <summary>
        /// Gets the RowState of the applied rows
        /// </summary>
        public DmRowState State { get; set; }

        /// <summary>
        /// Gets the table name where changes are going to be applied
        /// </summary>
        public string TableName { get; }
    }

    /// <summary>
    /// All table changes applied on a provider
    /// </summary>
    [Serializable]
    public class ChangesApplied
    {
        /// <summary>
        /// Get the view to be applied 
        /// </summary>
        public List<TableChangesApplied> TableChangesApplied { get; } = new List<TableChangesApplied>();

        /// <summary>
        /// Gets the total number of changes that have been applied during the synchronization session.
        /// </summary>
        public int TotalAppliedChanges
        {
            get
            {
                int changesApplied = 0;
                foreach (var tableProgress in this.TableChangesApplied)
                {
                    changesApplied = changesApplied + tableProgress.Applied;
                }
                return changesApplied;
            }
        }

        /// <summary>
        /// Gets the total number of changes that have failed to be applied during the synchronization session.
        /// </summary>
        public int TotalAppliedChangesFailed
        {
            get
            {
                int changesFailed = 0;
                foreach (var tableProgress in this.TableChangesApplied)
                    changesFailed = changesFailed + tableProgress.Failed;

                return changesFailed;
            }
        }

    }

    /// <summary>
    /// Summary of table changes applied on a source
    /// </summary>
    [Serializable]
    public class TableChangesApplied
    {
        /// <summary>
        /// Gets the table where changes were applied
        /// </summary>
        public string TableName { get; set; }

        /// <summary>
        /// Gets the RowState of the applied rows
        /// </summary>
        public DmRowState State { get; set; }

        /// <summary>
        /// Gets the rows changes applied count
        /// </summary>
        public int Applied { get; set; }

        /// <summary>
        /// Gets the rows changes failed count
        /// </summary>
        public int Failed { get; set; }
    }

}
