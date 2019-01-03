using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync
{
   
    /// <summary>
    /// Contains statistics about selected changes from local provider
    /// </summary>
    public class TableChangesSelectedEventArgs : BaseProgressEventArgs
    {
        public TableChangesSelectedEventArgs(string providerTypeName, SyncStage stage, TableChangesSelected tableChangesSelected, DbConnection connection, DbTransaction transaction) : base(providerTypeName, stage, connection, transaction)
        {
            this.TableChangesSelected = tableChangesSelected;
        }

        public TableChangesSelected TableChangesSelected { get; set; }
    }

    /// <summary>
    /// Raise before selecting changes will occur
    /// </summary>
    public class TableChangesSelectingEventArgs : BaseProgressEventArgs
    {
        public TableChangesSelectingEventArgs(string providerTypeName, SyncStage stage, string tableName, DbConnection connection, DbTransaction transaction) : base(providerTypeName, stage, connection, transaction)
        {
            TableName = tableName;
        }

        /// <summary>
        /// Gets the table name where the changes are going to be selected
        /// </summary>
        public string TableName { get; }
    }

    [Serializable]
    public class ChangesSelected
    {
        /// <summary>
        /// Get the changes selected to be applied for a current table
        /// </summary> 
        public List<TableChangesSelected> TableChangesSelected { get; } = new List<TableChangesSelected>();

        /// <summary>
        /// Gets the total number of changes that are to be applied during the synchronization session.
        /// </summary>
        public int TotalChangesSelected
        {
            get
            {
                int totalChanges = 0;

                foreach (var tableProgress in this.TableChangesSelected)
                    totalChanges = totalChanges + tableProgress.TotalChanges;

                return totalChanges;
            }
        }

        /// <summary>
        /// Gets the total number of deletes that are to be applied during the synchronization session.
        /// </summary>
        public int TotalChangesSelectedDeletes
        {
            get
            {
                int deletes = 0;
                foreach (var tableProgress in this.TableChangesSelected)
                    deletes = deletes + tableProgress.Deletes;

                return deletes;
            }
        }

        /// <summary>
        /// Gets the total number of inserts that are to be applied during the synchronization session.
        /// </summary>
        public int TotalChangesSelectedInserts
        {
            get
            {
                int inserts = 0;
                foreach (var tableProgress in this.TableChangesSelected)
                    inserts = inserts + tableProgress.Inserts;

                return inserts;
            }
        }

        /// <summary>
        /// Gets the total number of updates that are to be applied during the synchronization session.
        /// </summary>
        public int TotalChangesSelectedUpdates
        {
            get
            {
                int updates = 0;
                foreach (var tableProgress in this.TableChangesSelected)
                    updates = updates + tableProgress.Updates;

                return updates;
            }
        }

    }

    /// <summary>
    /// Get changes to be applied (contains Deletes AND Inserts AND Updates)
    /// </summary>
    [Serializable]
    public class TableChangesSelected
    {
        /// <summary>
        /// Gets the table name
        /// </summary>
        public string TableName { get; set; }

        /// <summary>
        /// Gets or sets the number of deletes that should be applied to a table during the synchronization session.
        /// </summary>
        public int Deletes { get; set; }

        /// <summary>
        /// Gets or sets the number of inserts that should be applied to a table during the synchronization session.
        /// </summary>
        public int Inserts { get; set; }

        /// <summary>
        /// Gets or sets the number of updates that should be applied to a table during the synchronization session.
        /// </summary>
        public int Updates { get; set; }

        /// <summary>
        /// Gets the total number of changes that are applied to a table during the synchronization session.
        /// TODO : DEBUG TIME : To be sure we have the correct number, I set this value from CoreProvider
        /// </summary>
        public int TotalChanges { get; set; } // => this.Inserts + this.Updates + this.Deletes;
    }

}
