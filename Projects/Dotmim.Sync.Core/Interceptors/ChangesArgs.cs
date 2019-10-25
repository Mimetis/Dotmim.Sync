using Dotmim.Sync.Data;
using Dotmim.Sync.Data.Surrogate;
using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Data.Common;

namespace Dotmim.Sync
{

    /// <summary>
    /// Event args generated before applying change on the target database
    /// </summary>
    public class DatabaseChangesApplyingArgs : ProgressArgs
    {
        public DatabaseChangesApplyingArgs(SyncContext context, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
        }

        public override string Message => $"Applying changes on database {Connection.Database}";
    }

    public class DatabaseChangesAppliedArgs : ProgressArgs
    {
        public DatabaseChangesAppliedArgs(SyncContext context, DatabaseChangesApplied changesApplied, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.ChangesApplied = changesApplied;
        }

        public DatabaseChangesApplied ChangesApplied { get; set; }

        public override string Message => $"Changes applied on database {Connection.Database}: Applied: {ChangesApplied.TotalAppliedChanges} Failed: {ChangesApplied.TotalAppliedChangesFailed}";

    }


    /// <summary>
    /// Contains statistics about selected changes from local provider
    /// </summary>
    public class TableChangesSelectedArgs : ProgressArgs
    {
        public TableChangesSelectedArgs(SyncContext context, TableChangesSelected changesSelected, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction) => this.TableChangesSelected = changesSelected;

        public TableChangesSelected TableChangesSelected { get; set; }

        public override string Message => $"{this.TableChangesSelected.TableName} Inserts:{this.TableChangesSelected.Inserts} Updates:{this.TableChangesSelected.Updates} Deletes:{this.TableChangesSelected.Deletes} TotalChanges:{this.TableChangesSelected.TotalChanges}" ;
    }

    /// <summary>
    /// Raise before selecting changes will occur
    /// </summary>
    public class TableChangesSelectingArgs : ProgressArgs
    {
        public TableChangesSelectingArgs(SyncContext context, string tableName, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction) => this.TableName = tableName;

        /// <summary>
        /// Gets the table name where the changes are going to be selected
        /// </summary>
        public string TableName { get; }

        public override string Message => $"{this.TableName}";

    }

    /// <summary>
    /// Event args raised to get Changes applied on a provider
    /// </summary>
    public class TableChangesAppliedArgs : ProgressArgs
    {
        public TableChangesAppliedArgs(SyncContext context, TableChangesApplied tableChangesApplied, DbConnection connection, DbTransaction transaction) 
            : base(context, connection, transaction)
        {
            this.TableChangesApplied = tableChangesApplied;
        }

        public TableChangesApplied TableChangesApplied { get; set; }

        public override string Message => $"{this.TableChangesApplied.Table.TableName} " +
            $"State:{this.TableChangesApplied.State} " +
            $"Applied:{this.TableChangesApplied.Applied} " +
            $"Failed:{this.TableChangesApplied.Failed}";
    }

    /// <summary>
    /// Event args before Changes are applied on a provider
    /// </summary>
    public class TableChangesApplyingArgs : ProgressArgs
    {
        public TableChangesApplyingArgs(SyncContext context, DmTable table, DmRowState state, DbConnection connection, DbTransaction transaction) 
            : base(context, connection, transaction)
        {
            this.Table = table;
            this.State = state;
        }

        /// <summary>
        /// Gets the RowState of the applied rows
        /// </summary>
        public DmRowState State { get; set; }

        /// <summary>
        /// Gets the table name where changes are going to be applied
        /// </summary>
        public DmTable Table { get; }

        public override string Message => $"{this.Table.TableName} State:{this.State}";

    }

    /// <summary>
    /// All table changes applied on a provider
    /// </summary>
    [Serializable]
    public class DatabaseChangesApplied
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
        public DmTable Table { get; set; }

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


    /// <summary>
    /// All tables changes selected
    /// </summary>
    [Serializable]
    public class DatabaseChangesSelected
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
                var totalChanges = 0;

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
                var deletes = 0;
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
                var inserts = 0;
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
                var updates = 0;
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
