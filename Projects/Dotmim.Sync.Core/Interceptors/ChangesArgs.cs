

using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Runtime.Serialization;
using System.Linq;

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
    /// Event args generated before applying a snapshot on the target database
    /// </summary>
    public class SnapshotApplyingArgs : ProgressArgs
    {
        public SnapshotApplyingArgs(SyncContext context) : base(context, null, null)
        {
        }

        public override string Message => $"Applying snapshot.";
    }


    /// <summary>
    /// Event args generated before applying a snapshot on the target database
    /// </summary>
    public class SnapshotAppliedArgs : ProgressArgs
    {
        public SnapshotAppliedArgs(SyncContext context) : base(context, null, null)
        {
        }

        public override string Message => $"Snapshot applied.";
    }




    /// <summary>
    /// Contains statistics about selected changes from local provider
    /// </summary>
    public class TableChangesSelectedArgs : ProgressArgs
    {
        public TableChangesSelectedArgs(SyncContext context, TableChangesSelected changesSelected, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction) => this.TableChangesSelected = changesSelected;

        public TableChangesSelected TableChangesSelected { get; set; }

        public override string Message => $"{this.TableChangesSelected.TableName} Upserts:{this.TableChangesSelected.Upserts} Deletes:{this.TableChangesSelected.Deletes} TotalChanges:{this.TableChangesSelected.TotalChanges}" ;
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
        public TableChangesApplyingArgs(SyncContext context, IEnumerable<SyncRow> rows, SyncTable schemaTable, DataRowState state, DbConnection connection, DbTransaction transaction) 
            : base(context, connection, transaction)
        {
            this.Rows = rows;
            this.State = state;
            this.SchemaTable = schemaTable;
        }

        /// <summary>
        /// Gets the RowState of the applied rows
        /// </summary>
        public DataRowState State { get;  }

        /// <summary>
        /// Schema table of the applied rows
        /// </summary>
        public SyncTable SchemaTable { get; }

        /// <summary>
        /// Gets the table containing changes that are going to be applied
        /// </summary>
        public IEnumerable<SyncRow> Rows { get; }

        public override string Message => $"{this.SchemaTable.TableName} State:{this.State}";

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
        public SyncTable Table { get; set; }

        /// <summary>
        /// Gets the RowState of the applied rows
        /// </summary>
        public DataRowState State { get; set; }

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
    [DataContract(Name = "dcs"), Serializable]
    public class DatabaseChangesSelected
    {
        /// <summary>
        /// Get the changes selected to be applied for a current table
        /// </summary> 
        [DataMember(Name = "tcs", IsRequired = false, EmitDefaultValue = false, Order = 1)]
        public List<TableChangesSelected> TableChangesSelected { get; } = new List<TableChangesSelected>();

        /// <summary>
        /// Gets the total number of changes that are to be applied during the synchronization session.
        /// </summary>
        [IgnoreDataMember]
        public int TotalChangesSelected => this.TableChangesSelected.Sum(t => t.TotalChanges);

        /// <summary>
        /// Gets the total number of deletes that are to be applied during the synchronization session.
        /// </summary>
        [IgnoreDataMember]
        public int TotalChangesSelectedDeletes => this.TableChangesSelected.Sum(t => t.Deletes);

        /// <summary>
        /// Gets the total number of updates OR inserts that are to be applied during the synchronization session.
        /// </summary>
        [IgnoreDataMember]
        public int TotalChangesSelectedUpdates => this.TableChangesSelected.Sum(t => t.Upserts);

    }

    /// <summary>
    /// Get changes to be applied (contains Deletes AND Inserts AND Updates)
    /// </summary>
    [DataContract(Name = "tcs"), Serializable]
    public class TableChangesSelected
    {
        public TableChangesSelected()
        {

        }
        public TableChangesSelected(string tableName) => this.TableName = tableName;
        
        /// <summary>
        /// Gets the table name
        /// </summary>
        [DataMember(Name = "tn", IsRequired = false, EmitDefaultValue = false, Order = 1)]
        public string TableName { get; set; }

        /// <summary>
        /// Gets or sets the number of deletes that should be applied to a table during the synchronization session.
        /// </summary>
        [DataMember(Name = "d", IsRequired = false, EmitDefaultValue = false, Order = 2)]
        public int Deletes { get; set; }

        /// <summary>
        /// Gets or sets the number of updates OR inserts that should be applied to a table during the synchronization session.
        /// </summary>
        [DataMember(Name = "u", IsRequired = false, EmitDefaultValue = false, Order = 3)]
        public int Upserts { get; set; }

        /// <summary>
        /// Gets the total number of changes that are applied to a table during the synchronization session.
        /// </summary>
        [IgnoreDataMember()]
        public int TotalChanges => this.Upserts + this.Deletes;
    }

}
