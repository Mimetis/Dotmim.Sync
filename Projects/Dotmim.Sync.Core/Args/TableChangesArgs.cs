using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync
{

    /// <summary>
    /// Contains statistics about selected changes from local provider
    /// </summary>
    public class TableChangesSelectedArgs : ProgressArgs
    {
        public TableChangesSelectedArgs(SyncContext context, SyncTable changes, TableChangesSelected changesSelected, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.Changes = changes;
            this.TableChangesSelected = changesSelected;
        }

        /// <summary>
        /// Gets the SyncTable instances containing all changes selected
        /// </summary>
        public SyncTable Changes { get; }

        /// <summary>
        /// Gets the incremental summary of changes selected
        /// </summary>
        public TableChangesSelected TableChangesSelected { get; }

        public override string Message => $"[{Connection.Database}] [{this.TableChangesSelected.TableName}] upserts:{this.TableChangesSelected.Upserts} deletes:{this.TableChangesSelected.Deletes} total:{this.TableChangesSelected.TotalChanges}";
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

        public override string Message => $"[{Connection.Database}] [{this.TableName}] getting changes...";

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

        public override string Message => $"[{Connection.Database}] [{this.TableChangesApplied.TableName}] {this.TableChangesApplied.State} applied:{this.TableChangesApplied.Applied} resolved conflicts:{this.TableChangesApplied.ResolvedConflicts}";
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
        public DataRowState State { get; }

        /// <summary>
        /// Schema table of the applied rows
        /// </summary>
        public SyncTable SchemaTable { get; }

        /// <summary>
        /// Gets the table containing changes that are going to be applied
        /// </summary>
        public IEnumerable<SyncRow> Rows { get; }

        public override string Message => $"{this.SchemaTable.TableName} state:{this.State}";

    }

}
