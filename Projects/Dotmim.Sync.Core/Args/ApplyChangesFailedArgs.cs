using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;

using Dotmim.Sync.Enumerations;

namespace Dotmim.Sync
{

    /// <summary>
    /// Raised as an argument when an apply is failing. Waiting from user for the conflict resolution
    /// </summary>
    public class ApplyChangesFailedArgs : ProgressArgs
    {
        ConflictResolution resolution;

        /// <summary>
        /// Gets or Sets the action to be taken when resolving the conflict. 
        /// If you choose MergeRow, you have to fill the FinalRow property
        /// </summary>
        public ConflictResolution Resolution
        {
            get => this.resolution;
            set
            {
                if (this.resolution != value)
                {
                    this.resolution = value;

                    if (this.resolution == ConflictResolution.MergeRow)
                    {
                        var finalRowArray = this.Conflict.RemoteRow.ToArray();
                        var finalTable = this.Conflict.RemoteRow.Table.Clone();
                        var finalSet = this.Conflict.RemoteRow.Table.Schema.Clone(false);
                        finalSet.Tables.Add(finalTable);
                        this.FinalRow = new SyncRow(finalTable.Columns.Count);
                        this.FinalRow.Table = finalTable;

                        this.FinalRow.FromArray(finalRowArray);
                        finalTable.Rows.Add(this.FinalRow);
                    }
                    else if (this.FinalRow != null)
                    {
                        var finalSet = this.FinalRow.Table.Schema;
                        this.FinalRow.Clear();
                        finalSet.Clear();
                        finalSet.Dispose();
                    }
                }
            }
        }

        /// <summary>
        /// Gets the object that contains data and metadata for the row being applied and for the existing row in the database that caused the failure.
        /// </summary>
        public SyncConflict Conflict { get; }

        
        /// <summary>
        /// Gets or Sets the scope id who will be marked as winner
        /// </summary>
        public Guid? SenderScopeId { get; set; }

        /// <summary>
        /// If we have a merge action, the final row represents the merged row
        /// </summary>
        public SyncRow FinalRow { get; set; }


        public ApplyChangesFailedArgs(SyncContext context, SyncConflict dbSyncConflict, ConflictResolution action, Guid? senderScopeId, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.Conflict = dbSyncConflict;
            this.resolution = action;
            this.SenderScopeId = senderScopeId;
        }

        public override string Message => $"{this.Conflict.Type}";

        public override int EventId => 10;

    }
}
