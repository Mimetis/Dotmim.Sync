using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    public class ApplyChangeFailedEventArgs : EventArgs
    {
        SyncConflict syncConflict;
        ConflictAction applyAction;
        DbTransaction transaction;
        DbConnection connection;
        DmRow finalRow;
        DmTable finalRowTable;

        /// <summary>
        /// Gets or Sets the action to be taken when resolving the conflict. 
        /// If you choose MergeRow, you have to fill the FinalRow property
        /// </summary>
        public ConflictAction Action
        {
            get
            {
                return this.applyAction;
            }
            set
            {
                if (this.applyAction != value)
                {
                    this.applyAction = value;

                    if (this.applyAction == ConflictAction.MergeRow)
                        this.finalRow = this.finalRowTable.ImportRow(this.syncConflict.RemoteRow);
                    else if (this.finalRow != null && this.finalRowTable.Rows.Count > 0)
                        this.finalRowTable.Clear();
                }
            }
        }

        /// <summary>
        /// Gets the object that contains data and metadata for the row being applied and for the existing row in the database that caused the failure.
        /// </summary>
        public SyncConflict Conflict
        {
            get
            {
                return this.syncConflict;
            }
        }

        /// <summary>
        /// Gets the active connection 
        /// </summary>
        public DbConnection Connection
        {
            get
            {
                return this.connection;
            }
        }

     
        /// <summary>
        /// Get the active transaction
        /// If your rollback the transaction, the sync will abort.
        /// </summary>
        public DbTransaction Transaction
        {
            get
            {
                return this.transaction;
            }
        }

        public DmRow FinalRow
        {
            get
            {
                return this.finalRow;
            }
        }

        public ApplyChangeFailedEventArgs(SyncConflict dbSyncConflict, ConflictAction action, DbConnection connection, DbTransaction transaction)
        {
            this.syncConflict = dbSyncConflict;
            this.connection = connection;
            this.transaction = transaction;
            this.applyAction = action;

            this.finalRowTable = dbSyncConflict.RemoteRow.Table.Clone();
            this.finalRowTable.TableName = dbSyncConflict.RemoteRow.Table.TableName;
        }
    }
}
