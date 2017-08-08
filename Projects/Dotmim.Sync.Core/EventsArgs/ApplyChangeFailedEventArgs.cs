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
        SyncConflict _syncConflict;
        ApplyAction _applyAction;
        DbTransaction _transaction;
        DbConnection _connection;

        /// <summary>Gets or sets a <see cref="T:Microsoft.Synchronization.Data.ApplyAction" /> enumeration value that specifies the action to handle the conflict.</summary>
        /// <returns>A <see cref="T:Microsoft.Synchronization.Data.ApplyAction" /> enumeration value that specifies the action to handle the conflict.</returns>
        public ApplyAction Action
        {
            get
            {
                return this._applyAction;
            }
            set
            {
                this._applyAction = value;
            }
        }

        /// <summary>Gets a <see cref="T:Microsoft.Synchronization.Data.DbSyncConflict" /> object that contains data and metadata for the row being applied and for the existing row in the database that caused the failure.</summary>
        /// <returns>A <see cref="T:Microsoft.Synchronization.Data.DbSyncConflict" /> object that contains conflict data and metadata.</returns>
        public SyncConflict Conflict
        {
            get
            {
                return this._syncConflict;
            }
        }

        /// <summary>Gets an <see cref="T:System.Data.IDbConnection" /> object for the connection over which changes were tried during synchronization.</summary>
        /// <returns>An <see cref="T:System.Data.IDbConnection" /> object that contains a connection to the peer database.</returns>
        public DbConnection Connection
        {
            get
            {
                return this._connection;
            }
        }

     
        public DbTransaction Transaction
        {
            get
            {
                return this._transaction;
            }
        }

        public ApplyChangeFailedEventArgs(SyncConflict dbSyncConflict, ApplyAction action, DbConnection connection, DbTransaction transaction)
        {
            this._syncConflict = dbSyncConflict;
            this._connection = connection;
            this._transaction = transaction;
            this._applyAction = action;
        }
    }
}
