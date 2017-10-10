using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Represents a synchronization conflict at the row level.
    /// Conflict could be resolved only on server side
    /// </summary>
    public class SyncConflict
    {
        private DmTable localRows;
        private DmTable remoteRows;

        /// <summary>
        /// Gets or sets the error message that is returned when a conflict is set to ConflictType.ErrorsOccurred
        /// </summary>
        public string ErrorMessage { get; set; }

        /// <summary>
        /// Gets the DmTable object that contains the conflicting rows from the local database.
        /// </summary>
        public DmTable LocalChanges => this.localRows;

        /// <summary>
        /// Gets the DmTable object that contains the conflicting rows from the remote database.
        /// </summary>
        public DmTable RemoteChanges => this.remoteRows;


        /// <summary>
        /// Gets or sets the ConflictType enumeration value that represents the type of synchronization conflict.
        /// </summary>
        public ConflictType Type { get; set; }

        /// <summary>
        /// Initializes a new instance of the SyncConflict class by using default values.
        /// </summary>
        public SyncConflict()
        {
        }

        /// <summary>
        /// Initializes a new instance of the SyncConflict class by using conflict type and conflict stage parameters.
        /// </summary>
        public SyncConflict(ConflictType type)
        {
            this.Type = type;
        }


        internal void AddLocalRow(DmRow row)
        {
            if (this.localRows == null)
            {
                this.localRows = row.Table.Clone();
                this.localRows.TableName = row.Table.TableName;
            }
            this.localRows.ImportRow(row);
        }

        internal void AddRemoteRow(DmRow row)
        {
            if (this.remoteRows == null)
            {
                this.remoteRows = row.Table.Clone();
                this.remoteRows.TableName = row.Table.TableName;
            }
            this.remoteRows.ImportRow(row);
        }
    }
}
