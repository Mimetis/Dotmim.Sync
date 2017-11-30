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
        /// Gets the DmRow row that contains the conflicting row from the local database.
        /// </summary>
        public DmRow LocalRow => this.localRows?.Rows?[0];

        /// <summary>
        /// Gets the DmRow row that contains the conflicting row from the remote database.
        /// </summary>
        public DmRow RemoteRow => this.remoteRows?.Rows?[0];

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


        private void EnsureTables(DmRow row)
        {
            if (this.localRows == null)
            {
                this.localRows = row.Table.Clone();
                this.localRows.TableName = row.Table.TableName;
            }

            if (this.remoteRows == null)
            {
                this.remoteRows = row.Table.Clone();
                this.remoteRows.TableName = row.Table.TableName;
            }

        }

        internal void AddLocalRow(DmRow row)
        {
            this.EnsureTables(row);

            this.localRows.ImportRow(row);
        }

        internal void AddRemoteRow(DmRow row)
        {
            this.EnsureTables(row);

            this.remoteRows.ImportRow(row);
        }
    }
}
