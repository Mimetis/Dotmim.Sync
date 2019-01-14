using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;
using System.Data.Common;

namespace Dotmim.Sync
{

    /// <summary>
    /// Args generated before ensuring a schema exists or after a schema has been readed
    /// </summary>
    public class SchemaArgs : BaseArgs
    {
        public SchemaArgs(SyncContext context, DmSet schema, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction) => this.Schema = schema;

        /// <summary>
        /// Gets or Sets a boolean for overwriting the current configuration. If True, all scripts are generated and applied
        /// </summary>
        public bool OverwriteConfiguration { get; set; }

        /// <summary>
        /// Gets the schema to be applied. If no tables are filled, the schema will be read.
        /// </summary>
        public DmSet Schema { get; }
    }
    public class OutdatedArgs : BaseArgs
    {
        public OutdatedArgs(SyncContext context, DbConnection connection, DbTransaction transaction) : base(context, connection, transaction)
        {
        }

        /// <summary>
        /// Gets or sets an action enumeration value for the action to handle the outdated peer.
        /// </summary>
        public new OutdatedSyncAction Action { get; set; } = OutdatedSyncAction.Rollback;
    }

    public enum OutdatedAction
    {
        /// <summary>
        /// Reinitialize the whole sync database, applying all rows from the server to the client
        /// </summary>
        Reinitialize,
        /// <summary>
        /// Reinitialize the whole sync database, applying all rows from the server to the client, after trying a client upload
        /// </summary>
        ReinitializeWithUpload,
        /// <summary>
        /// Rollback the synchronization request.
        /// </summary>
        Rollback
    }




}
