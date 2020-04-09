
using Dotmim.Sync.Enumerations;
using System.Data.Common;

namespace Dotmim.Sync
{

    /// <summary>
    /// Args generated before ensuring a schema exists or after a schema has been readed
    /// </summary>
    public class SchemaArgs : ProgressArgs
    {
        public SchemaArgs(SyncContext context, SyncSet schema, DbConnection connection, DbTransaction transaction = null)
            : base(context, connection, transaction) => this.Schema = schema;

        /// <summary>
        /// Gets or Sets a boolean for overwriting the current configuration. If True, all scripts are generated and applied
        /// </summary>
        public bool OverwriteConfiguration { get; set; }

        /// <summary>
        /// Gets the schema to be applied. If no tables are filled, the schema will be read.
        /// </summary>
        public SyncSet Schema { get; }
        public override string Message => $"synced tables count: {this.Schema.Tables.Count}";

    }
    public class OutdatedArgs : ProgressArgs
    {
        public OutdatedArgs(SyncContext context, ScopeInfo clientScopeInfo, ServerScopeInfo serverScopeInfo, DbConnection connection= null, DbTransaction transaction = null) : base(context, connection, transaction)
        {
            this.ClientScopeInfo = clientScopeInfo;
            this.ServerScopeInfo = serverScopeInfo;
        }

        /// <summary>
        /// Gets or sets an action enumeration value for the action to handle the outdated peer.
        /// </summary>
        public OutdatedAction Action { get; set; } = OutdatedAction.Rollback;

        public override string Message => $"";

        /// <summary>
        /// Gets the client scope info used to check if the client is outdated
        /// </summary>
        public ScopeInfo ClientScopeInfo { get; }

        /// <summary>
        /// Gets the server scope info to check if client is outdated
        /// </summary>
        public ServerScopeInfo ServerScopeInfo { get; }
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
