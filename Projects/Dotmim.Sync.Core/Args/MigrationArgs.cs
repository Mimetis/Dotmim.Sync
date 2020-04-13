using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync
{

    /// <summary>
    /// Object representing a migration about to start
    /// </summary>
    public class DatabaseMigratingArgs : ProgressArgs
    {
        public DatabaseMigratingArgs(SyncContext context, SyncSet newSchema, SyncSetup oldSetup, SyncSetup newSetup, DbConnection connection, DbTransaction transaction) : base(context, connection, transaction)
        {
            this.NewSchema = newSchema;
            this.OldSetup = oldSetup;
            this.NewSetup = newSetup;
        }


        /// <summary>
        /// Gets message about migration
        /// </summary>
        public override string Message => $"[{Connection.Database}] applying migration...";

        /// <summary>
        /// Gets the schema used to apply migration
        /// </summary>
        public SyncSet NewSchema { get; }

        /// <summary>
        /// Gets the old setup to migrate
        /// </summary>
        public SyncSetup OldSetup { get; }

        /// <summary>
        /// Gets the new setup to apply
        /// </summary>
        public SyncSetup NewSetup { get; }
    }

    /// <summary>
    /// Once migrated you have a new setup and schema available
    /// </summary>
    public class DatabaseMigratedArgs : ProgressArgs
    {
        public DatabaseMigratedArgs(SyncContext context, SyncSet schema, SyncSetup setup, DbConnection connection = null, DbTransaction transaction = null) : base(context, connection, transaction)
        {
            this.Schema = schema;
            this.Setup = setup;
        }

        /// <summary>
        /// Gets message about migration
        /// </summary>
        public override string Message => $"Migrated. Setup tables count:{Setup.Tables.Count}.";

        /// <summary>
        /// Gets the schema currently used
        /// </summary>
        public SyncSet Schema { get; }

        /// <summary>
        /// Gets the new setup applied
        /// </summary>
        public SyncSetup Setup { get; }
    }
}
