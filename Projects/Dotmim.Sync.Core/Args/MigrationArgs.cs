using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Args generated before ensuring a schema exists or after a schema has been readed
    /// </summary>
    public class MigrationArgs : ProgressArgs
    {
        private readonly SyncContext context;

        public MigrationArgs(SyncContext context, DbMigrationTools migrationTools, SyncSet currentSchema, SyncSet newSchema, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            this.context = context;
            this.MigrationTools = migrationTools;
            this.CurrentSchema = currentSchema;
            this.NewSchema = newSchema;
        }

        /// <summary>
        /// Gets or Sets the migration tools
        /// </summary>
        public DbMigrationTools MigrationTools { get; }

        /// <summary>
        /// Get the current schema, retrieved from scope table
        /// </summary>
        public SyncSet CurrentSchema { get; }

        /// <summary>
        /// Gets the schema retrieved from code
        /// </summary>
        public SyncSet NewSchema { get; }


        /// <summary>
        /// Apply migration
        /// </summary>
        public Task ApplyMigration() => MigrationTools.MigrateAsync(context);



        public override string Message => $"Migration beetween two schemas";

    }
}
