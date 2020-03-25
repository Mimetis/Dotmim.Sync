using System.Data.Common;
using System.Linq;

using Dotmim.Sync.Enumerations;

namespace Dotmim.Sync
{
    public class TableProvisionedArgs : ProgressArgs
    {
        public SyncProvision Provision { get; }
        public SyncTable SchemaTable { get; }

        public TableProvisionedArgs(SyncContext context, SyncProvision provision, SyncTable schemaTable, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            Provision = provision;
            SchemaTable = schemaTable;
        }

        public override string Message => $"[{Connection.Database}] [{SchemaTable.TableName}] provision:{Provision}";

    }


    public class DatabaseProvisionedArgs : ProgressArgs
    {
        public SyncProvision Provision { get; }
        public SyncSet Schema { get; }

        public DatabaseProvisionedArgs(SyncContext context, SyncProvision provision, SyncSet schema, DbConnection connection, DbTransaction transaction)
        : base(context, connection, transaction)

        {
            Provision = provision;
            Schema = schema;
        }

        public override string Message => $"[{Connection.Database}] tables count:{Schema.Tables.Count} provision:{Provision}";

    }

    public class DatabaseProvisioningArgs : ProgressArgs
    {
        /// <summary>
        /// Get the provision type (Flag enum)
        /// </summary>
        public SyncProvision Provision { get; }

        /// <summary>
        /// Gets the schema to be applied in the database
        /// </summary>
        public SyncSet Schema { get; }

        /// <summary>
        /// Gets or Sets a boolean for overwriting the current schema. If True, all scripts are generated and applied
        /// </summary>
        public bool OverwriteSchema { get; set; }

        public DatabaseProvisioningArgs(SyncContext context, SyncProvision provision, SyncSet schema, DbConnection connection, DbTransaction transaction)
        : base(context, connection, transaction)

        {
            Provision = provision;
            Schema = schema;
        }

        // public override string Message => $"[{Connection.Database}] tables count:{Schema.Tables.Sum(t => t.Columns.Count)} provision:{Provision}";
        public override string Message => $"[{Connection.Database}] tables count:{Schema.Tables.Count} provision:{Provision}";

    }

    public class TableDeprovisionedArgs : TableProvisionedArgs
    {
        public TableDeprovisionedArgs(SyncContext context, SyncProvision provision, SyncTable schemaTable, DbConnection connection, DbTransaction transaction) : base(context, provision, schemaTable, connection, transaction)
        {
        }
    }

    public class DatabaseDeprovisionedArgs : DatabaseProvisionedArgs
    {
        public DatabaseDeprovisionedArgs(SyncContext context, SyncProvision provision, SyncSet schema, DbConnection connection, DbTransaction transaction) 
            : base(context, provision, schema, connection, transaction)
        {
        }
    }

    public class DatabaseDeprovisioningArgs : DatabaseProvisioningArgs
    {
        public DatabaseDeprovisioningArgs(SyncContext context, SyncProvision provision, SyncSet schema, DbConnection connection, DbTransaction transaction) : base(context, provision, schema, connection, transaction)
        {
        }
    }
}