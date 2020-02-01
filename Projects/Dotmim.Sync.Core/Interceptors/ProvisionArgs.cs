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

        public override string Message => $"TableName: {SchemaTable.TableName} Provision:{Provision}";

    }

    public class TableProvisioningArgs : TableProvisionedArgs
    {
        public TableProvisioningArgs(SyncContext context, SyncProvision provision, SyncTable schemaTable, DbConnection connection, DbTransaction transaction) : base(context, provision, schemaTable, connection, transaction)
        {
        }
    }

    public class DatabaseProvisionedArgs : ProgressArgs
    {
        public SyncProvision Provision { get; }
        public SyncSet Schema { get; }

        /// <summary>
        /// Gets the script generated before applying on database
        /// </summary>
        public string Script { get; }

        public DatabaseProvisionedArgs(SyncContext context, SyncProvision provision, SyncSet schema, string script, DbConnection connection, DbTransaction transaction)
        : base(context, connection, transaction)

        {
            Provision = provision;
            Script = script;
            Schema = schema;
        }

        public override string Message => $"Tables count:{Schema.Tables.Count} Provision:{Provision}";

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

        public override string Message => $"Tables count:{Schema.Tables.Sum(t => t.Columns.Count)} Provision:{Provision}";

    }


    public class TableDeprovisioningArgs : TableProvisioningArgs
    {
        public TableDeprovisioningArgs(SyncContext context, SyncProvision provision, SyncTable schemaTable, DbConnection connection, DbTransaction transaction) : base(context, provision, schemaTable, connection, transaction)
        {
        }
    }
    public class TableDeprovisionedArgs : TableProvisionedArgs
    {
        public TableDeprovisionedArgs(SyncContext context, SyncProvision provision, SyncTable schemaTable, DbConnection connection, DbTransaction transaction) : base(context, provision, schemaTable, connection, transaction)
        {
        }
    }

    public class DatabaseDeprovisionedArgs : DatabaseProvisionedArgs
    {
        public DatabaseDeprovisionedArgs(SyncContext context, SyncProvision provision, SyncSet schema, string script, DbConnection connection, DbTransaction transaction) 
            : base(context, provision, schema, script, connection, transaction)
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