using System.Data.Common;
using System.Linq;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;

namespace Dotmim.Sync
{
    public class TableProvisionedArgs : ProgressArgs
    {
        public SyncProvision Provision { get; }
        public SyncTable SchemaTable { get; }

        public TableProvisionedArgs(SyncContext context, SyncProvision provision, SyncTable schemaTable, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, connection, transaction)
        {
            Provision = provision;
            SchemaTable = schemaTable;
        }

        public override string Message => $"[{Connection.Database}] [{SchemaTable.GetFullName()}] provision:{Provision}";

    }

    public class TableProvisioningArgs : ProgressArgs
    {
        public SyncProvision Provision { get; }
        public DbTableBuilder TableBuilder { get; }

        public TableProvisioningArgs(SyncContext context, SyncProvision provision, DbTableBuilder tableBuilder, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            Provision = provision;
            TableBuilder = tableBuilder;
        }

        public override string Message => $"[{Connection.Database}] [{TableBuilder.TableDescription.GetFullName()}] provisioning:{Provision}";

    }


    public class TableDeprovisionedArgs : TableProvisionedArgs
    {
        public TableDeprovisionedArgs(SyncContext context, SyncProvision provision, SyncTable schemaTable, DbConnection connection = null, DbTransaction transaction = null) 
            : base(context, provision, schemaTable, connection, transaction)
        {
        }
    }

    public class DatabaseProvisionedArgs : ProgressArgs
    {
        public SyncProvision Provision { get; }
        public SyncSet Schema { get; }

        public DatabaseProvisionedArgs(SyncContext context, SyncProvision provision, SyncSet schema, DbConnection connection = null, DbTransaction transaction = null)
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

        public DatabaseProvisioningArgs(SyncContext context, SyncProvision provision, SyncSet schema, DbConnection connection, DbTransaction transaction)
        : base(context, connection, transaction)

        {
            Provision = provision;
            Schema = schema;
        }

        // public override string Message => $"[{Connection.Database}] tables count:{Schema.Tables.Sum(t => t.Columns.Count)} provision:{Provision}";
        public override string Message => $"[{Connection.Database}] tables count:{Schema.Tables.Count} provision:{Provision}";

    }

    public class DatabaseDeprovisionedArgs : DatabaseProvisionedArgs
    {
        public DatabaseDeprovisionedArgs(SyncContext context, SyncProvision provision, SyncSet schema, DbConnection connection = null, DbTransaction transaction = null) 
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