using System.Data.Common;
using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;

namespace Dotmim.Sync
{
    public class TableProvisionedArgs : BaseArgs
    {
        public SyncProvision Provision { get; }
        public DmTable DmTable { get; }

        public TableProvisionedArgs(SyncContext context, SyncProvision provision, DmTable dmTable, DbConnection connection, DbTransaction transaction)
            : base(context, connection, transaction)
        {
            Provision = provision;
            DmTable = dmTable;
        }
    }

    public class TableProvisioningArgs : TableProvisionedArgs
    {
        public TableProvisioningArgs(SyncContext context, SyncProvision provision, DmTable dmTable, DbConnection connection, DbTransaction transaction) : base(context, provision, dmTable, connection, transaction)
        {
        }
    }

    public class DatabaseProvisionedArgs : BaseArgs
    {
        public SyncProvision Provision { get; }
        public DmSet Schema { get; }

        /// <summary>
        /// Gets the script generated before applying on database
        /// </summary>
        public string Script { get; }

        public DatabaseProvisionedArgs(SyncContext context, SyncProvision provision, string script, DbConnection connection, DbTransaction transaction)
        : base(context, connection, transaction)

        {
            Provision = provision;
            Script = script;
        }
    }

    public class DatabaseProvisioningArgs : BaseArgs
    {
        /// <summary>
        /// Get the provision type (Flag enum)
        /// </summary>
        public SyncProvision Provision { get; }

        /// <summary>
        /// Gets the schema to be applied in the database
        /// </summary>
        public DmSet Schema { get; }

        /// <summary>
        /// Gets or Sets a boolean for overwriting the current schema. If True, all scripts are generated and applied
        /// </summary>
        public bool OverwriteSchema { get; set; }

        /// <summary>
        /// Gets or Sets a boolean value to specify if scripts should be generated, before applied.
        /// </summary>
        public bool GenerateScript { get; set; }

        public DatabaseProvisioningArgs(SyncContext context, SyncProvision provision, DmSet schema, DbConnection connection, DbTransaction transaction)
        : base(context, connection, transaction)

        {
            Provision = provision;
            Schema = schema;
        }
    }


    public class TableDeprovisioningArgs : TableProvisioningArgs
    {
        public TableDeprovisioningArgs(SyncContext context, SyncProvision provision, DmTable dmTable, DbConnection connection, DbTransaction transaction) : base(context, provision, dmTable, connection, transaction)
        {
        }
    }
    public class TableDeprovisionedArgs : TableProvisionedArgs
    {
        public TableDeprovisionedArgs(SyncContext context, SyncProvision provision, DmTable dmTable, DbConnection connection, DbTransaction transaction) : base(context, provision, dmTable, connection, transaction)
        {
        }
    }

    public class DatabaseDeprovisionedArgs : DatabaseProvisionedArgs
    {
        public DatabaseDeprovisionedArgs(SyncContext context, SyncProvision provision, string script, DbConnection connection, DbTransaction transaction) : base(context, provision, script, connection, transaction)
        {
        }
    }

    public class DatabaseDeprovisioningArgs : DatabaseProvisioningArgs
    {
        public DatabaseDeprovisioningArgs(SyncContext context, SyncProvision provision, DmSet schema, DbConnection connection, DbTransaction transaction) : base(context, provision, schema, connection, transaction)
        {
        }
    }
}