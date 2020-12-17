using System.Data.Common;
using System.Linq;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Enumerations;

namespace Dotmim.Sync
{

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

        public override int EventId => 24;
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

        public override int EventId => 25;
    }

    public class DatabaseDeprovisionedArgs : DatabaseProvisionedArgs
    {
        public DatabaseDeprovisionedArgs(SyncContext context, SyncProvision provision, SyncSet schema, DbConnection connection = null, DbTransaction transaction = null)
            : base(context, provision, schema, connection, transaction)
        {
        }
        public override int EventId => 26;
    }

    public class DatabaseDeprovisioningArgs : DatabaseProvisioningArgs
    {
        public DatabaseDeprovisioningArgs(SyncContext context, SyncProvision provision, SyncSet schema, DbConnection connection, DbTransaction transaction) : base(context, provision, schema, connection, transaction)
        {
        }
        public override int EventId => 27;
    }


}