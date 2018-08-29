using System.Data.Common;
using Dotmim.Sync.Data;
using Dotmim.Sync.Enumerations;

namespace Dotmim.Sync.EventsArgs
{
    public class TableProvisionedEventArgs
    {
        public SyncProvision Provision { get; }
        public DmTable DmTable { get; }
        public DbConnection Connection { get; }
        public DbTransaction Transaction { get; }

        public TableProvisionedEventArgs(SyncProvision provision, DmTable dmTable, DbConnection connection, DbTransaction transaction)
        {
            Provision = provision;
            DmTable = dmTable;
            Connection = connection;
            Transaction = transaction;
        }
    }

    public class DatabaseProvisionedEventArgs
    {
        public SyncProvision Provision { get; }
        public DbConnection Connection { get; }
        public DbTransaction Transaction { get; }

        public DatabaseProvisionedEventArgs(SyncProvision provision, DbConnection connection, DbTransaction transaction)
        {
            Provision = provision;
            Connection = connection;
            Transaction = transaction;
        }
    }

    public class TableDeprovisionedEventArgs
    {
        public SyncProvision Provision { get; }
        public DmTable DmTable { get; }
        public DbConnection Connection { get; }
        public DbTransaction Transaction { get; }

        public TableDeprovisionedEventArgs(SyncProvision provision, DmTable dmTable, DbConnection connection, DbTransaction transaction)
        {
            Provision = provision;
            DmTable = dmTable;
            Connection = connection;
            Transaction = transaction;
        }
    }

    public class DatabaseDeprovisionedEventArgs
    {
        public SyncProvision Provision { get; }
        public DbConnection Connection { get; }
        public DbTransaction Transaction { get; }

        public DatabaseDeprovisionedEventArgs(SyncProvision provision, DbConnection connection, DbTransaction transaction)
        {
            Provision = provision;
            Connection = connection;
            Transaction = transaction;
        }
    }
}
