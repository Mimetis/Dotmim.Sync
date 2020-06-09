using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Dotmim.Sync
{

    public enum MigrationAction
    {
        None,
        CreateOrRecreate,
        Drop,
        Rename
    }

    public class MigrationResults
    {
        /// <summary>
        /// Gets or Sets a boolean indicating that all tables should recreate their own stored procedures
        /// </summary>
        public MigrationAction AllStoredProcedures { get; set; }

        /// <summary>
        /// Gets or Sets a boolean indicating that all tables should recreate their own triggers
        /// </summary>
        public MigrationAction AllTriggers { get; set; }

        /// <summary>
        /// Gets or Sets a boolean indicating that all tables should recreate their tracking table
        /// </summary>
        public MigrationAction AllTrackingTables { get; set; }

        /// <summary>
        /// Tables involved in the migration
        /// </summary>
        public List<MigrationSetupTable> Tables { get; set; } = new List<MigrationSetupTable>();


    }

    public class MigrationSetupTable
    {

        public MigrationSetupTable(SetupTable table)
        {
            this.SetupTable = table;
        }

        /// <summary>
        /// Table to migrate
        /// </summary>
        public SetupTable SetupTable { get; set; }

        /// <summary>
        /// Gets or Sets a boolean indicating that this table should recreate the stored procedures
        /// </summary>
        public MigrationAction StoredProcedures { get; set; }

        /// <summary>
        /// Gets or Sets a boolean indicating that this table should recreate triggers
        /// </summary>
        public MigrationAction Triggers { get; set; }

        /// <summary>
        /// Gets or Sets a boolean indicating that this table should recreate the tracking table
        /// </summary>
        public MigrationAction TrackingTable { get; set; }


        /// <summary>
        /// Gets or Sets a boolean indicating that this table should be recreated
        /// </summary>
        public MigrationAction Table { get; set; }

    }

    public class Migration
    {
        private readonly SyncSetup newSetup;
        private readonly SyncSetup oldSetup;



        public Migration(SyncSetup newSetup, SyncSetup oldSetup)
        {
            this.newSetup = newSetup;
            this.oldSetup = oldSetup;
        }

        public MigrationResults Compare()
        {
            MigrationResults migrationSetup = new MigrationResults();

            if (newSetup == oldSetup)
                return migrationSetup;

            var sc = SyncGlobalization.DataSourceStringComparison;

            // if we change the prefix / suffix, we should recreate all stored procedures
            if (!string.Equals(newSetup.StoredProceduresPrefix, oldSetup.StoredProceduresPrefix, sc) || !string.Equals(newSetup.StoredProceduresSuffix, oldSetup.StoredProceduresSuffix, sc))
                migrationSetup.AllStoredProcedures = MigrationAction.CreateOrRecreate;

            // if we change the prefix / suffix, we should recreate all triggers
            if (!string.Equals(newSetup.TriggersPrefix, oldSetup.TriggersPrefix, sc) || !string.Equals(newSetup.TriggersSuffix, oldSetup.TriggersSuffix, sc))
                migrationSetup.AllTriggers = MigrationAction.CreateOrRecreate;

            // If we change tracking tables prefix and suffix, we should:
            // - RENAME the tracking tables (and keep the rows)
            // - RECREATE the stored procedure
            // - RECREATE the triggers
            if (!string.Equals(newSetup.TrackingTablesPrefix, oldSetup.TrackingTablesPrefix, sc) || !string.Equals(newSetup.TrackingTablesSuffix, oldSetup.TrackingTablesSuffix, sc))
            {
                migrationSetup.AllStoredProcedures = MigrationAction.CreateOrRecreate;
                migrationSetup.AllTriggers = MigrationAction.CreateOrRecreate;
                migrationSetup.AllTrackingTables = MigrationAction.Rename;
            }

            // Search for deleted tables
            var deletedTables = oldSetup.Tables.Where(oldt => newSetup.Tables[oldt.TableName, oldt.SchemaName] == null);

            // We found some tables present in the old setup, but not in the new setup
            // So, we are removing all the sync elements from the table, but we do not remote the table itself
            foreach (var deletedTable in deletedTables)
            {
                var migrationDeletedSetupTable = new MigrationSetupTable(deletedTable);
                migrationDeletedSetupTable.StoredProcedures = MigrationAction.Drop;
                migrationDeletedSetupTable.TrackingTable = MigrationAction.Drop;
                migrationDeletedSetupTable.Triggers = MigrationAction.Drop;
                migrationDeletedSetupTable.Table = MigrationAction.Drop;

                migrationSetup.Tables.Add(migrationDeletedSetupTable);
            }

            // For all new tables, a classic ensure schema will be enough

            // Compare existing tables
            foreach (var newTable in newSetup.Tables)
            {
                // Getting corresponding table in old setup
                var oldTable = oldSetup.Tables[newTable.TableName, newTable.SchemaName];

                // We do not found the old setup table, we can conclude this "newTable" is a new table included in the new setup
                // And therefore will be setup during the last call the EnsureSchema()
                if (oldTable == null)
                    continue;

                // SyncDirection has no impact if different form old and new setup table.

                var migrationSetupTable = new MigrationSetupTable(newTable);

                // Then compare all columns
                if (oldTable.Columns.Count != newTable.Columns.Count || !oldTable.Columns.All(item1 => newTable.Columns.Any(item2 => string.Equals(item1, item2, sc))))
                {
                    migrationSetupTable.StoredProcedures = MigrationAction.CreateOrRecreate;
                    migrationSetupTable.TrackingTable = MigrationAction.CreateOrRecreate;
                    migrationSetupTable.Triggers = MigrationAction.CreateOrRecreate;
                    migrationSetupTable.Table = MigrationAction.CreateOrRecreate;
                }
                else
                {
                    migrationSetupTable.StoredProcedures = migrationSetup.AllStoredProcedures;
                    migrationSetupTable.TrackingTable = migrationSetup.AllTrackingTables;
                    migrationSetupTable.Triggers = migrationSetup.AllTriggers;
                }
                migrationSetup.Tables.Add(migrationSetupTable);
            }
            return migrationSetup;

        }

    }
}
