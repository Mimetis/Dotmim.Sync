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
        Alter,
        Create,
        Drop,
        Rename,
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
        private MigrationAction table;

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
        /// Gets a value indicating if the table should be migrated
        /// </summary>
        public bool ShouldMigrate => this.TrackingTable != MigrationAction.None ||
                                        this.Triggers != MigrationAction.None ||
                                        this.StoredProcedures != MigrationAction.None ||
                                        this.Table != MigrationAction.None;


        /// <summary>
        /// Gets or Sets a boolean indicating that this table should be recreated
        /// </summary>
        public MigrationAction Table
        {
            get => table;
            set
            {
                if (value == MigrationAction.Drop)
                    throw new MigrationTableDropNotAllowedException();

                table = value;
            }
        }

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

            if (newSetup.EqualsByProperties(oldSetup))
                return migrationSetup;

            var sc = SyncGlobalization.DataSourceStringComparison;

            // if we change the prefix / suffix, we should recreate all stored procedures
            if (!string.Equals(newSetup.StoredProceduresPrefix, oldSetup.StoredProceduresPrefix, sc) || !string.Equals(newSetup.StoredProceduresSuffix, oldSetup.StoredProceduresSuffix, sc))
                migrationSetup.AllStoredProcedures = MigrationAction.Create;

            // if we change the prefix / suffix, we should recreate all triggers
            if (!string.Equals(newSetup.TriggersPrefix, oldSetup.TriggersPrefix, sc) || !string.Equals(newSetup.TriggersSuffix, oldSetup.TriggersSuffix, sc))
                migrationSetup.AllTriggers = MigrationAction.Create;

            // If we change tracking tables prefix and suffix, we should:
            // - RENAME the tracking tables (and keep the rows)
            // - RECREATE the stored procedure
            // - RECREATE the triggers
            if (!string.Equals(newSetup.TrackingTablesPrefix, oldSetup.TrackingTablesPrefix, sc) || !string.Equals(newSetup.TrackingTablesSuffix, oldSetup.TrackingTablesSuffix, sc))
            {
                migrationSetup.AllStoredProcedures = MigrationAction.Create;
                migrationSetup.AllTriggers = MigrationAction.Create;
                migrationSetup.AllTrackingTables = MigrationAction.Rename;
            }

            // Search for deleted tables
            var deletedTables = oldSetup.Tables.Where(oldt => newSetup.Tables[oldt.TableName, oldt.SchemaName] == null);

            // We found some tables present in the old setup, but not in the new setup
            // So, we are removing all the sync elements from the table, but we do not remote the table itself
            foreach (var deletedTable in deletedTables)
            {
                var migrationDeletedSetupTable = new MigrationSetupTable(deletedTable)
                {
                    StoredProcedures = MigrationAction.Drop,
                    TrackingTable = MigrationAction.Drop,
                    Triggers = MigrationAction.Drop,
                    Table = MigrationAction.None
                };

                migrationSetup.Tables.Add(migrationDeletedSetupTable);
            }

            // Search for new tables
            var newTables = newSetup.Tables.Where(newdt => oldSetup.Tables[newdt.TableName, newdt.SchemaName] == null);

            // We found some tables present in the new setup, but not in the old setup
            foreach (var newTable in newTables)
            {
                var migrationAddedSetupTable = new MigrationSetupTable(newTable)
                {
                    StoredProcedures = MigrationAction.Create,
                    TrackingTable = MigrationAction.Create,
                    Triggers = MigrationAction.Create,
                    Table = MigrationAction.Create
                };

                migrationSetup.Tables.Add(migrationAddedSetupTable);
            }

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
                    migrationSetupTable.StoredProcedures = MigrationAction.Create;
                    migrationSetupTable.TrackingTable = MigrationAction.None;
                    migrationSetupTable.Triggers = MigrationAction.Create;
                    migrationSetupTable.Table = MigrationAction.Alter;
                }
                else
                {
                    migrationSetupTable.StoredProcedures = migrationSetup.AllStoredProcedures;
                    migrationSetupTable.TrackingTable = migrationSetup.AllTrackingTables;
                    migrationSetupTable.Triggers = migrationSetup.AllTriggers;
                    migrationSetupTable.Table = MigrationAction.None;
                }

                if (migrationSetupTable.ShouldMigrate)
                    migrationSetup.Tables.Add(migrationSetupTable);
            }


            // Search for deleted filters
            // TODO : what's the problem if we still have filters, even if not existing ?

            // Search for new filters
            // If we have any filter, just recreate them, just in case
            if (newSetup.Filters != null && newSetup.Filters.Count > 0)
            {
                foreach (var filter in newSetup.Filters)
                {
                    var setupTable = newSetup.Tables[filter.TableName, filter.SchemaName];

                    if (setupTable == null)
                        continue;

                    var migrationTable = migrationSetup.Tables.FirstOrDefault(ms => ms.SetupTable.EqualsByName(setupTable));

                    if (migrationTable == null)
                    {
                        migrationTable = new MigrationSetupTable(setupTable)
                        {
                            StoredProcedures = MigrationAction.Create,
                            Table = MigrationAction.None,
                            TrackingTable = MigrationAction.None,
                            Triggers = MigrationAction.None,
                        };
                        migrationSetup.Tables.Add(migrationTable);
                    }

                    migrationTable.StoredProcedures = MigrationAction.Create;
                }

            }


            return migrationSetup;

        }

    }
}
