using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Dotmim.Sync.Setup
{
    public class SetupMigration
    {

        public void Compare(SyncSetup newSetup, SyncSetup oldSetup)
        {
            if (newSetup == oldSetup)
                return;

            // if we have a different setup

            var sc = SyncGlobalization.DataSourceStringComparison;

            if (!string.Equals(newSetup.StoredProceduresPrefix, oldSetup.StoredProceduresPrefix, sc) || !string.Equals(newSetup.StoredProceduresSuffix, oldSetup.StoredProceduresSuffix, sc))
            {
                // Should RECREATE the stored procedure
            }

            if (!string.Equals(newSetup.TriggersPrefix, oldSetup.TriggersPrefix, sc) || !string.Equals(newSetup.TriggersSuffix, oldSetup.TriggersSuffix, sc))
            {
                // Should RECREATE the triggers
            }

            if (!string.Equals(newSetup.TrackingTablesPrefix, oldSetup.TrackingTablesPrefix, sc) || !string.Equals(newSetup.TrackingTablesSuffix, oldSetup.TrackingTablesSuffix, sc))
            {
                // We should :
                // - RENAMTE the tracking tables (and keep the rows)
                // - RECREATE the stored procedure
                // - RECREATE the triggers
            }

            // Search for deleted tables
            var deletedTables = oldSetup.Tables.Where(oldTable => !newSetup.Tables.Any(newTable => newTable == oldTable));

            foreach (var deletedTable in deletedTables)
            {
                // We should
                // DROP stored procedures
                // DROP triggers
                // DROP tracking table
            }

            // For all new tables, a classic ensure schema will be enough

            // Compare existing tables
            foreach (var newTable in newSetup.Tables)
            {
                // Getting corresponding table in old setup
                var oldTable = oldSetup.Tables.FirstOrDefault(t => string.Equals(t.TableName, newTable.TableName, sc) && string.Equals(t.SchemaName, newTable.SchemaName, sc));

                // We do not found the old setup table, we can conclude this "newTable" is a new table included in the new setup
                // And therefore will be setup during the last call the EnsureSchema()
                if (oldTable == null)
                    continue;

                // SyncDirection different has no impact

                // We are relying on table from server database, so nothing to do here
                if (oldTable.Columns.Count == 0 && newTable.Columns.Count == 0)
                    continue;


                // Then compare all columns
                if (oldTable.Columns.Count != newTable.Columns.Count || !oldTable.Columns.All(item1 => newTable.Columns.Any(item2 => string.Equals(item1, item2, sc))))
                {
                    // We should 
                    // RECREATE the table ? 
                    // REINIT the whole table ?
                }
            }



        }

    }
}
