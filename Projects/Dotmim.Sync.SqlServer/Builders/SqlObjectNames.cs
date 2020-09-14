using Dotmim.Sync.Builders;
using Dotmim.Sync.SqlServer.Manager;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Dotmim.Sync.SqlServer.Builders
{
    public class SqlObjectNames
    {
        internal const string insertTriggerNameTemplate = "[{0}].[{1}_insert_trigger]";
        internal const string updateTriggerNameTemplate = "[{0}].[{1}_update_trigger]";
        internal const string deleteTriggerNameTemplate = "[{0}].[{1}_delete_trigger]";

        public SyncTable TableDescription { get; }
        public SyncSetup Setup { get; }

        public SqlObjectNames(SyncTable tableDescription, SyncSetup setup)
        {
            this.TableDescription = tableDescription;
            this.Setup = setup;
        }

        public ParserName GetTableName()
        {
            var originalTableName = ParserName.Parse(this.TableDescription);
            return originalTableName;
        }

        public ParserName GetTrackingTableName()
        {
            var pref = this.Setup.TrackingTablesPrefix;
            var suf = this.Setup.TrackingTablesSuffix;
            var tableName = this.GetTableName();

            // be sure, at least, we have a suffix if we have empty values. 
            // othewise, we have the same name for both table and tracking table
            if (string.IsNullOrEmpty(pref) && string.IsNullOrEmpty(suf))
                suf = "_tracking";

            var trakingTableNameString = $"{pref}{tableName.ObjectName}{suf}";

            if (!string.IsNullOrEmpty(tableName.SchemaName))
                trakingTableNameString = $"{tableName.SchemaName}.{trakingTableNameString}";

            var trackingTableName = ParserName.Parse(trakingTableNameString);

            return trackingTableName;
        }

        public string GetInsertTriggerName()
        {
            var tableName = this.GetTableName();
            var schema = string.IsNullOrEmpty(tableName.SchemaName) ? "dbo" : tableName.SchemaName;
            var tpref = this.Setup.TriggersPrefix;
            var tsuf = this.Setup.TriggersSuffix;
            var insertTriggerName = string.Format(insertTriggerNameTemplate, schema, $"{tpref}{tableName.Unquoted().Normalized().ToString()}{tsuf}");
            return insertTriggerName;
        }

        public string GetUpdateTriggerName()
        {
            var tableName = this.GetTableName();
            var schema = string.IsNullOrEmpty(tableName.SchemaName) ? "dbo" : tableName.SchemaName;
            var tpref = this.Setup.TriggersPrefix;
            var tsuf = this.Setup.TriggersSuffix;
            var updateTriggerName = string.Format(updateTriggerNameTemplate, schema, $"{tpref}{tableName.Unquoted().Normalized().ToString()}{tsuf}");
            return updateTriggerName;
        }

        public string GetDeleteTriggerName()
        {
            var tableName = this.GetTableName();
            var schema = string.IsNullOrEmpty(tableName.SchemaName) ? "dbo" : tableName.SchemaName;
            var tpref = this.Setup.TriggersPrefix;
            var tsuf = this.Setup.TriggersSuffix;
            var deleteTriggerName = string.Format(deleteTriggerNameTemplate, schema, $"{tpref}{tableName.Unquoted().Normalized().ToString()}{tsuf}");
            return deleteTriggerName;
        }

    }
}
