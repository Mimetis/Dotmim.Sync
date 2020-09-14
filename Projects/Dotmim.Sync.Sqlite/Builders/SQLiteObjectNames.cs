using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Text;

using System.Linq;

using System.Data;

namespace Dotmim.Sync.Sqlite
{
    public class SqliteObjectNames
    {
        public const string TimestampValue = "replace(strftime('%Y%m%d%H%M%f', 'now'), '.', '')";

        internal const string insertTriggerNameTemplate = "[{0}_insert_trigger]";
        internal const string updateTriggerNameTemplate = "[{0}_update_trigger]";
        internal const string deleteTriggerNameTemplate = "[{0}_delete_trigger]";

        public SyncTable TableDescription { get; }
        public SyncSetup Setup { get; }

        public SqliteObjectNames(SyncTable tableDescription, SyncSetup setup)
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

            var trackingTableName = ParserName.Parse(trakingTableNameString);

            return trackingTableName;
        }

        public string GetInsertTriggerName()
        {
            var tableName = this.GetTableName();
            var tpref = this.Setup.TriggersPrefix != null ? this.Setup.TriggersPrefix : "";
            var tsuf = this.Setup.TriggersSuffix != null ? this.Setup.TriggersSuffix : "";
            var insertTriggerName = string.Format(insertTriggerNameTemplate, $"{tpref}{tableName.Unquoted().Normalized().ToString()}{tsuf}");
            return insertTriggerName;
        }

        public string GetUpdateTriggerName()
        {
            var tableName = this.GetTableName();
            var tpref = this.Setup.TriggersPrefix != null ? this.Setup.TriggersPrefix : "";
            var tsuf = this.Setup.TriggersSuffix != null ? this.Setup.TriggersSuffix : "";
            var updateTriggerName = string.Format(updateTriggerNameTemplate, $"{tpref}{tableName.Unquoted().Normalized().ToString()}{tsuf}");
            return updateTriggerName;
        }

        public string GetDeleteTriggerName()
        {
            var tableName = this.GetTableName();
            var tpref = this.Setup.TriggersPrefix != null ? this.Setup.TriggersPrefix : "";
            var tsuf = this.Setup.TriggersSuffix != null ? this.Setup.TriggersSuffix : "";
            var deleteTriggerName = string.Format(deleteTriggerNameTemplate, $"{tpref}{tableName.Unquoted().Normalized().ToString()}{tsuf}");
            return deleteTriggerName;
        }
    }
}
