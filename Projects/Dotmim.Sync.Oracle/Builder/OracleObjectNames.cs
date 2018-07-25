using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;

namespace Dotmim.Sync.Oracle.Builder
{
    internal class OracleObjectNames
    {
        public const string TimestampValue = "to_number(to_char(systimestamp, 'YYYYMMDDHH24MISSFF3'))";

        internal const string insertTriggerName = "{0}{1}_insert_trigger";
        internal const string updateTriggerName = "{0}{1}_update_trigger";
        internal const string deleteTriggerName = "{0}{1}_delete_trigger";

        internal const string selectChangesProcName = "{0}{1}_selectchanges";
        internal const string selectChangesProcNameWithFilters = "{0}{1}_{2}_selectchanges";
        internal const string selectRowProcName = "{0}{1}_selectrow";

        internal const string insertProcName = "{0}{1}_insert";
        internal const string updateProcName = "{0}{1}_update";
        internal const string deleteProcName = "{0}{1}_delete";

        internal const string insertMetadataProcName = "{0}{1}_insertmetadata";
        internal const string updateMetadataProcName = "{0}{1}_updatemetadata";
        internal const string deleteMetadataProcName = "{0}{1}_deletemetadata";

        internal const string resetMetadataProcName = "{0}{1}_reset";

        internal const string bulkTableTypeName = "{0}{1}_BulkType";
        internal const string bulkInsertProcName = "{0}{1}_bulkinsert";
        internal const string bulkUpdateProcName = "{0}{1}_bulkupdate";
        internal const string bulkDeleteProcName = "{0}{1}_bulkdelete";

        Dictionary<DbCommandType, String> names = new Dictionary<DbCommandType, string>();

        public DmTable TableDescription { get; }

        public OracleObjectNames(DmTable tableDescription)
        {
            this.TableDescription = tableDescription;
            SetDefaultNames();
        }

        /// <summary>
        /// Set the default stored procedures names
        /// </summary>
        private void SetDefaultNames()
        {
            (var tableName, var trackingName) = OracleBuilder.GetParsers(this.TableDescription);

            var schema = string.IsNullOrEmpty(tableName.SchemaName) ? "" : tableName.SchemaName + ".";

            this.AddName(DbCommandType.SelectChanges, string.Format(selectChangesProcName, schema, tableName.ObjectName));
            this.AddName(DbCommandType.SelectChangesWitFilters, string.Format(selectChangesProcNameWithFilters, schema, tableName.ObjectName, "{0}"));
            this.AddName(DbCommandType.SelectRow, string.Format(selectRowProcName, schema, tableName.ObjectName));
            this.AddName(DbCommandType.InsertRow, string.Format(insertProcName, schema, tableName.ObjectName));
            this.AddName(DbCommandType.UpdateRow, string.Format(updateProcName, schema, tableName.ObjectName));
            this.AddName(DbCommandType.DeleteRow, string.Format(deleteProcName, schema, tableName.ObjectName));
            this.AddName(DbCommandType.InsertMetadata, string.Format(insertMetadataProcName, schema, tableName.ObjectName));
            this.AddName(DbCommandType.UpdateMetadata, string.Format(updateMetadataProcName, schema, tableName.ObjectName));
            this.AddName(DbCommandType.DeleteMetadata, string.Format(deleteMetadataProcName, schema, tableName.ObjectName));
            this.AddName(DbCommandType.Reset, string.Format(resetMetadataProcName, schema, tableName.ObjectName));

            this.AddName(DbCommandType.InsertTrigger, string.Format(insertTriggerName, schema, tableName.ObjectName));
            this.AddName(DbCommandType.UpdateTrigger, string.Format(updateTriggerName, schema, tableName.ObjectName));
            this.AddName(DbCommandType.DeleteTrigger, string.Format(deleteTriggerName, schema, tableName.ObjectName));

            this.AddName(DbCommandType.BulkTableType, string.Format(bulkTableTypeName, schema, tableName.ObjectName));

            this.AddName(DbCommandType.BulkInsertRows, string.Format(bulkInsertProcName, schema, tableName.ObjectName));
            this.AddName(DbCommandType.BulkUpdateRows, string.Format(bulkUpdateProcName, schema, tableName.ObjectName));
            this.AddName(DbCommandType.BulkDeleteRows, string.Format(bulkDeleteProcName, schema, tableName.ObjectName));
        }

        public void AddName(DbCommandType objectType, string name)
        {
            if (names.ContainsKey(objectType))
                throw new Exception("Yous can't add an objectType multiple times");

            names.Add(objectType, name);
        }

        public string GetCommandName(DbCommandType objectType, IEnumerable<string> filters = null)
        {
            if (!names.ContainsKey(objectType))
                throw new Exception("Yous should provide a value for all DbCommandName");

            var commandName = names[objectType];

            if (filters != null)
            {
                string name = "";
                string sep = "";
                foreach (var c in filters)
                {
                    var unquotedColumnName = new ObjectNameParser(c).UnquotedString;
                    name += $"{unquotedColumnName}{sep}";
                    sep = "_";
                }

                commandName = String.Format(commandName, name);
            }
            return commandName;
        }
    }
}