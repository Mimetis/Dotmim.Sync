using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;


namespace Dotmim.Sync.SqlServer.Builders
{
    public class SqlObjectNames
    {
        internal const string insertTriggerName = "[{0}].[{1}_insert]";
        internal const string updateTriggerName = "[{0}].[{1}_update]";
        internal const string deleteTriggerName = "[{0}].[{1}_delete]";

        internal const string selectChangesProcName = "[{0}].[{1}_selectchanges]";
        internal const string selectChangesProcNameWithFilters = "[{0}].[{1}_{2}_selectchanges]";
        internal const string selectRowProcName = "[{0}].[{1}_selectrow]";

        internal const string insertProcName = "[{0}].[{1}_insert]";
        internal const string updateProcName = "[{0}].[{1}_update]";
        internal const string deleteProcName = "[{0}].[{1}_delete]";

        internal const string insertMetadataProcName = "[{0}].[{1}_insertmetadata]";
        internal const string updateMetadataProcName = "[{0}].[{1}_updatemetadata]";
        internal const string deleteMetadataProcName = "[{0}].[{1}_deletemetadata]";

        internal const string resetMetadataProcName = "[{0}].[{1}_reset]";

        internal const string bulkTableTypeName = "[{0}].[{1}_BulkType]";
        internal const string bulkInsertProcName = "[{0}].[{1}_bulkinsert]";
        internal const string bulkUpdateProcName = "[{0}].[{1}_bulkupdate]";
        internal const string bulkDeleteProcName = "[{0}].[{1}_bulkdelete]";


        Dictionary<DbCommandType, String> names = new Dictionary<DbCommandType, string>();
        public DmTable TableDescription { get; }


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
                    var unquotedColumnName = new ObjectNameParser(c).FullUnquotedString;
                    name += $"{unquotedColumnName}{sep}";
                    sep = "_";
                }

                commandName = String.Format(commandName, name);
            }
            return commandName;
        }

        public SqlObjectNames(DmTable tableDescription)
        {
            this.TableDescription = tableDescription;
            SetDefaultNames();
        }

        /// <summary>
        /// Set the default stored procedures names
        /// </summary>
        private void SetDefaultNames()
        {
            (var tableName, var trackingName) = SqlBuilder.GetParsers(this.TableDescription);

            var pref = this.TableDescription.StoredProceduresPrefix;
            var suf = this.TableDescription.StoredProceduresSuffix;
            var tpref = this.TableDescription.TriggersPrefix;
            var tsuf = this.TableDescription.TriggersSuffix;

            var schema = string.IsNullOrEmpty(tableName.SchemaName) ? "dbo" : tableName.SchemaName;

            this.AddName(DbCommandType.SelectChanges, string.Format(selectChangesProcName, schema, $"{pref}{tableName.ObjectNameNormalized}{suf}"));
            this.AddName(DbCommandType.SelectChangesWitFilters, string.Format(selectChangesProcNameWithFilters, schema, $"{pref}{tableName.ObjectNameNormalized}{suf}", "{0}"));
            this.AddName(DbCommandType.SelectRow, string.Format(selectRowProcName, schema, $"{pref}{tableName.ObjectNameNormalized}{suf}"));
            this.AddName(DbCommandType.InsertRow, string.Format(insertProcName, schema, $"{pref}{tableName.ObjectNameNormalized}{suf}"));
            this.AddName(DbCommandType.UpdateRow, string.Format(updateProcName, schema, $"{pref}{tableName.ObjectNameNormalized}{suf}"));
            this.AddName(DbCommandType.DeleteRow, string.Format(deleteProcName, schema, $"{pref}{tableName.ObjectNameNormalized}{suf}"));
            this.AddName(DbCommandType.InsertMetadata, string.Format(insertMetadataProcName, schema, $"{pref}{tableName.ObjectNameNormalized}{suf}"));
            this.AddName(DbCommandType.UpdateMetadata, string.Format(updateMetadataProcName, schema, $"{pref}{tableName.ObjectNameNormalized}{suf}"));
            this.AddName(DbCommandType.DeleteMetadata, string.Format(deleteMetadataProcName, schema, $"{pref}{tableName.ObjectNameNormalized}{suf}"));
            this.AddName(DbCommandType.Reset, string.Format(resetMetadataProcName, schema, $"{pref}{tableName.ObjectNameNormalized}{suf}"));

            this.AddName(DbCommandType.InsertTrigger, string.Format(insertTriggerName, schema, $"{tpref}{tableName.ObjectNameNormalized}{tsuf}"));
            this.AddName(DbCommandType.UpdateTrigger, string.Format(updateTriggerName, schema, $"{tpref}{tableName.ObjectNameNormalized}{tsuf}"));
            this.AddName(DbCommandType.DeleteTrigger, string.Format(deleteTriggerName, schema, $"{tpref}{tableName.ObjectNameNormalized}{tsuf}"));

            this.AddName(DbCommandType.BulkTableType, string.Format(bulkTableTypeName, schema, $"{pref}{tableName.ObjectNameNormalized}{suf}"));

            this.AddName(DbCommandType.BulkInsertRows, string.Format(bulkInsertProcName, schema, $"{pref}{tableName.ObjectNameNormalized}{suf}"));
            this.AddName(DbCommandType.BulkUpdateRows, string.Format(bulkUpdateProcName, schema, $"{pref}{tableName.ObjectNameNormalized}{suf}"));
            this.AddName(DbCommandType.BulkDeleteRows, string.Format(bulkDeleteProcName, schema, $"{pref}{tableName.ObjectNameNormalized}{suf}"));
        }

    }
}
