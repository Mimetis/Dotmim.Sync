using Dotmim.Sync.Builders;


using System;
using System.Collections.Generic;
using System.Linq;

namespace Dotmim.Sync.SqlServer.Builders
{
    public class SqlObjectNames
    {
        internal const string insertTriggerName = "[{0}].[{1}_insert_trigger]";
        internal const string updateTriggerName = "[{0}].[{1}_update_trigger]";
        internal const string deleteTriggerName = "[{0}].[{1}_delete_trigger]";

        internal const string selectChangesProcName = "[{0}].[{1}_changes]";
        internal const string selectChangesProcNameWithFilters = "[{0}].[{1}_{2}_changes]";

        internal const string initializeChangesProcName = "[{0}].[{1}_initialize]";
        internal const string initializeChangesProcNameWithFilters = "[{0}].[{1}_{2}_initialize]";

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

        internal const string disableConstraintsText = "ALTER TABLE {0} NOCHECK CONSTRAINT ALL";
        internal const string enableConstraintsText = "ALTER TABLE {0} CHECK CONSTRAINT ALL";
        //internal const string disableConstraintsText = "sp_msforeachtable";
        //internal const string enableConstraintsText = "sp_msforeachtable";

        Dictionary<DbCommandType, (string name, bool isStoredProcedure)> names = new Dictionary<DbCommandType, (string name, bool isStoredProcedure)>();
        
        public SyncTable TableDescription { get; }
        public SyncSetup Setup { get; }

        public void AddName(DbCommandType objectType, string name, bool isStoredProcedure)
        {
            if (names.ContainsKey(objectType))
                throw new Exception("Yous can't add an objectType multiple times");

            names.Add(objectType, (name, isStoredProcedure));
        }
        public (string name, bool isStoredProcedure) GetCommandName(DbCommandType objectType, SyncFilter filter = null)
        {
            if (!names.ContainsKey(objectType))
                throw new Exception("Yous should provide a value for all DbCommandName");

            (var commandName, var isStoredProc) = names[objectType];

            // concat filter name
            if (filter != null)
                commandName = string.Format(commandName, filter.GetFilterName());

            return (commandName, isStoredProc);
        }

        public SqlObjectNames(SyncTable tableDescription, SyncSetup setup)
        {
            this.TableDescription = tableDescription;
            this.Setup = setup;
            SetDefaultNames();
        }

        /// <summary>
        /// Set the default stored procedures names
        /// </summary>
        private void SetDefaultNames()
        {
            var pref = this.Setup.StoredProceduresPrefix;
            var suf = this.Setup.StoredProceduresSuffix;
            var tpref = this.Setup.TriggersPrefix;
            var tsuf = this.Setup.TriggersSuffix;

            var tableName = ParserName.Parse(TableDescription);

            var schema = string.IsNullOrEmpty(tableName.SchemaName) ? "dbo" : tableName.SchemaName;

            this.AddName(DbCommandType.SelectChanges, string.Format(selectChangesProcName, schema, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}"), true);
            this.AddName(DbCommandType.SelectChangesWithFilters, string.Format(selectChangesProcNameWithFilters, schema, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}", "{0}"), true);

            this.AddName(DbCommandType.SelectInitializedChanges, string.Format(initializeChangesProcName, schema, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}"), true);
            this.AddName(DbCommandType.SelectInitializedChangesWithFilters, string.Format(initializeChangesProcNameWithFilters, schema, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}", "{0}"), true);

            this.AddName(DbCommandType.SelectRow, string.Format(selectRowProcName, schema, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}"), true);
            this.AddName(DbCommandType.UpdateRow, string.Format(updateProcName, schema, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}"), true);
            this.AddName(DbCommandType.DeleteRow, string.Format(deleteProcName, schema, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}"), true);
            this.AddName(DbCommandType.DeleteMetadata, string.Format(deleteMetadataProcName, schema, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}"), true);
            this.AddName(DbCommandType.Reset, string.Format(resetMetadataProcName, schema, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}"), true);

            this.AddName(DbCommandType.InsertTrigger, string.Format(insertTriggerName, schema, $"{tpref}{tableName.Unquoted().Normalized().ToString()}{tsuf}"), true);
            this.AddName(DbCommandType.UpdateTrigger, string.Format(updateTriggerName, schema, $"{tpref}{tableName.Unquoted().Normalized().ToString()}{tsuf}"), true);
            this.AddName(DbCommandType.DeleteTrigger, string.Format(deleteTriggerName, schema, $"{tpref}{tableName.Unquoted().Normalized().ToString()}{tsuf}"), true);

            this.AddName(DbCommandType.BulkTableType, string.Format(bulkTableTypeName, schema, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}"), true);

            this.AddName(DbCommandType.BulkUpdateRows, string.Format(bulkUpdateProcName, schema, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}"), true);
            this.AddName(DbCommandType.BulkDeleteRows, string.Format(bulkDeleteProcName, schema, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}"), true);

            this.AddName(DbCommandType.DisableConstraints, string.Format(disableConstraintsText, ParserName.Parse(TableDescription).Schema().Quoted().ToString()), false);
            this.AddName(DbCommandType.EnableConstraints, string.Format(enableConstraintsText, ParserName.Parse(TableDescription).Schema().Quoted().ToString()), false);
        }

    }
}
