using Dotmim.Sync.Builders;


using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

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
        private readonly ParserName tableName;
        private readonly ParserName trackingName;

        //internal const string disableConstraintsText = "sp_msforeachtable";
        //internal const string enableConstraintsText = "sp_msforeachtable";

        Dictionary<DbStoredProcedureType, string> storedProceduresNames = new Dictionary<DbStoredProcedureType, string>();
        Dictionary<DbTriggerType, string> triggersNames = new Dictionary<DbTriggerType, string>();
        Dictionary<DbCommandType, string> commandNames = new Dictionary<DbCommandType, string>();

        public SyncTable TableDescription { get; }
        public SyncSetup Setup { get; }
        public void AddStoredProcedureName(DbStoredProcedureType objectType, string name)
        {
            if (storedProceduresNames.ContainsKey(objectType))
                throw new Exception("Yous can't add an objectType multiple times");

            storedProceduresNames.Add(objectType, name);
        }
        public string GetStoredProcedureCommandName(DbStoredProcedureType storedProcedureType, SyncFilter filter = null)
        {
            if (!storedProceduresNames.ContainsKey(storedProcedureType))
                throw new Exception("Yous should provide a value for all DbCommandName");

            var commandName = storedProceduresNames[storedProcedureType];

            // concat filter name
            if (filter != null && (storedProcedureType == DbStoredProcedureType.SelectChangesWithFilters || storedProcedureType == DbStoredProcedureType.SelectInitializedChangesWithFilters))
                commandName = string.Format(commandName, filter.GetFilterName());

            return commandName;
        }

        public void AddTriggerName(DbTriggerType objectType, string name)
        {
            if (triggersNames.ContainsKey(objectType))
                throw new Exception("Yous can't add an objectType multiple times");

            triggersNames.Add(objectType, name);
        }
        public string GetTriggerCommandName(DbTriggerType objectType, SyncFilter filter = null)
        {
            if (!triggersNames.ContainsKey(objectType))
                throw new Exception("Yous should provide a value for all DbCommandName");

            var commandName = triggersNames[objectType];

            // concat filter name
            if (filter != null)
                commandName = string.Format(commandName, filter.GetFilterName());

            return commandName;
        }
        public void AddCommandName(DbCommandType objectType, string name)
        {
            if (commandNames.ContainsKey(objectType))
                throw new Exception("Yous can't add an objectType multiple times");

            commandNames.Add(objectType, name);
        }
        public string GetCommandName(DbCommandType objectType, SyncFilter filter = null)
        {
            if (!commandNames.ContainsKey(objectType))
                throw new Exception("Yous should provide a value for all DbCommandName");

            var commandName = commandNames[objectType];

            // concat filter name
            if (filter != null)
                commandName = string.Format(commandName, filter.GetFilterName());

            return commandName;
        }

        public SqlObjectNames(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup)
        {
            this.TableDescription = tableDescription;
            this.tableName = tableName;
            this.trackingName = trackingName;
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

            this.AddStoredProcedureName(DbStoredProcedureType.SelectChanges, string.Format(selectChangesProcName, schema, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}"));
            this.AddStoredProcedureName(DbStoredProcedureType.SelectChangesWithFilters, string.Format(selectChangesProcNameWithFilters, schema, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}", "{0}"));

            this.AddStoredProcedureName(DbStoredProcedureType.SelectInitializedChanges, string.Format(initializeChangesProcName, schema, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}"));
            this.AddStoredProcedureName(DbStoredProcedureType.SelectInitializedChangesWithFilters, string.Format(initializeChangesProcNameWithFilters, schema, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}", "{0}"));

            this.AddStoredProcedureName(DbStoredProcedureType.SelectRow, string.Format(selectRowProcName, schema, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}"));
            this.AddStoredProcedureName(DbStoredProcedureType.UpdateRow, string.Format(updateProcName, schema, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}"));
            this.AddStoredProcedureName(DbStoredProcedureType.DeleteRow, string.Format(deleteProcName, schema, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}"));
            this.AddStoredProcedureName(DbStoredProcedureType.DeleteMetadata, string.Format(deleteMetadataProcName, schema, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}"));
            this.AddStoredProcedureName(DbStoredProcedureType.Reset, string.Format(resetMetadataProcName, schema, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}"));

            this.AddTriggerName(DbTriggerType.Insert, string.Format(insertTriggerName, schema, $"{tpref}{tableName.Unquoted().Normalized().ToString()}{tsuf}"));
            this.AddTriggerName(DbTriggerType.Update, string.Format(updateTriggerName, schema, $"{tpref}{tableName.Unquoted().Normalized().ToString()}{tsuf}"));
            this.AddTriggerName(DbTriggerType.Delete, string.Format(deleteTriggerName, schema, $"{tpref}{tableName.Unquoted().Normalized().ToString()}{tsuf}"));

            this.AddStoredProcedureName(DbStoredProcedureType.BulkTableType, string.Format(bulkTableTypeName, schema, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}"));
            this.AddStoredProcedureName(DbStoredProcedureType.BulkUpdateRows, string.Format(bulkUpdateProcName, schema, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}"));
            this.AddStoredProcedureName(DbStoredProcedureType.BulkDeleteRows, string.Format(bulkDeleteProcName, schema, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}"));

            this.AddCommandName(DbCommandType.DisableConstraints, string.Format(disableConstraintsText, ParserName.Parse(TableDescription).Schema().Quoted().ToString()));
            this.AddCommandName(DbCommandType.EnableConstraints, string.Format(enableConstraintsText, ParserName.Parse(TableDescription).Schema().Quoted().ToString()));

            this.AddCommandName(DbCommandType.UpdateUntrackedRows, CreateUpdateUntrackedRowsCommand());
            this.AddCommandName(DbCommandType.UpdateMetadata, CreateUpdateMetadataCommand());
        }


        private string CreateUpdateMetadataCommand()
        {
            var stringBuilder = new StringBuilder();
            var pkeysForUpdate = new StringBuilder();

            var pkeySelectForInsert = new StringBuilder();
            var pkeyISelectForInsert = new StringBuilder();
            var pkeyAliasSelectForInsert = new StringBuilder();
            var pkeysLeftJoinForInsert = new StringBuilder();
            var pkeysIsNullForInsert = new StringBuilder();

            string and = string.Empty;
            string comma = string.Empty;
            foreach (var pkColumn in TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                var parameterName = ParserName.Parse(pkColumn).Unquoted().Normalized().ToString();

                pkeysForUpdate.Append($"{and}[side].{columnName} = @{parameterName}");

                pkeySelectForInsert.Append($"{comma}{columnName}");
                pkeyISelectForInsert.Append($"{comma}[i].{columnName}");
                pkeyAliasSelectForInsert.Append($"{comma}@{parameterName} as {columnName}");
                pkeysLeftJoinForInsert.Append($"{and}[side].{columnName} = [i].{columnName}");
                pkeysIsNullForInsert.Append($"{and}[side].{columnName} IS NULL");
                and = " AND ";
                comma = ", ";
            }


            stringBuilder.AppendLine($"UPDATE [side] SET ");
            stringBuilder.AppendLine($" [update_scope_id] = @sync_scope_id, ");
            stringBuilder.AppendLine($" [sync_row_is_tombstone] = @sync_row_is_tombstone, ");
            stringBuilder.AppendLine($" [last_change_datetime] = GETUTCDATE() ");
            stringBuilder.AppendLine($"FROM {trackingName.Schema().Quoted().ToString()} [side]");
            stringBuilder.Append($"WHERE ");
            stringBuilder.Append(pkeysForUpdate.ToString());
            stringBuilder.AppendLine($";");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"INSERT INTO {trackingName.Schema().Quoted().ToString()} (");
            stringBuilder.AppendLine(pkeySelectForInsert.ToString());
            stringBuilder.AppendLine(",[update_scope_id], [sync_row_is_tombstone],[last_change_datetime] )");
            stringBuilder.AppendLine($"SELECT {pkeyISelectForInsert.ToString()} ");
            stringBuilder.AppendLine($"   , i.sync_scope_id, i.sync_row_is_tombstone, i.UtcDate");
            stringBuilder.AppendLine("FROM (");
            stringBuilder.AppendLine($"  SELECT {pkeyAliasSelectForInsert}");
            stringBuilder.AppendLine($"          ,@sync_scope_id as sync_scope_id, @sync_row_is_tombstone as sync_row_is_tombstone, GETUTCDATE() as UtcDate) as i");
            stringBuilder.AppendLine($"LEFT JOIN  {trackingName.Schema().Quoted().ToString()} [side] ON {pkeysLeftJoinForInsert.ToString()} ");
            stringBuilder.AppendLine($"WHERE {pkeysIsNullForInsert.ToString()};");


            return stringBuilder.ToString();
        }

        private string CreateUpdateUntrackedRowsCommand()
        {
            var stringBuilder = new StringBuilder();
            var str1 = new StringBuilder();
            var str2 = new StringBuilder();
            var str3 = new StringBuilder();
            var str4 = SqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "[side]", "[base]");

            stringBuilder.AppendLine($"INSERT INTO {trackingName.Schema().Quoted().ToString()} (");


            var comma = "";
            foreach (var pkeyColumn in TableDescription.GetPrimaryKeysColumns())
            {
                var pkeyColumnName = ParserName.Parse(pkeyColumn).Quoted().ToString();

                str1.Append($"{comma}{pkeyColumnName}");
                str2.Append($"{comma}[base].{pkeyColumnName}");
                str3.Append($"{comma}[side].{pkeyColumnName}");

                comma = ", ";
            }
            stringBuilder.Append(str1.ToString());
            stringBuilder.AppendLine($", [update_scope_id], [sync_row_is_tombstone], [last_change_datetime]");
            stringBuilder.AppendLine($")");
            stringBuilder.Append($"SELECT ");
            stringBuilder.Append(str2.ToString());
            stringBuilder.AppendLine($", NULL, 0, GetUtcDate()");
            stringBuilder.AppendLine($"FROM {tableName.Schema().Quoted().ToString()} as [base] WHERE NOT EXISTS");
            stringBuilder.Append($"(SELECT ");
            stringBuilder.Append(str3.ToString());
            stringBuilder.AppendLine($" FROM {trackingName.Schema().Quoted().ToString()} as [side] ");
            stringBuilder.AppendLine($"WHERE {str4})");

            var r = stringBuilder.ToString();

            return r;

        }

    }
}
