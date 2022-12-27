using Dotmim.Sync.Builders;


using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Dotmim.Sync.PostgreSql.Builders
{
    public class NpgsqlObjectNames
    {
        public const string TimestampValue = "(extract(epoch from now())*1000)";
        internal const string insertTriggerName = "{0}insert_trigger";
        internal const string updateTriggerName = "{0}update_trigger";
        internal const string deleteTriggerName = "{0}delete_trigger";

        internal const string selectChangesProcName = @"{0}.""{1}{2}changes""";
        internal const string selectChangesProcNameWithFilters = @"{0}.""{1}{2}{3}changes""";

        internal const string initializeChangesProcName = @"{0}.""{1}{2}initialize""";
        internal const string initializeChangesProcNameWithFilters = @"{0}.""{1}{2}{3}initialize""";

        internal const string selectRowProcName = @"{0}.""{1}{2}selectrow""";

        internal const string insertProcName = @"{0}.""{1}{2}insert""";
        internal const string updateProcName = @"{0}.""{1}{2}update""";
        internal const string deleteProcName = @"{0}.""{1}{2}delete""";

        internal const string deleteMetadataProcName = @"{0}.""{1}{2}deletemetadata""";

        internal const string resetMetadataProcName = @"{0}.""{1}{2}reset""";

        //internal const string bulkTableTypeName = "{0}.{1}{2}BulkType";
        //internal const string bulkInsertProcName = "{0}.{1}{2}bulkinsert";
        //internal const string bulkUpdateProcName = "{0}.{1}{2}bulkupdate";
        //internal const string bulkDeleteProcName = "{0}.{1}{2}bulkdelete";

        internal const string disableConstraintsText = "ALTER TABLE {0}.{1} DISABLE TRIGGER ALL";
        internal const string enableConstraintsText = "ALTER TABLE {0}.{1} ENABLE TRIGGER ALL";

        Dictionary<DbCommandType, string> commandNames = new Dictionary<DbCommandType, string>();
        private string scopeName;
        private SyncSetup setup;
        Dictionary<DbStoredProcedureType, string> storedProceduresNames = new Dictionary<DbStoredProcedureType, string>();
        private SyncTable tableDescription;
        private ParserName tableName;
        private ParserName trackingName;
        Dictionary<DbTriggerType, string> triggersNames = new Dictionary<DbTriggerType, string>();

        public NpgsqlObjectNames(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup, string scopeName)
        {
            this.tableDescription = tableDescription;
            this.tableName = tableName;
            this.trackingName = trackingName;
            this.setup = setup;
            this.scopeName = scopeName;
            this.SetDefaultNames();
        }



        public void AddCommandName(DbCommandType objectType, string name)
        {
            if (commandNames.ContainsKey(objectType))
                throw new Exception("Yous can't add an objectType multiple times");

            commandNames.Add(objectType, name);
        }

        public void AddStoredProcedureName(DbStoredProcedureType objectType, string name)
        {
            if (storedProceduresNames.ContainsKey(objectType))
                throw new Exception("You can't add an objectType multiple times");

            storedProceduresNames.Add(objectType, name);
        }

        public void AddTriggerName(DbTriggerType objectType, string name)
        {
            if (triggersNames.ContainsKey(objectType))
                throw new Exception("You can't add an objectType multiple times");

            triggersNames.Add(objectType, name);
        }

        public string GetCommandName(DbCommandType objectType, SyncFilter filter)
        {
            if (!commandNames.ContainsKey(objectType))
                throw new Exception("You should provide a value for all DbCommandName");

            var commandName = commandNames[objectType];

            // concat filter name
            if (filter != null)
                commandName = string.Format(commandName, filter.GetFilterName());

            return commandName;
        }

        public string GetStoredProcedureCommandName(DbStoredProcedureType storedProcedureType, SyncFilter filter = null)
        {
            if (!storedProceduresNames.ContainsKey(storedProcedureType))
                throw new Exception("You should provide a value for all storedProcedureType");

            var commandName = storedProceduresNames[storedProcedureType];

            // concat filter name
            if (filter != null && (storedProcedureType == DbStoredProcedureType.SelectChangesWithFilters || storedProcedureType == DbStoredProcedureType.SelectInitializedChangesWithFilters))
                commandName = string.Format(commandName, filter.GetFilterName());

            return commandName;
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

        private string CreateSelectMetadataCommand()
        {
            var trackingTableQuoted = ParserName.Parse(trackingName.ToString(), "\"").Quoted().ToString();
            var tableQuoted = ParserName.Parse(tableName.ToString(), "\"").Quoted().ToString();
            var tableUnquoted = tableName.Unquoted().ToString();
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(tableName);

            var stringBuilder = new StringBuilder();
            var pkeysSelect = new StringBuilder();
            var pkeysWhere = new StringBuilder();


            string and = string.Empty;
            string comma = string.Empty;
            foreach (var pkColumn in tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn, "\"").Quoted().ToString();
                var parameterName = ParserName.Parse(pkColumn).Unquoted().Normalized().ToString();
                pkeysSelect.Append($"{comma}side.{columnName}");

                pkeysWhere.Append($"{and}side.{columnName} = @{parameterName}");

                and = " AND ";
                comma = ", ";
            }

            stringBuilder.AppendLine($"SELECT {pkeysSelect}, side.update_scope_id, side.timestamp_bigint as timestamp, side.sync_row_is_tombstone");
            stringBuilder.AppendLine($"FROM {schema}.{trackingTableQuoted} side");
            stringBuilder.AppendLine($"WHERE {pkeysWhere}");

            return stringBuilder.ToString();
        }

        private string CreateUpdateMetadataCommand()
        {
            var trackingTableQuoted = ParserName.Parse(trackingName.ToString(), "\"").Quoted().ToString();
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(tableName);

            var stringBuilder = new StringBuilder();
            var pkeysForUpdate = new StringBuilder();

            var pkeySelectForInsert = new StringBuilder();
            var pkeyISelectForInsert = new StringBuilder();
            var pkeyAliasSelectForInsert = new StringBuilder();
            var pkeysLeftJoinForInsert = new StringBuilder();
            var pkeysIsNullForInsert = new StringBuilder();

            string and = string.Empty;
            string comma = string.Empty;
            foreach (var pkColumn in tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn,"\"").Quoted().ToString();
                var parameterName = ParserName.Parse(pkColumn).Unquoted().Normalized().ToString();

                pkeysForUpdate.Append($"{and}side.{columnName} = @{parameterName}");

                pkeySelectForInsert.Append($"{comma}{columnName}");
                pkeyISelectForInsert.Append($"{comma}i.{columnName}");
                pkeyAliasSelectForInsert.Append($"{comma}@{parameterName} as {columnName}");
                pkeysLeftJoinForInsert.Append($"{and}side.{columnName} = i.{columnName}");
                pkeysIsNullForInsert.Append($"{and}side.{columnName} IS NULL");
                and = " AND ";
                comma = ", ";
            }


            stringBuilder.AppendLine($"UPDATE side SET ");
            stringBuilder.AppendLine($@" ""update_scope_id"" = @sync_scope_id, ");
            stringBuilder.AppendLine($@" ""sync_row_is_tombstone"" = @sync_row_is_tombstone, ");
            stringBuilder.AppendLine($@" ""last_change_datetime"" = now() ");
            stringBuilder.AppendLine($"FROM {schema}.{trackingTableQuoted} side ");
            stringBuilder.Append($"WHERE ");
            stringBuilder.Append(pkeysForUpdate.ToString());
            stringBuilder.AppendLine($";");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"INSERT INTO {schema}.{trackingTableQuoted} (");
            stringBuilder.AppendLine(pkeySelectForInsert.ToString());
            stringBuilder.AppendLine(@",""update_scope_id"", ""sync_row_is_tombstone"",""last_change_datetime"" )");
            stringBuilder.AppendLine($"SELECT {pkeyISelectForInsert.ToString()} ");
            stringBuilder.AppendLine($@"   , i.""sync_scope_id"", i.""sync_row_is_tombstone"", i.""UtcDate""");
            stringBuilder.AppendLine("FROM (");
            stringBuilder.AppendLine($"  SELECT {pkeyAliasSelectForInsert}");
            stringBuilder.AppendLine($"          ,@sync_scope_id as sync_scope_id, @sync_row_is_tombstone as sync_row_is_tombstone, now() as UtcDate) as i");
            stringBuilder.AppendLine($"LEFT JOIN  {schema}.{trackingTableQuoted} side ON {pkeysLeftJoinForInsert.ToString()} ");
            stringBuilder.AppendLine($"WHERE {pkeysIsNullForInsert.ToString()};");


            return stringBuilder.ToString();
        }

        private string CreateUpdateUntrackedRowsCommand()
        {
            var tableQuoted = ParserName.Parse(tableName.ToString(), "\"").Quoted().ToString();
            var trackingTableQuoted = ParserName.Parse(trackingName.ToString(), "\"").Quoted().ToString();
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(tableName);

            var stringBuilder = new StringBuilder();
            var str1 = new StringBuilder();
            var str2 = new StringBuilder();
            var str3 = new StringBuilder();
            var str4 = NpgsqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "side", "base");

            stringBuilder.AppendLine($"INSERT INTO {schema}.{trackingTableQuoted} (");


            var comma = "";
            foreach (var pkeyColumn in tableDescription.GetPrimaryKeysColumns())
            {
                var pkeyColumnName = ParserName.Parse(pkeyColumn,"\"").Quoted().ToString();

                str1.Append($"{comma}{pkeyColumnName}");
                str2.Append($"{comma}base.{pkeyColumnName}");
                str3.Append($"{comma}side.{pkeyColumnName}");

                comma = ", ";
            }
            stringBuilder.Append(str1.ToString());
            stringBuilder.AppendLine($@", ""update_scope_id"", ""timestamp"", ""sync_row_is_tombstone"", ""last_change_datetime""");
            stringBuilder.AppendLine($")");
            stringBuilder.Append($"SELECT ");
            stringBuilder.Append(str2.ToString());
            stringBuilder.AppendLine($", NULL, {TimestampValue}, False, now()");
            stringBuilder.AppendLine($"FROM {schema}.{tableQuoted} as base WHERE NOT EXISTS");
            stringBuilder.Append($"(SELECT ");
            stringBuilder.Append(str3.ToString());
            stringBuilder.AppendLine($" FROM {schema}.{trackingTableQuoted} as side ");
            stringBuilder.AppendLine($"WHERE {str4})");

            var r = stringBuilder.ToString();

            return r;

        }

        private void SetDefaultNames()
        {
            var pref = this.setup?.StoredProceduresPrefix;
            var suf = this.setup?.StoredProceduresSuffix;
            var tpref = this.setup?.TriggersPrefix;
            var tsuf = this.setup?.TriggersSuffix;

            var tableName = ParserName.Parse(tableDescription, "\"");

            var scopeNameWithoutDefaultScope = scopeName == SyncOptions.DefaultScopeName ? "" : $"{scopeName}_";

            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(tableName);

            var storedProcedureName = $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}_";
            //var storedProcedureName = ParserName.Parse(storedProcedure, "\"").Quoted().ToString();

            var triggerName = $"{tpref}{tableName.Unquoted().Normalized().ToString()}{tsuf}_";

            this.AddStoredProcedureName(DbStoredProcedureType.SelectChanges, string.Format(selectChangesProcName, schema, storedProcedureName, scopeNameWithoutDefaultScope));
            this.AddStoredProcedureName(DbStoredProcedureType.SelectChangesWithFilters, string.Format(selectChangesProcNameWithFilters, schema, storedProcedureName, scopeNameWithoutDefaultScope, "{0}_"));

            this.AddStoredProcedureName(DbStoredProcedureType.SelectInitializedChanges, string.Format(initializeChangesProcName, schema, storedProcedureName, scopeNameWithoutDefaultScope));
            this.AddStoredProcedureName(DbStoredProcedureType.SelectInitializedChangesWithFilters, string.Format(initializeChangesProcNameWithFilters, schema, storedProcedureName, scopeNameWithoutDefaultScope, "{0}_"));

            this.AddStoredProcedureName(DbStoredProcedureType.SelectRow, string.Format(selectRowProcName, schema, storedProcedureName, scopeNameWithoutDefaultScope));
            this.AddStoredProcedureName(DbStoredProcedureType.UpdateRow, string.Format(updateProcName, schema, storedProcedureName, scopeNameWithoutDefaultScope));
            this.AddStoredProcedureName(DbStoredProcedureType.DeleteRow, string.Format(deleteProcName, schema, storedProcedureName, scopeNameWithoutDefaultScope));
            this.AddStoredProcedureName(DbStoredProcedureType.DeleteMetadata, string.Format(deleteMetadataProcName, schema, storedProcedureName, scopeNameWithoutDefaultScope));
            this.AddStoredProcedureName(DbStoredProcedureType.Reset, string.Format(resetMetadataProcName, schema, storedProcedureName, scopeNameWithoutDefaultScope));

            this.AddTriggerName(DbTriggerType.Insert, string.Format(insertTriggerName, triggerName));
            this.AddTriggerName(DbTriggerType.Update, string.Format(updateTriggerName, triggerName));
            this.AddTriggerName(DbTriggerType.Delete, string.Format(deleteTriggerName, triggerName));

            //this.AddStoredProcedureName(DbStoredProcedureType.BulkTableType, string.Format(bulkTableTypeName, schema, storedProcedureName, scopeNameWithoutDefaultScope));
            //this.AddStoredProcedureName(DbStoredProcedureType.BulkUpdateRows, string.Format(bulkUpdateProcName, schema, storedProcedureName, scopeNameWithoutDefaultScope));
            //this.AddStoredProcedureName(DbStoredProcedureType.BulkDeleteRows, string.Format(bulkDeleteProcName, schema, storedProcedureName, scopeNameWithoutDefaultScope));


            this.AddCommandName(DbCommandType.DisableConstraints, string.Format(disableConstraintsText, schema, tableName.Quoted()));
            this.AddCommandName(DbCommandType.EnableConstraints, string.Format(enableConstraintsText, schema, tableName.Quoted()));

            this.AddCommandName(DbCommandType.UpdateUntrackedRows, CreateUpdateUntrackedRowsCommand());
            this.AddCommandName(DbCommandType.SelectMetadata, CreateSelectMetadataCommand());
            this.AddCommandName(DbCommandType.UpdateMetadata, CreateUpdateMetadataCommand());
        }
    }
}
