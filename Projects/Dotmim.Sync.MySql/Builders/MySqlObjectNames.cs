using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

#if MARIADB
namespace Dotmim.Sync.MariaDB.Builders
#elif MYSQL
namespace Dotmim.Sync.MySql.Builders
#endif
{
    public class MySqlObjectNames
    {
        public const string TimestampValue = "ROUND(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(6)) * 10000)";

        internal const string insertTriggerName = "`{0}_insert_trigger`";
        internal const string updateTriggerName = "`{0}_update_trigger`";
        internal const string deleteTriggerName = "`{0}_delete_trigger`";

        internal const string selectChangesProcName = "`{0}_changes`";
        internal const string selectChangesProcNameWithFilters = "`{0}_{1}_changes`";

        internal const string initializeChangesProcName = "`{0}_initialize`";
        internal const string initializeChangesProcNameWithFilters = "`{0}_{1}_initialize`";

        internal const string selectRowProcName = "`{0}_selectrow`";

        internal const string insertProcName = "`{0}_insert`";
        internal const string updateProcName = "`{0}_update`";
        internal const string deleteProcName = "`{0}_delete`";

        internal const string resetProcName = "`{0}_reset`";

        internal const string insertMetadataProcName = "`{0}_insertmetadata`";
        internal const string updateMetadataProcName = "`{0}_updatemetadata`";
        internal const string deleteMetadataProcName = "`{0}_deletemetadata`";

        internal const string disableConstraintsText = "SET FOREIGN_KEY_CHECKS=0;";
        internal const string enableConstraintsText = "SET FOREIGN_KEY_CHECKS=1;";


        Dictionary<DbStoredProcedureType, string> storedProceduresNames = new Dictionary<DbStoredProcedureType, string>();
        Dictionary<DbTriggerType, string> triggersNames = new Dictionary<DbTriggerType, string>();
        Dictionary<DbCommandType, string> commandNames = new Dictionary<DbCommandType, string>();
        
        private ParserName tableName, trackingName;

        public SyncTable TableDescription { get; }
        public SyncSetup Setup { get; }
        public string ScopeName { get; }

        public void AddCommandName(DbCommandType objectType, string name)
        {
            if (commandNames.ContainsKey(objectType))
                throw new Exception("Yous can't add an objectType multiple times");

            commandNames.Add(objectType, name);
        }
        public string GetCommandName(DbCommandType objectType, SyncFilter filter = null)
        {
            if (!commandNames.ContainsKey(objectType))
                throw new NotSupportedException($"MySql provider does not support the command type {objectType.ToString()}");

            var commandName = commandNames[objectType];

            // concat filter name
            if (filter != null)
                commandName = string.Format(commandName, filter.GetFilterName());

            return commandName;
        }

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


        public MySqlObjectNames(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup, string scopeName)
        {
            this.TableDescription = tableDescription;
            this.Setup = setup;
            this.ScopeName = scopeName;
            this.tableName = tableName;
            this.trackingName = trackingName;

            SetDefaultNames();
        }

        /// <summary>
        /// Set the default stored procedures names
        /// </summary>
        private void SetDefaultNames()
        {
            var spPref = this.Setup.StoredProceduresPrefix != null ? this.Setup.StoredProceduresPrefix : "";
            var spSuf = this.Setup.StoredProceduresSuffix != null ? this.Setup.StoredProceduresSuffix : "";
            var trigPref = this.Setup.TriggersPrefix != null ? this.Setup.TriggersPrefix : "";
            var trigSuf = this.Setup.TriggersSuffix != null ? this.Setup.TriggersSuffix : "";

            // TODO
            var scopeNameWithoutDefaultScope = ScopeName == SyncOptions.DefaultScopeName ? "" : ScopeName;

            this.AddTriggerName(DbTriggerType.Insert, string.Format(insertTriggerName, $"{trigPref}{tableName.Unquoted().Normalized().ToString()}{trigSuf}"));
            this.AddTriggerName(DbTriggerType.Update, string.Format(updateTriggerName, $"{trigPref}{tableName.Unquoted().Normalized().ToString()}{trigSuf}"));
            this.AddTriggerName(DbTriggerType.Delete, string.Format(deleteTriggerName, $"{trigPref}{tableName.Unquoted().Normalized().ToString()}{trigSuf}"));

            this.AddStoredProcedureName(DbStoredProcedureType.SelectChanges, string.Format(selectChangesProcName, $"{spPref}{tableName.Unquoted().Normalized().ToString()}{spSuf}"));
            this.AddStoredProcedureName(DbStoredProcedureType.SelectChangesWithFilters, string.Format(selectChangesProcNameWithFilters, $"{spPref}{tableName.Unquoted().Normalized().ToString()}{spSuf}", "{0}"));

            this.AddStoredProcedureName(DbStoredProcedureType.SelectInitializedChanges, string.Format(initializeChangesProcName, $"{spPref}{tableName.Unquoted().Normalized().ToString()}{spSuf}"));
            this.AddStoredProcedureName(DbStoredProcedureType.SelectInitializedChangesWithFilters, string.Format(initializeChangesProcNameWithFilters, $"{spPref}{tableName.Unquoted().Normalized().ToString()}{spSuf}", "{0}"));

            this.AddStoredProcedureName(DbStoredProcedureType.SelectRow, string.Format(selectRowProcName, $"{spPref}{tableName.Unquoted().Normalized().ToString()}{spSuf}"));
            this.AddStoredProcedureName(DbStoredProcedureType.UpdateRow, string.Format(updateProcName, $"{spPref}{tableName.Unquoted().Normalized().ToString()}{spSuf}"));
            this.AddStoredProcedureName(DbStoredProcedureType.DeleteRow, string.Format(deleteProcName, $"{spPref}{tableName.Unquoted().Normalized().ToString()}{spSuf}"));
            this.AddStoredProcedureName(DbStoredProcedureType.DeleteMetadata, string.Format(deleteMetadataProcName, $"{spPref}{tableName.Unquoted().Normalized().ToString()}{spSuf}"));
            this.AddStoredProcedureName(DbStoredProcedureType.Reset, string.Format(resetProcName, $"{spPref}{tableName.Unquoted().Normalized().ToString()}{spSuf}"));

            this.AddCommandName(DbCommandType.DisableConstraints, string.Format(disableConstraintsText, ParserName.Parse(TableDescription).Quoted().ToString()));
            this.AddCommandName(DbCommandType.EnableConstraints, string.Format(enableConstraintsText, ParserName.Parse(TableDescription).Quoted().ToString()));

            this.AddCommandName(DbCommandType.UpdateUntrackedRows, CreateUpdateUntrackedRowsCommand());
            this.AddCommandName(DbCommandType.UpdateMetadata, CreateUpdateMetadataCommand());
        }


        private string CreateUpdateMetadataCommand()
        {
            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.AppendLine($"\tINSERT INTO {trackingName.Quoted().ToString()} (");

            var pkeySelect = new StringBuilder();
            var pkeyValues = new StringBuilder();

            string argComma = string.Empty;
            string argAnd = string.Empty;
            foreach (var mutableColumn in this.TableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn, "`").Quoted().ToString();
                var unquotedColumnName = ParserName.Parse(mutableColumn, "`").Unquoted().ToString();

                // Select
                pkeySelect.AppendLine($"\t\t{argComma}{columnName}");

                // Values
                pkeyValues.AppendLine($"\t\t{argComma}@{unquotedColumnName}");
                
                argComma = ",";
                argAnd = " AND ";
            }

            stringBuilder.Append(pkeySelect.ToString());
            stringBuilder.AppendLine("\t\t,`update_scope_id`");
            stringBuilder.AppendLine("\t\t,`timestamp`");
            stringBuilder.AppendLine("\t\t,`sync_row_is_tombstone`");
            stringBuilder.AppendLine("\t\t,`last_change_datetime`");

            var filterColumnsString = new StringBuilder();
            var filterColumnsString2 = new StringBuilder();
            var filterColumnsString3 = new StringBuilder();

            stringBuilder.AppendLine("\t) ");
            stringBuilder.AppendLine("\tVALUES (");
            stringBuilder.Append(pkeyValues.ToString());
            stringBuilder.AppendLine("\t\t,@sync_scope_id");
            stringBuilder.AppendLine($"\t\t,{TimestampValue}");
            stringBuilder.AppendLine("\t\t,@sync_row_is_tombstone");
            stringBuilder.AppendLine("\t\t,utc_timestamp()");


            stringBuilder.AppendLine("\t)");
            stringBuilder.AppendLine("ON DUPLICATE KEY UPDATE");
            stringBuilder.AppendLine("\t`update_scope_id` = @sync_scope_id, ");
            stringBuilder.AppendLine("\t`sync_row_is_tombstone` = @sync_row_is_tombstone, ");
            stringBuilder.AppendLine($"\t`timestamp` = {TimestampValue}, ");
            stringBuilder.AppendLine("\t`last_change_datetime` = utc_timestamp();");
            return stringBuilder.ToString();
        }

        private string CreateUpdateUntrackedRowsCommand()
        {
            var stringBuilder = new StringBuilder();
            var str1 = new StringBuilder();
            var str2 = new StringBuilder();
            var str3 = new StringBuilder();
            var str4 = MySqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.GetPrimaryKeysColumns(), "`side`", "`base`");

            stringBuilder.AppendLine($"INSERT INTO {trackingName.Schema().Quoted().ToString()} (");


            var comma = "";
            foreach (var pkeyColumn in TableDescription.GetPrimaryKeysColumns())
            {
                var pkeyColumnName = ParserName.Parse(pkeyColumn, "`").Quoted().ToString();

                str1.Append($"{comma}{pkeyColumnName}");
                str2.Append($"{comma}`base`.{pkeyColumnName}");
                str3.Append($"{comma}`side`.{pkeyColumnName}");

                comma = ", ";
            }
            stringBuilder.Append(str1.ToString());
            stringBuilder.AppendLine($", `update_scope_id`, `sync_row_is_tombstone`, `timestamp`, `last_change_datetime`");
            stringBuilder.AppendLine($")");
            stringBuilder.Append($"SELECT ");
            stringBuilder.Append(str2.ToString());
            stringBuilder.AppendLine($", NULL, 0, {TimestampValue}, now()");
            stringBuilder.AppendLine($"FROM {tableName.Schema().Quoted().ToString()} as `base` WHERE NOT EXISTS");
            stringBuilder.Append($"(SELECT ");
            stringBuilder.Append(str3.ToString());
            stringBuilder.AppendLine($" FROM {trackingName.Schema().Quoted().ToString()} as `side` ");
            stringBuilder.AppendLine($"WHERE {str4})");

            var r = stringBuilder.ToString();

            return r;

        }


    }
}
