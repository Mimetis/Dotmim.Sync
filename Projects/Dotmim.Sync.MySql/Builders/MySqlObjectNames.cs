using Dotmim.Sync.Builders;
#if NET6_0 || NET8_0 
using MySqlConnector;
#elif NETSTANDARD
using MySql.Data.MySqlClient;
#endif
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;


#if MARIADB
namespace Dotmim.Sync.MariaDB.Builders
#elif MYSQL
namespace Dotmim.Sync.MySql.Builders
#endif
{
    public partial class MySqlObjectNames
    {
        public const string MYSQL_PREFIX_PARAMETER = "in_";

        public const string TimestampValue = "ROUND(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(6)) * 10000)";

        internal const string insertTriggerName = "`{0}insert_trigger`";
        internal const string updateTriggerName = "`{0}update_trigger`";
        internal const string deleteTriggerName = "`{0}delete_trigger`";

        internal const string selectChangesProcName = "`{0}{1}changes`";
        internal const string selectChangesProcNameWithFilters = "`{0}{1}{2}changes`";

        internal const string initializeChangesProcName = "`{0}{1}initialize`";
        internal const string initializeChangesProcNameWithFilters = "`{0}{1}{2}initialize`";

        internal const string selectRowProcName = "`{0}{1}selectrow`";

        internal const string insertProcName = "`{0}{1}insert`";
        internal const string updateProcName = "`{0}{1}update`";
        internal const string deleteProcName = "`{0}{1}delete`";

        internal const string resetProcName = "`{0}{1}reset`";

        internal const string insertMetadataProcName = "`{0}{1}insertmetadata`";
        internal const string updateMetadataProcName = "`{0}{1}updatemetadata`";
        internal const string deleteMetadataProcName = "`{0}{1}deletemetadata`";

        internal const string disableConstraintsText = "SET FOREIGN_KEY_CHECKS=0;";
        internal const string enableConstraintsText = "SET FOREIGN_KEY_CHECKS=1;";


        Dictionary<DbStoredProcedureType, string> storedProceduresNames = new Dictionary<DbStoredProcedureType, string>();
        Dictionary<DbTriggerType, string> triggersNames = new Dictionary<DbTriggerType, string>();
        Dictionary<DbCommandType, string> commandNames = new Dictionary<DbCommandType, string>();
        
        private ParserName tableName, trackingName;
        private MySqlDbMetadata dbMetadata;

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
            if (!commandNames.TryGetValue(objectType, out var commandName))
                throw new NotSupportedException($"MySql provider does not support the command type {objectType.ToString()}");

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
            if (!storedProceduresNames.TryGetValue(storedProcedureType, out var commandName))
                throw new Exception("Yous should provide a value for all DbCommandName");

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
            if (!triggersNames.TryGetValue(objectType, out var commandName))
                throw new Exception("Yous should provide a value for all DbCommandName");

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
            this.dbMetadata = new MySqlDbMetadata();
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

            var scopeNameWithoutDefaultScope = ScopeName == SyncOptions.DefaultScopeName ? "" : $"{ScopeName}_";

            var storedProcedureName = $"{spPref}{tableName.Unquoted().Normalized().ToString()}{spSuf}_";

            var triggerName = $"{trigPref}{tableName.Unquoted().Normalized().ToString()}{trigSuf}_";
            this.AddTriggerName(DbTriggerType.Insert, string.Format(insertTriggerName, triggerName));
            this.AddTriggerName(DbTriggerType.Update, string.Format(updateTriggerName, triggerName));
            this.AddTriggerName(DbTriggerType.Delete, string.Format(deleteTriggerName, triggerName));

            this.AddStoredProcedureName(DbStoredProcedureType.SelectChanges, string.Format(selectChangesProcName, storedProcedureName, scopeNameWithoutDefaultScope));
            this.AddStoredProcedureName(DbStoredProcedureType.SelectChangesWithFilters, string.Format(selectChangesProcNameWithFilters, storedProcedureName, scopeNameWithoutDefaultScope, "{0}_"));

            this.AddStoredProcedureName(DbStoredProcedureType.SelectInitializedChanges, string.Format(initializeChangesProcName, storedProcedureName, scopeNameWithoutDefaultScope));
            this.AddStoredProcedureName(DbStoredProcedureType.SelectInitializedChangesWithFilters, string.Format(initializeChangesProcNameWithFilters, storedProcedureName, scopeNameWithoutDefaultScope, "{0}_"));

            this.AddStoredProcedureName(DbStoredProcedureType.SelectRow, string.Format(selectRowProcName, storedProcedureName, scopeNameWithoutDefaultScope));
            this.AddStoredProcedureName(DbStoredProcedureType.UpdateRow, string.Format(updateProcName, storedProcedureName, scopeNameWithoutDefaultScope));
            this.AddStoredProcedureName(DbStoredProcedureType.DeleteRow, string.Format(deleteProcName, storedProcedureName, scopeNameWithoutDefaultScope));
            this.AddStoredProcedureName(DbStoredProcedureType.DeleteMetadata, string.Format(deleteMetadataProcName, storedProcedureName, scopeNameWithoutDefaultScope));
            this.AddStoredProcedureName(DbStoredProcedureType.Reset, string.Format(resetProcName, storedProcedureName, scopeNameWithoutDefaultScope));

            this.AddCommandName(DbCommandType.DisableConstraints, disableConstraintsText);
            this.AddCommandName(DbCommandType.EnableConstraints, enableConstraintsText);

            this.AddCommandName(DbCommandType.UpdateUntrackedRows, CreateUpdateUntrackedRowsCommand());
            this.AddCommandName(DbCommandType.UpdateMetadata, CreateUpdateMetadataCommand());
            this.AddCommandName(DbCommandType.SelectMetadata, CreateSelectMetadataCommand());
            this.AddCommandName(DbCommandType.Reset, CreateResetCommand());
            this.AddCommandName(DbCommandType.SelectRow, CreateSelectRowCommand());
            this.AddCommandName(DbCommandType.DeleteRow, CreateDeleteCommand());
            this.AddCommandName(DbCommandType.UpdateRow, CreateUpdateCommand());
            this.AddCommandName(DbCommandType.SelectChanges, CreateSelectIncrementalChangesCommand());
            this.AddCommandName(DbCommandType.SelectChangesWithFilters, CreateSelectIncrementalChangesCommand());
            this.AddCommandName(DbCommandType.SelectInitializedChanges, CreateSelectInitializedChangesCommand());
            this.AddCommandName(DbCommandType.SelectInitializedChangesWithFilters, CreateSelectInitializedChangesCommand());
        }

        internal MySqlParameter GetMySqlParameter(SyncColumn column)
        {
#if MARIADB
            var originalProvider = MariaDBSyncProvider.ProviderType;
#elif MYSQL
            var originalProvider = MySqlSyncProvider.ProviderType;
#endif
            var parameterName = ParserName.Parse(column, "`").Unquoted().Normalized().ToString();

            // Get the good SqlDbType (even if we are not from Sql Server def)
            var mySqlDbType = this.TableDescription.OriginalProvider == originalProvider ?
                this.dbMetadata.GetMySqlDbType(column) : this.dbMetadata.GetOwnerDbTypeFromDbType(column);

            var sqlParameter = new MySqlParameter
            {
                ParameterName = $"{MYSQL_PREFIX_PARAMETER}{parameterName}",
                DbType = column.GetDbType(),
                IsNullable = column.AllowDBNull,
                MySqlDbType = mySqlDbType,
                SourceColumn = string.IsNullOrEmpty(column.ExtraProperty1) ? null : column.ExtraProperty1,
            };


            (byte precision, byte scale) = this.dbMetadata.GetCompatibleColumnPrecisionAndScale(column, this.TableDescription.OriginalProvider);

            if ((sqlParameter.DbType == DbType.Decimal || sqlParameter.DbType == DbType.Double
                 || sqlParameter.DbType == DbType.Single || sqlParameter.DbType == DbType.VarNumeric) && precision > 0)
            {
                sqlParameter.Precision = precision;
                if (scale > 0)
                    sqlParameter.Scale = scale;
            }
            else if (column.MaxLength > 0)
            {
                sqlParameter.Size = (int)column.MaxLength;
            }
            else if (sqlParameter.DbType == DbType.Guid)
            {
                sqlParameter.Size = 36;
            }
            else
            {
                sqlParameter.Size = -1;
            }

            return sqlParameter;
        }


        /// <summary>
        /// From a SqlParameter, create the declaration
        /// </summary>
        internal string CreateParameterDeclaration(MySqlParameter param)
        {

            var tmpColumn = new SyncColumn(param.ParameterName)
            {
                OriginalDbType = param.MySqlDbType.ToString(),
                OriginalTypeName = param.MySqlDbType.ToString().ToLowerInvariant(),
                MaxLength = param.Size,
                Precision = param.Precision,
                Scale = param.Scale,
                DbType = (int)param.DbType,
                ExtraProperty1 = string.IsNullOrEmpty(param.SourceColumn) ? null : param.SourceColumn
            };

            var stringBuilder3 = new StringBuilder();
            string columnDeclarationString = this.dbMetadata.GetCompatibleColumnTypeDeclarationString(tmpColumn, this.TableDescription.OriginalProvider);

            string output = string.Empty;
            string isNull = string.Empty;
            string defaultValue = string.Empty;

            if (param.Direction == ParameterDirection.Output || param.Direction == ParameterDirection.InputOutput)
                output = "OUT ";

            var parameterName = ParserName.Parse(param.ParameterName, "`").Quoted().ToString();

            stringBuilder3.Append($"{output}{parameterName} {columnDeclarationString} {isNull} {defaultValue}");

            return stringBuilder3.ToString();

        }


        public string CreateSelectMetadataCommand()
        {
            var pkeySelect = new StringBuilder();
            var pkeyValues = new StringBuilder();

            string argComma = string.Empty;
            string argAnd = string.Empty;
            foreach (var mutableColumn in this.TableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn, "`").Quoted().ToString();
                var unquotedColumnName = ParserName.Parse(mutableColumn, "`").Unquoted().ToString();
                pkeySelect.Append($"{argComma}{columnName}");
                pkeyValues.Append($"{argAnd} {columnName} = @{unquotedColumnName}");

                argComma = ",";
                argAnd = " AND ";
            }

            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.AppendLine($"\tSELECT {pkeySelect}, `update_scope_id`, `timestamp`, `sync_row_is_tombstone`");
            stringBuilder.AppendLine($"\tFROM {trackingName.Quoted().ToString()} ");
            stringBuilder.AppendLine($"\tWHERE {pkeyValues};");

            var commandText = stringBuilder.ToString();
            return commandText;
        }
        public string CreateUpdateMetadataCommand()
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

        public string CreateUpdateUntrackedRowsCommand()
        {
            var stringBuilder = new StringBuilder();
            var str1 = new StringBuilder();
            var str2 = new StringBuilder();
            var str3 = new StringBuilder();
            var str4 = MySqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.GetPrimaryKeysColumns(), "`side`", "`base`");

            stringBuilder.AppendLine($"INSERT INTO {trackingName.Quoted().ToString()} (");


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
            stringBuilder.AppendLine($"FROM {tableName.Quoted().ToString()} as `base` WHERE NOT EXISTS");
            stringBuilder.Append($"(SELECT ");
            stringBuilder.Append(str3.ToString());
            stringBuilder.AppendLine($" FROM {trackingName.Quoted().ToString()} as `side` ");
            stringBuilder.AppendLine($"WHERE {str4})");

            var r = stringBuilder.ToString();

            return r;

        }

        public string CreateResetCommand()
        {

           
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"DELETE FROM {tableName.Quoted().ToString()};");
            stringBuilder.AppendLine($"DELETE FROM {trackingName.Quoted().ToString()};");
            stringBuilder.AppendLine();

           return stringBuilder.ToString();

        }

        public string CreateSelectRowCommand()
        {

            StringBuilder stringBuilder = new StringBuilder("SELECT ");
            stringBuilder.AppendLine();
            StringBuilder stringBuilder1 = new StringBuilder();
            string empty = string.Empty;
            foreach (var pkColumn in this.TableDescription.PrimaryKeys)
            {
                var columnName = ParserName.Parse(pkColumn, "`").Quoted().ToString();
                var parameterName = ParserName.Parse(pkColumn, "`").Unquoted().Normalized().ToString();

                stringBuilder1.Append($"{empty}`side`.{columnName} = @{parameterName}");
                empty = " AND ";
            }
            foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false, true))
            {
                var nonPkColumnName = ParserName.Parse(mutableColumn, "`").Quoted().ToString();

                var isPrimaryKey = this.TableDescription.PrimaryKeys.Any(pkey => mutableColumn.ColumnName.Equals(pkey, SyncGlobalization.DataSourceStringComparison));

                if (isPrimaryKey)
                    stringBuilder.AppendLine($"\t`side`.{nonPkColumnName}, ");
                else
                    stringBuilder.AppendLine($"\t`base`.{nonPkColumnName}, ");
            }

            stringBuilder.AppendLine("\t`side`.`sync_row_is_tombstone`, ");
            stringBuilder.AppendLine("\t`side`.`update_scope_id` as `sync_update_scope_id`");
            stringBuilder.AppendLine($"FROM {tableName.Quoted()} `base`");
            stringBuilder.AppendLine($"RIGHT JOIN {trackingName.Quoted()} `side` ON");

            string str = string.Empty;
            foreach (var pkColumn in this.TableDescription.PrimaryKeys)
            {
                var columnName = ParserName.Parse(pkColumn, "`").Quoted().ToString();

                stringBuilder.Append($"{str}`base`.{columnName} = `side`.{columnName}");
                str = " AND ";
            }
            stringBuilder.AppendLine();
            stringBuilder.Append("WHERE ");
            stringBuilder.Append(stringBuilder1.ToString());
            stringBuilder.Append(";");
            return stringBuilder.ToString(); 
        }

        public string CreateDeleteCommand()
        {

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("DECLARE ts BIGINT;");
            stringBuilder.AppendLine("DECLARE t_update_scope_id VARCHAR(36);");
            stringBuilder.AppendLine($"SELECT `timestamp`, `update_scope_id` FROM {trackingName.Quoted()} WHERE {MySqlManagementUtils.WhereColumnAndParameters(this.TableDescription.GetPrimaryKeysColumns(), trackingName.Quoted().ToString(), "@")} LIMIT 1 INTO ts, t_update_scope_id;");
            stringBuilder.AppendLine($"DELETE FROM {tableName.Quoted()} WHERE");
            stringBuilder.AppendLine(MySqlManagementUtils.WhereColumnAndParameters(this.TableDescription.GetPrimaryKeysColumns(), ""));
            stringBuilder.AppendLine("AND (ts <= @sync_min_timestamp OR ts IS NULL OR t_update_scope_id = @sync_scope_id OR @sync_force_write = 1);");

            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"SELECT ROW_COUNT() INTO @sync_row_count;"); //[AB] LIMIT 1 removed to be compatible with MariaDB 10.3.x
            stringBuilder.AppendLine();

            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"IF (@sync_row_count > 0) THEN");
            stringBuilder.AppendLine($"\tUPDATE {trackingName.Quoted()}");
            stringBuilder.AppendLine($"\tSET `update_scope_id` = @sync_scope_id, ");
            stringBuilder.AppendLine($"\t\t `sync_row_is_tombstone` = 1, ");
            stringBuilder.AppendLine($"\t\t `timestamp` = {TimestampValue}, ");
            stringBuilder.AppendLine($"\t\t `last_change_datetime` = now() ");
            stringBuilder.AppendLine($"\tWHERE {MySqlManagementUtils.WhereColumnAndParameters(this.TableDescription.GetPrimaryKeysColumns(), "", "@")};");
            stringBuilder.AppendLine($"END IF;");

            return stringBuilder.ToString();
        }

        public string CreateUpdateCommand()
        {
            var stringBuilder = new StringBuilder();

            var listColumnsTmp = new StringBuilder();
            var listColumnsTmp2 = new StringBuilder();
            var listColumnsTmp3 = new StringBuilder();

            var hasMutableColumns = this.TableDescription.GetMutableColumns(false).Any();

            var and = "";
            foreach (var column in this.TableDescription.GetPrimaryKeysColumns())
            {
                var param = GetMySqlParameter(column);
                param.ParameterName = $"t_{param.ParameterName}";
                var declar = CreateParameterDeclaration(param);
                var columnNameQuoted = ParserName.Parse(column, "`").Quoted().ToString();

                var parameterNameQuoted = ParserName.Parse(param.ParameterName, "`").Quoted().ToString();

                // Primary keys column name, with quote
                listColumnsTmp.Append($"{columnNameQuoted}, ");

                // param name without type
                listColumnsTmp2.Append($"{parameterNameQuoted}, ");

                // param name with type
                stringBuilder.AppendLine($"DECLARE {declar};");

                // Param equal IS NULL
                listColumnsTmp3.Append($"{and}{parameterNameQuoted} IS NULL");

                and = " AND ";

            }

            stringBuilder.AppendLine("DECLARE ts BIGINT;");
            stringBuilder.AppendLine("DECLARE t_update_scope_id VARCHAR(36);");
            stringBuilder.AppendLine($"SELECT {listColumnsTmp}`timestamp`, `update_scope_id` FROM {trackingName.Quoted()} ");
            stringBuilder.AppendLine($"WHERE {MySqlManagementUtils.WhereColumnAndParameters(this.TableDescription.GetPrimaryKeysColumns(), trackingName.Quoted().ToString(), "@")} LIMIT 1 ");
            stringBuilder.AppendLine($"INTO {listColumnsTmp2} ts, t_update_scope_id;");
            stringBuilder.AppendLine();

            if (hasMutableColumns)
            {
                stringBuilder.AppendLine($"UPDATE {tableName.Quoted()}");
                stringBuilder.Append($"SET {MySqlManagementUtils.CommaSeparatedUpdateFromParameters(this.TableDescription, "", "@")}");
                stringBuilder.Append($"WHERE {MySqlManagementUtils.WhereColumnAndParameters(this.TableDescription.GetPrimaryKeysColumns(), "", "@")}");
                stringBuilder.AppendLine($" AND (ts <= @sync_min_timestamp OR ts IS NULL OR t_update_scope_id  = @sync_scope_id OR @sync_force_write = 1);");
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"SELECT ROW_COUNT() INTO @sync_row_count;"); //[AB] LIMIT 1 removed to be compatible with MariaDB 10.3.x
                stringBuilder.AppendLine($"IF (@sync_row_count = 0) THEN");

            }

            string empty = string.Empty;
            var stringBuilderArguments = new StringBuilder();
            var stringBuilderParameters = new StringBuilder();
            foreach (var mutableColumn in this.TableDescription.Columns.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn, "`").Quoted().ToString();
                var parameterName = ParserName.Parse(mutableColumn, "`").Quoted().ToString();

                var paramQuotedColumn = ParserName.Parse($"@{mutableColumn.ColumnName}", "`");

                stringBuilderArguments.Append(string.Concat(empty, columnName));
                stringBuilderParameters.Append(string.Concat(empty, paramQuotedColumn.Quoted().Normalized().ToString()));
                empty = ", ";
            }

            // If we don't have any mutable column, we can't update, and the Insert
            // will fail if we don't ignore the insert (on Reinitialize for example)
            var ignoreKeyWord = hasMutableColumns ? "" : "IGNORE";
            stringBuilder.AppendLine($"\tINSERT {ignoreKeyWord} INTO {tableName.Quoted()}");
            stringBuilder.AppendLine($"\t({stringBuilderArguments})");
            stringBuilder.AppendLine($"\tSELECT * FROM ( SELECT {stringBuilderParameters}) as TMP ");
            stringBuilder.AppendLine($"\tWHERE ( {listColumnsTmp3} )");
            stringBuilder.AppendLine($"\tOR (ts <= @sync_min_timestamp OR ts IS NULL OR t_update_scope_id = @sync_scope_id OR @sync_force_write = 1)");
            stringBuilder.AppendLine($"\tLIMIT 1;");
            stringBuilder.AppendLine($"");
            stringBuilder.AppendLine($"SELECT ROW_COUNT() INTO @sync_row_count;"); // [AB] LIMIT 1 removed to be compatible with MariaDB 10.3.x
            stringBuilder.AppendLine($"");

            if (hasMutableColumns)
                stringBuilder.AppendLine("END IF;");

            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"IF (@sync_row_count > 0) THEN");
            stringBuilder.AppendLine($"\tUPDATE {trackingName.Quoted()}");
            stringBuilder.AppendLine($"\tSET `update_scope_id` = @sync_scope_id, ");
            stringBuilder.AppendLine($"\t\t `sync_row_is_tombstone` = 0, ");
            stringBuilder.AppendLine($"\t\t `timestamp` = {TimestampValue}, ");
            stringBuilder.AppendLine($"\t\t `last_change_datetime` = now() ");
            stringBuilder.AppendLine($"\tWHERE {MySqlManagementUtils.WhereColumnAndParameters(this.TableDescription.GetPrimaryKeysColumns(), "", "@")};");
            stringBuilder.AppendLine($"END IF;");

            return stringBuilder.ToString();
        }

    }
}
