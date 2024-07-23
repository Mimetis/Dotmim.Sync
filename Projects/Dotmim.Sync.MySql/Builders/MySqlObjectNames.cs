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
    /// <summary>
    /// My SQL Object Names.
    /// </summary>
    public partial class MySqlObjectNames
    {
        public const string MYSQLPREFIXPARAMETER = "in_";

        public const string TimestampValue = "ROUND(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(6)) * 10000)";

        internal const string InsertTriggerName = "`{0}insert_trigger`";
        internal const string UpdateTriggerName = "`{0}update_trigger`";
        internal const string DeleteTriggerName = "`{0}delete_trigger`";

        internal const string SelectChangesProcName = "`{0}{1}changes`";
        internal const string SelectChangesProcNameWithFilters = "`{0}{1}{2}changes`";

        internal const string InitializeChangesProcName = "`{0}{1}initialize`";
        internal const string InitializeChangesProcNameWithFilters = "`{0}{1}{2}initialize`";

        internal const string SelectRowProcName = "`{0}{1}selectrow`";

        internal const string InsertProcName = "`{0}{1}insert`";
        internal const string UpdateProcName = "`{0}{1}update`";
        internal const string DeleteProcName = "`{0}{1}delete`";

        internal const string ResetProcName = "`{0}{1}reset`";

        internal const string InsertMetadataProcName = "`{0}{1}insertmetadata`";
        internal const string UpdateMetadataProcName = "`{0}{1}updatemetadata`";
        internal const string DeleteMetadataText = "DELETE FROM {0} WHERE `timestamp` <= @sync_row_timestamp;";

        internal const string DisableConstraintsText = "SET FOREIGN_KEY_CHECKS=0;";
        internal const string EnableConstraintsText = "SET FOREIGN_KEY_CHECKS=1;";

        private Dictionary<DbStoredProcedureType, string> storedProceduresNames = [];
        private Dictionary<DbTriggerType, string> triggersNames = [];
        private Dictionary<DbCommandType, string> commandNames = [];

        private ParserName tableName;
        private ParserName trackingName;
        private MySqlDbMetadata dbMetadata;

        public SyncTable TableDescription { get; }

        public SyncSetup Setup { get; }

        public string ScopeName { get; }

        public void AddCommandName(DbCommandType objectType, string name)
        {
            if (this.commandNames.ContainsKey(objectType))
                throw new Exception("Yous can't add an objectType multiple times");

            this.commandNames.Add(objectType, name);
        }

        public string GetCommandName(DbCommandType objectType, SyncFilter filter = null)
        {
            if (!this.commandNames.TryGetValue(objectType, out var commandName))
                throw new NotSupportedException($"MySql provider does not support the command type {objectType}");

            // concat filter name
            if (filter != null)
                commandName = string.Format(commandName, filter.GetFilterName());

            return commandName;
        }

        public void AddStoredProcedureName(DbStoredProcedureType objectType, string name)
        {
            if (this.storedProceduresNames.ContainsKey(objectType))
                throw new Exception("Yous can't add an objectType multiple times");

            this.storedProceduresNames.Add(objectType, name);
        }

        public string GetStoredProcedureCommandName(DbStoredProcedureType storedProcedureType, SyncFilter filter = null)
        {
            if (!this.storedProceduresNames.TryGetValue(storedProcedureType, out var commandName))
                throw new Exception("Yous should provide a value for all DbCommandName");

            // concat filter name
            if (filter != null && (storedProcedureType == DbStoredProcedureType.SelectChangesWithFilters || storedProcedureType == DbStoredProcedureType.SelectInitializedChangesWithFilters))
                commandName = string.Format(commandName, filter.GetFilterName());

            return commandName;
        }

        public void AddTriggerName(DbTriggerType objectType, string name)
        {
            if (this.triggersNames.ContainsKey(objectType))
                throw new Exception("Yous can't add an objectType multiple times");

            this.triggersNames.Add(objectType, name);
        }

        public string GetTriggerCommandName(DbTriggerType objectType, SyncFilter filter = null)
        {
            if (!this.triggersNames.TryGetValue(objectType, out var commandName))
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
            this.SetDefaultNames();
        }

        /// <summary>
        /// Set the default stored procedures names.
        /// </summary>
        private void SetDefaultNames()
        {
            var spPref = this.Setup.StoredProceduresPrefix != null ? this.Setup.StoredProceduresPrefix : string.Empty;
            var spSuf = this.Setup.StoredProceduresSuffix != null ? this.Setup.StoredProceduresSuffix : string.Empty;
            var trigPref = this.Setup.TriggersPrefix != null ? this.Setup.TriggersPrefix : string.Empty;
            var trigSuf = this.Setup.TriggersSuffix != null ? this.Setup.TriggersSuffix : string.Empty;

            var scopeNameWithoutDefaultScope = this.ScopeName == SyncOptions.DefaultScopeName ? string.Empty : $"{this.ScopeName}_";

            var storedProcedureName = $"{spPref}{this.tableName.Unquoted().Normalized()}{spSuf}_";

            var triggerName = $"{trigPref}{this.tableName.Unquoted().Normalized()}{trigSuf}_";
            this.AddTriggerName(DbTriggerType.Insert, string.Format(InsertTriggerName, triggerName));
            this.AddTriggerName(DbTriggerType.Update, string.Format(UpdateTriggerName, triggerName));
            this.AddTriggerName(DbTriggerType.Delete, string.Format(DeleteTriggerName, triggerName));

            this.AddStoredProcedureName(DbStoredProcedureType.SelectChanges, string.Format(SelectChangesProcName, storedProcedureName, scopeNameWithoutDefaultScope));
            this.AddStoredProcedureName(DbStoredProcedureType.SelectChangesWithFilters, string.Format(SelectChangesProcNameWithFilters, storedProcedureName, scopeNameWithoutDefaultScope, "{0}_"));

            this.AddStoredProcedureName(DbStoredProcedureType.SelectInitializedChanges, string.Format(InitializeChangesProcName, storedProcedureName, scopeNameWithoutDefaultScope));
            this.AddStoredProcedureName(DbStoredProcedureType.SelectInitializedChangesWithFilters, string.Format(InitializeChangesProcNameWithFilters, storedProcedureName, scopeNameWithoutDefaultScope, "{0}_"));

            this.AddStoredProcedureName(DbStoredProcedureType.SelectRow, string.Format(SelectRowProcName, storedProcedureName, scopeNameWithoutDefaultScope));
            this.AddStoredProcedureName(DbStoredProcedureType.UpdateRow, string.Format(UpdateProcName, storedProcedureName, scopeNameWithoutDefaultScope));
            this.AddStoredProcedureName(DbStoredProcedureType.DeleteRow, string.Format(DeleteProcName, storedProcedureName, scopeNameWithoutDefaultScope));
            this.AddCommandName(DbCommandType.DeleteMetadata, string.Format(DeleteMetadataText, this.trackingName.Quoted().ToString()));
            this.AddStoredProcedureName(DbStoredProcedureType.Reset, string.Format(ResetProcName, storedProcedureName, scopeNameWithoutDefaultScope));

            this.AddCommandName(DbCommandType.DisableConstraints, DisableConstraintsText);
            this.AddCommandName(DbCommandType.EnableConstraints, EnableConstraintsText);

            this.AddCommandName(DbCommandType.UpdateUntrackedRows, this.CreateUpdateUntrackedRowsCommand());
            this.AddCommandName(DbCommandType.UpdateMetadata, this.CreateUpdateMetadataCommand());
            this.AddCommandName(DbCommandType.SelectMetadata, this.CreateSelectMetadataCommand());
            this.AddCommandName(DbCommandType.Reset, this.CreateResetCommand());
            this.AddCommandName(DbCommandType.SelectRow, this.CreateSelectRowCommand());
            this.AddCommandName(DbCommandType.DeleteRow, this.CreateDeleteCommand());
            this.AddCommandName(DbCommandType.UpdateRow, this.CreateUpdateCommand());
            this.AddCommandName(DbCommandType.SelectChanges, this.CreateSelectIncrementalChangesCommand());
            this.AddCommandName(DbCommandType.SelectChangesWithFilters, this.CreateSelectIncrementalChangesCommand());
            this.AddCommandName(DbCommandType.SelectInitializedChanges, this.CreateSelectInitializedChangesCommand());
            this.AddCommandName(DbCommandType.SelectInitializedChangesWithFilters, this.CreateSelectInitializedChangesCommand());
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
                ParameterName = $"{MYSQLPREFIXPARAMETER}{parameterName}",
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
                sqlParameter.Size = column.MaxLength;
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
        /// From a SqlParameter, create the declaration.
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
                ExtraProperty1 = string.IsNullOrEmpty(param.SourceColumn) ? null : param.SourceColumn,
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
            stringBuilder.AppendLine($"\tFROM {this.trackingName.Quoted()} ");
            stringBuilder.AppendLine($"\tWHERE {pkeyValues};");

            var commandText = stringBuilder.ToString();
            return commandText;
        }

        public string CreateUpdateMetadataCommand()
        {
            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.AppendLine($"\tINSERT INTO {this.trackingName.Quoted()} (");

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

            stringBuilder.Append(pkeySelect);
            stringBuilder.AppendLine("\t\t,`update_scope_id`");
            stringBuilder.AppendLine("\t\t,`timestamp`");
            stringBuilder.AppendLine("\t\t,`sync_row_is_tombstone`");
            stringBuilder.AppendLine("\t\t,`last_change_datetime`");

            var filterColumnsString = new StringBuilder();
            var filterColumnsString2 = new StringBuilder();
            var filterColumnsString3 = new StringBuilder();

            stringBuilder.AppendLine("\t) ");
            stringBuilder.AppendLine("\tVALUES (");
            stringBuilder.Append(pkeyValues);
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

            stringBuilder.AppendLine($"INSERT INTO {this.trackingName.Quoted()} (");

            var comma = string.Empty;
            foreach (var pkeyColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var pkeyColumnName = ParserName.Parse(pkeyColumn, "`").Quoted().ToString();

                str1.Append($"{comma}{pkeyColumnName}");
                str2.Append($"{comma}`base`.{pkeyColumnName}");
                str3.Append($"{comma}`side`.{pkeyColumnName}");

                comma = ", ";
            }

            stringBuilder.Append(str1);
            stringBuilder.AppendLine($", `update_scope_id`, `sync_row_is_tombstone`, `timestamp`, `last_change_datetime`");
            stringBuilder.AppendLine($")");
            stringBuilder.Append($"SELECT ");
            stringBuilder.Append(str2);
            stringBuilder.AppendLine($", NULL, 0, {TimestampValue}, now()");
            stringBuilder.AppendLine($"FROM {this.tableName.Quoted()} as `base` WHERE NOT EXISTS");
            stringBuilder.Append($"(SELECT ");
            stringBuilder.Append(str3);
            stringBuilder.AppendLine($" FROM {this.trackingName.Quoted()} as `side` ");
            stringBuilder.AppendLine($"WHERE {str4})");

            var r = stringBuilder.ToString();

            return r;
        }

        public string CreateResetCommand()
        {

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"DELETE FROM {this.tableName.Quoted()};");
            stringBuilder.AppendLine($"DELETE FROM {this.trackingName.Quoted()};");
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
            stringBuilder.AppendLine($"FROM {this.tableName.Quoted()} `base`");
            stringBuilder.AppendLine($"RIGHT JOIN {this.trackingName.Quoted()} `side` ON");

            string str = string.Empty;
            foreach (var pkColumn in this.TableDescription.PrimaryKeys)
            {
                var columnName = ParserName.Parse(pkColumn, "`").Quoted().ToString();

                stringBuilder.Append($"{str}`base`.{columnName} = `side`.{columnName}");
                str = " AND ";
            }

            stringBuilder.AppendLine();
            stringBuilder.Append("WHERE ");
            stringBuilder.Append(stringBuilder1);
            stringBuilder.Append(";");
            return stringBuilder.ToString();
        }

        public string CreateDeleteCommand()
        {

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("DECLARE ts BIGINT;");
            stringBuilder.AppendLine("DECLARE t_update_scope_id VARCHAR(36);");
            stringBuilder.AppendLine($"SELECT `timestamp`, `update_scope_id` FROM {this.trackingName.Quoted()} WHERE {MySqlManagementUtils.WhereColumnAndParameters(this.TableDescription.GetPrimaryKeysColumns(), this.trackingName.Quoted().ToString(), "@")} LIMIT 1 INTO ts, t_update_scope_id;");
            stringBuilder.AppendLine($"DELETE FROM {this.tableName.Quoted()} WHERE");
            stringBuilder.AppendLine(MySqlManagementUtils.WhereColumnAndParameters(this.TableDescription.GetPrimaryKeysColumns(), string.Empty));
            stringBuilder.AppendLine("AND (ts <= @sync_min_timestamp OR ts IS NULL OR t_update_scope_id = @sync_scope_id OR @sync_force_write = 1);");

            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"SELECT ROW_COUNT() INTO @sync_row_count;"); // [AB] LIMIT 1 removed to be compatible with MariaDB 10.3.x
            stringBuilder.AppendLine();

            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"IF (@sync_row_count > 0) THEN");
            stringBuilder.AppendLine($"\tUPDATE {this.trackingName.Quoted()}");
            stringBuilder.AppendLine($"\tSET `update_scope_id` = @sync_scope_id, ");
            stringBuilder.AppendLine($"\t\t `sync_row_is_tombstone` = 1, ");
            stringBuilder.AppendLine($"\t\t `timestamp` = {TimestampValue}, ");
            stringBuilder.AppendLine($"\t\t `last_change_datetime` = now() ");
            stringBuilder.AppendLine($"\tWHERE {MySqlManagementUtils.WhereColumnAndParameters(this.TableDescription.GetPrimaryKeysColumns(), string.Empty, "@")};");
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

            var and = string.Empty;
            foreach (var column in this.TableDescription.GetPrimaryKeysColumns())
            {
                var param = this.GetMySqlParameter(column);
                param.ParameterName = $"t_{param.ParameterName}";
                var declar = this.CreateParameterDeclaration(param);
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
            stringBuilder.AppendLine($"SELECT {listColumnsTmp}`timestamp`, `update_scope_id` FROM {this.trackingName.Quoted()} ");
            stringBuilder.AppendLine($"WHERE {MySqlManagementUtils.WhereColumnAndParameters(this.TableDescription.GetPrimaryKeysColumns(), this.trackingName.Quoted().ToString(), "@")} LIMIT 1 ");
            stringBuilder.AppendLine($"INTO {listColumnsTmp2} ts, t_update_scope_id;");
            stringBuilder.AppendLine();

            if (hasMutableColumns)
            {
                stringBuilder.AppendLine($"UPDATE {this.tableName.Quoted()}");
                stringBuilder.Append($"SET {MySqlManagementUtils.CommaSeparatedUpdateFromParameters(this.TableDescription, string.Empty, "@")}");
                stringBuilder.Append($"WHERE {MySqlManagementUtils.WhereColumnAndParameters(this.TableDescription.GetPrimaryKeysColumns(), string.Empty, "@")}");
                stringBuilder.AppendLine($" AND (ts <= @sync_min_timestamp OR ts IS NULL OR t_update_scope_id  = @sync_scope_id OR @sync_force_write = 1);");
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"SELECT ROW_COUNT() INTO @sync_row_count;"); // [AB] LIMIT 1 removed to be compatible with MariaDB 10.3.x
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
            var ignoreKeyWord = hasMutableColumns ? string.Empty : "IGNORE";
            stringBuilder.AppendLine($"\tINSERT {ignoreKeyWord} INTO {this.tableName.Quoted()}");
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
            stringBuilder.AppendLine($"\tUPDATE {this.trackingName.Quoted()}");
            stringBuilder.AppendLine($"\tSET `update_scope_id` = @sync_scope_id, ");
            stringBuilder.AppendLine($"\t\t `sync_row_is_tombstone` = 0, ");
            stringBuilder.AppendLine($"\t\t `timestamp` = {TimestampValue}, ");
            stringBuilder.AppendLine($"\t\t `last_change_datetime` = now() ");
            stringBuilder.AppendLine($"\tWHERE {MySqlManagementUtils.WhereColumnAndParameters(this.TableDescription.GetPrimaryKeysColumns(), string.Empty, "@")};");
            stringBuilder.AppendLine($"END IF;");

            return stringBuilder.ToString();
        }
    }
}