using Dotmim.Sync.Builders;
using Dotmim.Sync.DatabaseStringParsers;

#if NET6_0 || NET8_0
using MySqlConnector;
#elif NETSTANDARD
using MySql.Data.MySqlClient;
#endif
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

        /// <summary>
        /// Gets the prefix parameter for MySql.
        /// </summary>
        public const string MYSQLPREFIXPARAMETER = "in_";

        /// <summary>
        /// Gets the timestamp value to use for a rowversion column.
        /// </summary>
        public const string TimestampValue = "ROUND(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(6)) * 10000)";

        /// <summary>
        /// Gets the left quote character.
        /// </summary>
        public const char LeftQuote = '`';

        /// <summary>
        /// Gets the right quote character.
        /// </summary>
        public const char RightQuote = '`';

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

        /// <summary>
        /// Gets the table description.
        /// </summary>
        public SyncTable TableDescription { get; }

        /// <summary>
        /// Gets the scope info.
        /// </summary>
        public ScopeInfo ScopeInfo { get; }

        /// <summary>
        /// Gets the parsed tracking table name, wihtout any quotes characters.
        /// </summary>
        public string TrackingTableName { get; private set; }

        /// <summary>
        /// Gets the parsed normalized tracking table full name.
        /// </summary>
        public string TrackingTableNormalizedFullName { get; private set; }

        /// <summary>
        /// Gets the parsed normalized tracking table short name.
        /// </summary>
        public string TrackingTableNormalizedShortName { get; private set; }

        /// <summary>
        /// Gets the parsed quoted tracking table full name.
        /// </summary>
        public string TrackingTableQuotedFullName { get; private set; }

        /// <summary>
        /// Gets the parsed quoted tracking table short name.
        /// </summary>
        public string TrackingTableQuotedShortName { get; private set; }

        /// <summary>
        /// Gets the parsed tracking table schema name. if empty, "public" is returned.
        /// </summary>
        public string TrackingTableSchemaName { get; private set; }

        /// <summary>
        /// Gets the parsed table name, without any quotes characters.
        /// </summary>
        public string TableName { get; private set; }

        /// <summary>
        /// Gets the parsed normalized table full name (with schema, if any).
        /// </summary>
        public string TableNormalizedFullName { get; private set; }

        /// <summary>
        /// Gets the parsed normalized table short name (without schema, if any).
        /// </summary>
        public string TableNormalizedShortName { get; private set; }

        /// <summary>
        /// Gets the parsed quoted table full name (with schema, if any).
        /// </summary>
        public string TableQuotedFullName { get; private set; }

        /// <summary>
        /// Gets the parsed quoted table short name (without schema, if any).
        /// </summary>
        public string TableQuotedShortName { get; private set; }

        /// <summary>
        /// Gets the parsed table schema name. if empty, "public" is returned.
        /// </summary>
        public string TableSchemaName { get; private set; }

        /// <inheritdoc cref="MySqlObjectNames"/>
        public MySqlObjectNames(SyncTable tableDescription, ScopeInfo scopeInfo)
        {
            this.TableDescription = tableDescription;
            this.ScopeInfo = scopeInfo;

            //-------------------------------------------------
            // set table names
            var tableParser = new TableParser(this.TableDescription.GetFullName(), LeftQuote, RightQuote);

            this.TableName = tableParser.TableName;
            this.TableNormalizedFullName = tableParser.NormalizedFullName;
            this.TableNormalizedShortName = tableParser.NormalizedShortName;
            this.TableQuotedFullName = tableParser.QuotedFullName;
            this.TableQuotedShortName = tableParser.QuotedShortName;
            this.TableSchemaName = tableParser.SchemaName;

            //-------------------------------------------------
            // define tracking table name with prefix and suffix.
            // if no pref / suf, use default value
            var trakingTableNameString = string.IsNullOrEmpty(this.ScopeInfo.Setup?.TrackingTablesPrefix) && string.IsNullOrEmpty(this.ScopeInfo.Setup?.TrackingTablesSuffix)
                ? $"{this.TableDescription.TableName}_tracking"
                : $"{this.ScopeInfo.Setup?.TrackingTablesPrefix}{this.TableDescription.TableName}{this.ScopeInfo.Setup?.TrackingTablesSuffix}";

            if (!string.IsNullOrEmpty(this.TableDescription.SchemaName))
                trakingTableNameString = $"{this.TableDescription.SchemaName}.{trakingTableNameString}";

            // Parse
            var trackingTableParser = new TableParser(trakingTableNameString, LeftQuote, RightQuote);

            // set the tracking table names
            this.TrackingTableName = trackingTableParser.TableName;
            this.TrackingTableNormalizedFullName = trackingTableParser.NormalizedFullName;
            this.TrackingTableNormalizedShortName = trackingTableParser.NormalizedShortName;
            this.TrackingTableQuotedFullName = trackingTableParser.QuotedFullName;
            this.TrackingTableQuotedShortName = trackingTableParser.QuotedShortName;
            this.TrackingTableSchemaName = trackingTableParser.SchemaName;
        }

        /// <summary>
        /// Returns the stored procedure name for the given stored procedure type.
        /// </summary>
        public string GetStoredProcedureCommandName(DbStoredProcedureType storedProcedureType, SyncFilter filter = null)
        {
            var scopeNameWithoutDefaultScope = this.ScopeInfo.Name == SyncOptions.DefaultScopeName ? string.Empty : $"{this.ScopeInfo.Name}_";
            var storedProcedureNormalizedName = $"{this.ScopeInfo.Setup?.StoredProceduresPrefix}{this.TableNormalizedFullName}{this.ScopeInfo.Setup?.StoredProceduresSuffix}_";

            return storedProcedureType switch
            {
                DbStoredProcedureType.SelectChanges => string.Format(SelectChangesProcName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope),
                DbStoredProcedureType.SelectChangesWithFilters => string.Format(SelectChangesProcNameWithFilters, storedProcedureNormalizedName, scopeNameWithoutDefaultScope, filter.GetFilterName()),
                DbStoredProcedureType.SelectInitializedChanges => string.Format(InitializeChangesProcName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope),
                DbStoredProcedureType.SelectInitializedChangesWithFilters => string.Format(InitializeChangesProcNameWithFilters, storedProcedureNormalizedName, scopeNameWithoutDefaultScope, filter.GetFilterName()),
                DbStoredProcedureType.SelectRow => string.Format(SelectRowProcName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope),
                DbStoredProcedureType.UpdateRow => string.Format(UpdateProcName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope),
                DbStoredProcedureType.DeleteRow => string.Format(DeleteProcName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope),
                _ => null,
            };
        }

        /// <summary>
        /// Returns the trigger name for the given trigger type.
        /// </summary>
        public string GetTriggerCommandName(DbTriggerType objectType, SyncFilter filter = null)
        {
            var triggerNormalizedName = $"{this.ScopeInfo.Setup?.TriggersPrefix}{this.TableNormalizedFullName}{this.ScopeInfo.Setup?.TriggersSuffix}_";

            return objectType switch
            {
                DbTriggerType.Update => string.Format(UpdateTriggerName, triggerNormalizedName),
                DbTriggerType.Insert => string.Format(InsertTriggerName, triggerNormalizedName),
                DbTriggerType.Delete => string.Format(DeleteTriggerName, triggerNormalizedName),
                _ => null,
            };
        }

        /// <summary>
        /// Get a command string from the command type.
        /// </summary>
        public string GetCommandName(DbCommandType commandType, SyncFilter filter = null)
        {
            var scopeNameWithoutDefaultScope = this.ScopeInfo.Name == SyncOptions.DefaultScopeName ? string.Empty : $"{this.ScopeInfo.Name}_";

            //-------------------------------------------------
            // Stored procedures & Triggers
            var storedProcedureNormalizedName = $"{this.ScopeInfo.Setup?.StoredProceduresPrefix}{this.TableNormalizedFullName}{this.ScopeInfo.Setup?.StoredProceduresSuffix}_";
            var triggerNormalizedName = $"{this.ScopeInfo.Setup?.TriggersPrefix}{this.TableNormalizedFullName}{this.ScopeInfo.Setup?.TriggersSuffix}_";

            return commandType switch
            {
                DbCommandType.SelectChanges => string.Format(SelectChangesProcName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope),
                DbCommandType.SelectInitializedChanges => string.Format(InitializeChangesProcName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope),
                DbCommandType.SelectInitializedChangesWithFilters => string.Format(InitializeChangesProcNameWithFilters, storedProcedureNormalizedName, scopeNameWithoutDefaultScope, filter.GetFilterName()),
                DbCommandType.SelectChangesWithFilters => string.Format(SelectChangesProcNameWithFilters, storedProcedureNormalizedName, scopeNameWithoutDefaultScope, filter.GetFilterName()),
                DbCommandType.SelectRow => this.CreateSelectRowCommand(),
                DbCommandType.UpdateRow or DbCommandType.InsertRow => string.Format(UpdateProcName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope),
                DbCommandType.DeleteRow => string.Format(DeleteProcName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope),
                DbCommandType.DisableConstraints => DisableConstraintsText,
                DbCommandType.EnableConstraints => EnableConstraintsText,
                DbCommandType.DeleteMetadata => string.Format(DeleteMetadataText, this.TrackingTableQuotedFullName),
                DbCommandType.UpdateMetadata => this.CreateUpdateMetadataCommand(),
                DbCommandType.SelectMetadata => this.CreateSelectMetadataCommand(),
                DbCommandType.InsertTrigger => string.Format(InsertTriggerName, triggerNormalizedName),
                DbCommandType.UpdateTrigger => string.Format(UpdateTriggerName, triggerNormalizedName),
                DbCommandType.DeleteTrigger => string.Format(DeleteTriggerName, triggerNormalizedName),
                DbCommandType.UpdateRows or DbCommandType.InsertRows => string.Format(UpdateProcName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope),
                DbCommandType.DeleteRows => string.Format(DeleteProcName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope),
                DbCommandType.UpdateUntrackedRows => this.CreateUpdateUntrackedRowsCommand(),
                DbCommandType.Reset => this.CreateResetCommand(),
                _ => null,
            };
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
                this.MySqlDbMetadata.GetMySqlDbType(column) : this.dbMetadata.GetOwnerDbTypeFromDbType(column);

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
            else
            {
                sqlParameter.Size = column.MaxLength > 0 ? column.MaxLength : sqlParameter.DbType == DbType.Guid ? 36 : -1;
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