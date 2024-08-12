using Dotmim.Sync.Builders;
using Dotmim.Sync.DatabaseStringParsers;
using System.Data;
using System.Linq;
using System.Text;

namespace Dotmim.Sync.Sqlite
{
    /// <summary>
    /// Sqlite object names.
    /// </summary>
    public class SqliteObjectNames
    {
        /// <summary>
        /// Gets the left quote.
        /// </summary>
        public const char LeftQuote = '[';

        /// <summary>
        /// Gets the right quote.
        /// </summary>
        public const char RightQuote = ']';

        /// <summary>
        /// Gets the timestamp sqlite generation query.
        /// </summary>
        public const string TimestampValue = "replace(strftime('%Y%m%d%H%M%f', 'now'), '.', '')";

        internal const string InsertTriggerName = "[{0}_insert_trigger]";
        internal const string UpdateTriggerName = "[{0}_update_trigger]";
        internal const string DeleteTriggerName = "[{0}_delete_trigger]";

        private bool disableSqlFiltersGeneration;

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
        /// Gets the parsed tracking table schema name. if empty, "dbo" is returned.
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
        /// Gets the parsed table schema name. if empty, "dbo" is returned.
        /// </summary>
        public string TableSchemaName { get; private set; }

        /// <summary>
        /// Get a command string from the command type.
        /// </summary>
        public string GetCommandName(DbCommandType commandType, SyncFilter filter = null)
        {
            var triggerNormalizedName = $"{this.ScopeInfo.Setup?.TriggersPrefix}{this.TableNormalizedFullName}{this.ScopeInfo.Setup?.TriggersSuffix}_";

            return commandType switch
            {
                DbCommandType.SelectChanges or DbCommandType.SelectChangesWithFilters => this.CreateSelectChangesCommandText(filter),
                DbCommandType.SelectRow => this.CreateSelectRowCommandText(),
                DbCommandType.UpdateRow or DbCommandType.UpdateRows => this.CreateUpdateCommandText(),
                DbCommandType.InsertRow or DbCommandType.InsertRows => this.CreateInsertCommandText(),
                DbCommandType.DeleteRow or DbCommandType.DeleteRows => this.CreateDeleteCommandText(),
                DbCommandType.DeleteMetadata => this.CreateDeleteMetadataCommandText(),
                DbCommandType.UpdateMetadata => this.CreateUpdateMetadataCommandText(),
                DbCommandType.SelectMetadata => this.CreateSelectMetadataCommandText(),
                DbCommandType.InsertTrigger => string.Format(InsertTriggerName, triggerNormalizedName),
                DbCommandType.UpdateTrigger => string.Format(UpdateTriggerName, triggerNormalizedName),
                DbCommandType.DeleteTrigger => string.Format(DeleteTriggerName, triggerNormalizedName),
                DbCommandType.UpdateUntrackedRows => this.CreateUpdateUntrackedRowsCommandText(),
                DbCommandType.Reset => this.CreateResetCommandText(),
                DbCommandType.DisableConstraints or DbCommandType.EnableConstraints or DbCommandType.PreDeleteRow
                or DbCommandType.PreDeleteRows or DbCommandType.PreInsertRow or DbCommandType.PreInsertRows
                or DbCommandType.PreUpdateRow or DbCommandType.PreUpdateRows => "Select 0",
                _ => null,
            };
        }

        /// <summary>
        /// Get a trigger command name from the object type.
        /// </summary>
        public string GetTriggerCommandName(DbTriggerType objectType)
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

        /// <inheritdoc cref="SqliteObjectNames" />
        public SqliteObjectNames(SyncTable tableDescription, ScopeInfo scopeInfo, bool disableSqlFiltersGeneration)
        {
            this.TableDescription = tableDescription;
            this.ScopeInfo = scopeInfo;
            this.disableSqlFiltersGeneration = disableSqlFiltersGeneration;

            this.SetDefaultNames();
        }

        /// <summary>
        /// Set the default stored procedures names.
        /// </summary>
        private void SetDefaultNames()
        {

            //-------------------------------------------------
            // set table names
            var tableParser = new TableParser(this.TableDescription.GetFullName(), LeftQuote, RightQuote);

            this.TableName = tableParser.TableName;
            this.TableNormalizedFullName = tableParser.NormalizedFullName;
            this.TableNormalizedShortName = tableParser.NormalizedShortName;
            this.TableQuotedFullName = tableParser.QuotedFullName;
            this.TableQuotedShortName = tableParser.QuotedShortName;
            this.TableSchemaName = string.Empty;

            //-------------------------------------------------
            // define tracking table name with prefix and suffix.
            // if no pref / suf, use default value
            var trakingTableNameString = string.IsNullOrEmpty(this.ScopeInfo.Setup?.TrackingTablesPrefix) && string.IsNullOrEmpty(this.ScopeInfo.Setup?.TrackingTablesSuffix)
                ? $"{this.TableDescription.TableName}_tracking"
                : $"{this.ScopeInfo.Setup?.TrackingTablesPrefix}{this.TableDescription.TableName}{this.ScopeInfo.Setup?.TrackingTablesSuffix}";

            // Parse
            var trackingTableParser = new TableParser(trakingTableNameString, LeftQuote, RightQuote);

            // set the tracking table names
            this.TrackingTableName = trackingTableParser.TableName;
            this.TrackingTableNormalizedFullName = trackingTableParser.NormalizedFullName;
            this.TrackingTableNormalizedShortName = trackingTableParser.NormalizedShortName;
            this.TrackingTableQuotedFullName = trackingTableParser.QuotedFullName;
            this.TrackingTableQuotedShortName = trackingTableParser.QuotedShortName;
            this.TrackingTableSchemaName = string.Empty;
        }

        private string CreateResetCommandText()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"DELETE FROM {this.TableQuotedShortName};");
            stringBuilder.AppendLine($"DELETE FROM {this.TrackingTableQuotedShortName};");

            return stringBuilder.ToString();
        }

        private string CreateSelectMetadataCommandText()
        {
            var stringBuilder = new StringBuilder();
            var pkeysSelect = new StringBuilder();
            var pkeysWhere = new StringBuilder();

            string and = string.Empty;
            string comma = string.Empty;
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(pkColumn.ColumnName, LeftQuote, RightQuote);

                pkeysSelect.Append($"{comma}[side].{columnParser.QuotedShortName}");
                pkeysWhere.Append($"{and}[side].{columnParser.QuotedShortName} = @{columnParser.NormalizedShortName}");

                and = " AND ";
                comma = ", ";
            }

            stringBuilder.AppendLine($"SELECT {pkeysSelect}, [side].[update_scope_id], [side].[timestamp], [side].[sync_row_is_tombstone]");
            stringBuilder.AppendLine($"FROM {this.TrackingTableQuotedShortName} [side]");
            stringBuilder.AppendLine($"WHERE {pkeysWhere}");

            var commandText = stringBuilder.ToString();

            return commandText;
        }

        private string CreateUpdateMetadataCommandText()
        {
            var stringBuilder = new StringBuilder();

            var pkeySelectForInsert = new StringBuilder();
            var pkeyISelectForInsert = new StringBuilder();
            var pkeyAliasSelectForInsert = new StringBuilder();
            var pkeysLeftJoinForInsert = new StringBuilder();
            var pkeysIsNullForInsert = new StringBuilder();

            string and = string.Empty;
            string comma = string.Empty;
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(pkColumn.ColumnName, LeftQuote, RightQuote);

                pkeySelectForInsert.Append($"{comma}{columnParser.QuotedShortName}");
                pkeyISelectForInsert.Append($"{comma}[i].{columnParser.QuotedShortName}");
                pkeyAliasSelectForInsert.Append($"{comma}@{columnParser.NormalizedShortName} as {columnParser.NormalizedShortName}");
                pkeysLeftJoinForInsert.Append($"{and}[side].{columnParser.QuotedShortName} = [i].{columnParser.QuotedShortName}");
                pkeysIsNullForInsert.Append($"{and}[side].{columnParser.QuotedShortName} IS NULL");
                and = " AND ";
                comma = ", ";
            }

            stringBuilder.AppendLine($"INSERT OR REPLACE INTO {this.TrackingTableQuotedShortName} (");
            stringBuilder.AppendLine(pkeySelectForInsert.ToString());
            stringBuilder.AppendLine(",[update_scope_id], [sync_row_is_tombstone], [timestamp], [last_change_datetime] )");
            stringBuilder.AppendLine($"SELECT {pkeyISelectForInsert} ");
            stringBuilder.AppendLine($"   , i.sync_scope_id, i.sync_row_is_tombstone, i.sync_timestamp, i.UtcDate");
            stringBuilder.AppendLine("FROM (");
            stringBuilder.AppendLine($"  SELECT {pkeyAliasSelectForInsert}");
            stringBuilder.AppendLine($"          ,@sync_scope_id as sync_scope_id, @sync_row_is_tombstone as sync_row_is_tombstone, {SqliteObjectNames.TimestampValue} as sync_timestamp, datetime('now') as UtcDate) as i;");

            var cmdtext = stringBuilder.ToString();

            return cmdtext;
        }

        private string CreateInsertCommandText()
        {
            var stringBuilderArguments = new StringBuilder();
            var stringBuilderParameters = new StringBuilder();
            var stringBuilderParametersValues = new StringBuilder();
            var stringBuilderParametersValues2 = new StringBuilder();
            string empty = string.Empty;

            // Generate Update command
            var stringBuilder = new StringBuilder();

            foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false, true))
            {
                var columnParser = new ObjectParser(mutableColumn.ColumnName, LeftQuote, RightQuote);

                stringBuilderParametersValues.Append($"{empty}@{columnParser.NormalizedShortName} as {columnParser.NormalizedShortName}");
                stringBuilderParametersValues2.Append($"{empty}@{columnParser.NormalizedShortName}");
                stringBuilderArguments.Append($"{empty}{columnParser.QuotedShortName}");
                stringBuilderParameters.Append($"{empty}[c].{columnParser.QuotedShortName}");
                empty = ", ";
            }

            stringBuilder.AppendLine($"INSERT OR REPLACE INTO {this.TableQuotedShortName}");
            stringBuilder.AppendLine($"({stringBuilderArguments})");
            stringBuilder.Append($"VALUES ({stringBuilderParametersValues2}) ");
            stringBuilder.AppendLine($";");

            stringBuilder.AppendLine($"UPDATE {this.TrackingTableQuotedShortName} SET ");
            stringBuilder.AppendLine("[update_scope_id] = @sync_scope_id,");
            stringBuilder.AppendLine("[sync_row_is_tombstone] = 0,");
            stringBuilder.AppendLine("[last_change_datetime] = datetime('now')");
            stringBuilder.AppendLine($"WHERE {SqliteManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKeys, string.Empty)}");
            stringBuilder.Append($" AND (select changes()) > 0");
            stringBuilder.AppendLine($";");
            var cmdtext = stringBuilder.ToString();

            return cmdtext;
        }

        private string CreateUpdateCommandText()
        {
            var stringBuilderArguments = new StringBuilder();
            var stringBuilderParameters = new StringBuilder();
            var stringBuilderParametersValues = new StringBuilder();
            string empty = string.Empty;

            string str1 = SqliteManagementUtils.JoinOneTablesOnParametersValues(this.TableDescription.PrimaryKeys, "[side]");
            string str2 = SqliteManagementUtils.JoinOneTablesOnParametersValues(this.TableDescription.PrimaryKeys, "[base]");

            // Generate Update command
            var stringBuilder = new StringBuilder();

            foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false, true))
            {
                var columnParser = new ObjectParser(mutableColumn.ColumnName, LeftQuote, RightQuote);

                stringBuilderParametersValues.Append($"{empty}@{columnParser.NormalizedShortName} as {columnParser.QuotedShortName}");
                stringBuilderArguments.Append($"{empty}{columnParser.QuotedShortName}");
                stringBuilderParameters.Append($"{empty}[c].{columnParser.QuotedShortName}");
                empty = "\n, ";
            }

            // create update statement without PK
            var emptyUpdate = string.Empty;
            var columnsToUpdate = false;
            var stringBuilderUpdateSet = new StringBuilder();
            foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false, false))
            {
                var columnParser = new ObjectParser(mutableColumn.ColumnName, LeftQuote, RightQuote);

                stringBuilderUpdateSet.Append($"{emptyUpdate}{columnParser.QuotedShortName}=excluded.{columnParser.QuotedShortName}");
                emptyUpdate = "\n, ";

                columnsToUpdate = true;
            }

            var primaryKeys = string.Join(
                ",",
                this.TableDescription.PrimaryKeys.Select(name => new ObjectParser(name, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote).QuotedShortName));

            // add CTE
            stringBuilder.AppendLine($"WITH CHANGESET as (SELECT {stringBuilderParameters} ");
            stringBuilder.AppendLine($"FROM (SELECT {stringBuilderParametersValues}) as [c]");
            stringBuilder.AppendLine($"LEFT JOIN {this.TrackingTableQuotedShortName} AS [side] ON {str1}");
            stringBuilder.AppendLine($"LEFT JOIN {this.TableQuotedShortName} AS [base] ON {str2}");
            stringBuilder.AppendLine($"WHERE ([side].[timestamp] < @sync_min_timestamp OR [side].[update_scope_id] = @sync_scope_id) ");
            stringBuilder.Append($"OR ({SqliteManagementUtils.WhereColumnIsNull(this.TableDescription.PrimaryKeys, "[base]")} ");
            stringBuilder.AppendLine($"AND ([side].[timestamp] < @sync_min_timestamp OR [side].[timestamp] IS NULL)) ");
            stringBuilder.Append($"OR @sync_force_write = 1");
            stringBuilder.AppendLine($")");

            stringBuilder.AppendLine($"INSERT INTO {this.TableQuotedShortName}");
            stringBuilder.AppendLine($"({stringBuilderArguments})");

            // use CTE here. The CTE is required in order to make the "ON CONFLICT" statement work. Otherwise SQLite cannot parse it
            // Note, that we have to add the pseudo WHERE TRUE clause here, as otherwise the SQLite parser may confuse the following ON
            // with a join clause, thus, throwing a parsing error
            // See a detailed explanation here at the official SQLite documentation: "Parsing Ambiguity" on page https://www.sqlite.org/lang_UPSERT.html
            stringBuilder.AppendLine($" SELECT * from CHANGESET WHERE TRUE");
            if (columnsToUpdate)
            {
                stringBuilder.AppendLine($" ON CONFLICT ({primaryKeys}) DO UPDATE SET ");
                stringBuilder.Append(stringBuilderUpdateSet).AppendLine(";");
            }
            else
            {
                stringBuilder.AppendLine($" ON CONFLICT ({primaryKeys}) DO NOTHING; ");
            }

            stringBuilder.AppendLine();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"UPDATE OR IGNORE {this.TrackingTableQuotedShortName} SET ");
            stringBuilder.AppendLine($"[update_scope_id] = @sync_scope_id,");
            stringBuilder.AppendLine($"[sync_row_is_tombstone] = 0,");
            stringBuilder.AppendLine($"[timestamp] = {SqliteObjectNames.TimestampValue},");
            stringBuilder.AppendLine($"[last_change_datetime] = datetime('now')");
            stringBuilder.AppendLine($"WHERE {SqliteManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKeys, string.Empty)}");
            stringBuilder.AppendLine($" AND (select changes()) > 0;");

            var cmdtext = stringBuilder.ToString();

            return cmdtext;
        }

        private string CreateDeleteMetadataCommandText()
        {

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"DELETE FROM {this.TrackingTableQuotedShortName} WHERE [timestamp] <= @sync_row_timestamp;");

            return stringBuilder.ToString();
        }

        private string CreateDeleteCommandText()
        {
            var stringBuilder = new StringBuilder();
            string str7 = SqliteManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "[p]", "[side]");

            stringBuilder.AppendLine(";WITH [c] AS (");
            stringBuilder.Append("\tSELECT ");
            foreach (var c in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(c.ColumnName, LeftQuote, RightQuote);
                stringBuilder.Append($"[p].{columnParser.QuotedShortName}, ");
            }

            stringBuilder.AppendLine($"[side].[update_scope_id] as [sync_update_scope_id], [side].[timestamp] as [sync_timestamp], [side].[sync_row_is_tombstone]");
            stringBuilder.Append($"\tFROM (SELECT ");
            string comma = string.Empty;
            foreach (var c in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(c.ColumnName, LeftQuote, RightQuote);
                stringBuilder.Append($"{comma}@{columnParser.NormalizedShortName} as {columnParser.QuotedShortName}");
                comma = ", ";
            }

            stringBuilder.AppendLine($") AS [p]");
            stringBuilder.Append($"\tLEFT JOIN {this.TrackingTableQuotedShortName} [side] ON ");
            stringBuilder.AppendLine($"\t{str7}");
            stringBuilder.AppendLine($"\t)");

            stringBuilder.AppendLine($"DELETE FROM {this.TableQuotedShortName} ");
            stringBuilder.AppendLine($"WHERE {SqliteManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKeys, string.Empty)}");
            stringBuilder.AppendLine($"AND (EXISTS (");
            stringBuilder.AppendLine($"     SELECT * FROM [c] ");
            stringBuilder.AppendLine($"     WHERE {SqliteManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKeys, "[c]")}");
            stringBuilder.AppendLine($"     AND ([sync_timestamp] < @sync_min_timestamp OR [sync_timestamp] IS NULL OR [sync_update_scope_id] = @sync_scope_id))");
            stringBuilder.AppendLine($"  OR @sync_force_write = 1");
            stringBuilder.AppendLine($" );");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"UPDATE OR IGNORE {this.TrackingTableQuotedShortName} SET ");
            stringBuilder.AppendLine("[update_scope_id] = @sync_scope_id,");
            stringBuilder.AppendLine("[sync_row_is_tombstone] = 1,");
            stringBuilder.AppendLine("[last_change_datetime] = datetime('now')");
            stringBuilder.AppendLine($"WHERE {SqliteManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKeys, string.Empty)}");
            stringBuilder.AppendLine($" AND (select changes()) > 0");

            var cmdText = stringBuilder.ToString();

            return cmdText;
        }

        private string CreateSelectRowCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder("SELECT ");
            stringBuilder.AppendLine();
            StringBuilder stringBuilder1 = new StringBuilder();
            string empty = string.Empty;
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(pkColumn.ColumnName, LeftQuote, RightQuote);
                stringBuilder1.Append($"{empty}[side].{columnParser.QuotedShortName} = @{columnParser.NormalizedShortName}");
                empty = " AND ";
            }

            foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false, true))
            {
                var nonPkColumnParser = new ObjectParser(mutableColumn.ColumnName, LeftQuote, RightQuote);

                var isPrimaryKey = this.TableDescription.PrimaryKeys.Any(pkey => mutableColumn.ColumnName.Equals(pkey, SyncGlobalization.DataSourceStringComparison));

                if (isPrimaryKey)
                    stringBuilder.AppendLine($"\t[side].{nonPkColumnParser.QuotedShortName}, ");
                else
                    stringBuilder.AppendLine($"\t[base].{nonPkColumnParser.QuotedShortName}, ");
            }

            stringBuilder.AppendLine("\t[side].[sync_row_is_tombstone], ");
            stringBuilder.AppendLine("\t[side].[update_scope_id] as [sync_update_scope_id]");

            stringBuilder.AppendLine($"FROM {this.TrackingTableQuotedShortName} [side] ");
            stringBuilder.AppendLine($"LEFT JOIN {this.TableQuotedShortName} [base] ON ");

            string str = string.Empty;
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(pkColumn.ColumnName, LeftQuote, RightQuote);
                stringBuilder.Append($"{str}[base].{columnParser.QuotedShortName} = [side].{columnParser.QuotedShortName}");
                str = " AND ";
            }

            stringBuilder.AppendLine();
            stringBuilder.Append(string.Concat("WHERE ", stringBuilder1.ToString()));
            stringBuilder.Append(";");

            return stringBuilder.ToString();
        }

        /// <summary>
        /// Create all custom joins from within a filter.
        /// </summary>
        protected string CreateFilterCustomJoins(SyncFilter filter)
        {
            var customJoins = filter.Joins;

            if (customJoins.Count == 0)
                return string.Empty;

            var stringBuilder = new StringBuilder();

            stringBuilder.AppendLine();
            foreach (var customJoin in customJoins)
            {
                switch (customJoin.JoinEnum)
                {
                    case Join.Left:
                        stringBuilder.Append("LEFT JOIN ");
                        break;
                    case Join.Right:
                        stringBuilder.Append("RIGHT JOIN ");
                        break;
                    case Join.Outer:
                        stringBuilder.Append("OUTER JOIN ");
                        break;
                    case Join.Inner:
                    default:
                        stringBuilder.Append("INNER JOIN ");
                        break;
                }

                var filterTableName = new TableParser(filter.TableName, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote).QuotedShortName;
                var joinTableName = new TableParser(customJoin.TableName, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote).QuotedShortName;
                var leftTableName = new TableParser(customJoin.LeftTableName, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote).QuotedShortName;

                if (string.Equals(filterTableName, leftTableName, SyncGlobalization.DataSourceStringComparison))
                    leftTableName = "[base]";

                var rightTableName = new TableParser(customJoin.RightTableName, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote).QuotedShortName;

                if (string.Equals(filterTableName, rightTableName, SyncGlobalization.DataSourceStringComparison))
                    rightTableName = "[base]";

                var leftColumName = new TableParser(customJoin.LeftColumnName, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote).QuotedShortName;
                var rightColumName = new TableParser(customJoin.RightColumnName, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote).QuotedShortName;

                stringBuilder.AppendLine($"{joinTableName} ON {leftTableName}.{leftColumName} = {rightTableName}.{rightColumName}");
            }

            return stringBuilder.ToString();
        }

        /// <summary>
        /// Create all side where criteria from within a filter.
        /// </summary>
        protected string CreateFilterWhereSide(SyncFilter filter, bool checkTombstoneRows = false)
        {
            var sideWhereFilters = filter.Wheres;

            if (sideWhereFilters.Count == 0)
                return string.Empty;

            var stringBuilder = new StringBuilder();

            // Managing when state is tombstone
            if (checkTombstoneRows)
                stringBuilder.AppendLine($"(");

            stringBuilder.AppendLine($" (");

            var and2 = "   ";

            foreach (var whereFilter in sideWhereFilters)
            {
                var tableFilter = this.TableDescription.Schema.Tables[whereFilter.TableName, whereFilter.SchemaName];
                if (tableFilter == null)
                    throw new FilterParamTableNotExistsException(whereFilter.TableName);

                var columnFilter = tableFilter.Columns[whereFilter.ColumnName];
                if (columnFilter == null)
                    throw new FilterParamColumnNotExistsException(whereFilter.ColumnName, whereFilter.TableName);

                var tableName = new TableParser(tableFilter.TableName, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote).QuotedShortName;
                tableName = string.Equals(tableName, filter.TableName, SyncGlobalization.DataSourceStringComparison)
                    ? "[base]"
                    : tableName;

                var columnName = new ObjectParser(columnFilter.ColumnName).QuotedShortName;
                var parameterName = new ObjectParser(whereFilter.ParameterName).QuotedShortName;

                var param = filter.Parameters[parameterName];

                if (param == null)
                    throw new FilterParamColumnNotExistsException(columnName, whereFilter.TableName);

                stringBuilder.Append($"{and2}({tableName}.{columnName} = @{parameterName}");

                if (param.AllowNull)
                    stringBuilder.Append($" OR @{parameterName} IS NULL");

                stringBuilder.Append($")");

                and2 = " AND ";
            }

            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"  )");

            if (checkTombstoneRows)
            {
                stringBuilder.AppendLine($" OR [side].[sync_row_is_tombstone] = 1");
                stringBuilder.AppendLine($")");
            }

            // Managing when state is tombstone
            return stringBuilder.ToString();
        }

        /// <summary>
        /// Create all custom wheres from witing a filter.
        /// </summary>
        protected string CreateFilterCustomWheres(SyncFilter filter)
        {
            var customWheres = filter.CustomWheres;

            if (customWheres.Count == 0)
                return string.Empty;

            var stringBuilder = new StringBuilder();
            var and2 = "  ";
            stringBuilder.AppendLine($"(");

            foreach (var customWhere in customWheres)
            {
                // Template escape character
                var customWhereIteration = customWhere;
                customWhereIteration = customWhereIteration.Replace("{{{", "[", SyncGlobalization.DataSourceStringComparison);
                customWhereIteration = customWhereIteration.Replace("}}}", "]", SyncGlobalization.DataSourceStringComparison);
                stringBuilder.Append($"{and2}{customWhereIteration}");
                and2 = " AND ";
            }

            stringBuilder.AppendLine();
            stringBuilder.AppendLine($")");

            return stringBuilder.ToString();
        }

        private string CreateSelectChangesCommandText(SyncFilter filter = null)
        {
            var stringBuilder = new StringBuilder(string.Empty);

            if (filter != null)
                stringBuilder.AppendLine("SELECT DISTINCT ");
            else
                stringBuilder.AppendLine("SELECT ");

            foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false, true))
            {
                var columnParser = new ObjectParser(mutableColumn.ColumnName, LeftQuote, RightQuote);

                var isPrimaryKey = this.TableDescription.PrimaryKeys.Any(pkey => mutableColumn.ColumnName.Equals(pkey, SyncGlobalization.DataSourceStringComparison));

                if (isPrimaryKey)
                    stringBuilder.AppendLine($"\t[side].{columnParser.QuotedShortName}, ");
                else
                    stringBuilder.AppendLine($"\t[base].{columnParser.QuotedShortName}, ");
            }

            stringBuilder.AppendLine($"\t[side].[sync_row_is_tombstone], ");
            stringBuilder.AppendLine($"\t[side].[update_scope_id] as [sync_update_scope_id] ");
            stringBuilder.AppendLine($"FROM {this.TrackingTableQuotedShortName} [side]");
            stringBuilder.AppendLine($"LEFT JOIN {this.TableQuotedShortName} [base]");
            stringBuilder.Append($"ON ");

            string empty = string.Empty;
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(pkColumn.ColumnName, LeftQuote, RightQuote);

                stringBuilder.Append($"{empty}[base].{columnParser.QuotedShortName} = [side].{columnParser.QuotedShortName}");
                empty = " AND ";
            }

            stringBuilder.AppendLine();

            // Conditionally add filters if they are not disabled
            if (this.disableSqlFiltersGeneration)
            {
                stringBuilder.AppendLine("WHERE ");
            }
            else
            {
                // ----------------------------------
                // Custom Joins
                // ----------------------------------
                if (filter != null)
                    stringBuilder.Append(this.CreateFilterCustomJoins(filter));

                // Looking at discussion https://github.com/Mimetis/Dotmim.Sync/discussions/453, trying to remove ([side].[update_scope_id] <> @sync_scope_id)
                // since we are sure that sqlite will never be a server side database
                stringBuilder.AppendLine("WHERE ");

                // ----------------------------------
                // Where filters and Custom Where string
                // ----------------------------------
                if (filter != null)
                {
                    var createFilterWhereSide = this.CreateFilterWhereSide(filter, true);
                    stringBuilder.Append(createFilterWhereSide);

                    if (!string.IsNullOrEmpty(createFilterWhereSide))
                        stringBuilder.AppendLine($"AND ");

                    var createFilterCustomWheres = this.CreateFilterCustomWheres(filter);
                    stringBuilder.Append(createFilterCustomWheres);

                    if (!string.IsNullOrEmpty(createFilterCustomWheres))
                        stringBuilder.AppendLine($"AND ");
                }

                // ----------------------------------
            }

            stringBuilder.AppendLine("([side].[timestamp] > @sync_min_timestamp AND [side].[update_scope_id] IS NULL)");

            return stringBuilder.ToString();
        }

        private string CreateUpdateUntrackedRowsCommandText()
        {
            var stringBuilder = new StringBuilder();
            var str1 = new StringBuilder();
            var str2 = new StringBuilder();
            var str3 = new StringBuilder();
            var str4 = SqliteManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "[side]", "[base]");

            stringBuilder.AppendLine($"INSERT INTO {this.TrackingTableQuotedShortName} (");

            var comma = string.Empty;
            foreach (var pkeyColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(pkeyColumn.ColumnName, LeftQuote, RightQuote);

                str1.Append($"{comma}{columnParser.QuotedShortName}");
                str2.Append($"{comma}[base].{columnParser.QuotedShortName}");
                str3.Append($"{comma}[side].{columnParser.QuotedShortName}");

                comma = ", ";
            }

            stringBuilder.Append(str1);
            stringBuilder.AppendLine($", [update_scope_id], [sync_row_is_tombstone], [timestamp], [last_change_datetime]");
            stringBuilder.AppendLine($")");
            stringBuilder.Append($"SELECT ");
            stringBuilder.Append(str2);
            stringBuilder.AppendLine($", NULL, 0, {SqliteObjectNames.TimestampValue}, datetime('now')");
            stringBuilder.AppendLine($"FROM {this.TableQuotedShortName} as [base] WHERE NOT EXISTS");
            stringBuilder.Append($"(SELECT ");
            stringBuilder.Append(str3);
            stringBuilder.AppendLine($" FROM {this.TrackingTableQuotedShortName} as [side] ");
            stringBuilder.AppendLine($"WHERE {str4})");

            return stringBuilder.ToString();
        }
    }
}