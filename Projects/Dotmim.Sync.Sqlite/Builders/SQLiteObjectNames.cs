using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace Dotmim.Sync.Sqlite
{
    public class SqliteObjectNames
    {
        public const string TimestampValue = "replace(strftime('%Y%m%d%H%M%f', 'now'), '.', '')";

        internal const string InsertTriggerName = "[{0}_insert_trigger]";
        internal const string UpdateTriggerName = "[{0}_update_trigger]";
        internal const string DeleteTriggerName = "[{0}_delete_trigger]";

        private Dictionary<DbCommandType, string> commandNames = [];
        private Dictionary<DbTriggerType, string> triggersNames = [];

        private ParserName tableName;
        private ParserName trackingName;
        private bool disableSqlFiltersGeneration;

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
                throw new NotSupportedException($"Sqlite provider does not support the command type {objectType}");

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

            //// concat filter name
            // if (filter != null)
            //    commandName = string.Format(commandName, filter.GetFilterName());
            return commandName;
        }

        public SqliteObjectNames(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup, string scopeName, bool disableSqlFiltersGeneration)
        {
            this.TableDescription = tableDescription;
            this.Setup = setup;
            this.ScopeName = scopeName;
            this.tableName = tableName;
            this.trackingName = trackingName;
            this.disableSqlFiltersGeneration = disableSqlFiltersGeneration;

            this.SetDefaultNames();
        }

        /// <summary>
        /// Set the default stored procedures names.
        /// </summary>
        private void SetDefaultNames()
        {
            var tpref = this.Setup.TriggersPrefix != null ? this.Setup.TriggersPrefix : string.Empty;
            var tsuf = this.Setup.TriggersSuffix != null ? this.Setup.TriggersSuffix : string.Empty;

            this.AddTriggerName(DbTriggerType.Insert, string.Format(InsertTriggerName, $"{tpref}{this.tableName.Unquoted().Normalized()}{tsuf}"));
            this.AddTriggerName(DbTriggerType.Update, string.Format(UpdateTriggerName, $"{tpref}{this.tableName.Unquoted().Normalized()}{tsuf}"));
            this.AddTriggerName(DbTriggerType.Delete, string.Format(DeleteTriggerName, $"{tpref}{this.tableName.Unquoted().Normalized()}{tsuf}"));

            var filter = this.TableDescription.GetFilter();

            // Select changes
            this.CreateSelectChangesCommandText(filter);
            this.CreateSelectRowCommandText();
            this.CreateDeleteCommandText();
            this.CreateDeleteMetadataCommandText();
            this.CreateUpdateCommandText();
            this.CreateInsertCommandText();
            this.CreateResetCommandText();
            this.CreateUpdateUntrackedRowsCommandText();
            this.CreateUpdateMetadataCommandText();
            this.CreateSelectMetadataCommandText();

            // Sqlite does not have any constraints, so just return a simple statement
            this.AddCommandName(DbCommandType.DisableConstraints, "Select 0"); // PRAGMA foreign_keys = OFF
            this.AddCommandName(DbCommandType.EnableConstraints, "Select 0");

            this.AddCommandName(DbCommandType.PreDeleteRow, "Select 0");
            this.AddCommandName(DbCommandType.PreDeleteRows, "Select 0");
            this.AddCommandName(DbCommandType.PreInsertRow, "Select 0");
            this.AddCommandName(DbCommandType.PreInsertRows, "Select 0");
            this.AddCommandName(DbCommandType.PreUpdateRow, "Select 0");
            this.AddCommandName(DbCommandType.PreUpdateRows, "Select 0");
        }

        private void CreateResetCommandText()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"DELETE FROM {this.tableName.Quoted()};");
            stringBuilder.AppendLine($"DELETE FROM {this.trackingName.Quoted()};");
            this.AddCommandName(DbCommandType.Reset, stringBuilder.ToString());
        }

        private void CreateSelectMetadataCommandText()
        {
            var stringBuilder = new StringBuilder();
            var pkeysSelect = new StringBuilder();
            var pkeysWhere = new StringBuilder();

            string and = string.Empty;
            string comma = string.Empty;
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                var parameterName = ParserName.Parse(pkColumn).Unquoted().Normalized().ToString();
                pkeysSelect.Append($"{comma}[side].{columnName}");

                pkeysWhere.Append($"{and}[side].{columnName} = @{parameterName}");

                and = " AND ";
                comma = ", ";
            }

            stringBuilder.AppendLine($"SELECT {pkeysSelect}, [side].[update_scope_id], [side].[timestamp], [side].[sync_row_is_tombstone]");
            stringBuilder.AppendLine($"FROM {this.trackingName.Quoted()} [side]");
            stringBuilder.AppendLine($"WHERE {pkeysWhere}");

            var commandText = stringBuilder.ToString();

            this.AddCommandName(DbCommandType.SelectMetadata, commandText);
        }

        private void CreateUpdateMetadataCommandText()
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
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                var parameterName = ParserName.Parse(pkColumn).Unquoted().Normalized().ToString();

                pkeySelectForInsert.Append($"{comma}{columnName}");
                pkeyISelectForInsert.Append($"{comma}[i].{columnName}");
                pkeyAliasSelectForInsert.Append($"{comma}@{parameterName} as {columnName}");
                pkeysLeftJoinForInsert.Append($"{and}[side].{columnName} = [i].{columnName}");
                pkeysIsNullForInsert.Append($"{and}[side].{columnName} IS NULL");
                and = " AND ";
                comma = ", ";
            }

            stringBuilder.AppendLine($"INSERT OR REPLACE INTO {this.trackingName.Quoted()} (");
            stringBuilder.AppendLine(pkeySelectForInsert.ToString());
            stringBuilder.AppendLine(",[update_scope_id], [sync_row_is_tombstone], [timestamp], [last_change_datetime] )");
            stringBuilder.AppendLine($"SELECT {pkeyISelectForInsert} ");
            stringBuilder.AppendLine($"   , i.sync_scope_id, i.sync_row_is_tombstone, i.sync_timestamp, i.UtcDate");
            stringBuilder.AppendLine("FROM (");
            stringBuilder.AppendLine($"  SELECT {pkeyAliasSelectForInsert}");
            stringBuilder.AppendLine($"          ,@sync_scope_id as sync_scope_id, @sync_row_is_tombstone as sync_row_is_tombstone, {SqliteObjectNames.TimestampValue} as sync_timestamp, datetime('now') as UtcDate) as i;");

            var cmdtext = stringBuilder.ToString();

            this.AddCommandName(DbCommandType.UpdateMetadata, cmdtext);
        }

        private void CreateInsertCommandText()
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
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                var columnParameterName = ParserName.Parse(mutableColumn).Unquoted().Normalized().ToString();
                stringBuilderParametersValues.Append($"{empty}@{columnParameterName} as {columnName}");
                stringBuilderParametersValues2.Append($"{empty}@{columnParameterName}");
                stringBuilderArguments.Append($"{empty}{columnName}");
                stringBuilderParameters.Append($"{empty}[c].{columnName}");
                empty = ", ";
            }

            stringBuilder.AppendLine($"INSERT OR REPLACE INTO {this.tableName.Quoted()}");
            stringBuilder.AppendLine($"({stringBuilderArguments})");
            stringBuilder.Append($"VALUES ({stringBuilderParametersValues2}) ");
            stringBuilder.AppendLine($";");

            stringBuilder.AppendLine($"UPDATE {this.trackingName.Quoted()} SET ");
            stringBuilder.AppendLine("[update_scope_id] = @sync_scope_id,");
            stringBuilder.AppendLine("[sync_row_is_tombstone] = 0,");
            stringBuilder.AppendLine("[last_change_datetime] = datetime('now')");
            stringBuilder.AppendLine($"WHERE {SqliteManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKeys, string.Empty)}");
            stringBuilder.Append($" AND (select changes()) > 0");
            stringBuilder.AppendLine($";");
            var cmdtext = stringBuilder.ToString();

            this.AddCommandName(DbCommandType.InsertRow, cmdtext);
            this.AddCommandName(DbCommandType.InsertRows, cmdtext);
        }

        private void CreateUpdateCommandText()
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
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                var columnParameterName = ParserName.Parse(mutableColumn).Unquoted().Normalized().ToString();

                stringBuilderParametersValues.Append($"{empty}@{columnParameterName} as {columnName}");
                stringBuilderArguments.Append($"{empty}{columnName}");
                stringBuilderParameters.Append($"{empty}[c].{columnName}");
                empty = "\n, ";
            }

            // create update statement without PK
            var emptyUpdate = string.Empty;
            var columnsToUpdate = false;
            var stringBuilderUpdateSet = new StringBuilder();
            foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false, false))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                stringBuilderUpdateSet.Append($"{emptyUpdate}{columnName}=excluded.{columnName}");
                emptyUpdate = "\n, ";

                columnsToUpdate = true;
            }

            var primaryKeys = string.Join(
                ",",
                this.TableDescription.PrimaryKeys.Select(name => ParserName.Parse(name).Quoted().ToString()));

            // add CTE
            stringBuilder.AppendLine($"WITH CHANGESET as (SELECT {stringBuilderParameters} ");
            stringBuilder.AppendLine($"FROM (SELECT {stringBuilderParametersValues}) as [c]");
            stringBuilder.AppendLine($"LEFT JOIN {this.trackingName.Quoted()} AS [side] ON {str1}");
            stringBuilder.AppendLine($"LEFT JOIN {this.tableName.Quoted()} AS [base] ON {str2}");
            stringBuilder.AppendLine($"WHERE ([side].[timestamp] < @sync_min_timestamp OR [side].[update_scope_id] = @sync_scope_id) ");
            stringBuilder.Append($"OR ({SqliteManagementUtils.WhereColumnIsNull(this.TableDescription.PrimaryKeys, "[base]")} ");
            stringBuilder.AppendLine($"AND ([side].[timestamp] < @sync_min_timestamp OR [side].[timestamp] IS NULL)) ");
            stringBuilder.Append($"OR @sync_force_write = 1");
            stringBuilder.AppendLine($")");

            stringBuilder.AppendLine($"INSERT INTO {this.tableName.Quoted()}");
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
            stringBuilder.AppendLine($"UPDATE OR IGNORE {this.trackingName.Quoted()} SET ");
            stringBuilder.AppendLine($"[update_scope_id] = @sync_scope_id,");
            stringBuilder.AppendLine($"[sync_row_is_tombstone] = 0,");
            stringBuilder.AppendLine($"[timestamp] = {SqliteObjectNames.TimestampValue},");
            stringBuilder.AppendLine($"[last_change_datetime] = datetime('now')");
            stringBuilder.AppendLine($"WHERE {SqliteManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKeys, string.Empty)}");
            stringBuilder.AppendLine($" AND (select changes()) > 0;");

            var cmdtext = stringBuilder.ToString();

            this.AddCommandName(DbCommandType.UpdateRow, cmdtext);
            this.AddCommandName(DbCommandType.UpdateRows, cmdtext);
        }

        private void CreateDeleteMetadataCommandText()
        {

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"DELETE FROM {this.trackingName.Quoted()} WHERE [timestamp] <= @sync_row_timestamp;");

            this.AddCommandName(DbCommandType.DeleteMetadata, stringBuilder.ToString());
        }

        private void CreateDeleteCommandText()
        {
            var stringBuilder = new StringBuilder();
            string str7 = SqliteManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "[p]", "[side]");

            stringBuilder.AppendLine(";WITH [c] AS (");
            stringBuilder.Append("\tSELECT ");
            foreach (var c in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();
                stringBuilder.Append($"[p].{columnName}, ");
            }

            stringBuilder.AppendLine($"[side].[update_scope_id] as [sync_update_scope_id], [side].[timestamp] as [sync_timestamp], [side].[sync_row_is_tombstone]");
            stringBuilder.Append($"\tFROM (SELECT ");
            string comma = string.Empty;
            foreach (var c in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();
                var columnParameterName = ParserName.Parse(c).Unquoted().Normalized().ToString();

                stringBuilder.Append($"{comma}@{columnParameterName} as {columnName}");
                comma = ", ";
            }

            stringBuilder.AppendLine($") AS [p]");
            stringBuilder.Append($"\tLEFT JOIN {this.trackingName.Quoted()} [side] ON ");
            stringBuilder.AppendLine($"\t{str7}");
            stringBuilder.AppendLine($"\t)");

            stringBuilder.AppendLine($"DELETE FROM {this.tableName.Quoted()} ");
            stringBuilder.AppendLine($"WHERE {SqliteManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKeys, string.Empty)}");
            stringBuilder.AppendLine($"AND (EXISTS (");
            stringBuilder.AppendLine($"     SELECT * FROM [c] ");
            stringBuilder.AppendLine($"     WHERE {SqliteManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKeys, "[c]")}");
            stringBuilder.AppendLine($"     AND ([sync_timestamp] < @sync_min_timestamp OR [sync_timestamp] IS NULL OR [sync_update_scope_id] = @sync_scope_id))");
            stringBuilder.AppendLine($"  OR @sync_force_write = 1");
            stringBuilder.AppendLine($" );");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"UPDATE OR IGNORE {this.trackingName.Quoted()} SET ");
            stringBuilder.AppendLine("[update_scope_id] = @sync_scope_id,");
            stringBuilder.AppendLine("[sync_row_is_tombstone] = 1,");
            stringBuilder.AppendLine("[last_change_datetime] = datetime('now')");
            stringBuilder.AppendLine($"WHERE {SqliteManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKeys, string.Empty)}");
            stringBuilder.AppendLine($" AND (select changes()) > 0");

            var cmdText = stringBuilder.ToString();

            this.AddCommandName(DbCommandType.DeleteRow, cmdText);
            this.AddCommandName(DbCommandType.DeleteRows, cmdText);
        }

        private void CreateSelectRowCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder("SELECT ");
            stringBuilder.AppendLine();
            StringBuilder stringBuilder1 = new StringBuilder();
            string empty = string.Empty;
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                var unquotedColumnName = ParserName.Parse(pkColumn).Unquoted().Normalized().ToString();
                stringBuilder1.Append($"{empty}[side].{columnName} = @{unquotedColumnName}");
                empty = " AND ";
            }

            foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false, true))
            {
                var nonPkColumnName = ParserName.Parse(mutableColumn).Quoted().ToString();

                var isPrimaryKey = this.TableDescription.PrimaryKeys.Any(pkey => mutableColumn.ColumnName.Equals(pkey, SyncGlobalization.DataSourceStringComparison));

                if (isPrimaryKey)
                    stringBuilder.AppendLine($"\t[side].{nonPkColumnName}, ");
                else
                    stringBuilder.AppendLine($"\t[base].{nonPkColumnName}, ");
            }

            stringBuilder.AppendLine("\t[side].[sync_row_is_tombstone], ");
            stringBuilder.AppendLine("\t[side].[update_scope_id] as [sync_update_scope_id]");

            stringBuilder.AppendLine($"FROM {this.trackingName.Quoted()} [side] ");
            stringBuilder.AppendLine($"LEFT JOIN {this.tableName.Quoted()} [base] ON ");

            string str = string.Empty;
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                stringBuilder.Append($"{str}[base].{columnName} = [side].{columnName}");
                str = " AND ";
            }

            stringBuilder.AppendLine();
            stringBuilder.Append(string.Concat("WHERE ", stringBuilder1.ToString()));
            stringBuilder.Append(";");
            this.AddCommandName(DbCommandType.SelectRow, stringBuilder.ToString());
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

                var filterTableName = ParserName.Parse(filter.TableName).Quoted().ToString();
                var joinTableName = ParserName.Parse(customJoin.TableName).Quoted().ToString();
                var leftTableName = ParserName.Parse(customJoin.LeftTableName).Quoted().ToString();

                if (string.Equals(filterTableName, leftTableName, SyncGlobalization.DataSourceStringComparison))
                    leftTableName = "[base]";

                var rightTableName = ParserName.Parse(customJoin.RightTableName).Quoted().ToString();

                if (string.Equals(filterTableName, rightTableName, SyncGlobalization.DataSourceStringComparison))
                    rightTableName = "[base]";

                var leftColumName = ParserName.Parse(customJoin.LeftColumnName).Quoted().ToString();
                var rightColumName = ParserName.Parse(customJoin.RightColumnName).Quoted().ToString();

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

                var tableName = ParserName.Parse(tableFilter).Unquoted().ToString();
                if (string.Equals(tableName, filter.TableName, SyncGlobalization.DataSourceStringComparison))
                    tableName = "[base]";
                else
                    tableName = ParserName.Parse(tableFilter).Quoted().ToString();

                var columnName = ParserName.Parse(columnFilter).Quoted().ToString();
                var parameterName = ParserName.Parse(whereFilter.ParameterName).Unquoted().Normalized().ToString();

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
#if NET6_0_OR_GREATER
                customWhereIteration = customWhereIteration.Replace("{{{", "[", SyncGlobalization.DataSourceStringComparison);
                customWhereIteration = customWhereIteration.Replace("}}}", "]", SyncGlobalization.DataSourceStringComparison);
#else
                customWhereIteration = customWhereIteration.Replace("{{{", "[");
                customWhereIteration = customWhereIteration.Replace("}}}", "]");
#endif
                stringBuilder.Append($"{and2}{customWhereIteration}");
                and2 = " AND ";
            }

            stringBuilder.AppendLine();
            stringBuilder.AppendLine($")");

            return stringBuilder.ToString();
        }

        private void CreateSelectChangesCommandText(SyncFilter filter = null)
        {
            var stringBuilder = new StringBuilder(string.Empty);

            if (filter != null)
                stringBuilder.AppendLine("SELECT DISTINCT ");
            else
                stringBuilder.AppendLine("SELECT ");

            foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false, true))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();

                var isPrimaryKey = this.TableDescription.PrimaryKeys.Any(pkey => mutableColumn.ColumnName.Equals(pkey, SyncGlobalization.DataSourceStringComparison));

                if (isPrimaryKey)
                    stringBuilder.AppendLine($"\t[side].{columnName}, ");
                else
                    stringBuilder.AppendLine($"\t[base].{columnName}, ");
            }

            stringBuilder.AppendLine($"\t[side].[sync_row_is_tombstone], ");
            stringBuilder.AppendLine($"\t[side].[update_scope_id] as [sync_update_scope_id] ");
            stringBuilder.AppendLine($"FROM {this.trackingName.Quoted()} [side]");
            stringBuilder.AppendLine($"LEFT JOIN {this.tableName.Quoted()} [base]");
            stringBuilder.Append($"ON ");

            string empty = string.Empty;
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();

                stringBuilder.Append($"{empty}[base].{columnName} = [side].{columnName}");
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

            this.AddCommandName(DbCommandType.SelectChanges, stringBuilder.ToString());
            this.AddCommandName(DbCommandType.SelectChangesWithFilters, stringBuilder.ToString());
        }

        // private void CreateSelectInitializedCommandText(SyncFilter filter = null)
        // {
        //    StringBuilder stringBuilder = new StringBuilder();

        // if (filter != null)
        //        stringBuilder.AppendLine("SELECT DISTINCT ");
        //    else
        //        stringBuilder.AppendLine("SELECT ");

        // foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
        //    {
        //        var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
        //        stringBuilder.AppendLine($"\t[base].{columnName}, ");
        //    }
        //    var columns = this.TableDescription.GetMutableColumns().ToList();

        // for (var i = 0; i < columns.Count; i++)
        //    {
        //        var mutableColumn = columns[i];
        //        var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
        //        stringBuilder.Append($"\t[base].{columnName}");

        // if (i < columns.Count - 1)
        //            stringBuilder.AppendLine(", ");
        //    }
        //    stringBuilder.AppendLine($"FROM {tableName.Quoted().ToString()} [base]");

        // this.AddCommandName(DbCommandType.SelectInitializedChanges, stringBuilder.ToString());
        //    this.AddCommandName(DbCommandType.SelectInitializedChangesWithFilters, stringBuilder.ToString());
        // }
        private void CreateUpdateUntrackedRowsCommandText()
        {
            var stringBuilder = new StringBuilder();
            var str1 = new StringBuilder();
            var str2 = new StringBuilder();
            var str3 = new StringBuilder();
            var str4 = SqliteManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "[side]", "[base]");

            stringBuilder.AppendLine($"INSERT INTO {this.trackingName.Quoted()} (");

            var comma = string.Empty;
            foreach (var pkeyColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var pkeyColumnName = ParserName.Parse(pkeyColumn).Quoted().ToString();

                str1.Append($"{comma}{pkeyColumnName}");
                str2.Append($"{comma}[base].{pkeyColumnName}");
                str3.Append($"{comma}[side].{pkeyColumnName}");

                comma = ", ";
            }

            stringBuilder.Append(str1);
            stringBuilder.AppendLine($", [update_scope_id], [sync_row_is_tombstone], [timestamp], [last_change_datetime]");
            stringBuilder.AppendLine($")");
            stringBuilder.Append($"SELECT ");
            stringBuilder.Append(str2);
            stringBuilder.AppendLine($", NULL, 0, {SqliteObjectNames.TimestampValue}, datetime('now')");
            stringBuilder.AppendLine($"FROM {this.tableName.Quoted()} as [base] WHERE NOT EXISTS");
            stringBuilder.Append($"(SELECT ");
            stringBuilder.Append(str3);
            stringBuilder.AppendLine($" FROM {this.trackingName.Quoted()} as [side] ");
            stringBuilder.AppendLine($"WHERE {str4})");

            var r = stringBuilder.ToString();

            this.AddCommandName(DbCommandType.UpdateUntrackedRows, r);
        }
    }
}