using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Text;

using System.Linq;
using Dotmim.Sync.Filter;
using System.Data;

namespace Dotmim.Sync.Sqlite
{
    public class SqliteObjectNames
    {
        public const string TimestampValue = "replace(strftime('%Y%m%d%H%M%f', 'now'), '.', '')";

        internal const string insertTriggerName = "[{0}_insert_trigger]";
        internal const string updateTriggerName = "[{0}_update_trigger]";
        internal const string deleteTriggerName = "[{0}_delete_trigger]";

        private Dictionary<DbCommandType, string> names = new Dictionary<DbCommandType, string>();
        private ParserName tableName, trackingName;

        public SyncTable TableDescription { get; }


        public void AddName(DbCommandType objectType, string name)
        {
            if (names.ContainsKey(objectType))
                throw new Exception("Yous can't add an objectType multiple times");

            names.Add(objectType, name);
        }
        public string GetCommandName(DbCommandType objectType, IEnumerable<SyncFilter> filters = null)
        {
            if (!names.ContainsKey(objectType))
                throw new NotSupportedException($"Sqlite provider does not support the command type {objectType.ToString()}");

            var commandName = names[objectType];

            if (filters != null)
            {
                string name = "";
                string sep = "";
                foreach (var c in filters)
                {
                    var columnName = ParserName.Parse(c.ColumnName).Unquoted().Normalized().ToString();
                    name += $"{columnName}{sep}";
                    sep = "_";
                }

                commandName = string.Format(commandName, name);
            }

            return commandName;
        }

        public SqliteObjectNames(SyncTable tableDescription)
        {
            this.TableDescription = tableDescription;
            (tableName, trackingName) = SqliteTableBuilder.GetParsers(this.TableDescription);

            SetDefaultNames();
        }

        /// <summary>
        /// Set the default stored procedures names
        /// </summary>
        private void SetDefaultNames()
        {
            var tpref = this.TableDescription.Schema.TriggersPrefix != null ? this.TableDescription.Schema.TriggersPrefix : "";
            var tsuf = this.TableDescription.Schema.TriggersSuffix != null ? this.TableDescription.Schema.TriggersSuffix : "";

            this.AddName(DbCommandType.InsertTrigger, string.Format(insertTriggerName, $"{tpref}{tableName.Unquoted().Normalized().ToString()}{tsuf}"));
            this.AddName(DbCommandType.UpdateTrigger, string.Format(updateTriggerName, $"{tpref}{tableName.Unquoted().Normalized().ToString()}{tsuf}"));
            this.AddName(DbCommandType.DeleteTrigger, string.Format(deleteTriggerName, $"{tpref}{tableName.Unquoted().Normalized().ToString()}{tsuf}"));

            // Check if we have mutables columns
            var hasMutableColumns = TableDescription.GetMutableColumns(false).Any();


            // Select changes
            this.CreateSelectChangesCommandText();
            this.CreateSelectRowCommandText();
            this.CreateDeleteCommandText();
            this.CreateDeleteMetadataCommandText();
            this.CreateUpdateCommandText(hasMutableColumns);
            this.CreateUpdatedMetadataCommandText(hasMutableColumns);
            this.CreateResetCommandText();

            // Sqlite does not have any constraints, so just return a simple statement
            this.AddName(DbCommandType.DisableConstraints, "Select 0"); // PRAGMA foreign_keys = OFF
            this.AddName(DbCommandType.EnableConstraints, "Select 0");

        }

        private void CreateResetCommandText()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"DELETE FROM {tableName.Quoted().ToString()};");
            stringBuilder.AppendLine($"DELETE FROM {trackingName.Quoted().ToString()};");
            this.AddName(DbCommandType.Reset, stringBuilder.ToString());
        }

        private void CreateUpdateCommandText(bool hasMutableColumns)
        {
            var stringBuilderArguments = new StringBuilder();
            var stringBuilderParameters = new StringBuilder();
            string empty = string.Empty;

            string str1 = SqliteManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "[c]", "[base]");
            string str7 = SqliteManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "[p]", "[side]");

            // Generate Update command
            var stringBuilder = new StringBuilder();


            stringBuilder.AppendLine(";WITH [c] AS (");
            stringBuilder.Append("\tSELECT ");
            foreach (var c in this.TableDescription.Columns.Where(col => !col.IsReadOnly))
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();
                stringBuilder.Append($"[p].{columnName}, ");
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"\t[side].[update_scope_id], [side].[timestamp], [side].[sync_row_is_tombstone]");
            stringBuilder.AppendLine($"\tFROM (SELECT ");
            stringBuilder.Append($"\t\t ");
            string comma = "";
            foreach (var c in this.TableDescription.GetMutableColumns(false, true))
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();
                var columnParameterName = ParserName.Parse(c).Unquoted().Normalized().ToString();

                stringBuilder.Append($"{comma}@{columnParameterName} as {columnName}");
                comma = ", ";
            }
            stringBuilder.AppendLine($") AS [p]");
            stringBuilder.Append($"\tLEFT JOIN {trackingName.Quoted().ToString()} [side] ON ");
            stringBuilder.AppendLine($"\t{str7}");
            stringBuilder.AppendLine($"\t)");


            foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false, true))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                stringBuilderArguments.Append($"{empty}{columnName}");
                stringBuilderParameters.Append($"{empty}[c].{columnName}");
                empty = ", ";
            }

            stringBuilder.AppendLine($"INSERT OR REPLACE INTO {tableName.Quoted().ToString()}");
            stringBuilder.AppendLine($"({stringBuilderArguments.ToString()})");
            stringBuilder.AppendLine($"SELECT {stringBuilderParameters.ToString()} ");
            stringBuilder.AppendLine($"FROM [c] ");
            stringBuilder.AppendLine($"LEFT JOIN {tableName.Quoted().ToString()} AS [base] ON {str1}");
            stringBuilder.Append($"WHERE ({SqliteManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKeys, "[base]")} ");
            stringBuilder.AppendLine($"AND ([c].[timestamp] < @sync_min_timestamp OR [c].[update_scope_id] = @sync_scope_id)) ");
            stringBuilder.Append($"OR ({SqliteManagementUtils.WhereColumnIsNull(this.TableDescription.PrimaryKeys, "[base]")} ");
            stringBuilder.AppendLine($"AND ([c].[timestamp] < @sync_min_timestamp OR [c].[timestamp] IS NULL)) ");
            stringBuilder.AppendLine($"OR @sync_force_write = 1;");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"UPDATE OR IGNORE {trackingName.Quoted().ToString()} SET ");
            stringBuilder.AppendLine("[update_scope_id] = @sync_scope_id,");
            stringBuilder.AppendLine("[sync_row_is_tombstone] = 0,");
            stringBuilder.AppendLine("[last_change_datetime] = datetime('now')");
            stringBuilder.AppendLine($"WHERE {SqliteManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKeys, "")}");
            stringBuilder.AppendLine($" AND (select changes()) > 0;");

            var cmdtext = stringBuilder.ToString();

            this.AddName(DbCommandType.UpdateRow, cmdtext);

        }


        //private void CreateUpdateCommandText(bool hasMutableColumns)
        //{
        //    var stringBuilderArguments = new StringBuilder();
        //    var stringBuilderParameters = new StringBuilder();
        //    string empty = string.Empty;


        //    // Generate Update command
        //    var stringBuilder = new StringBuilder();
        //    //if (hasMutableColumns)
        //    //{

        //    //    stringBuilder.AppendLine($"UPDATE {tableName.Quoted().ToString()}");
        //    //    stringBuilder.Append($"SET {SqliteManagementUtils.CommaSeparatedUpdateFromParameters(this.TableDescription)}");
        //    //    stringBuilder.Append($"WHERE {SqliteManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKeys, "")}");
        //    //    stringBuilder.AppendLine($" AND (");
        //    //    stringBuilder.AppendLine($"       EXISTS (");
        //    //    stringBuilder.AppendLine($"         SELECT * FROM {trackingName.Quoted().ToString()} ");
        //    //    stringBuilder.AppendLine($"         WHERE {SqliteManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKeys, "")}");
        //    //    stringBuilder.AppendLine($"         AND (timestamp < @sync_min_timestamp OR update_scope_id = @sync_scope_id)");
        //    //    stringBuilder.AppendLine($"         )");
        //    //    stringBuilder.AppendLine($"       OR @sync_force_write = 1");
        //    //    stringBuilder.AppendLine($"      );");
        //    //    stringBuilder.AppendLine($"");
        //    //    stringBuilder.AppendLine($"UPDATE OR IGNORE {trackingName.Quoted().ToString()} SET ");
        //    //    stringBuilder.AppendLine("[update_scope_id] = @sync_scope_id,");
        //    //    stringBuilder.AppendLine("[sync_row_is_tombstone] = 0,");
        //    //    stringBuilder.AppendLine("[last_change_datetime] = datetime('now')");
        //    //    stringBuilder.AppendLine($"WHERE {SqliteManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKeys, "")}");
        //    //    stringBuilder.AppendLine($" AND (select changes()) > 0;") ;
        //    //}

        //    // Generate Insert or replace command
        //    foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false, true))
        //    {
        //        var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
        //        var unquotedColumnName = ParserName.Parse(mutableColumn).Unquoted().Normalized().ToString();

        //        stringBuilderArguments.Append(string.Concat(empty, columnName));
        //        stringBuilderParameters.Append(string.Concat(empty, $"@{unquotedColumnName}"));

        //        empty = ", ";
        //    }
        //    stringBuilder.AppendLine($"INSERT OR REPLACE INTO {tableName.Quoted().ToString()}");
        //    stringBuilder.AppendLine($"({stringBuilderArguments.ToString()})");
        //    stringBuilder.AppendLine($"SELECT {stringBuilderParameters.ToString()} ");
        //    stringBuilder.AppendLine($"WHERE (");
        //    stringBuilder.AppendLine($"       EXISTS (");
        //    stringBuilder.AppendLine($"         SELECT * FROM {trackingName.Quoted().ToString()} ");
        //    stringBuilder.AppendLine($"         WHERE {SqliteManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKeys, "")}");
        //    stringBuilder.AppendLine($"         AND (timestamp < @sync_min_timestamp OR update_scope_id = @sync_scope_id)");
        //    stringBuilder.AppendLine($"         )");
        //    stringBuilder.AppendLine($"        OR NOT EXISTS ( ");
        //    stringBuilder.AppendLine($"         SELECT * FROM {trackingName.Quoted().ToString()} ");
        //    stringBuilder.AppendLine($"         WHERE ({SqliteManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKeys, "")})");
        //    stringBuilder.AppendLine($"         )");
        //    stringBuilder.AppendLine($"       OR @sync_force_write = 1");
        //    stringBuilder.AppendLine($"      );");
        //    stringBuilder.AppendLine();
        //    stringBuilder.AppendLine($"UPDATE OR IGNORE {trackingName.Quoted().ToString()} SET ");
        //    stringBuilder.AppendLine("[update_scope_id] = @sync_scope_id,");
        //    stringBuilder.AppendLine("[sync_row_is_tombstone] = 0,");
        //    stringBuilder.AppendLine("[last_change_datetime] = datetime('now')");
        //    stringBuilder.AppendLine($"WHERE {SqliteManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKeys, "")}");
        //    stringBuilder.AppendLine($" AND (select changes()) > 0;");

        //    var cmdtext = stringBuilder.ToString();

        //    this.AddName(DbCommandType.UpdateRow, cmdtext);

        //}


        private void CreateUpdatedMetadataCommandText(bool hasMutableColumns)
        {
            var stringBuilder = new StringBuilder();
            var stringBuilderArguments = new StringBuilder();
            var stringBuilderParameters = new StringBuilder();

            stringBuilder.AppendLine($"\tINSERT OR REPLACE INTO {trackingName.Quoted().ToString()}");

            string empty = string.Empty;
            foreach (var pkColumn in this.TableDescription.PrimaryKeys)
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                var unquotedColumnName = ParserName.Parse(pkColumn).Unquoted().Normalized().ToString();

                stringBuilderArguments.Append(string.Concat(empty, columnName));
                stringBuilderParameters.Append(string.Concat(empty, $"@{unquotedColumnName}"));
                empty = ", ";
            }
            stringBuilder.AppendLine($"\t({stringBuilderArguments.ToString()}, ");
            stringBuilder.AppendLine($"\t[update_scope_id], [sync_row_is_tombstone], [timestamp], [last_change_datetime])");
            stringBuilder.AppendLine($"\tVALUES ({stringBuilderParameters.ToString()}, ");
            stringBuilder.AppendLine($"\t@sync_scope_id, @sync_row_is_tombstone, {SqliteObjectNames.TimestampValue}, datetime('now'));");

            this.AddName(DbCommandType.UpdateMetadata, stringBuilder.ToString());

        }

        private void CreateDeleteMetadataCommandText()
        {

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"DELETE FROM {trackingName.Quoted().ToString()} ");
            stringBuilder.Append($"WHERE ");
            stringBuilder.AppendLine(SqliteManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKeys, ""));
            stringBuilder.Append(";");

            this.AddName(DbCommandType.DeleteMetadata, stringBuilder.ToString());
        }
        private void CreateDeleteCommandText()
        {
            var stringBuilder = new StringBuilder();
            string str1 = SqliteManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "[c]", "[base]");
            string str7 = SqliteManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "[p]", "[side]");

            stringBuilder.AppendLine(";WITH [c] AS (");
            stringBuilder.Append("\tSELECT ");
            foreach (var c in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();
                stringBuilder.Append($"[p].{columnName}, ");
            }
            stringBuilder.AppendLine($"[side].[update_scope_id], [side].[timestamp], [side].[sync_row_is_tombstone]");
            stringBuilder.Append($"\tFROM (SELECT ");
            string comma = "";
            foreach (var c in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();
                var columnParameterName = ParserName.Parse(c).Unquoted().Normalized().ToString();

                stringBuilder.Append($"{comma}@{columnParameterName} as {columnName}");
                comma = ", ";
            }
            stringBuilder.AppendLine($") AS [p]");
            stringBuilder.Append($"\tLEFT JOIN {trackingName.Quoted().ToString()} [side] ON ");
            stringBuilder.AppendLine($"\t{str7}");
            stringBuilder.AppendLine($"\t)");

            stringBuilder.AppendLine($"DELETE FROM {tableName.Quoted().ToString()} ");
            stringBuilder.AppendLine($"WHERE {SqliteManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKeys, "")}");
            stringBuilder.AppendLine($"AND (EXISTS (");
            stringBuilder.AppendLine($"     SELECT * FROM [c] ");
            stringBuilder.AppendLine($"     WHERE {SqliteManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKeys, "[c]")}");
            stringBuilder.AppendLine($"     AND (timestamp < @sync_min_timestamp OR update_scope_id = @sync_scope_id))");
            stringBuilder.AppendLine($"  OR @sync_force_write = 1");
            stringBuilder.AppendLine($" );");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"UPDATE OR IGNORE {trackingName.Quoted().ToString()} SET ");
            stringBuilder.AppendLine("[update_scope_id] = @sync_scope_id,");
            stringBuilder.AppendLine("[sync_row_is_tombstone] = 1,");
            stringBuilder.AppendLine("[last_change_datetime] = datetime('now')");
            stringBuilder.AppendLine($"WHERE {SqliteManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKeys, "")}");
            stringBuilder.AppendLine($" AND (select changes()) > 0");

            var cmdText = stringBuilder.ToString();

            this.AddName(DbCommandType.DeleteRow, cmdText);
        }
        private void CreateSelectRowCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder("SELECT ");
            stringBuilder.AppendLine();
            StringBuilder stringBuilder1 = new StringBuilder();
            string empty = string.Empty;
            foreach (var pkColumn in this.TableDescription.PrimaryKeys)
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                var unquotedColumnName = ParserName.Parse(pkColumn).Unquoted().Normalized().ToString();
                stringBuilder.AppendLine($"\t[side].{columnName}, ");
                stringBuilder1.Append($"{empty}[side].{columnName} = @{unquotedColumnName}");
                empty = " AND ";
            }
            foreach (var mutableColumn in this.TableDescription.GetMutableColumns())
            {
                var nonPkColumnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                stringBuilder.AppendLine($"\t[base].{nonPkColumnName}, ");
            }
            stringBuilder.AppendLine("\t[side].[sync_row_is_tombstone], ");
            stringBuilder.AppendLine("\t[side].[update_scope_id]");

            stringBuilder.AppendLine($"FROM {trackingName.Quoted().ToString()} [side] ");
            stringBuilder.AppendLine($"LEFT JOIN {tableName.Quoted().ToString()} [base] ON ");

            string str = string.Empty;
            foreach (var pkColumn in this.TableDescription.PrimaryKeys)
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                stringBuilder.Append($"{str}[base].{columnName} = [side].{columnName}");
                str = " AND ";
            }
            stringBuilder.AppendLine();
            stringBuilder.Append(string.Concat("WHERE ", stringBuilder1.ToString()));
            stringBuilder.Append(";");
            this.AddName(DbCommandType.SelectRow, stringBuilder.ToString());
        }
        private void CreateSelectChangesCommandText()
        {
            var stringBuilder = new StringBuilder("SELECT ");
            foreach (var pkColumn in this.TableDescription.PrimaryKeys)
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                stringBuilder.AppendLine($"\t[side].{columnName}, ");
            }
            foreach (var mutableColumn in this.TableDescription.GetMutableColumns())
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                stringBuilder.AppendLine($"\t[base].{columnName}, ");
            }
            stringBuilder.AppendLine($"\t[side].[sync_row_is_tombstone], ");
            stringBuilder.AppendLine($"\t[side].[update_scope_id] ");
            stringBuilder.AppendLine($"FROM {trackingName.Quoted().ToString()} [side]");
            stringBuilder.AppendLine($"LEFT JOIN {tableName.Quoted().ToString()} [base]");
            stringBuilder.Append($"ON ");

            string empty = "";
            foreach (var pkColumn in this.TableDescription.PrimaryKeys)
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();

                stringBuilder.Append($"{empty}[base].{columnName} = [side].{columnName}");
                empty = " AND ";
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("WHERE (");
            stringBuilder.AppendLine("\t[side].[timestamp] > @sync_min_timestamp");
            stringBuilder.AppendLine("\tAND ([side].[update_scope_id] <> @sync_scope_id OR [side].[update_scope_id] IS NULL)");

            //stringBuilder.AppendLine("\tAND (([side].[sync_row_is_frozen] = 0 AND ([side].[update_scope_id] <> @sync_scope_id OR [side].[update_scope_id] IS NULL))");
            //stringBuilder.AppendLine("\tOR ([side].[sync_row_is_frozen] = 1 AND [side].[update_scope_id] <> @sync_scope_id AND [side].[update_scope_id] IS NOT NULL))");
            stringBuilder.AppendLine(")");

            this.AddName(DbCommandType.SelectChanges, stringBuilder.ToString());
            this.AddName(DbCommandType.SelectChangesWithFilters, stringBuilder.ToString());
        }


        private void CreateSelectInitializedCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder("SELECT ");
            foreach (var pkColumn in this.TableDescription.PrimaryKeys)
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                stringBuilder.AppendLine($"\t[base].{columnName}, ");
            }
            var columns = this.TableDescription.GetMutableColumns().ToList();

            for (var i = 0; i < columns.Count; i++)
            {
                var mutableColumn = columns[i];
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                stringBuilder.Append($"\t[base].{columnName}");

                if (i < columns.Count - 1)
                    stringBuilder.AppendLine(", ");
            }
            stringBuilder.AppendLine($"FROM {tableName.Quoted().ToString()} [base]");


            this.AddName(DbCommandType.SelectInitializedChanges, stringBuilder.ToString());
            this.AddName(DbCommandType.SelectInitializedChangesWithFilters, stringBuilder.ToString());
        }

    }
}
