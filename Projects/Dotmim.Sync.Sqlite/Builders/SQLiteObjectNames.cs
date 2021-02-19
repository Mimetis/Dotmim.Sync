using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Text;

using System.Linq;

using System.Data;

namespace Dotmim.Sync.Sqlite
{
    public class SqliteObjectNames
    {
        public const string TimestampValue = "replace(strftime('%Y%m%d%H%M%f', 'now'), '.', '')";

        internal const string insertTriggerName = "[{0}_insert_trigger]";
        internal const string updateTriggerName = "[{0}_update_trigger]";
        internal const string deleteTriggerName = "[{0}_delete_trigger]";

        private Dictionary<DbCommandType, string> commandNames = new Dictionary<DbCommandType, string>();
        Dictionary<DbTriggerType, string> triggersNames = new Dictionary<DbTriggerType, string>();

        private ParserName tableName, trackingName;

        public SyncTable TableDescription { get; }
        public SyncSetup Setup { get; }

        public void AddCommandName(DbCommandType objectType, string name)
        {
            if (commandNames.ContainsKey(objectType))
                throw new Exception("Yous can't add an objectType multiple times");

            commandNames.Add(objectType, name);
        }
        public string GetCommandName(DbCommandType objectType, SyncFilter filter = null)
        {
            if (!commandNames.ContainsKey(objectType))
                throw new NotSupportedException($"Sqlite provider does not support the command type {objectType.ToString()}");

            var commandName = commandNames[objectType];

            // concat filter name
            //if (filter != null)
            //    commandName = string.Format(commandName, filter.GetFilterName());

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

            //// concat filter name
            //if (filter != null)
            //    commandName = string.Format(commandName, filter.GetFilterName());

            return commandName;
        }



        public SqliteObjectNames(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup)
        {
            this.TableDescription = tableDescription;
            this.Setup = setup;
            this.tableName = tableName;
            this.trackingName = trackingName;

            SetDefaultNames();
        }

        /// <summary>
        /// Set the default stored procedures names
        /// </summary>
        private void SetDefaultNames()
        {
            var tpref = this.Setup.TriggersPrefix != null ? this.Setup.TriggersPrefix : "";
            var tsuf = this.Setup.TriggersSuffix != null ? this.Setup.TriggersSuffix : "";

            this.AddTriggerName(DbTriggerType.Insert, string.Format(insertTriggerName, $"{tpref}{tableName.Unquoted().Normalized().ToString()}{tsuf}"));
            this.AddTriggerName(DbTriggerType.Update, string.Format(updateTriggerName, $"{tpref}{tableName.Unquoted().Normalized().ToString()}{tsuf}"));
            this.AddTriggerName(DbTriggerType.Delete, string.Format(deleteTriggerName, $"{tpref}{tableName.Unquoted().Normalized().ToString()}{tsuf}"));

            // Check if we have mutables columns
            var hasMutableColumns = TableDescription.GetMutableColumns(false).Any();


            // Select changes
            this.CreateSelectChangesCommandText();
            this.CreateSelectRowCommandText();
            this.CreateSelectInitializedCommandText();
            this.CreateDeleteCommandText();
            this.CreateDeleteMetadataCommandText();
            this.CreateUpdateCommandText(hasMutableColumns);
            this.CreateResetCommandText();
            this.CreateUpdateUntrackedRowsCommandText();
            this.CreateUpdateMetadataCommandText();

            // Sqlite does not have any constraints, so just return a simple statement
            this.AddCommandName(DbCommandType.DisableConstraints, "Select 0"); // PRAGMA foreign_keys = OFF
            this.AddCommandName(DbCommandType.EnableConstraints, "Select 0");

        }

        private void CreateResetCommandText()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"DELETE FROM {tableName.Quoted().ToString()};");
            stringBuilder.AppendLine($"DELETE FROM {trackingName.Quoted().ToString()};");
            this.AddCommandName(DbCommandType.Reset, stringBuilder.ToString());
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
            foreach (var pkColumn in TableDescription.GetPrimaryKeysColumns())
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

            stringBuilder.AppendLine($"INSERT OR REPLACE INTO {trackingName.Schema().Quoted().ToString()} (");
            stringBuilder.AppendLine(pkeySelectForInsert.ToString());
            stringBuilder.AppendLine(",[update_scope_id], [sync_row_is_tombstone], [timestamp], [last_change_datetime] )");
            stringBuilder.AppendLine($"SELECT {pkeyISelectForInsert.ToString()} ");
            stringBuilder.AppendLine($"   , i.sync_scope_id, i.sync_row_is_tombstone, i.sync_timestamp, i.UtcDate");
            stringBuilder.AppendLine("FROM (");
            stringBuilder.AppendLine($"  SELECT {pkeyAliasSelectForInsert}");
            stringBuilder.AppendLine($"          ,@sync_scope_id as sync_scope_id, @sync_row_is_tombstone as sync_row_is_tombstone, {SqliteObjectNames.TimestampValue} as sync_timestamp, datetime('now') as UtcDate) as i;");


            var cmdtext = stringBuilder.ToString();

            this.AddCommandName(DbCommandType.UpdateMetadata, cmdtext);
        }

        private void CreateUpdateCommandText(bool hasMutableColumns)
        {
            var stringBuilderArguments = new StringBuilder();
            var stringBuilderParameters = new StringBuilder();
            var stringBuilderParametersValues = new StringBuilder();
            string empty = string.Empty;

            string str1 = SqliteManagementUtils.JoinOneTablesOnParametersValues(this.TableDescription.PrimaryKeys, "[side]");
            string str2 = SqliteManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "[c]", "[base]");
            string str7 = SqliteManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "[p]", "[side]");

            // Generate Update command
            var stringBuilder = new StringBuilder();

            foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false, true))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                var columnParameterName = ParserName.Parse(mutableColumn).Unquoted().Normalized().ToString();
                stringBuilderParametersValues.Append($"{empty}@{columnParameterName} as {columnName}");
                stringBuilderArguments.Append($"{empty}{columnName}");
                stringBuilderParameters.Append($"{empty}[c].{columnName}");
                empty = ", ";
            }

            stringBuilder.AppendLine($"INSERT OR REPLACE INTO {tableName.Quoted().ToString()}");
            stringBuilder.AppendLine($"({stringBuilderArguments.ToString()})");
            stringBuilder.AppendLine($"SELECT {stringBuilderParameters.ToString()} ");
            stringBuilder.AppendLine($"FROM (SELECT {stringBuilderParametersValues.ToString()}) as [c]");
            stringBuilder.AppendLine($"LEFT JOIN {trackingName.Quoted().ToString()} AS [side] ON {str1}");
            stringBuilder.AppendLine($"LEFT JOIN {tableName.Quoted().ToString()} AS [base] ON {str2}");

            stringBuilder.Append($"WHERE ({SqliteManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKeys, "[base]")} ");
            stringBuilder.AppendLine($"AND ([side].[timestamp] < @sync_min_timestamp OR [side].[update_scope_id] = @sync_scope_id)) ");
            stringBuilder.Append($"OR ({SqliteManagementUtils.WhereColumnIsNull(this.TableDescription.PrimaryKeys, "[base]")} ");
            stringBuilder.AppendLine($"AND ([side].[timestamp] < @sync_min_timestamp OR [side].[timestamp] IS NULL)) ");
            stringBuilder.AppendLine($"OR @sync_force_write = 1;");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"UPDATE OR IGNORE {trackingName.Quoted().ToString()} SET ");
            stringBuilder.AppendLine("[update_scope_id] = @sync_scope_id,");
            stringBuilder.AppendLine("[sync_row_is_tombstone] = 0,");
            stringBuilder.AppendLine("[last_change_datetime] = datetime('now')");
            stringBuilder.AppendLine($"WHERE {SqliteManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKeys, "")}");
            stringBuilder.AppendLine($" AND (select changes()) > 0;");

            var cmdtext = stringBuilder.ToString();

            this.AddCommandName(DbCommandType.UpdateRow, cmdtext);
        }

        //private void CreateUpdateCommandTextBack(bool hasMutableColumns)
        //{
        //    var stringBuilderArguments = new StringBuilder();
        //    var stringBuilderParameters = new StringBuilder();
        //    string empty = string.Empty;

        //    string str1 = SqliteManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "[c]", "[base]");
        //    string str7 = SqliteManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "[p]", "[side]");

        //    // Generate Update command
        //    var stringBuilder = new StringBuilder();


        //    stringBuilder.AppendLine(";WITH [c] AS (");
        //    stringBuilder.Append("\tSELECT ");
        //    foreach (var c in this.TableDescription.Columns.Where(col => !col.IsReadOnly))
        //    {
        //        var columnName = ParserName.Parse(c).Quoted().ToString();
        //        stringBuilder.Append($"[p].{columnName}, ");
        //    }
        //    stringBuilder.AppendLine();
        //    stringBuilder.AppendLine($"\t[side].[update_scope_id], [side].[timestamp], [side].[sync_row_is_tombstone]");
        //    stringBuilder.AppendLine($"\tFROM (SELECT ");
        //    stringBuilder.Append($"\t\t ");
        //    string comma = "";
        //    foreach (var c in this.TableDescription.GetMutableColumns(false, true))
        //    {
        //        var columnName = ParserName.Parse(c).Quoted().ToString();
        //        var columnParameterName = ParserName.Parse(c).Unquoted().Normalized().ToString();

        //        stringBuilder.Append($"{comma}@{columnParameterName} as {columnName}");
        //        comma = ", ";
        //    }
        //    stringBuilder.AppendLine($") AS [p]");
        //    stringBuilder.Append($"\tLEFT JOIN {trackingName.Quoted().ToString()} [side] ON ");
        //    stringBuilder.AppendLine($"\t{str7}");
        //    stringBuilder.AppendLine($"\t)");


        //    foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false, true))
        //    {
        //        var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
        //        stringBuilderArguments.Append($"{empty}{columnName}");
        //        stringBuilderParameters.Append($"{empty}[c].{columnName}");
        //        empty = ", ";
        //    }

        //    stringBuilder.AppendLine($"INSERT OR REPLACE INTO {tableName.Quoted().ToString()}");
        //    stringBuilder.AppendLine($"({stringBuilderArguments.ToString()})");
        //    stringBuilder.AppendLine($"SELECT {stringBuilderParameters.ToString()} ");
        //    stringBuilder.AppendLine($"FROM [c] ");
        //    stringBuilder.AppendLine($"LEFT JOIN {tableName.Quoted().ToString()} AS [base] ON {str1}");
        //    stringBuilder.Append($"WHERE ({SqliteManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKeys, "[base]")} ");
        //    stringBuilder.AppendLine($"AND ([c].[timestamp] < @sync_min_timestamp OR [c].[update_scope_id] = @sync_scope_id)) ");
        //    stringBuilder.Append($"OR ({SqliteManagementUtils.WhereColumnIsNull(this.TableDescription.PrimaryKeys, "[base]")} ");
        //    stringBuilder.AppendLine($"AND ([c].[timestamp] < @sync_min_timestamp OR [c].[timestamp] IS NULL)) ");
        //    stringBuilder.AppendLine($"OR @sync_force_write = 1;");
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
              

        private void CreateDeleteMetadataCommandText()
        {

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"DELETE FROM {trackingName.Quoted().ToString()} WHERE [timestamp] < @sync_row_timestamp;");

            this.AddCommandName(DbCommandType.DeleteMetadata, stringBuilder.ToString());
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
            stringBuilder.AppendLine($"     AND (timestamp < @sync_min_timestamp OR timestamp IS NULL OR update_scope_id = @sync_scope_id))");
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

            this.AddCommandName(DbCommandType.DeleteRow, cmdText);
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
        private void CreateSelectChangesCommandText()
        {
            var stringBuilder = new StringBuilder("SELECT ");
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
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
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();

                stringBuilder.Append($"{empty}[base].{columnName} = [side].{columnName}");
                empty = " AND ";
            }
            stringBuilder.AppendLine();
            //stringBuilder.AppendLine("WHERE (");
            //stringBuilder.AppendLine("\t[side].[timestamp] > @sync_min_timestamp");
            //stringBuilder.AppendLine("\tAND ([side].[update_scope_id] <> @sync_scope_id OR [side].[update_scope_id] IS NULL)");
            //stringBuilder.AppendLine(")");

            // Looking at discussion https://github.com/Mimetis/Dotmim.Sync/discussions/453, trying to remove ([side].[update_scope_id] <> @sync_scope_id)
            // since we are sure that sqlite will never be a server side database

            stringBuilder.AppendLine("WHERE ([side].[timestamp] > @sync_min_timestamp AND [side].[update_scope_id] IS NULL)");


            this.AddCommandName(DbCommandType.SelectChanges, stringBuilder.ToString());
            this.AddCommandName(DbCommandType.SelectChangesWithFilters, stringBuilder.ToString());
        }


        private void CreateSelectInitializedCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder("SELECT ");
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
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


            this.AddCommandName(DbCommandType.SelectInitializedChanges, stringBuilder.ToString());
            this.AddCommandName(DbCommandType.SelectInitializedChangesWithFilters, stringBuilder.ToString());
        }

        private void CreateUpdateUntrackedRowsCommandText()
        {
            var stringBuilder = new StringBuilder();
            var str1 = new StringBuilder();
            var str2 = new StringBuilder();
            var str3 = new StringBuilder();
            var str4 = SqliteManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "[side]", "[base]");

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
            stringBuilder.AppendLine($", [update_scope_id], [sync_row_is_tombstone], [timestamp], [last_change_datetime]");
            stringBuilder.AppendLine($")");
            stringBuilder.Append($"SELECT ");
            stringBuilder.Append(str2.ToString());
            stringBuilder.AppendLine($", NULL, 0, {SqliteObjectNames.TimestampValue}, datetime('now')");
            stringBuilder.AppendLine($"FROM {tableName.Schema().Quoted().ToString()} as [base] WHERE NOT EXISTS");
            stringBuilder.Append($"(SELECT ");
            stringBuilder.Append(str3.ToString());
            stringBuilder.AppendLine($" FROM {trackingName.Schema().Quoted().ToString()} as [side] ");
            stringBuilder.AppendLine($"WHERE {str4})");

            var r = stringBuilder.ToString();

            this.AddCommandName(DbCommandType.UpdateUntrackedRows, r);

        }

    }
}
