﻿using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Text;

using System.Linq;

using System.Data;
using System.Xml;

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



        public SqliteObjectNames(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup, string scopeName)
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
            var tpref = this.Setup.TriggersPrefix != null ? this.Setup.TriggersPrefix : "";
            var tsuf = this.Setup.TriggersSuffix != null ? this.Setup.TriggersSuffix : "";

            this.AddTriggerName(DbTriggerType.Insert, string.Format(insertTriggerName, $"{tpref}{tableName.Unquoted().Normalized().ToString()}{tsuf}"));
            this.AddTriggerName(DbTriggerType.Update, string.Format(updateTriggerName, $"{tpref}{tableName.Unquoted().Normalized().ToString()}{tsuf}"));
            this.AddTriggerName(DbTriggerType.Delete, string.Format(deleteTriggerName, $"{tpref}{tableName.Unquoted().Normalized().ToString()}{tsuf}"));

            // Select changes
            this.CreateSelectChangesCommandText();
            this.CreateSelectRowCommandText();
            this.CreateSelectInitializedCommandText();
            this.CreateDeleteCommandText();
            this.CreateDeleteMetadataCommandText();
            this.CreateUpdateCommandText();
            this.CreateInitializeCommandText();
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
            stringBuilder.Append("DELETE FROM ").Append(tableName.Quoted().ToString()).AppendLine(";");
            stringBuilder.Append("DELETE FROM ").Append(trackingName.Quoted().ToString()).AppendLine(";");
            this.AddCommandName(DbCommandType.Reset, stringBuilder.ToString());
        }

        private void CreateSelectMetadataCommandText()
        {
            var stringBuilder = new StringBuilder();
            var pkeysSelect = new StringBuilder();
            var pkeysWhere = new StringBuilder();


            string and = string.Empty;
            string comma = string.Empty;
            foreach (var pkColumn in TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                var parameterName = ParserName.Parse(pkColumn).Unquoted().Normalized().ToString();
                pkeysSelect.Append(comma).Append("[side].").Append(columnName);

                pkeysWhere.Append(and).Append("[side].").Append(columnName).Append(" = @").Append(parameterName);

                and = " AND ";
                comma = ", ";
            }

            stringBuilder.Append("SELECT ").Append(pkeysSelect).AppendLine(", [side].[update_scope_id], [side].[timestamp], [side].[sync_row_is_tombstone]");
            stringBuilder.Append("FROM ").Append(trackingName.Quoted().ToString()).AppendLine(" [side]");
            stringBuilder.Append("WHERE ").Append(pkeysWhere).AppendLine();

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
            foreach (var pkColumn in TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                var parameterName = ParserName.Parse(pkColumn).Unquoted().Normalized().ToString();

                pkeySelectForInsert.Append(comma).Append(columnName);
                pkeyISelectForInsert.Append(comma).Append("[i].").Append(columnName);
                pkeyAliasSelectForInsert.Append(comma).Append('@').Append(parameterName).Append(" as ").Append(columnName);
                pkeysLeftJoinForInsert.Append(and).Append("[side].").Append(columnName).Append(" = [i].").Append(columnName);
                pkeysIsNullForInsert.Append(and).Append("[side].").Append(columnName).Append(" IS NULL");
                and = " AND ";
                comma = ", ";
            }

            stringBuilder.Append("INSERT OR REPLACE INTO ").Append(trackingName.Quoted().ToString()).AppendLine(" (");
            stringBuilder.AppendLine(pkeySelectForInsert.ToString());
            stringBuilder.AppendLine(",[update_scope_id], [sync_row_is_tombstone], [timestamp], [last_change_datetime] )");
            stringBuilder.Append("SELECT ").Append(pkeyISelectForInsert).AppendLine(" ");
            stringBuilder.AppendLine($"   , i.sync_scope_id, i.sync_row_is_tombstone, i.sync_timestamp, i.UtcDate");
            stringBuilder.AppendLine("FROM (");
            stringBuilder.Append("  SELECT ").Append(pkeyAliasSelectForInsert).AppendLine();
            stringBuilder.AppendLine($"          ,@sync_scope_id as sync_scope_id, @sync_row_is_tombstone as sync_row_is_tombstone, {SqliteObjectNames.TimestampValue} as sync_timestamp, datetime('now') as UtcDate) as i;");


            var cmdtext = stringBuilder.ToString();

            this.AddCommandName(DbCommandType.UpdateMetadata, cmdtext);
        }

        private void CreateInitializeCommandText()
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
                stringBuilderParametersValues.Append(empty).Append('@').Append(columnParameterName).Append(" as ").Append(columnName);
                stringBuilderParametersValues2.Append(empty).Append('@').Append(columnParameterName);
                stringBuilderArguments.Append(empty).Append(columnName);
                stringBuilderParameters.Append(empty).Append("[c].").Append(columnName);
                empty = ", ";
            }

            stringBuilder.Append("INSERT OR REPLACE INTO ").Append(tableName.Quoted()).AppendLine();
            stringBuilder.Append('(').Append(stringBuilderArguments).AppendLine(")");
            stringBuilder.Append("VALUES (").Append(stringBuilderParametersValues2).Append(") ");
            stringBuilder.AppendLine($";");

            stringBuilder.Append("UPDATE ").Append(trackingName.Quoted()).AppendLine(" SET ");
            stringBuilder.AppendLine("[update_scope_id] = @sync_scope_id,");
            stringBuilder.AppendLine("[sync_row_is_tombstone] = 0,");
            stringBuilder.AppendLine("[last_change_datetime] = datetime('now')");
            stringBuilder.Append("WHERE ").AppendLine(SqliteManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKeys, ""));
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

                stringBuilderParametersValues.Append(empty).Append('@').Append(columnParameterName).Append(" as ").Append(columnName);
                stringBuilderArguments.Append(empty).Append(columnName);
                stringBuilderParameters.Append(empty).Append("[c].").Append(columnName);
                empty = "\n, ";
            }

            // create update statement without PK
            var emptyUpdate = string.Empty;
            var columnsToUpdate = false;
            var stringBuilderUpdateSet = new StringBuilder();
            foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false, false))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                stringBuilderUpdateSet.Append(emptyUpdate).Append(columnName).Append("=excluded.").Append(columnName);
                emptyUpdate = "\n, ";

                columnsToUpdate = true;
            }

            var primaryKeys = string.Join(",",
                this.TableDescription.PrimaryKeys.Select(name => ParserName.Parse(name).Quoted().ToString()));

            // add CTE
            stringBuilder.Append("WITH CHANGESET as (SELECT ").Append(stringBuilderParameters).AppendLine(" ");
            stringBuilder.Append("FROM (SELECT ").Append(stringBuilderParametersValues).AppendLine(") as [c]");
            stringBuilder.Append("LEFT JOIN ").Append(trackingName.Quoted().ToString()).Append(" AS [side] ON ").AppendLine(str1);
            stringBuilder.Append("LEFT JOIN ").Append(tableName.Quoted().ToString()).Append(" AS [base] ON ").AppendLine(str2);
            stringBuilder.AppendLine($"WHERE ([side].[timestamp] < @sync_min_timestamp OR [side].[update_scope_id] = @sync_scope_id) ");
            stringBuilder.Append("OR (").Append(SqliteManagementUtils.WhereColumnIsNull(this.TableDescription.PrimaryKeys, "[base]")).Append(' ');
            stringBuilder.AppendLine($"AND ([side].[timestamp] < @sync_min_timestamp OR [side].[timestamp] IS NULL)) ");
            stringBuilder.Append($"OR @sync_force_write = 1");
            stringBuilder.AppendLine($")");

            stringBuilder.Append("INSERT INTO ").AppendLine(tableName.Quoted().ToString());
            stringBuilder.Append('(').Append(stringBuilderArguments).AppendLine(")");
            // use CTE here. The CTE is required in order to make the "ON CONFLICT" statement work. Otherwise SQLite cannot parse it
            // Note, that we have to add the pseudo WHERE TRUE clause here, as otherwise the SQLite parser may confuse the following ON
            // with a join clause, thus, throwing a parsing error
            // See a detailed explanation here at the official SQLite documentation: "Parsing Ambiguity" on page https://www.sqlite.org/lang_UPSERT.html
            stringBuilder.AppendLine($" SELECT * from CHANGESET WHERE TRUE");
            if (columnsToUpdate)
            {
                stringBuilder.Append(" ON CONFLICT (").Append(primaryKeys).AppendLine(") DO UPDATE SET ");
                stringBuilder.Append(stringBuilderUpdateSet.ToString()).AppendLine(";");
            }
            else
                stringBuilder.Append(" ON CONFLICT (").Append(primaryKeys).AppendLine(") DO NOTHING; ");

            stringBuilder.AppendLine();
            stringBuilder.AppendLine();
            stringBuilder.Append("UPDATE OR IGNORE ").Append(trackingName.Quoted().ToString()).AppendLine(" SET ");
            stringBuilder.AppendLine($"[update_scope_id] = @sync_scope_id,");
            stringBuilder.AppendLine($"[sync_row_is_tombstone] = 0,");
            stringBuilder.AppendLine($"[timestamp] = {SqliteObjectNames.TimestampValue},");
            stringBuilder.AppendLine($"[last_change_datetime] = datetime('now')");
            stringBuilder.Append("WHERE ").AppendLine(SqliteManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKeys, ""));
            stringBuilder.AppendLine($" AND (select changes()) > 0;");

            var cmdtext = stringBuilder.ToString();

            this.AddCommandName(DbCommandType.UpdateRow, cmdtext);
            this.AddCommandName(DbCommandType.UpdateRows, cmdtext);
        }

       
        private void CreateDeleteMetadataCommandText()
        {

            var stringBuilder = new StringBuilder();
            stringBuilder.Append("DELETE FROM ").Append(trackingName.Quoted().ToString()).AppendLine(" WHERE [timestamp] < @sync_row_timestamp;");

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
                stringBuilder.Append("[p].").Append(columnName).Append(", ");
            }
            stringBuilder.AppendLine($"[side].[update_scope_id] as [sync_update_scope_id], [side].[timestamp] as [sync_timestamp], [side].[sync_row_is_tombstone]");
            stringBuilder.Append($"\tFROM (SELECT ");
            string comma = "";
            foreach (var c in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();
                var columnParameterName = ParserName.Parse(c).Unquoted().Normalized().ToString();

                stringBuilder.Append(comma).Append('@').Append(columnParameterName).Append(" as ").Append(columnName);
                comma = ", ";
            }
            stringBuilder.AppendLine($") AS [p]");
            stringBuilder.Append("\tLEFT JOIN ").Append(trackingName.Quoted().ToString()).Append(" [side] ON ");
            stringBuilder.Append('\t').AppendLine(str7);
            stringBuilder.AppendLine($"\t)");

            stringBuilder.Append("DELETE FROM ").Append(tableName.Quoted().ToString()).AppendLine(" ");
            stringBuilder.Append("WHERE ").AppendLine(SqliteManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKeys, ""));
            stringBuilder.AppendLine($"AND (EXISTS (");
            stringBuilder.AppendLine($"     SELECT * FROM [c] ");
            stringBuilder.Append("     WHERE ").AppendLine(SqliteManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKeys, "[c]"));
            stringBuilder.AppendLine($"     AND ([sync_timestamp] < @sync_min_timestamp OR [sync_timestamp] IS NULL OR [sync_update_scope_id] = @sync_scope_id))");
            stringBuilder.AppendLine($"  OR @sync_force_write = 1");
            stringBuilder.AppendLine($" );");
            stringBuilder.AppendLine();
            stringBuilder.Append("UPDATE OR IGNORE ").Append(trackingName.Quoted().ToString()).AppendLine(" SET ");
            stringBuilder.AppendLine("[update_scope_id] = @sync_scope_id,");
            stringBuilder.AppendLine("[sync_row_is_tombstone] = 1,");
            stringBuilder.AppendLine("[last_change_datetime] = datetime('now')");
            stringBuilder.Append("WHERE ").AppendLine(SqliteManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKeys, ""));
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
                stringBuilder1.Append(empty).Append("[side].").Append(columnName).Append(" = @").Append(unquotedColumnName);
                empty = " AND ";
            }
            foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false, true))
            {
                var nonPkColumnName = ParserName.Parse(mutableColumn).Quoted().ToString();

                var isPrimaryKey = this.TableDescription.PrimaryKeys.Any(pkey => mutableColumn.ColumnName.Equals(pkey, SyncGlobalization.DataSourceStringComparison));

                if (isPrimaryKey)
                    stringBuilder.Append("\t[side].").Append(nonPkColumnName).AppendLine(", ");
                else
                    stringBuilder.Append("\t[base].").Append(nonPkColumnName).AppendLine(", ");

            }
            stringBuilder.AppendLine("\t[side].[sync_row_is_tombstone], ");
            stringBuilder.AppendLine("\t[side].[update_scope_id] as [sync_update_scope_id]");

            stringBuilder.Append("FROM ").Append(trackingName.Quoted().ToString()).AppendLine(" [side] ");
            stringBuilder.Append("LEFT JOIN ").Append(tableName.Quoted().ToString()).AppendLine(" [base] ON ");

            string str = string.Empty;
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                stringBuilder.Append(str).Append("[base].").Append(columnName).Append(" = [side].").Append(columnName);
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

            foreach (var mutableColumn in this.TableDescription.GetMutableColumns(false, true))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();

                var isPrimaryKey = this.TableDescription.PrimaryKeys.Any(pkey => mutableColumn.ColumnName.Equals(pkey, SyncGlobalization.DataSourceStringComparison));

                if (isPrimaryKey)
                    stringBuilder.Append("\t[side].").Append(columnName).AppendLine(", ");
                else
                    stringBuilder.Append("\t[base].").Append(columnName).AppendLine(", ");
            }
            stringBuilder.AppendLine($"\t[side].[sync_row_is_tombstone], ");
            stringBuilder.AppendLine($"\t[side].[update_scope_id] as [sync_update_scope_id] ");
            stringBuilder.Append("FROM ").Append(trackingName.Quoted()).AppendLine(" [side]");
            stringBuilder.Append("LEFT JOIN ").Append(tableName.Quoted()).AppendLine(" [base]");
            stringBuilder.Append($"ON ");

            string empty = "";
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();

                stringBuilder.Append(empty).Append("[base].").Append(columnName).Append(" = [side].").Append(columnName);
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
                stringBuilder.Append("\t[base].").Append(columnName).AppendLine(", ");
            }
            var columns = this.TableDescription.GetMutableColumns().ToList();

            for (var i = 0; i < columns.Count; i++)
            {
                var mutableColumn = columns[i];
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                stringBuilder.Append("\t[base].").Append(columnName);

                if (i < columns.Count - 1)
                    stringBuilder.AppendLine(", ");
            }
            stringBuilder.Append("FROM ").Append(tableName.Quoted().ToString()).AppendLine(" [base]");


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

            stringBuilder.Append("INSERT INTO ").Append(trackingName.Quoted().ToString()).AppendLine(" (");


            var comma = "";
            foreach (var pkeyColumn in TableDescription.GetPrimaryKeysColumns())
            {
                var pkeyColumnName = ParserName.Parse(pkeyColumn).Quoted().ToString();

                str1.Append(comma).Append(pkeyColumnName);
                str2.Append(comma).Append("[base].").Append(pkeyColumnName);
                str3.Append(comma).Append("[side].").Append(pkeyColumnName);

                comma = ", ";
            }
            stringBuilder.Append(str1.ToString());
            stringBuilder.AppendLine($", [update_scope_id], [sync_row_is_tombstone], [timestamp], [last_change_datetime]");
            stringBuilder.AppendLine($")");
            stringBuilder.Append($"SELECT ");
            stringBuilder.Append(str2.ToString());
            stringBuilder.AppendLine($", NULL, 0, {SqliteObjectNames.TimestampValue}, datetime('now')");
            stringBuilder.Append("FROM ").Append(tableName.Quoted().ToString()).AppendLine(" as [base] WHERE NOT EXISTS");
            stringBuilder.Append($"(SELECT ");
            stringBuilder.Append(str3.ToString());
            stringBuilder.Append(" FROM ").Append(trackingName.Quoted().ToString()).AppendLine(" as [side] ");
            stringBuilder.Append("WHERE ").Append(str4).AppendLine(")");

            var r = stringBuilder.ToString();

            this.AddCommandName(DbCommandType.UpdateUntrackedRows, r);

        }

    }
}
