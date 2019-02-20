using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Text;
using Dotmim.Sync.Data;
using System.Linq;
using Dotmim.Sync.Filter;

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

        public DmTable TableDescription { get; }


        public void AddName(DbCommandType objectType, string name)
        {
            if (names.ContainsKey(objectType))
                throw new Exception("Yous can't add an objectType multiple times");

            names.Add(objectType, name);
        }
        public string GetCommandName(DbCommandType objectType, IEnumerable<FilterClause> filters = null)
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

        public SqliteObjectNames(DmTable tableDescription)
        {
            this.TableDescription = tableDescription;
            (tableName, trackingName) = SqliteBuilder.GetParsers(this.TableDescription);

            SetDefaultNames();
        }

        /// <summary>
        /// Set the default stored procedures names
        /// </summary>
        private void SetDefaultNames()
        {
            var tpref = this.TableDescription.TriggersPrefix != null ? this.TableDescription.TriggersPrefix : "";
            var tsuf = this.TableDescription.TriggersSuffix != null ? this.TableDescription.TriggersSuffix : "";

            this.AddName(DbCommandType.InsertTrigger, string.Format(insertTriggerName, $"{tpref}{tableName.Unquoted().Normalized().ToString()}{tsuf}"));
            this.AddName(DbCommandType.UpdateTrigger, string.Format(updateTriggerName, $"{tpref}{tableName.Unquoted().Normalized().ToString()}{tsuf}"));
            this.AddName(DbCommandType.DeleteTrigger, string.Format(deleteTriggerName, $"{tpref}{tableName.Unquoted().Normalized().ToString()}{tsuf}"));

            // Select changes
            this.CreateSelectChangesCommandText();
            this.CreateSelectRowCommandText();
            this.CreateDeleteCommandText();
            this.CreateDeleteMetadataCommandText();
            this.CreateInsertCommandText();
            this.CreateInsertMetadataCommandText();
            this.CreateUpdateCommandText();
            this.CreateUpdatedMetadataCommandText();
            this.CreateResetCommandText();

            // SQLite does not have any constraints, so just return a simple statement
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

        private void CreateUpdateCommandText()
        {

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"UPDATE {tableName.Quoted().ToString()}");
            stringBuilder.Append($"SET {SqliteManagementUtils.CommaSeparatedUpdateFromParameters(this.TableDescription)}");
            stringBuilder.Append($"WHERE {SqliteManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKey.Columns, "")}");
            stringBuilder.AppendLine($" AND ((SELECT [timestamp] FROM {trackingName.Quoted().ToString()} ");
            stringBuilder.AppendLine($"  WHERE {SqliteManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKey.Columns, tableName.Quoted().ToString(), trackingName.Quoted().ToString())}");
            stringBuilder.AppendLine(" ) <= @sync_min_timestamp OR @sync_force_write = 1");
            stringBuilder.AppendLine(");");
            this.AddName(DbCommandType.UpdateRow, stringBuilder.ToString());

        }

        private void CreateUpdatedMetadataCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.AppendLine($"UPDATE {trackingName.Quoted().ToString()}");
            stringBuilder.AppendLine($"SET [update_scope_id] = @update_scope_id, ");
            stringBuilder.AppendLine($"\t [update_timestamp] = @update_timestamp, ");
            stringBuilder.AppendLine($"\t [sync_row_is_tombstone] = @sync_row_is_tombstone, ");
            stringBuilder.AppendLine($"\t [timestamp] = {SqliteObjectNames.TimestampValue}, ");
            stringBuilder.AppendLine($"\t [last_change_datetime] = datetime('now') ");
            stringBuilder.Append($"WHERE {SqliteManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKey.Columns, "")}");

            this.AddName(DbCommandType.UpdateMetadata, stringBuilder.ToString());

        }
        private void CreateInsertMetadataCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            StringBuilder stringBuilderArguments = new StringBuilder();
            StringBuilder stringBuilderParameters = new StringBuilder();

            stringBuilder.AppendLine($"\tINSERT OR REPLACE INTO {trackingName.Quoted().ToString()}");

            string empty = string.Empty;
            foreach (var pkColumn in this.TableDescription.PrimaryKey.Columns)
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                var unquotedColumnName = ParserName.Parse(pkColumn).Unquoted().Normalized().ToString();

                stringBuilderArguments.Append(string.Concat(empty, columnName));
                stringBuilderParameters.Append(string.Concat(empty, $"@{unquotedColumnName}"));
                empty = ", ";
            }
            stringBuilder.AppendLine($"\t({stringBuilderArguments.ToString()}, ");
            stringBuilder.AppendLine($"\t[create_scope_id], [create_timestamp], [update_scope_id], [update_timestamp],");
            stringBuilder.AppendLine($"\t[sync_row_is_tombstone], [timestamp], [last_change_datetime])");
            stringBuilder.AppendLine($"\tVALUES ({stringBuilderParameters.ToString()}, ");
            stringBuilder.AppendLine($"\t@create_scope_id, @create_timestamp, @update_scope_id, @update_timestamp, ");
            stringBuilder.AppendLine($"\t@sync_row_is_tombstone, {SqliteObjectNames.TimestampValue}, datetime('now'));");

            this.AddName(DbCommandType.InsertMetadata, stringBuilder.ToString());

        }
        private void CreateInsertCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            StringBuilder stringBuilderArguments = new StringBuilder();
            StringBuilder stringBuilderParameters = new StringBuilder();
            string empty = string.Empty;
            foreach (var mutableColumn in this.TableDescription.Columns.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                var unquotedColumnName = ParserName.Parse(mutableColumn).Unquoted().Normalized().ToString();

                stringBuilderArguments.Append(string.Concat(empty, columnName));
                stringBuilderParameters.Append(string.Concat(empty, $"@{unquotedColumnName}"));
                empty = ", ";
            }
            stringBuilder.AppendLine($"\tINSERT OR IGNORE INTO {tableName.Quoted().ToString()}");
            stringBuilder.AppendLine($"\t({stringBuilderArguments.ToString()})");
            stringBuilder.AppendLine($"\tVALUES ({stringBuilderParameters.ToString()});");

            this.AddName(DbCommandType.InsertRow, stringBuilder.ToString());

        }
        private void CreateDeleteMetadataCommandText()
        {

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"DELETE FROM {trackingName.Quoted().ToString()} ");
            stringBuilder.Append($"WHERE ");
            stringBuilder.AppendLine(SqliteManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKey.Columns, ""));
            stringBuilder.Append(";");

            this.AddName(DbCommandType.DeleteMetadata, stringBuilder.ToString());
        }
        private void CreateDeleteCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.AppendLine($"DELETE FROM {tableName.Quoted().ToString()} ");
            stringBuilder.Append($"WHERE {SqliteManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKey.Columns, "")}");
            stringBuilder.AppendLine($" AND ((SELECT [timestamp] FROM {trackingName.Quoted().ToString()} ");
            stringBuilder.AppendLine($"  WHERE {SqliteManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKey.Columns, tableName.Quoted().ToString(), trackingName.Quoted().ToString())}");
            stringBuilder.AppendLine(" ) <= @sync_min_timestamp OR @sync_force_write = 1");
            stringBuilder.AppendLine(");");

            this.AddName(DbCommandType.DeleteRow, stringBuilder.ToString());
        }
        private void CreateSelectRowCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder("SELECT ");
            stringBuilder.AppendLine();
            StringBuilder stringBuilder1 = new StringBuilder();
            string empty = string.Empty;
            foreach (var pkColumn in this.TableDescription.PrimaryKey.Columns)
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                var unquotedColumnName = ParserName.Parse(pkColumn).Unquoted().Normalized().ToString();
                stringBuilder.AppendLine($"\t[side].{columnName}, ");
                stringBuilder1.Append($"{empty}[side].{columnName} = @{unquotedColumnName}");
                empty = " AND ";
            }
            foreach (var mutableColumn in this.TableDescription.MutableColumns)
            {
                var nonPkColumnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                stringBuilder.AppendLine($"\t[base].{nonPkColumnName}, ");
            }
            stringBuilder.AppendLine("\t[side].[sync_row_is_tombstone],");
            stringBuilder.AppendLine("\t[side].[create_scope_id],");
            stringBuilder.AppendLine("\t[side].[create_timestamp],");
            stringBuilder.AppendLine("\t[side].[update_scope_id],");
            stringBuilder.AppendLine("\t[side].[update_timestamp]");

            stringBuilder.AppendLine($"FROM {trackingName.Quoted().ToString()} [side] ");
            stringBuilder.AppendLine($"LEFT JOIN {tableName.Quoted().ToString()} [base] ON ");

            string str = string.Empty;
            foreach (var pkColumn in this.TableDescription.PrimaryKey.Columns)
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
            StringBuilder stringBuilder = new StringBuilder("SELECT ");
            foreach (var pkColumn in this.TableDescription.PrimaryKey.Columns)
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                stringBuilder.AppendLine($"\t[side].{columnName}, ");
            }
            foreach (var mutableColumn in this.TableDescription.MutableColumns)
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                stringBuilder.AppendLine($"\t[base].{columnName}, ");
            }
            stringBuilder.AppendLine($"\t[side].[sync_row_is_tombstone], ");
            stringBuilder.AppendLine($"\t[side].[create_scope_id], ");
            stringBuilder.AppendLine($"\t[side].[create_timestamp], ");
            stringBuilder.AppendLine($"\t[side].[update_scope_id], ");
            stringBuilder.AppendLine($"\t[side].[update_timestamp] ");
            stringBuilder.AppendLine($"FROM {trackingName.Quoted().ToString()} [side]");
            stringBuilder.AppendLine($"LEFT JOIN {tableName.Quoted().ToString()} [base]");
            stringBuilder.Append($"ON ");

            string empty = "";
            foreach (var pkColumn in this.TableDescription.PrimaryKey.Columns)
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();

                stringBuilder.Append($"{empty}[base].{columnName} = [side].{columnName}");
                empty = " AND ";
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("WHERE (");
            string str = string.Empty;

            //if (!SqlManagementUtils.IsStringNullOrWhitespace(this._filterClause))
            //{
            //    StringBuilder stringBuilder1 = new StringBuilder();
            //    stringBuilder1.Append("((").Append(this._filterClause).Append(") OR (");
            //    stringBuilder1.Append(SqlSyncProcedureHelper.TrackingTableAlias).Append(".").Append(this._trackingColNames.SyncRowIsTombstone).Append(" = 1 AND ");
            //    stringBuilder1.Append("(");
            //    stringBuilder1.Append(SqlSyncProcedureHelper.TrackingTableAlias).Append(".").Append(this._trackingColNames.UpdateScopeLocalId).Append(" = ").Append(sqlParameter.ParameterName);
            //    stringBuilder1.Append(" OR ");
            //    stringBuilder1.Append(SqlSyncProcedureHelper.TrackingTableAlias).Append(".").Append(this._trackingColNames.UpdateScopeLocalId).Append(" IS NULL");
            //    stringBuilder1.Append(") AND ");
            //    string empty1 = string.Empty;
            //    foreach (DbSyncColumnDescription _filterColumn in this._filterColumns)
            //    {
            //        stringBuilder1.Append(empty1).Append(SqlSyncProcedureHelper.TrackingTableAlias).Append(".").Append(_filterColumn.QuotedName).Append(" IS NULL");
            //        empty1 = " AND ";
            //    }
            //    stringBuilder1.Append("))");
            //    stringBuilder.Append(stringBuilder1.ToString());
            //    str = " AND ";
            //}

            stringBuilder.AppendLine("\t-- Update made by the local instance");
            stringBuilder.AppendLine("\t[side].[update_scope_id] IS NULL");
            stringBuilder.AppendLine("\t-- Or Update different from remote");
            stringBuilder.AppendLine("\tOR [side].[update_scope_id] <> @sync_scope_id");
            stringBuilder.AppendLine("\t-- Or we are in reinit mode so we take rows even thoses updated by the scope");
            stringBuilder.AppendLine("\tOR @sync_scope_is_reinit = 1");
            stringBuilder.AppendLine("    )");
            stringBuilder.AppendLine("AND (");
            stringBuilder.AppendLine("\t-- And Timestamp is > from remote timestamp");
            stringBuilder.AppendLine("\t[side].[timestamp] > @sync_min_timestamp");
            stringBuilder.AppendLine("\tOR");
            stringBuilder.AppendLine("\t-- remote instance is new, so we don't take the last timestamp");
            stringBuilder.AppendLine("\t@sync_scope_is_new = 1");
            stringBuilder.AppendLine("\t)");
            stringBuilder.AppendLine("AND (");
            stringBuilder.AppendLine("\t[side].[sync_row_is_tombstone] = 1 ");
            stringBuilder.AppendLine("\tOR");
            stringBuilder.Append("\t([side].[sync_row_is_tombstone] = 0");

            empty = " AND ";
            foreach (var pkColumn in this.TableDescription.PrimaryKey.Columns)
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                stringBuilder.Append($"{empty}[base].{columnName} is not null");
            }
            stringBuilder.AppendLine("\t)");
            stringBuilder.AppendLine(")");


            this.AddName(DbCommandType.SelectChanges, stringBuilder.ToString());
            this.AddName(DbCommandType.SelectChangesWitFilters, stringBuilder.ToString());
        }

    }
}
