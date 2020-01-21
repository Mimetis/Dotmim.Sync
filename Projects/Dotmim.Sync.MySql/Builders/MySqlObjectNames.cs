using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Text;
using Dotmim.Sync.Data;
using System.Linq;
using Dotmim.Sync.Filter;

namespace Dotmim.Sync.MySql
{
    public class MySqlObjectNames
    {
        public const string TimestampValue = "ROUND(UNIX_TIMESTAMP(CURTIME(4)) * 10000)";

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


        Dictionary<DbCommandType, (string name, bool isStoredProcedure)> names = new Dictionary<DbCommandType, (string name, bool isStoredProcedure)>();
        private ParserName tableName, trackingName;

        public SyncTable TableDescription { get; }


        public void AddName(DbCommandType objectType, string name, bool isStoredProcedure)
        {
            if (names.ContainsKey(objectType))
                throw new Exception("Yous can't add an objectType multiple times");

            names.Add(objectType, (name, isStoredProcedure));
        }
        public (string name, bool isStoredProcedure) GetCommandName(DbCommandType objectType, IEnumerable<SyncFilter> filters = null)
        {
            if (!names.ContainsKey(objectType))
                throw new NotSupportedException($"MySql provider does not support the command type {objectType.ToString()}");


            (var commandName, var isStoredProc) = names[objectType];

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

                commandName = String.Format(commandName, name);
            }

            return (commandName, isStoredProc);
        }

        public MySqlObjectNames(SyncTable tableDescription)
        {
            this.TableDescription = tableDescription;
            (tableName, trackingName) = MyTableSqlBuilder.GetParsers(this.TableDescription);

            SetDefaultNames();
        }

        /// <summary>
        /// Set the default stored procedures names
        /// </summary>
        private void SetDefaultNames()
        {
            var pref = this.TableDescription.Schema.TrackingTablesPrefix != null ? this.TableDescription.Schema.TrackingTablesPrefix : "";
            var suf = this.TableDescription.Schema.TrackingTablesSuffix != null ? this.TableDescription.Schema.TrackingTablesSuffix : "";
            var tpref = this.TableDescription.Schema.TriggersPrefix != null ? this.TableDescription.Schema.TriggersPrefix : "";
            var tsuf = this.TableDescription.Schema.TriggersSuffix != null ? this.TableDescription.Schema.TriggersSuffix : "";

            this.AddName(DbCommandType.InsertTrigger, string.Format(insertTriggerName, $"{tpref}{tableName.Unquoted().Normalized().ToString()}{tsuf}"), true);
            this.AddName(DbCommandType.UpdateTrigger, string.Format(updateTriggerName, $"{tpref}{tableName.Unquoted().Normalized().ToString()}{tsuf}"), true);
            this.AddName(DbCommandType.DeleteTrigger, string.Format(deleteTriggerName, $"{tpref}{tableName.Unquoted().Normalized().ToString()}{tsuf}"), true);

            this.AddName(DbCommandType.SelectChanges, string.Format(selectChangesProcName, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}"), true);
            this.AddName(DbCommandType.SelectChangesWithFilters, string.Format(selectChangesProcNameWithFilters, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}", "{0}"), true);

            this.AddName(DbCommandType.SelectInitializedChanges, string.Format(initializeChangesProcName, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}"), true);
            this.AddName(DbCommandType.SelectInitializedChangesWithFilters, string.Format(initializeChangesProcNameWithFilters, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}", "{0}"), true);

            this.AddName(DbCommandType.SelectRow, string.Format(selectRowProcName, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}"), true);
            this.AddName(DbCommandType.UpdateRow, string.Format(updateProcName, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}"), true);
            this.AddName(DbCommandType.DeleteRow, string.Format(deleteProcName, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}"), true);
            this.AddName(DbCommandType.UpdateMetadata, string.Format(updateMetadataProcName, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}"), true);
            this.AddName(DbCommandType.DeleteMetadata, string.Format(deleteMetadataProcName, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}"), true);
            this.AddName(DbCommandType.Reset, string.Format(resetProcName, $"{pref}{tableName.Unquoted().Normalized().ToString()}{suf}"), true);

            this.AddName(DbCommandType.DisableConstraints, string.Format(disableConstraintsText, ParserName.Parse(TableDescription).Quoted().ToString()), false);
            this.AddName(DbCommandType.EnableConstraints, string.Format(enableConstraintsText, ParserName.Parse(TableDescription).Quoted().ToString()), false);

        }

        private void CreateUpdateCommandText()
        {

            //StringBuilder stringBuilder = new StringBuilder();
            //stringBuilder.AppendLine($"SELECT @ts := `timestamp` ");
            //stringBuilder.AppendLine($"FROM {trackingName.QuotedObjectName}");
            //stringBuilder.AppendLine($"WHERE {MySqlManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKey.Columns, "")};");
            //stringBuilder.AppendLine();
            //stringBuilder.AppendLine($"UPDATE {tableName.QuotedString}");
            //stringBuilder.AppendLine($"SET {MySqlManagementUtils.CommaSeparatedUpdateFromParameters(this.TableDescription)}");
            //stringBuilder.AppendLine($"WHERE {MySqlManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKey.Columns, "")}");
            //stringBuilder.AppendLine($"AND (@ts <= @sync_min_timestamp OR @sync_force_write = 1);");

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"UPDATE {tableName.Quoted().ToString()}");
            stringBuilder.Append($"SET {MySqlManagementUtils.CommaSeparatedUpdateFromParameters(this.TableDescription)}");
            stringBuilder.Append($"WHERE {MySqlManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKeys, "")}");
            stringBuilder.AppendLine($" AND ((SELECT `timestamp` FROM {trackingName.Quoted().ToString()} ");
            stringBuilder.AppendLine($"  WHERE {MySqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, tableName.Quoted().ToString(), trackingName.Quoted().ToString())}");
            stringBuilder.AppendLine(" ) <= @sync_min_timestamp OR @sync_force_write = 1");
            stringBuilder.AppendLine(");");
            this.AddName(DbCommandType.UpdateRow, stringBuilder.ToString(), false);

        }

        private void CreateUpdatedMetadataCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.AppendLine($"UPDATE {trackingName.Quoted().ToString()}");
            stringBuilder.AppendLine($"SET `update_scope_id` = @update_scope_id, ");
            stringBuilder.AppendLine($"\t `update_timestamp` = @update_timestamp, ");
            stringBuilder.AppendLine($"\t `sync_row_is_tombstone` = @sync_row_is_tombstone, ");
            stringBuilder.AppendLine($"\t `timestamp` = {MySqlObjectNames.TimestampValue}, ");
            stringBuilder.AppendLine($"\t `last_change_datetime` = now() ");
            stringBuilder.Append($"WHERE {MySqlManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKeys, "")}");

            this.AddName(DbCommandType.UpdateMetadata, stringBuilder.ToString(), false);

        }
        //private void CreateInsertMetadataCommandText()
        //{
        //    StringBuilder stringBuilder = new StringBuilder();
        //    StringBuilder stringBuilderArguments = new StringBuilder();
        //    StringBuilder stringBuilderParameters = new StringBuilder();

        //    stringBuilder.AppendLine($"\tINSERT INTO {trackingName.Quoted().ToString()}");

        //    string empty = string.Empty;
        //    foreach (var pkColumn in this.TableDescription.PrimaryKeys)
        //    {
        //        var columnName = ParserName.Parse(pkColumn, "`").Quoted().ToString();
        //        var parameterName = ParserName.Parse(pkColumn, "`").Normalized().Unquoted().ToString();

        //        stringBuilderArguments.Append(string.Concat(empty, columnName));
        //        stringBuilderParameters.Append(string.Concat(empty, $"@{parameterName}"));
        //        empty = ", ";
        //    }
        //    stringBuilder.Append($"\t({stringBuilderArguments.ToString()}, ");
        //    stringBuilder.AppendLine($"\t`create_scope_id`, `create_timestamp`, `update_scope_id`, `update_timestamp`,");
        //    stringBuilder.AppendLine($"\t`sync_row_is_tombstone`, `timestamp`, `last_change_datetime`)");
        //    stringBuilder.Append($"\tVALUES ({stringBuilderParameters.ToString()}, ");
        //    stringBuilder.AppendLine($"\t@create_scope_id, @create_timestamp, @update_scope_id, @update_timestamp, ");
        //    stringBuilder.AppendLine($"\t@sync_row_is_tombstone, {MySqlObjectNames.TimestampValue}, now())");
        //    stringBuilder.AppendLine($"\tON DUPLICATE KEY UPDATE");
        //    stringBuilder.AppendLine($"\t `create_scope_id` = @create_scope_id, ");
        //    stringBuilder.AppendLine($"\t `create_timestamp` = @create_timestamp, ");
        //    stringBuilder.AppendLine($"\t `update_scope_id` = @update_scope_id, ");
        //    stringBuilder.AppendLine($"\t `update_timestamp` = @update_timestamp, ");
        //    stringBuilder.AppendLine($"\t `sync_row_is_tombstone` = @sync_row_is_tombstone, ");
        //    stringBuilder.AppendLine($"\t `timestamp` = {MySqlObjectNames.TimestampValue}, ");
        //    stringBuilder.AppendLine($"\t `last_change_datetime` = now() ");
        //    //stringBuilder.AppendLine($"\tWHERE {MySqlManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKey.Columns, "")};");
        //    stringBuilder.AppendLine();

        //    this.AddName(DbCommandType.InsertMetadata, stringBuilder.ToString(), false);

        //}
        //private void CreateInsertCommandText()
        //{
        //    StringBuilder stringBuilder = new StringBuilder();
        //    StringBuilder stringBuilderArguments = new StringBuilder();
        //    StringBuilder stringBuilderParameters = new StringBuilder();
        //    string empty = string.Empty;
        //    foreach (var mutableColumn in this.TableDescription.Columns.Where(c => !c.IsReadOnly))
        //    {
        //        var columnName = ParserName.Parse(mutableColumn, "`").Quoted().ToString();
        //        var parameterName = ParserName.Parse(mutableColumn, "`").Normalized().Unquoted().ToString();

        //        stringBuilderArguments.Append(string.Concat(empty, columnName));
        //        stringBuilderParameters.Append(string.Concat(empty, $"@{parameterName}"));
        //        empty = ", ";
        //    }
        //    stringBuilder.AppendLine($"\tINSERT INTO {tableName.Quoted().ToString()}");
        //    stringBuilder.AppendLine($"\t({stringBuilderArguments.ToString()})");
        //    stringBuilder.AppendLine($"\tVALUES ({stringBuilderParameters.ToString()});");

        //    this.AddName(DbCommandType.InsertRow, stringBuilder.ToString(), false);

        //}
        private void CreateDeleteMetadataCommandText()
        {

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"DELETE FROM {trackingName.Quoted().ToString()} ");
            stringBuilder.Append($"WHERE ");
            stringBuilder.AppendLine(MySqlManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKeys, ""));
            stringBuilder.Append(";");

            this.AddName(DbCommandType.DeleteMetadata, stringBuilder.ToString(), false);
        }
        private void CreateDeleteCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.AppendLine($"DELETE FROM {tableName.Quoted().ToString()} ");
            stringBuilder.Append($"WHERE {MySqlManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKeys, "")}");
            stringBuilder.AppendLine($" AND ((SELECT `timestamp` FROM {trackingName.Quoted().ToString()} ");
            stringBuilder.AppendLine($"  WHERE {MySqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, tableName.Quoted().ToString(), trackingName.Quoted().ToString())}");
            stringBuilder.AppendLine(" ) <= @sync_min_timestamp OR @sync_force_write = 1");
            stringBuilder.AppendLine(");");

            this.AddName(DbCommandType.DeleteRow, stringBuilder.ToString(), false);
        }
        private void CreateSelectRowCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder("SELECT ");
            stringBuilder.AppendLine();
            StringBuilder stringBuilder1 = new StringBuilder();
            string empty = string.Empty;
            foreach (var pkColumn in this.TableDescription.PrimaryKeys)
            {
                var columnName = ParserName.Parse(pkColumn, "`").Quoted().ToString();
                var parameterName = ParserName.Parse(pkColumn, "`").Normalized().Unquoted().ToString();

                stringBuilder.AppendLine($"\t`side`.{columnName}, ");
                stringBuilder1.Append($"{empty}`side`.{columnName} = @{parameterName}");
                empty = " AND ";
            }
            foreach (SyncColumn mutableColumn in this.TableDescription.GetMutableColumns())
            {
                var nonPkColumnName = ParserName.Parse(mutableColumn, "`").Quoted().ToString();
                stringBuilder.AppendLine($"\t`base`.{nonPkColumnName}, ");
            }
            stringBuilder.AppendLine("\t`side`.`sync_row_is_tombstone`,");
            stringBuilder.AppendLine("\t`side`.`create_scope_id`,");
            stringBuilder.AppendLine("\t`side`.`create_timestamp`,");
            stringBuilder.AppendLine("\t`side`.`update_scope_id`,");
            stringBuilder.AppendLine("\t`side`.`update_timestamp`");

            stringBuilder.AppendLine($"FROM {trackingName.Quoted().ToString()} `side` ");
            stringBuilder.AppendLine($"LEFT JOIN {tableName.Quoted().ToString()} `base` ON ");

            string str = string.Empty;
            foreach (var pkColumn in this.TableDescription.PrimaryKeys)
            {
                var pkColumnName = ParserName.Parse(pkColumn, "`").Quoted().ToString();
                stringBuilder.Append($"{str}`base`.{pkColumnName} = `side`.{pkColumnName}");
                str = " AND ";
            }
            stringBuilder.AppendLine();
            stringBuilder.Append(string.Concat("WHERE ", stringBuilder1.ToString()));
            stringBuilder.Append(";");
            this.AddName(DbCommandType.SelectRow, stringBuilder.ToString(), false);
        }
        private void CreateSelectChangesCommandText()
        {

            StringBuilder stringBuilder = new StringBuilder("SELECT ");
            foreach (var pkColumn in this.TableDescription.PrimaryKeys)
            {
                var pkColumnName = ParserName.Parse(pkColumn, "`").Quoted().ToString();
                stringBuilder.AppendLine($"\t`side`.{pkColumnName}, ");
            }
            foreach (var mutableColumn in this.TableDescription.GetMutableColumns())
            {
                var columnName = ParserName.Parse(mutableColumn, "`").Quoted().ToString();
                stringBuilder.AppendLine($"\t`base`.{columnName}, ");
            }
            stringBuilder.AppendLine($"\t`side`.`sync_row_is_tombstone`, ");
            stringBuilder.AppendLine($"\t`side`.`create_scope_id`, ");
            stringBuilder.AppendLine($"\t`side`.`create_timestamp`, ");
            stringBuilder.AppendLine($"\t`side`.`update_scope_id`, ");
            stringBuilder.AppendLine($"\t`side`.`update_timestamp` ");
            stringBuilder.AppendLine($"FROM {trackingName.Quoted().ToString()} `side`");
            stringBuilder.AppendLine($"LEFT JOIN {tableName.Quoted().ToString()} `base`");
            stringBuilder.Append($"ON ");

            string empty = "";
            foreach (var pkColumn in this.TableDescription.PrimaryKeys)
            {
                var pkColumnName = ParserName.Parse(pkColumn, "`").Quoted().ToString();
                stringBuilder.Append($"{empty}`base`.{pkColumnName} = `side`.{pkColumnName}");
                empty = " AND ";
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("WHERE (");
            string str = string.Empty;


            //if (!MySqlManagementUtils.IsStringNullOrWhitespace(this._filterClause))
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
            stringBuilder.AppendLine("\t`side`.`update_scope_id` IS NULL");
            stringBuilder.AppendLine("\t-- Or Update different from remote");
            stringBuilder.AppendLine("\tOR `side`.`update_scope_id` <> @sync_scope_id");
            stringBuilder.AppendLine("    )");
            stringBuilder.AppendLine("AND (");
            stringBuilder.AppendLine("\t-- And Timestamp is > from remote timestamp");
            stringBuilder.AppendLine("\t`side`.`timestamp` > @sync_min_timestamp");
            stringBuilder.AppendLine("\tOR");
            stringBuilder.AppendLine("\t-- remote instance is new, so we don't take the last timestamp");
            stringBuilder.AppendLine("\t@sync_scope_is_new = 1");
            stringBuilder.AppendLine("\t);");

            this.AddName(DbCommandType.SelectChanges, stringBuilder.ToString(), false);
            this.AddName(DbCommandType.SelectChangesWithFilters, stringBuilder.ToString(), false);
        }

    }
}
