﻿using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Text;
using Dotmim.Sync.Data;
using System.Linq;

namespace Dotmim.Sync.MySql
{
    public class MySqlObjectNames
    {
        public const string TimestampValue = "ROUND(UNIX_TIMESTAMP(CURTIME(4)) * 10000)";

        internal const string insertTriggerName = "`{0}_insert_trigger`";
        internal const string updateTriggerName = "`{0}_update_trigger`";
        internal const string deleteTriggerName = "`{0}_delete_trigger`";

        internal const string selectChangesProcName = "`{0}_selectchanges`";
        internal const string selectChangesProcNameWithFilters = "`{0}_{1}_selectchanges`";
        internal const string selectRowProcName = "`{0}_selectrow`";

        internal const string insertProcName = "`{0}_insert`";
        internal const string updateProcName = "`{0}_update`";
        internal const string deleteProcName = "`{0}_delete`";

        internal const string resetProcName = "`{0}_reset`";

        internal const string insertMetadataProcName = "`{0}_insertmetadata`";
        internal const string updateMetadataProcName = "`{0}_updatemetadata`";
        internal const string deleteMetadataProcName = "`{0}_deletemetadata`";


        private Dictionary<DbCommandType, String> names = new Dictionary<DbCommandType, string>();
        private ObjectNameParser tableName, trackingName;

        public DmTable TableDescription { get; }


        public void AddName(DbCommandType objectType, string name)
        {
            if (names.ContainsKey(objectType))
                throw new Exception("Yous can't add an objectType multiple times");

            names.Add(objectType, name);
        }
        public string GetCommandName(DbCommandType objectType, IEnumerable<string> adds = null)
        {
            if (!names.ContainsKey(objectType))
                throw new NotSupportedException($"MySql provider does not support the command type {objectType.ToString()}");

            return names[objectType];
        }

        public MySqlObjectNames(DmTable tableDescription)
        {
            this.TableDescription = tableDescription;
            (tableName, trackingName) = MySqlBuilder.GetParsers(this.TableDescription);

            SetDefaultNames();
        }

        /// <summary>
        /// Set the default stored procedures names
        /// </summary>
        private void SetDefaultNames()
        {
            var pref = this.TableDescription.TrackingTablesPrefix != null ?  this.TableDescription.TrackingTablesPrefix.ToLowerInvariant() : "";
            var suf = this.TableDescription.TrackingTablesSuffix != null ? this.TableDescription.TrackingTablesSuffix.ToLowerInvariant() : "";

            this.AddName(DbCommandType.InsertTrigger, string.Format(insertTriggerName, tableName.UnquotedStringWithUnderScore));
            this.AddName(DbCommandType.UpdateTrigger, string.Format(updateTriggerName, tableName.UnquotedStringWithUnderScore));
            this.AddName(DbCommandType.DeleteTrigger, string.Format(deleteTriggerName, tableName.UnquotedStringWithUnderScore));

            this.AddName(DbCommandType.SelectChanges, string.Format(selectChangesProcName, $"{pref}{tableName.UnquotedStringWithUnderScore}{suf}"));
            this.AddName(DbCommandType.SelectChangesWitFilters, string.Format(selectChangesProcNameWithFilters, $"{pref}{tableName.UnquotedStringWithUnderScore}{suf}", "{0}"));
            this.AddName(DbCommandType.SelectRow, string.Format(selectRowProcName, $"{pref}{tableName.UnquotedStringWithUnderScore}{suf}"));
            this.AddName(DbCommandType.InsertRow, string.Format(insertProcName, $"{pref}{tableName.UnquotedStringWithUnderScore}{suf}"));
            this.AddName(DbCommandType.UpdateRow, string.Format(updateProcName, $"{pref}{tableName.UnquotedStringWithUnderScore}{suf}"));
            this.AddName(DbCommandType.DeleteRow, string.Format(deleteProcName, $"{pref}{tableName.UnquotedStringWithUnderScore}{suf}"));
            this.AddName(DbCommandType.InsertMetadata, string.Format(insertMetadataProcName, $"{pref}{tableName.UnquotedStringWithUnderScore}{suf}"));
            this.AddName(DbCommandType.UpdateMetadata, string.Format(updateMetadataProcName, $"{pref}{tableName.UnquotedStringWithUnderScore}{suf}"));
            this.AddName(DbCommandType.DeleteMetadata, string.Format(deleteMetadataProcName, $"{pref}{tableName.UnquotedStringWithUnderScore}{suf}"));
            this.AddName(DbCommandType.Reset, string.Format(resetProcName, $"{pref}{tableName.UnquotedStringWithUnderScore}{suf}"));

            //// Select changes
            //this.CreateSelectChangesCommandText();
            //this.CreateSelectRowCommandText();
            //this.CreateDeleteCommandText();
            //this.CreateDeleteMetadataCommandText();
            //this.CreateInsertCommandText();
            //this.CreateInsertMetadataCommandText();
            //this.CreateUpdateCommandText();
            //this.CreateUpdatedMetadataCommandText();

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
            stringBuilder.AppendLine($"UPDATE {tableName.QuotedString}");
            stringBuilder.Append($"SET {MySqlManagementUtils.CommaSeparatedUpdateFromParameters(this.TableDescription)}");
            stringBuilder.Append($"WHERE {MySqlManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKey.Columns, "")}");
            stringBuilder.AppendLine($" AND ((SELECT `timestamp` FROM {trackingName.QuotedObjectName} ");
            stringBuilder.AppendLine($"  WHERE {MySqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKey.Columns, tableName.QuotedObjectName, trackingName.QuotedObjectName)}");
            stringBuilder.AppendLine(" ) <= @sync_min_timestamp OR @sync_force_write = 1");
            stringBuilder.AppendLine(");");
            this.AddName(DbCommandType.UpdateRow, stringBuilder.ToString());

        }

        private void CreateUpdatedMetadataCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.AppendLine($"UPDATE {trackingName.QuotedString}");
            stringBuilder.AppendLine($"SET `update_scope_id` = @update_scope_id, ");
            stringBuilder.AppendLine($"\t `update_timestamp` = @update_timestamp, ");
            stringBuilder.AppendLine($"\t `sync_row_is_tombstone` = @sync_row_is_tombstone, ");
            stringBuilder.AppendLine($"\t `timestamp` = {MySqlObjectNames.TimestampValue}, ");
            stringBuilder.AppendLine($"\t `last_change_datetime` = now() ");
            stringBuilder.Append($"WHERE {MySqlManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKey.Columns, "")}");

            this.AddName(DbCommandType.UpdateMetadata, stringBuilder.ToString());

        }
        private void CreateInsertMetadataCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            StringBuilder stringBuilderArguments = new StringBuilder();
            StringBuilder stringBuilderParameters = new StringBuilder();

            stringBuilder.AppendLine($"\tINSERT INTO {trackingName.QuotedString}");

            string empty = string.Empty;
            foreach (var pkColumn in this.TableDescription.PrimaryKey.Columns)
            {
                ObjectNameParser columnName = new ObjectNameParser(pkColumn.ColumnName, "`", "`");
                stringBuilderArguments.Append(string.Concat(empty, columnName.QuotedString.ToLowerInvariant()));
                stringBuilderParameters.Append(string.Concat(empty, $"@{columnName.UnquotedString.ToLowerInvariant()}"));
                empty = ", ";
            }
            stringBuilder.Append($"\t({stringBuilderArguments.ToString()}, ");
            stringBuilder.AppendLine($"\t`create_scope_id`, `create_timestamp`, `update_scope_id`, `update_timestamp`,");
            stringBuilder.AppendLine($"\t`sync_row_is_tombstone`, `timestamp`, `last_change_datetime`)");
            stringBuilder.Append($"\tVALUES ({stringBuilderParameters.ToString()}, ");
            stringBuilder.AppendLine($"\t@create_scope_id, @create_timestamp, @update_scope_id, @update_timestamp, ");
            stringBuilder.AppendLine($"\t@sync_row_is_tombstone, {MySqlObjectNames.TimestampValue}, now())");
            stringBuilder.AppendLine($"\tON DUPLICATE KEY UPDATE");
            stringBuilder.AppendLine($"\t `create_scope_id` = @create_scope_id, ");
            stringBuilder.AppendLine($"\t `create_timestamp` = @create_timestamp, ");
            stringBuilder.AppendLine($"\t `update_scope_id` = @update_scope_id, ");
            stringBuilder.AppendLine($"\t `update_timestamp` = @update_timestamp, ");
            stringBuilder.AppendLine($"\t `sync_row_is_tombstone` = @sync_row_is_tombstone, ");
            stringBuilder.AppendLine($"\t `timestamp` = {MySqlObjectNames.TimestampValue}, ");
            stringBuilder.AppendLine($"\t `last_change_datetime` = now() ");
            //stringBuilder.AppendLine($"\tWHERE {MySqlManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKey.Columns, "")};");
            stringBuilder.AppendLine();

            this.AddName(DbCommandType.InsertMetadata, stringBuilder.ToString());

        }
        private void CreateInsertCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            StringBuilder stringBuilderArguments = new StringBuilder();
            StringBuilder stringBuilderParameters = new StringBuilder();
            string empty = string.Empty;
            foreach (var mutableColumn in this.TableDescription.Columns.Where(c => !c.ReadOnly))
            {
                ObjectNameParser columnName = new ObjectNameParser(mutableColumn.ColumnName, "`", "`");
                stringBuilderArguments.Append(string.Concat(empty, columnName.QuotedString.ToLowerInvariant()));
                stringBuilderParameters.Append(string.Concat(empty, $"@{columnName.UnquotedString.ToLowerInvariant()}"));
                empty = ", ";
            }
            stringBuilder.AppendLine($"\tINSERT INTO {tableName.QuotedString}");
            stringBuilder.AppendLine($"\t({stringBuilderArguments.ToString()})");
            stringBuilder.AppendLine($"\tVALUES ({stringBuilderParameters.ToString()});");

            this.AddName(DbCommandType.InsertRow, stringBuilder.ToString());

        }
        private void CreateDeleteMetadataCommandText()
        {

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"DELETE FROM {trackingName.QuotedString} ");
            stringBuilder.Append($"WHERE ");
            stringBuilder.AppendLine(MySqlManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKey.Columns, ""));
            stringBuilder.Append(";");

            this.AddName(DbCommandType.DeleteMetadata, stringBuilder.ToString());
        }
        private void CreateDeleteCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.AppendLine($"DELETE FROM {tableName.QuotedString} ");
            stringBuilder.Append($"WHERE {MySqlManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKey.Columns, "")}");
            stringBuilder.AppendLine($" AND ((SELECT `timestamp` FROM {trackingName.QuotedObjectName} ");
            stringBuilder.AppendLine($"  WHERE {MySqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKey.Columns, tableName.QuotedObjectName, trackingName.QuotedObjectName)}");
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
                ObjectNameParser pkColumnName = new ObjectNameParser(pkColumn.ColumnName, "`", "`");
                stringBuilder.AppendLine($"\t`side`.{pkColumnName.QuotedString.ToLowerInvariant()}, ");
                stringBuilder1.Append($"{empty}`side`.{pkColumnName.QuotedString.ToLowerInvariant()} = @{pkColumnName.UnquotedString.ToLowerInvariant()}");
                empty = " AND ";
            }
            foreach (DmColumn nonPkMutableColumn in this.TableDescription.NonPkColumns.Where(c => !c.ReadOnly))
            {
                ObjectNameParser nonPkColumnName = new ObjectNameParser(nonPkMutableColumn.ColumnName, "`", "`");
                stringBuilder.AppendLine($"\t`base`.{nonPkColumnName.QuotedString}, ");
            }
            stringBuilder.AppendLine("\t`side`.`sync_row_is_tombstone`,");
            stringBuilder.AppendLine("\t`side`.`create_scope_id`,");
            stringBuilder.AppendLine("\t`side`.`create_timestamp`,");
            stringBuilder.AppendLine("\t`side`.`update_scope_id`,");
            stringBuilder.AppendLine("\t`side`.`update_timestamp`");

            stringBuilder.AppendLine($"FROM {trackingName.QuotedString} `side` ");
            stringBuilder.AppendLine($"LEFT JOIN {tableName.QuotedString} `base` ON ");

            string str = string.Empty;
            foreach (var pkColumn in this.TableDescription.PrimaryKey.Columns)
            {
                ObjectNameParser pkColumnName = new ObjectNameParser(pkColumn.ColumnName, "`", "`");
                stringBuilder.Append($"{str}`base`.{pkColumnName.QuotedString.ToLowerInvariant()} = `side`.{pkColumnName.QuotedString.ToLowerInvariant()}");
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
                var pkColumnName = new ObjectNameParser(pkColumn.ColumnName, "`", "`");
                stringBuilder.AppendLine($"\t`side`.{pkColumnName.QuotedString.ToLowerInvariant()}, ");
            }
            foreach (var column in this.TableDescription.NonPkColumns.Where(col => !col.ReadOnly))
            {
                var columnName = new ObjectNameParser(column.ColumnName, "`", "`");
                stringBuilder.AppendLine($"\t`base`.{columnName.QuotedString.ToLowerInvariant()}, ");
            }
            stringBuilder.AppendLine($"\t`side`.`sync_row_is_tombstone`, ");
            stringBuilder.AppendLine($"\t`side`.`create_scope_id`, ");
            stringBuilder.AppendLine($"\t`side`.`create_timestamp`, ");
            stringBuilder.AppendLine($"\t`side`.`update_scope_id`, ");
            stringBuilder.AppendLine($"\t`side`.`update_timestamp` ");
            stringBuilder.AppendLine($"FROM {trackingName.QuotedString} `side`");
            stringBuilder.AppendLine($"LEFT JOIN {tableName.QuotedString} `base`");
            stringBuilder.Append($"ON ");

            string empty = "";
            foreach (var pkColumn in this.TableDescription.PrimaryKey.Columns)
            {
                var pkColumnName = new ObjectNameParser(pkColumn.ColumnName, "`", "`");
                stringBuilder.Append($"{empty}`base`.{pkColumnName.QuotedString.ToLowerInvariant()} = `side`.{pkColumnName.QuotedString.ToLowerInvariant()}");
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

            this.AddName(DbCommandType.SelectChanges, stringBuilder.ToString());
            this.AddName(DbCommandType.SelectChangesWitFilters, stringBuilder.ToString());
        }

    }
}
