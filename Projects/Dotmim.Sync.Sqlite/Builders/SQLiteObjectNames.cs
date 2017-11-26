using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Text;
using Dotmim.Sync.Data;
using System.Linq;

namespace Dotmim.Sync.Sqlite
{
    public class SqliteObjectNames
    {
        public const string TimestampValue = "replace(strftime('%Y%m%d%H%M%f', 'now'), '.', '')";

        internal const string insertTriggerName = "[{0}_insert_trigger]";
        internal const string updateTriggerName = "[{0}_update_trigger]";
        internal const string deleteTriggerName = "[{0}_delete_trigger]";

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
                throw new NotSupportedException($"Sqlite provider does not support the command type {objectType.ToString()}");

            return names[objectType];
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
            this.AddName(DbCommandType.InsertTrigger, string.Format(insertTriggerName, tableName.UnquotedStringWithUnderScore));
            this.AddName(DbCommandType.UpdateTrigger, string.Format(updateTriggerName, tableName.UnquotedStringWithUnderScore));
            this.AddName(DbCommandType.DeleteTrigger, string.Format(deleteTriggerName, tableName.UnquotedStringWithUnderScore));

            // Select changes
            this.CreateSelectChangesCommandText();
            this.CreateSelectRowCommandText();
            this.CreateDeleteCommandText();
            this.CreateDeleteMetadataCommandText();
            this.CreateInsertCommandText();
            this.CreateInsertMetadataCommandText();
            this.CreateUpdateCommandText();
            this.CreateUpdatedMetadataCommandText();

        }

        private void CreateUpdateCommandText()
        {

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"UPDATE {tableName.QuotedString}");
            stringBuilder.Append($"SET {SqliteManagementUtils.CommaSeparatedUpdateFromParameters(this.TableDescription)}");
            stringBuilder.Append($"WHERE {SqliteManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKey.Columns, "")}");
            stringBuilder.AppendLine($" AND ((SELECT [timestamp] FROM {trackingName.QuotedObjectName} ");
            stringBuilder.AppendLine($"  WHERE {SqliteManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKey.Columns, tableName.QuotedObjectName, trackingName.QuotedObjectName)}");
            stringBuilder.AppendLine(" ) <= @sync_min_timestamp OR @sync_force_write = 1");
            stringBuilder.AppendLine(");");
            this.AddName(DbCommandType.UpdateRow, stringBuilder.ToString());

        }

        private void CreateUpdatedMetadataCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.AppendLine($"UPDATE {trackingName.QuotedString}");
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

            stringBuilder.AppendLine($"\tINSERT OR REPLACE INTO {trackingName.QuotedString}");

            string empty = string.Empty;
            foreach (var pkColumn in this.TableDescription.PrimaryKey.Columns)
            {
                ObjectNameParser columnName = new ObjectNameParser(pkColumn.ColumnName);
                stringBuilderArguments.Append(string.Concat(empty, columnName.QuotedString));
                stringBuilderParameters.Append(string.Concat(empty, $"@{columnName.UnquotedString}"));
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
            foreach (var mutableColumn in this.TableDescription.Columns.Where(c => !c.ReadOnly))
            {
                ObjectNameParser columnName = new ObjectNameParser(mutableColumn.ColumnName);
                stringBuilderArguments.Append(string.Concat(empty, columnName.QuotedString));
                stringBuilderParameters.Append(string.Concat(empty, $"@{columnName.UnquotedString}"));
                empty = ", ";
            }
            stringBuilder.AppendLine($"\tINSERT OR IGNORE INTO {tableName.QuotedString}");
            stringBuilder.AppendLine($"\t({stringBuilderArguments.ToString()})");
            stringBuilder.AppendLine($"\tVALUES ({stringBuilderParameters.ToString()});");

            this.AddName(DbCommandType.InsertRow, stringBuilder.ToString());

        }
        private void CreateDeleteMetadataCommandText()
        {

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"DELETE FROM {trackingName.QuotedString} ");
            stringBuilder.Append($"WHERE ");
            stringBuilder.AppendLine(SqliteManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKey.Columns, ""));
            stringBuilder.Append(";");

            this.AddName(DbCommandType.DeleteMetadata, stringBuilder.ToString());
        }
        private void CreateDeleteCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.AppendLine($"DELETE FROM {tableName.QuotedString} ");
            stringBuilder.Append($"WHERE {SqliteManagementUtils.WhereColumnAndParameters(this.TableDescription.PrimaryKey.Columns, "")}");
            stringBuilder.AppendLine($" AND ((SELECT [timestamp] FROM {trackingName.QuotedObjectName} ");
            stringBuilder.AppendLine($"  WHERE {SqliteManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKey.Columns, tableName.QuotedObjectName, trackingName.QuotedObjectName)}");
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
                ObjectNameParser pkColumnName = new ObjectNameParser(pkColumn.ColumnName);
                stringBuilder.AppendLine($"\t[side].{pkColumnName.QuotedString}, ");
                stringBuilder1.Append($"{empty}[side].{pkColumnName.QuotedString} = @{pkColumnName.UnquotedString}");
                empty = " AND ";
            }
            foreach (DmColumn nonPkMutableColumn in this.TableDescription.NonPkColumns.Where(c => !c.ReadOnly))
            {
                ObjectNameParser nonPkColumnName = new ObjectNameParser(nonPkMutableColumn.ColumnName);
                stringBuilder.AppendLine($"\t[base].{nonPkColumnName.QuotedString}, ");
            }
            stringBuilder.AppendLine("\t[side].[sync_row_is_tombstone],");
            stringBuilder.AppendLine("\t[side].[create_scope_id],");
            stringBuilder.AppendLine("\t[side].[create_timestamp],");
            stringBuilder.AppendLine("\t[side].[update_scope_id],");
            stringBuilder.AppendLine("\t[side].[update_timestamp]");

            stringBuilder.AppendLine($"FROM {trackingName.QuotedString} [side] ");
            stringBuilder.AppendLine($"LEFT JOIN {tableName.QuotedString} [base] ON ");

            string str = string.Empty;
            foreach (var pkColumn in this.TableDescription.PrimaryKey.Columns)
            {
                ObjectNameParser pkColumnName = new ObjectNameParser(pkColumn.ColumnName);
                stringBuilder.Append($"{str}[base].{pkColumnName.QuotedString} = [side].{pkColumnName.QuotedString}");
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
                var pkColumnName = new ObjectNameParser(pkColumn.ColumnName);
                stringBuilder.AppendLine($"\t[side].{pkColumnName.QuotedString}, ");
            }
            foreach (var column in this.TableDescription.NonPkColumns.Where(col => !col.ReadOnly))
            {
                var columnName = new ObjectNameParser(column.ColumnName);
                stringBuilder.AppendLine($"\t[base].{columnName.QuotedString}, ");
            }
            stringBuilder.AppendLine($"\t[side].[sync_row_is_tombstone], ");
            stringBuilder.AppendLine($"\t[side].[create_scope_id], ");
            stringBuilder.AppendLine($"\t[side].[create_timestamp], ");
            stringBuilder.AppendLine($"\t[side].[update_scope_id], ");
            stringBuilder.AppendLine($"\t[side].[update_timestamp] ");
            stringBuilder.AppendLine($"FROM {trackingName.QuotedString} [side]");
            stringBuilder.AppendLine($"LEFT JOIN {tableName.QuotedString} [base]");
            stringBuilder.Append($"ON ");

            string empty = "";
            foreach (var pkColumn in this.TableDescription.PrimaryKey.Columns)
            {
                var pkColumnName = new ObjectNameParser(pkColumn.ColumnName);
                stringBuilder.Append($"{empty}[base].{pkColumnName.QuotedString} = [side].{pkColumnName.QuotedString}");
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
                var pkColumnName = new ObjectNameParser(pkColumn.ColumnName, "[", "]");
                stringBuilder.Append($"{empty}[base].{pkColumnName.QuotedString} is not null");
            }
            stringBuilder.AppendLine("\t)");
            stringBuilder.AppendLine(")");


            this.AddName(DbCommandType.SelectChanges, stringBuilder.ToString());
            this.AddName(DbCommandType.SelectChangesWitFilters, stringBuilder.ToString());
        }

    }
}
