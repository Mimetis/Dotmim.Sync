using Dotmim.Sync.Core.Builders;
using System;
using System.Collections.Generic;
using System.Text;
using Dotmim.Sync.Data;
using System.Data.SQLite;
using Dotmim.Sync.Core.Common;
using System.Data;
using System.Linq;

namespace Dotmim.Sync.SQLite
{
    public class SQLiteObjectNames
    {
        internal const string insertTriggerName = "[{0}_insert_trigger]";
        internal const string updateTriggerName = "[{0}_update_trigger]";
        internal const string deleteTriggerName = "[{0}_delete_trigger]";

        Dictionary<DbCommandType, String> names = new Dictionary<DbCommandType, string>();
        public DmTable TableDescription { get; }


        public void AddName(DbCommandType objectType, string name)
        {
            if (names.ContainsKey(objectType))
                throw new Exception("Yous can't add an objectType multiple times");

            names.Add(objectType, name);
        }
        public string GetCommandName(DbCommandType objectType)
        {
            if (!names.ContainsKey(objectType))
                throw new NotSupportedException($"SQLite provider does not support the command type {objectType.ToString()}");

            return names[objectType];
        }

        public SQLiteObjectNames(DmTable tableDescription)
        {
            this.TableDescription = tableDescription;
            SetDefaultNames();
        }

        /// <summary>
        /// Set the default stored procedures names
        /// </summary>
        private void SetDefaultNames()
        {
            (var tableName, var trackingName) = SQLiteBuilder.GetParsers(this.TableDescription);

            this.AddName(DbCommandType.InsertTrigger, string.Format(insertTriggerName, tableName.UnquotedStringWithUnderScore));
            this.AddName(DbCommandType.UpdateTrigger, string.Format(updateTriggerName, tableName.UnquotedStringWithUnderScore));
            this.AddName(DbCommandType.DeleteTrigger, string.Format(deleteTriggerName, tableName.UnquotedStringWithUnderScore));

            // Select changes
            this.CreateSelectChangesCommandText();
        }


        private void CreateSelectChangesCommandText()
        {
            (var tableName, var trackingName) = SQLiteBuilder.GetParsers(this.TableDescription);

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

            this.AddName(DbCommandType.SelectChanges, stringBuilder.ToString());
        }

    }
}
