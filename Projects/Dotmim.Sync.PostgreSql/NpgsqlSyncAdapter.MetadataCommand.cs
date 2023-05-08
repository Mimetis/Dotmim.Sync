using Npgsql;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data;
using System.Text;
using Dotmim.Sync.Builders;

namespace Dotmim.Sync.PostgreSql
{
    public partial class NpgsqlSyncAdapter : DbSyncAdapter
    {

        // ---------------------------------------------------
        // Select Metadata Command
        // ---------------------------------------------------


        /// <summary>
        /// Get the Select Metadata Command
        /// </summary>
        private (DbCommand, bool) CreateSelectMetadataCommand()
        {
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(TableName);
            var stringBuilder = new StringBuilder();
            var pkeysSelect = new StringBuilder();
            var pkeysWhere = new StringBuilder();
            var and = string.Empty;
            var comma = string.Empty;

            foreach (var pkColumn in TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn, "\"").Quoted().ToString();
                var parameterName = ParserName.Parse(pkColumn).Unquoted().Normalized().ToString();
                pkeysSelect.Append(comma).Append("side.").Append(columnName);

                pkeysWhere.Append(and).Append("side.").Append(columnName).Append(" = @").Append(parameterName);

                and = " AND ";
                comma = ", ";
            }

            stringBuilder.Append("SELECT ").Append(pkeysSelect).AppendLine(", side.update_scope_id, side.timestamp_bigint as timestamp, side.sync_row_is_tombstone");
            stringBuilder.Append("FROM \"").Append(schema).Append("\".").Append(this.TrackingTableName.Quoted()).AppendLine(" side");
            stringBuilder.Append("WHERE ").Append(pkeysWhere).AppendLine();

            var command = new NpgsqlCommand
            {
                CommandType = CommandType.Text,
                CommandText = stringBuilder.ToString()
            };
            return (command, false);
        }

  
        // ---------------------------------------------------
        // Update Metadata Command
        // ---------------------------------------------------

        private (DbCommand, bool) CreateUpdateMetadataCommand()
        {
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(TableName);

            var stringBuilder = new StringBuilder();
            var pkeysForUpdate = new StringBuilder();

            var pkeySelectForInsert = new StringBuilder();
            var pkeyISelectForInsert = new StringBuilder();
            var pkeyAliasSelectForInsert = new StringBuilder();
            var pkeysLeftJoinForInsert = new StringBuilder();
            var pkeysIsNullForInsert = new StringBuilder();

            string and = string.Empty;
            string comma = string.Empty;
            foreach (var pkColumn in TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn, "\"").Quoted().ToString();
                var parameterName = ParserName.Parse(pkColumn).Unquoted().Normalized().ToString();

                pkeysForUpdate.Append(and).Append("side.").Append(columnName).Append(" = @").Append(parameterName);

                pkeySelectForInsert.Append(comma).Append(columnName);
                pkeyISelectForInsert.Append(comma).Append("i.").Append(columnName);
                pkeyAliasSelectForInsert.Append(comma).Append('@').Append(parameterName).Append(" as ").Append(columnName);
                pkeysLeftJoinForInsert.Append(and).Append("side.").Append(columnName).Append(" = i.").Append(columnName);
                pkeysIsNullForInsert.Append(and).Append("side.").Append(columnName).Append(" IS NULL");
                and = " AND ";
                comma = ", ";
            }
            stringBuilder.Append("UPDATE \"").Append(schema).Append("\".").Append(TrackingTableName.Quoted()).AppendLine(" SET ");
            stringBuilder.AppendLine($" \"update_scope_id\" = @sync_scope_id, ");
            stringBuilder.AppendLine($" \"sync_row_is_tombstone\" = @sync_row_is_tombstone, ");
            stringBuilder.AppendLine($" \"timestamp\" = {TimestampValue}, ");
            stringBuilder.AppendLine($" \"last_change_datetime\" = now() ");
            stringBuilder.Append("FROM \"").Append(schema).Append("\".").Append(TrackingTableName.Quoted()).AppendLine(" side ");
            stringBuilder.Append("WHERE ").Append(pkeysForUpdate).AppendLine(";");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine();
            stringBuilder.Append("INSERT INTO \"").Append(schema).Append("\".").Append(TrackingTableName.Quoted()).AppendLine(" (");
            stringBuilder.Append(pkeySelectForInsert).AppendLine(", \"update_scope_id\", \"sync_row_is_tombstone\", \"timestamp\", \"last_change_datetime\" )");
            stringBuilder.Append("SELECT ").Append(pkeyISelectForInsert).Append(", i.\"sync_scope_id\", i.\"sync_row_is_tombstone\", ").Append(TimestampValue).AppendLine(", now()");
            stringBuilder.Append("FROM (SELECT ").Append(pkeyAliasSelectForInsert).AppendLine(", @sync_scope_id as sync_scope_id, @sync_row_is_tombstone as sync_row_is_tombstone) as i");
            stringBuilder.Append("LEFT JOIN \"").Append(schema).Append("\".").Append(TrackingTableName.Quoted()).Append(" side ON ").Append(pkeysLeftJoinForInsert).AppendLine(" ");
            stringBuilder.Append("WHERE ").Append(pkeysIsNullForInsert).AppendLine(";");

            var command = new NpgsqlCommand
            {
                CommandType = CommandType.Text,
                CommandText = stringBuilder.ToString()
            };
            return (command, false);
        }


        // ---------------------------------------------------
        // Delete Metadata Command
        // ---------------------------------------------------

        private (DbCommand, bool) GetDeleteMetadataCommand()
        {
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(TableName);

            var stringBuilder = new StringBuilder();
            stringBuilder.Append("DELETE FROM \"").Append(schema).Append("\".").Append(this.TrackingTableName.Quoted()).AppendLine(" side");
            stringBuilder.AppendLine($"WHERE side.\"timestamp\" < @sync_row_timestamp;");

            var command = new NpgsqlCommand
            {
                CommandType = CommandType.Text,
                CommandText = stringBuilder.ToString()
            };
            return (command, false);

        }
  
        // ---------------------------------------------------
        // Update Untracked Rows Command
        // ---------------------------------------------------

        private (DbCommand, bool) CreateUpdateUntrackedRowsCommand()
        {
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(TableName);

            var stringBuilder = new StringBuilder();
            var strPkeysList = new StringBuilder();
            var strBasePkeyList = new StringBuilder();
            var strSidePkeyList = new StringBuilder();
            var strWherePkeyList = NpgsqlManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, "side", "base");

            stringBuilder.Append("INSERT INTO \"").Append(schema).Append("\".").Append(TrackingTableName.Quoted()).AppendLine(" (");

            var comma = "";
            foreach (var pkeyColumn in TableDescription.GetPrimaryKeysColumns())
            {
                var pkeyColumnName = ParserName.Parse(pkeyColumn, "\"").Quoted().ToString();
                strPkeysList.Append(comma).Append(pkeyColumnName);
                strBasePkeyList.Append(comma).Append("base.").Append(pkeyColumnName);
                strSidePkeyList.Append(comma).Append("side.").Append(pkeyColumnName);
                comma = ", ";
            }

            stringBuilder.Append(strPkeysList).AppendLine(", \"update_scope_id\", \"timestamp\", \"sync_row_is_tombstone\", \"last_change_datetime\"");
            stringBuilder.AppendLine($")");
            stringBuilder.Append("SELECT ").Append(strBasePkeyList).Append(", NULL, ").Append(TimestampValue).AppendLine(", 0, now()");
            stringBuilder.Append("FROM \"").Append(schema).Append("\".").Append(TableName.Quoted()).AppendLine(" as base WHERE NOT EXISTS");
            stringBuilder.Append("   (SELECT ").Append(strSidePkeyList).AppendLine();
            stringBuilder.Append("    FROM \"").Append(schema).Append("\".").Append(TrackingTableName.Quoted()).AppendLine(" as side ");
            stringBuilder.Append("    WHERE ").Append(strWherePkeyList).AppendLine(")");

            var command = new NpgsqlCommand
            {
                CommandType = CommandType.Text,
                CommandText = stringBuilder.ToString()
            };
            return (command, false);


        }


    }
}
