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
                pkeysSelect.Append($"{comma}side.{columnName}");

                pkeysWhere.Append($"{and}side.{columnName} = @{parameterName}");

                and = " AND ";
                comma = ", ";
            }

            stringBuilder.AppendLine($"SELECT {pkeysSelect}, side.update_scope_id, side.timestamp_bigint as timestamp, side.sync_row_is_tombstone");
            stringBuilder.AppendLine($"FROM \"{schema}\".{this.TrackingTableName.Quoted()} side");
            stringBuilder.AppendLine($"WHERE {pkeysWhere}");

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

                pkeysForUpdate.Append($"{and}side.{columnName} = @{parameterName}");

                pkeySelectForInsert.Append($"{comma}{columnName}");
                pkeyISelectForInsert.Append($"{comma}i.{columnName}");
                pkeyAliasSelectForInsert.Append($"{comma}@{parameterName} as {columnName}");
                pkeysLeftJoinForInsert.Append($"{and}side.{columnName} = i.{columnName}");
                pkeysIsNullForInsert.Append($"{and}side.{columnName} IS NULL");
                and = " AND ";
                comma = ", ";
            }
            stringBuilder.AppendLine($"UPDATE \"{schema}\".{TrackingTableName.Quoted()} SET ");
            stringBuilder.AppendLine($" \"update_scope_id\" = @sync_scope_id, ");
            stringBuilder.AppendLine($" \"sync_row_is_tombstone\" = @sync_row_is_tombstone, ");
            stringBuilder.AppendLine($" \"timestamp\" = {TimestampValue}, ");
            stringBuilder.AppendLine($" \"last_change_datetime\" = now() ");
            stringBuilder.AppendLine($"FROM \"{schema}\".{TrackingTableName.Quoted()} side ");
            stringBuilder.AppendLine($"WHERE {pkeysForUpdate};");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"INSERT INTO \"{schema}\".{TrackingTableName.Quoted()} (");
            stringBuilder.AppendLine($"{pkeySelectForInsert}, \"update_scope_id\", \"sync_row_is_tombstone\", \"timestamp\", \"last_change_datetime\" )");
            stringBuilder.AppendLine($"SELECT {pkeyISelectForInsert}, i.\"sync_scope_id\", i.\"sync_row_is_tombstone\", {TimestampValue}, now()");
            stringBuilder.AppendLine($"FROM (SELECT {pkeyAliasSelectForInsert}, @sync_scope_id as sync_scope_id, @sync_row_is_tombstone as sync_row_is_tombstone) as i");
            stringBuilder.AppendLine($"LEFT JOIN \"{schema}\".{TrackingTableName.Quoted()} side ON {pkeysLeftJoinForInsert} ");
            stringBuilder.AppendLine($"WHERE {pkeysIsNullForInsert};");

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
            stringBuilder.AppendLine($"DELETE FROM \"{schema}\".{this.TrackingTableName.Quoted()} side");
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

            stringBuilder.AppendLine($"INSERT INTO \"{schema}\".{TrackingTableName.Quoted()} (");

            var comma = "";
            foreach (var pkeyColumn in TableDescription.GetPrimaryKeysColumns())
            {
                var pkeyColumnName = ParserName.Parse(pkeyColumn, "\"").Quoted().ToString();
                strPkeysList.Append($"{comma}{pkeyColumnName}");
                strBasePkeyList.Append($"{comma}base.{pkeyColumnName}");
                strSidePkeyList.Append($"{comma}side.{pkeyColumnName}");
                comma = ", ";
            }

            stringBuilder.AppendLine($"{strPkeysList}, \"update_scope_id\", \"timestamp\", \"sync_row_is_tombstone\", \"last_change_datetime\"");
            stringBuilder.AppendLine($")");
            stringBuilder.AppendLine($"SELECT {strBasePkeyList}, NULL, {TimestampValue}, False, now()");
            stringBuilder.AppendLine($"FROM \"{schema}\".{TableName.Quoted()} as base WHERE NOT EXISTS");
            stringBuilder.AppendLine($"   (SELECT {strSidePkeyList}");
            stringBuilder.AppendLine($"    FROM \"{schema}\".{TrackingTableName.Quoted()} as side ");
            stringBuilder.AppendLine($"    WHERE {strWherePkeyList})");

            var command = new NpgsqlCommand
            {
                CommandType = CommandType.Text,
                CommandText = stringBuilder.ToString()
            };
            return (command, false);


        }


    }
}
