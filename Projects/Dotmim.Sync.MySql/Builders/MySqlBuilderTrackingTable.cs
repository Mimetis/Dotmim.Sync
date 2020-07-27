using Dotmim.Sync.Builders;
using System;
using System.Text;

using System.Data.Common;

using System.Data;
using MySql.Data.MySqlClient;
using System.Linq;
using Dotmim.Sync.MySql.Builders;
using System.Diagnostics;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Dotmim.Sync.MySql
{
    public class MySqlBuilderTrackingTable : IDbBuilderTrackingTableHelper
    {
        private ParserName tableName;
        private ParserName trackingName;
        private readonly SyncSetup setup;
        private readonly SyncTable tableDescription;
        private readonly MySqlDbMetadata mySqlDbMetadata;


        public MySqlBuilderTrackingTable(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup)
        {
            this.tableDescription = tableDescription;
            this.tableName = tableName;
            this.trackingName = trackingName;
            this.setup = setup;
            this.mySqlDbMetadata = new MySqlDbMetadata();
        }

        public async Task<bool> NeedToCreateTrackingTableAsync(DbConnection connection, DbTransaction transaction)
             => !await MySqlManagementUtils.TableExistsAsync((MySqlConnection)connection, (MySqlTransaction)transaction, trackingName).ConfigureAwait(false);

        public Task CreateIndexAsync(DbConnection connection, DbTransaction transaction) => Task.CompletedTask;
        public Task CreatePkAsync(DbConnection connection, DbTransaction transaction) => Task.CompletedTask;

        public async Task CreateTableAsync(DbConnection connection, DbTransaction transaction)
        {
            var commandText = this.CreateTableCommandText();

            using (var command = new MySqlCommand(commandText, (MySqlConnection)connection, (MySqlTransaction)transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }

        public string CreateTableCommandText()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"CREATE TABLE {trackingName.Quoted().ToString()} (");

            // Adding the primary key
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn, "`").Quoted().ToString();
                var columnTypeString = this.mySqlDbMetadata.TryGetOwnerDbTypeString(pkColumn.OriginalDbType, pkColumn.GetDbType(), false, false, pkColumn.MaxLength, this.tableDescription.OriginalProvider, MySqlSyncProvider.ProviderType);
                var unQuotedColumnType = ParserName.Parse(columnTypeString, "`").Unquoted().Normalized().ToString();

                var columnPrecisionString = this.mySqlDbMetadata.TryGetOwnerDbTypePrecision(pkColumn.OriginalDbType, pkColumn.GetDbType(), false, false, pkColumn.MaxLength, pkColumn.Precision, pkColumn.Scale, this.tableDescription.OriginalProvider, MySqlSyncProvider.ProviderType);
                var columnType = $"{unQuotedColumnType} {columnPrecisionString}";

                stringBuilder.AppendLine($"{columnName} {columnType} NOT NULL, ");
            }

            // adding the tracking columns
            stringBuilder.AppendLine($"`update_scope_id` VARCHAR(36) NULL, ");
            stringBuilder.AppendLine($"`timestamp` BIGINT NULL, ");
            stringBuilder.AppendLine($"`sync_row_is_tombstone` BIT NOT NULL default 0, ");
            stringBuilder.AppendLine($"`last_change_datetime` DATETIME NULL, ");

            stringBuilder.Append(" PRIMARY KEY (");

            var comma = "";
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var quotedColumnName = ParserName.Parse(pkColumn, "`").Quoted().ToString();

                stringBuilder.Append(comma);
                stringBuilder.Append(quotedColumnName);

                comma = ", ";
            }
            stringBuilder.Append("))");

            return stringBuilder.ToString();
        }


        public async Task DropTableAsync(DbConnection connection, DbTransaction transaction)
        {
            var commandText = $"drop table if exists {trackingName.Quoted().ToString()}";

            using (var command = new MySqlCommand(commandText, (MySqlConnection)connection, (MySqlTransaction)transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }

        public async Task RenameTableAsync(ParserName oldTableName, DbConnection connection, DbTransaction transaction)
        {
            var tableNameString = this.trackingName.Quoted().ToString();
            var oldTableNameString = oldTableName.Quoted().ToString();

            var commandText = $"RENAME TABLE {oldTableNameString} TO {tableNameString}; ";

            using (var command = new MySqlCommand(commandText, (MySqlConnection)connection, (MySqlTransaction)transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }
    }
}
