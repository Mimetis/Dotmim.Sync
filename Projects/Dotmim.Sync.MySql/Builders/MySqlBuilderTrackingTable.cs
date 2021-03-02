using Dotmim.Sync.Builders;
using System;
using System.Text;

using System.Data.Common;

using System.Data;
#if NET5_0
using MySqlConnector;
#elif NETSTANDARD || NETCOREAPP3_1
using MySql.Data.MySqlClient;
#endif
using System.Linq;
using Dotmim.Sync.MySql.Builders;
using System.Diagnostics;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Dotmim.Sync.MySql
{
    public class MySqlBuilderTrackingTable
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

        public Task<DbCommand> GetCreateTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"CREATE TABLE {trackingName.Quoted().ToString()} (");

            // Adding the primary key
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn, "`").Quoted().ToString();

#if MARIADB
                var columnTypeString = this.mySqlDbMetadata.TryGetOwnerDbTypeString(pkColumn.OriginalDbType, pkColumn.GetDbType(), false, false, pkColumn.MaxLength, this.tableDescription.OriginalProvider, MariaDB.MariaDBSyncProvider.ProviderType);
                var columnPrecisionString = this.mySqlDbMetadata.TryGetOwnerDbTypePrecision(pkColumn.OriginalDbType, pkColumn.GetDbType(), false, false, pkColumn.MaxLength, pkColumn.Precision, pkColumn.Scale, this.tableDescription.OriginalProvider, MariaDB.MariaDBSyncProvider.ProviderType);
#elif MYSQL
                var columnTypeString = this.mySqlDbMetadata.TryGetOwnerDbTypeString(pkColumn.OriginalDbType, pkColumn.GetDbType(), false, false, pkColumn.MaxLength, this.tableDescription.OriginalProvider, MySqlSyncProvider.ProviderType);
                var columnPrecisionString = this.mySqlDbMetadata.TryGetOwnerDbTypePrecision(pkColumn.OriginalDbType, pkColumn.GetDbType(), false, false, pkColumn.MaxLength, pkColumn.Precision, pkColumn.Scale, this.tableDescription.OriginalProvider, MySqlSyncProvider.ProviderType);
#endif

                var unQuotedColumnType = ParserName.Parse(columnTypeString, "`").Unquoted().Normalized().ToString();

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
            stringBuilder.Append("));");

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = stringBuilder.ToString();

            return Task.FromResult(command);
        }


        public Task<DbCommand> GetDropTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var commandText = $"drop table {trackingName.Quoted().ToString()}";

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = commandText;

            return Task.FromResult(command);
        }

        public Task<DbCommand> GetRenameTrackingTableCommandAsync(ParserName oldTableName, DbConnection connection, DbTransaction transaction)
        {
            var tableNameString = this.trackingName.Quoted().ToString();
            var oldTableNameString = oldTableName.Quoted().ToString();

            var commandText = $"RENAME TABLE {oldTableNameString} TO {tableNameString}; ";

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = commandText;

            return Task.FromResult(command);

        }

        public Task<DbCommand> GetExistsTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();

            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = "select COUNT(*) from information_schema.TABLES where TABLE_NAME = @tableName and TABLE_SCHEMA = schema() and TABLE_TYPE = 'BASE TABLE'";

            var parameter = command.CreateParameter();
            parameter.ParameterName = "@tableName";
            parameter.Value = trackingName.Unquoted().ToString();

            command.Parameters.Add(parameter);


            return Task.FromResult(command);
        }
   
    }
}
