using Dotmim.Sync.Builders;
using System;
using System.Text;

using System.Data.Common;

using System.Data;
#if NET5_0 || NET6_0 || NET7_0 || NETCOREAPP3_1
using MySqlConnector;
#elif NETSTANDARD 
using MySql.Data.MySqlClient;
#endif
using System.Linq;
using System.Diagnostics;
using System.Collections.Generic;
using System.Threading.Tasks;

#if MARIADB
namespace Dotmim.Sync.MariaDB.Builders
#elif MYSQL
namespace Dotmim.Sync.MySql.Builders
#endif
{
    public class MySqlBuilderTrackingTable
    {
        private ParserName tableName;
        private ParserName trackingName;
        private readonly SyncSetup setup;
        private readonly SyncTable tableDescription;


        private readonly MySqlDbMetadata dbMetadata;
        public MySqlBuilderTrackingTable(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup)
        {
            this.dbMetadata = new MySqlDbMetadata();
            this.tableDescription = tableDescription;
            this.tableName = tableName;
            this.trackingName = trackingName;
            this.setup = setup;
        }

        public Task<DbCommand> GetCreateTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.Append("CREATE TABLE ").Append(trackingName.Quoted()).AppendLine(" (");

            // Adding the primary key
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn, "`").Quoted().ToString();
                var columnType = this.dbMetadata.GetCompatibleColumnTypeDeclarationString(pkColumn, this.tableDescription.OriginalProvider);
                stringBuilder.Append(columnName).Append(' ').Append(columnType).AppendLine(" NOT NULL, ");
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
