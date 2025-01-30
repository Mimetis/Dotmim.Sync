using Dotmim.Sync.DatabaseStringParsers;
using Microsoft.Extensions.Primitives;
using System.Data.Common;
using System.Text;

#if NET6_0 || NET8_0_OR_GREATER
using MySqlConnector;
#elif NETSTANDARD
using MySql.Data.MySqlClient;
#endif
using System.Threading.Tasks;

#if MARIADB
namespace Dotmim.Sync.MariaDB.Builders
#elif MYSQL
namespace Dotmim.Sync.MySql.Builders
#endif
{
    /// <summary>
    /// Represents a MySql Tracking table builder.
    /// </summary>
    public class MySqlBuilderTrackingTable
    {
        /// <summary>
        /// Gets the table description.
        /// </summary>
        protected SyncTable TableDescription { get; }

        /// <summary>
        /// Gets the MySql object names.
        /// </summary>
        protected MySqlObjectNames MySqlObjectNames { get; }

        /// <summary>
        /// Gets the MySql database metadata.
        /// </summary>
        protected MySqlDbMetadata MySqlDbMetadata { get; }

        /// <inheritdoc cref="MySqlBuilderTrackingTable"/>
        public MySqlBuilderTrackingTable(SyncTable tableDescription, MySqlObjectNames mysqlObjectNames, MySqlDbMetadata mysqlDbMetadata)
        {
            this.TableDescription = tableDescription;
            this.MySqlObjectNames = mysqlObjectNames;
            this.MySqlDbMetadata = mysqlDbMetadata;
        }

        /// <summary>
        /// Returns a command to create a tracking table.
        /// </summary>
        public Task<DbCommand> GetCreateTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"CREATE TABLE {this.MySqlObjectNames.TrackingTableQuotedShortName} (");

            // Adding the primary key
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(pkColumn.ColumnName, MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote);
                var columnType = this.MySqlDbMetadata.GetCompatibleColumnTypeDeclarationString(pkColumn, this.TableDescription.OriginalProvider);
                stringBuilder.AppendLine($"{columnParser.QuotedShortName} {columnType} NOT NULL, ");
            }

            // adding the tracking columns
            stringBuilder.AppendLine($"`update_scope_id` VARCHAR(36) NULL, ");
            stringBuilder.AppendLine($"`timestamp` BIGINT NULL, ");
            stringBuilder.AppendLine($"`sync_row_is_tombstone` BIT NOT NULL default 0, ");
            stringBuilder.AppendLine($"`last_change_datetime` DATETIME NULL, ");

            stringBuilder.Append(" PRIMARY KEY (");

            var comma = string.Empty;
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(pkColumn.ColumnName, MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote);

                stringBuilder.Append(comma);
                stringBuilder.Append(columnParser.QuotedShortName);

                comma = ", ";
            }

            stringBuilder.Append("));");

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = stringBuilder.ToString();

            return Task.FromResult(command);
        }

        /// <summary>
        /// Returns a command to drop a tracking table.
        /// </summary>
        public Task<DbCommand> GetDropTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var commandText = $"drop table {this.MySqlObjectNames.TrackingTableQuotedShortName}";

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = commandText;

            return Task.FromResult(command);
        }

        /// <summary>
        /// Returns a command to check if atracking table exists.
        /// </summary>
        public Task<DbCommand> GetExistsTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();

            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = "select COUNT(*) from information_schema.TABLES where TABLE_NAME = @tableName and TABLE_SCHEMA = schema() and TABLE_TYPE = 'BASE TABLE'";

            var parameter = command.CreateParameter();
            parameter.ParameterName = "@tableName";
            parameter.Value = this.MySqlObjectNames.TrackingTableName;

            command.Parameters.Add(parameter);

            return Task.FromResult(command);
        }
    }
}