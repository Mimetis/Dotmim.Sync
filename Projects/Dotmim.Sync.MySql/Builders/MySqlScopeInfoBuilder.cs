using Dotmim.Sync.Builders;
using Dotmim.Sync.DatabaseStringParsers;
using System.Data;
using System.Data.Common;
using System.Text;

#if NET6_0 || NET8_0
using MySqlConnector;
#elif NETSTANDARD
using MySql.Data.MySqlClient;
#endif

#if MARIADB
namespace Dotmim.Sync.MariaDB.Builders
#elif MYSQL
namespace Dotmim.Sync.MySql.Builders
#endif
{
    /// <summary>
    /// Represents a MySql Scope Info table builder.
    /// </summary>
    public class MySqlScopeInfoBuilder : DbScopeBuilder
    {
        /// <summary>
        /// Gets the scope info table names.
        /// </summary>
        protected DbTableNames ScopeInfoTableNames { get; }

        /// <summary>
        /// Gets the scope info client table names.
        /// </summary>
        protected DbTableNames ScopeInfoClientTableNames { get; }

        /// <inheritdoc cref="MySqlScopeInfoBuilder"/>
        public MySqlScopeInfoBuilder(string scopeInfoTableName)
        {

            var tableParser = new TableParser(scopeInfoTableName, MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote);
            var scopeInfoClientFullTableName = $"`{tableParser.TableName}_client`";

            this.ScopeInfoTableNames = new DbTableNames(MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote,
                tableParser.TableName, tableParser.NormalizedFullName, tableParser.NormalizedShortName,
                tableParser.QuotedFullName, tableParser.QuotedShortName, tableParser.SchemaName);

            tableParser = new TableParser(scopeInfoClientFullTableName, MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote);

            this.ScopeInfoClientTableNames = new DbTableNames(MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote,
                tableParser.TableName, tableParser.NormalizedFullName, tableParser.NormalizedShortName,
                tableParser.QuotedFullName, tableParser.QuotedShortName, tableParser.SchemaName);
        }

        /// <inheritdoc />
        public override DbTableNames GetParsedScopeInfoTableNames() => this.ScopeInfoTableNames;

        /// <inheritdoc />
        public override DbTableNames GetParsedScopeInfoClientTableNames() => this.ScopeInfoClientTableNames;

        /// <inheritdoc />
        public override DbCommand GetLocalTimestampCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText = $"Select {MySqlObjectNames.TimestampValue}";
            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = commandText;
            return command;
        }

        /// <inheritdoc />
        public override DbCommand GetExistsScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();
            command.Transaction = transaction;

            command.CommandText = $"select count(*) from information_schema.TABLES where TABLE_NAME = '{this.ScopeInfoTableNames.Name}' and TABLE_SCHEMA = schema() and TABLE_TYPE = 'BASE TABLE'";

            return command;
        }

        /// <inheritdoc />
        public override DbCommand GetExistsScopeInfoClientTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();
            command.Transaction = transaction;

            command.CommandText = $"select count(*) from information_schema.TABLES where TABLE_NAME = '{this.ScopeInfoClientTableNames.Name}' and TABLE_SCHEMA = schema() and TABLE_TYPE = 'BASE TABLE'";

            return command;
        }

        /// <inheritdoc />
        public override DbCommand GetCreateScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText =
                    $@"CREATE TABLE IF NOT EXISTS {this.ScopeInfoTableNames.QuotedName}(
                         sync_scope_name varchar(100) NOT NULL,
                         sync_scope_schema longtext NULL,
                         sync_scope_setup longtext NULL,
                         sync_scope_version varchar(10) NULL,
                         sync_scope_last_clean_timestamp bigint NULL,
                         sync_scope_properties longtext NULL,
                         PRIMARY KEY (sync_scope_name)
                         )";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandText;
            return command;
        }

        /// <inheritdoc />
        public override DbCommand GetCreateScopeInfoClientTableCommand(DbConnection connection, DbTransaction transaction)
        {

            var commandText =
                $@"CREATE TABLE IF NOT EXISTS {this.ScopeInfoClientTableNames.QuotedName}(
                        sync_scope_id varchar(36) NOT NULL,
                        sync_scope_name varchar(100) NOT NULL,
                        sync_scope_hash varchar(100) NOT NULL,
                        sync_scope_parameters longtext NULL,
                        scope_last_sync_timestamp bigint NULL,
                        scope_last_server_sync_timestamp bigint NULL,
                        scope_last_sync_duration bigint NULL,
                        scope_last_sync datetime NULL,
                        sync_scope_errors longtext NULL,
                        sync_scope_properties longtext NULL,
                        PRIMARY KEY (sync_scope_id, sync_scope_name, sync_scope_hash)
                        )";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandText;
            return command;
        }

        // ------------------------------

        // Get all scopes
        // ------------------------------

        /// <inheritdoc />
        public override DbCommand GetAllScopeInfosCommand(DbConnection connection, DbTransaction transaction)
        {

            var commandText =
                $@"SELECT sync_scope_name, 
                          sync_scope_schema, 
                          sync_scope_setup, 
                          sync_scope_version,
                          sync_scope_last_clean_timestamp,
                          sync_scope_properties
                        FROM {this.ScopeInfoTableNames.QuotedName}";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandText;

            return command;
        }

        /// <inheritdoc />
        public override DbCommand GetAllScopeInfoClientsCommand(DbConnection connection, DbTransaction transaction)
        {

            var commandText = $@"SELECT  sync_scope_id
                         , sync_scope_name
                         , sync_scope_hash
                         , sync_scope_parameters
                         , scope_last_sync_timestamp
                         , scope_last_server_sync_timestamp
                         , scope_last_sync_duration
                         , scope_last_sync
                         , sync_scope_errors
                         , sync_scope_properties
                    FROM {this.ScopeInfoClientTableNames.QuotedName}";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandText;

            return command;
        }

        // ------------------------------

        // Get scope
        // ------------------------------

        /// <inheritdoc />
        public override DbCommand GetScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {

            var commandText =
                $@"SELECT sync_scope_name, 
                          sync_scope_schema, 
                          sync_scope_setup, 
                          sync_scope_version,
                          sync_scope_last_clean_timestamp,
                          sync_scope_properties
                    FROM  {this.ScopeInfoTableNames.QuotedName}
                    WHERE sync_scope_name = @sync_scope_name";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandText;

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            return command;
        }

        /// <inheritdoc/>
        public override DbCommand GetScopeInfoClientCommand(DbConnection connection, DbTransaction transaction)
        {

            var commandText =
                    $@"SELECT sync_scope_id
                         , sync_scope_name
                         , sync_scope_hash
                         , sync_scope_parameters
                         , scope_last_sync_timestamp
                         , scope_last_server_sync_timestamp
                         , scope_last_sync_duration
                         , scope_last_sync
                         , sync_scope_errors
                         , sync_scope_properties
                    FROM  {this.ScopeInfoClientTableNames.QuotedName}
                    WHERE sync_scope_name = @sync_scope_name and sync_scope_id = @sync_scope_id and sync_scope_hash = @sync_scope_hash";

            var command = connection.CreateCommand();
            command.Transaction = transaction;

            command.CommandText = commandText;

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_id";
            p.DbType = DbType.Guid;
            p.Size = 36;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_hash";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            return command;
        }

        /// <inheritdoc/>
        public override DbCommand GetInsertScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {

            var stmtText = new StringBuilder(
                $"INSERT INTO {this.ScopeInfoTableNames.QuotedName} " +
                      $"(sync_scope_name, sync_scope_schema, sync_scope_setup, sync_scope_version, " +
                      $"sync_scope_last_clean_timestamp, sync_scope_properties) " +
                      $"VALUES " +
                      $"(@sync_scope_name, @sync_scope_schema, @sync_scope_setup, @sync_scope_version, " +
                      $"@sync_scope_last_clean_timestamp, @sync_scope_properties);");

            stmtText.AppendLine();

            stmtText.AppendLine($"SELECT sync_scope_name, sync_scope_schema, sync_scope_setup, sync_scope_version, " +
                $"sync_scope_last_clean_timestamp, sync_scope_properties " +
                $"FROM {this.ScopeInfoTableNames.QuotedName} " +
                $"WHERE sync_scope_name=@sync_scope_name;");

            var command = connection.CreateCommand();

            command.Connection = connection;
            command.Transaction = transaction;

            command.CommandText = stmtText.ToString();

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_schema";
            p.DbType = DbType.String;
            p.Size = -1;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_setup";
            p.DbType = DbType.String;
            p.Size = -1;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_version";
            p.DbType = DbType.String;
            p.Size = 10;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_last_clean_timestamp";
            p.DbType = DbType.DateTime;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_properties";
            p.DbType = DbType.String;
            p.Size = -1;
            command.Parameters.Add(p);

            return command;
        }

        /// <inheritdoc/>
        public override DbCommand GetInsertScopeInfoClientCommand(DbConnection connection, DbTransaction transaction)
        {

            var stmtText = new StringBuilder(
                $"INSERT INTO {this.ScopeInfoClientTableNames.QuotedName} " +
                      $"(sync_scope_name, sync_scope_id, sync_scope_hash, sync_scope_parameters, scope_last_sync_timestamp, scope_last_server_sync_timestamp, " +
                      $"scope_last_sync, scope_last_sync_duration, sync_scope_errors, sync_scope_properties) " +
                      $"VALUES " +
                      $"(@sync_scope_name, @sync_scope_id, @sync_scope_hash, @sync_scope_parameters, @scope_last_sync_timestamp, @scope_last_server_sync_timestamp, " +
                      $"@scope_last_sync, @scope_last_sync_duration, @sync_scope_errors, @sync_scope_properties);");

            stmtText.AppendLine();

            stmtText.AppendLine($"SELECT sync_scope_id, sync_scope_name, sync_scope_hash, " +
                $"sync_scope_parameters, scope_last_sync_timestamp, scope_last_server_sync_timestamp, " +
                $"scope_last_sync, scope_last_sync_duration, sync_scope_errors, sync_scope_properties " +
                $"FROM {this.ScopeInfoClientTableNames.QuotedName} " +
                $"WHERE sync_scope_id=@sync_scope_id AND sync_scope_name=@sync_scope_name AND sync_scope_hash=@sync_scope_hash;");

            var command = connection.CreateCommand();

            command.Connection = connection;
            command.Transaction = transaction;

            command.CommandText = stmtText.ToString();

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_id";
            p.DbType = DbType.Guid;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_hash";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_parameters";
            p.DbType = DbType.String;
            p.Size = -1;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@scope_last_sync_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@scope_last_server_sync_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@scope_last_sync";
            p.DbType = DbType.DateTime;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@scope_last_sync_duration";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_errors";
            p.DbType = DbType.String;
            p.Size = -1;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_properties";
            p.DbType = DbType.String;
            p.Size = -1;
            command.Parameters.Add(p);

            return command;
        }

        // ------------------------------
        // Update Scope
        // ------------------------------

        /// <inheritdoc/>
        public override DbCommand GetUpdateScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {

            var stmtText = new StringBuilder(
                $"UPDATE {this.ScopeInfoTableNames.QuotedName} " +
                $"SET sync_scope_schema=@sync_scope_schema, " +
                $"sync_scope_setup=@sync_scope_setup, " +
                $"sync_scope_version=@sync_scope_version, " +
                $"sync_scope_last_clean_timestamp=@sync_scope_last_clean_timestamp, " +
                $"sync_scope_properties=@sync_scope_properties " +
                $"WHERE sync_scope_name=@sync_scope_name;");
            stmtText.AppendLine();
            stmtText.AppendLine();
            stmtText.AppendLine(
                $"SELECT sync_scope_name, sync_scope_schema, sync_scope_setup, sync_scope_version, " +
                $"sync_scope_last_clean_timestamp, sync_scope_properties " +
                $"FROM {this.ScopeInfoTableNames.QuotedName} " +
                $"WHERE sync_scope_name=@sync_scope_name;");

            var command = connection.CreateCommand();

            command.Connection = connection;
            command.Transaction = transaction;

            command.CommandText = stmtText.ToString();
            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_schema";
            p.DbType = DbType.String;
            p.Size = -1;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_setup";
            p.DbType = DbType.String;
            p.Size = -1;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_version";
            p.DbType = DbType.String;
            p.Size = 10;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_last_clean_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_properties";
            p.DbType = DbType.String;
            p.Size = -1;
            command.Parameters.Add(p);

            return command;
        }

        /// <inheritdoc/>
        public override DbCommand GetUpdateScopeInfoClientCommand(DbConnection connection, DbTransaction transaction)
        {

            var stmtText = new StringBuilder(
                $"UPDATE {this.ScopeInfoClientTableNames.QuotedName} " +
                $"SET scope_last_sync_timestamp=@scope_last_sync_timestamp, " +
                $"scope_last_server_sync_timestamp=@scope_last_server_sync_timestamp, " +
                $"scope_last_sync=@scope_last_sync, " +
                $"scope_last_sync_duration=@scope_last_sync_duration, " +
                $"sync_scope_properties=@sync_scope_properties,  " +
                $"sync_scope_errors=@sync_scope_errors,  " +
                $"sync_scope_parameters=@sync_scope_parameters  " +
                $"WHERE sync_scope_id=@sync_scope_id and sync_scope_name=@sync_scope_name and sync_scope_hash=@sync_scope_hash;");

            stmtText.AppendLine();

            stmtText.AppendLine(
                $"SELECT sync_scope_id, sync_scope_name, sync_scope_hash, sync_scope_parameters, " +
                $"scope_last_sync_timestamp, scope_last_server_sync_timestamp, scope_last_sync, " +
                $"scope_last_sync_duration, sync_scope_errors, sync_scope_properties " +
                $"FROM  {this.ScopeInfoClientTableNames.QuotedName} " +
                $"WHERE sync_scope_name=@sync_scope_name and sync_scope_id=@sync_scope_id and sync_scope_hash=@sync_scope_hash; ");

            var command = connection.CreateCommand();

            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = stmtText.ToString();

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_id";
            p.DbType = DbType.Guid;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_hash";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_parameters";
            p.DbType = DbType.String;
            p.Size = -1;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@scope_last_sync_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@scope_last_server_sync_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@scope_last_sync";
            p.DbType = DbType.DateTime;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@scope_last_sync_duration";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_errors";
            p.DbType = DbType.String;
            p.Size = -1;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_properties";
            p.DbType = DbType.String;
            p.Size = -1;
            command.Parameters.Add(p);

            return command;
        }

        // ------------------------------
        // Delete scope
        // ------------------------------

        /// <inheritdoc/>
        public override DbCommand GetDeleteScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var stmtText = $"Delete From {this.ScopeInfoTableNames.QuotedName} where sync_scope_name=@sync_scope_name;";

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = stmtText;

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            return command;
        }

        /// <inheritdoc/>
        public override DbCommand GetDeleteScopeInfoClientCommand(DbConnection connection, DbTransaction transaction)
        {

            var stmtText = $"Delete From {this.ScopeInfoClientTableNames.QuotedName} where sync_scope_id=@sync_scope_id and sync_scope_name=@sync_scope_name and sync_scope_hash=@sync_scope_hash;";

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = stmtText;

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_hash";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_id";
            p.DbType = DbType.Guid;
            p.Size = 36;
            command.Parameters.Add(p);

            return command;
        }

        // ------------------------------
        // Drop Scope table
        // ------------------------------

        /// <inheritdoc/>
        public override DbCommand GetDropScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
        {

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = $@"Drop Table {this.ScopeInfoTableNames.QuotedName};";

            return command;
        }

        /// <inheritdoc/>
        public override DbCommand GetDropScopeInfoClientTableCommand(DbConnection connection, DbTransaction transaction)
        {

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = $@"Drop Table {this.ScopeInfoClientTableNames.QuotedName};";

            return command;
        }

        // ------------------------------
        // Exist Client Scope
        // ------------------------------

        /// <inheritdoc/>
        public override DbCommand GetExistsScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = $@"Select count(*) from {this.ScopeInfoTableNames.QuotedName} where sync_scope_name=@sync_scope_name;";

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            return command;
        }

        /// <inheritdoc/>
        public override DbCommand GetExistsScopeInfoClientCommand(DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = $@"Select count(*) from {this.ScopeInfoClientTableNames.QuotedName} where sync_scope_name=@sync_scope_name and sync_scope_id=@sync_scope_id and sync_scope_hash=@sync_scope_hash";

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_id";
            p.DbType = DbType.Guid;
            p.Size = 36;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_hash";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            return command;
        }
    }
}