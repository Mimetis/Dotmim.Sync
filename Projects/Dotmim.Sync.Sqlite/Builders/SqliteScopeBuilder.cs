using Dotmim.Sync.Builders;
using Dotmim.Sync.DatabaseStringParsers;
using Microsoft.Data.Sqlite;
using System.Data;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync.Sqlite
{
    /// <summary>
    /// Sqlite scope builder.
    /// </summary>
    public class SqliteScopeBuilder : DbScopeBuilder
    {
        /// <summary>
        /// Gets the scope info table names.
        /// </summary>
        protected DbTableNames ScopeInfoTableNames { get; }

        /// <summary>
        /// Gets the scope info client table names.
        /// </summary>
        protected DbTableNames ScopeInfoClientTableNames { get; }

        /// <inheritdoc cref="SqliteScopeBuilder" />
        public SqliteScopeBuilder(string scopeInfoTableName)
            : base(scopeInfoTableName)
        {
            var tableParser = new TableParser(scopeInfoTableName, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote);

            this.ScopeInfoTableNames = new DbTableNames(SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote,
                tableParser.TableName, tableParser.NormalizedFullName, tableParser.NormalizedShortName,
                tableParser.QuotedFullName, tableParser.QuotedShortName, tableParser.SchemaName);

            var scopeInfoClientFullTableName = $"[{tableParser.TableName}_client]";

            tableParser = new TableParser(scopeInfoClientFullTableName, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote);

            this.ScopeInfoClientTableNames = new DbTableNames(SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote,
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
            var commandText = $"Select {SqliteObjectNames.TimestampValue}";
            var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.Connection = connection;
            command.Transaction = transaction;
            return command;
        }

        /// <inheritdoc />
        public override DbCommand GetExistsScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText = $@"SELECT count(*) FROM sqlite_master WHERE type='table' AND name='{this.ScopeInfoTableNames.NormalizedName}'";
            var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.Connection = connection;
            command.Transaction = transaction;
            return command;
        }

        /// <inheritdoc />
        public override DbCommand GetExistsScopeInfoClientTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText = $@"SELECT count(*) FROM sqlite_master WHERE type='table' AND name='{this.ScopeInfoTableNames.NormalizedName}_client'";
            var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.Connection = connection;
            command.Transaction = transaction;
            return command;
        }

        /// <inheritdoc />
        public override DbCommand GetExistsScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText = $@"Select count(*) from {this.ScopeInfoTableNames.NormalizedName} where sync_scope_name = @sync_scope_name";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandText;

            var p0 = command.CreateParameter();
            p0.ParameterName = "@sync_scope_name";
            p0.DbType = DbType.String;
            p0.Size = 100;
            command.Parameters.Add(p0);
            return command;
        }

        /// <inheritdoc />
        public override DbCommand GetExistsScopeInfoClientCommand(DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();
            command.CommandText = $@"Select count(*) from [{this.ScopeInfoTableNames.NormalizedName}_client] 
                                     where sync_scope_id = @sync_scope_id 
                                     and sync_scope_name = @sync_scope_name
                                     and sync_scope_hash = @sync_scope_hash;";

            command.Transaction = transaction;

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_id";
            p.DbType = DbType.Guid;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_hash";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            return command;
        }

        /// <inheritdoc />
        public override DbCommand GetCreateScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText =
                    $@"CREATE TABLE [{this.ScopeInfoTableNames.NormalizedName}](
                        sync_scope_name text NOT NULL,
                        sync_scope_schema text NULL,
                        sync_scope_setup text NULL,
                        sync_scope_version text NULL,
                        sync_scope_last_clean_timestamp integer NULL,
                        sync_scope_properties text NULL,
                        CONSTRAINT PKey_{this.ScopeInfoTableNames.NormalizedName} PRIMARY KEY(sync_scope_name))";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandText;
            return command;
        }

        /// <inheritdoc />
        public override DbCommand GetCreateScopeInfoClientTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText =
                    $@"CREATE TABLE [{this.ScopeInfoTableNames.NormalizedName}_client](
                        sync_scope_id blob NOT NULL,
                        sync_scope_name text NOT NULL,
                        sync_scope_hash text NOT NULL,
                        sync_scope_parameters text NULL,
                        scope_last_sync_timestamp integer NULL,
                        scope_last_server_sync_timestamp integer NULL,
                        scope_last_sync_duration integer NULL,
                        scope_last_sync datetime NULL,
                        sync_scope_errors text NULL,
                        sync_scope_properties text NULL,
                        CONSTRAINT PKey_{this.ScopeInfoTableNames.NormalizedName}_client PRIMARY KEY(sync_scope_id, sync_scope_name, sync_scope_hash))";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandText;
            return command;
        }

        /// <inheritdoc />
        public override DbCommand GetAllScopeInfosCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText =
                $@"SELECT [sync_scope_name], 
                          [sync_scope_schema], 
                          [sync_scope_setup], 
                          [sync_scope_version],
                          [sync_scope_last_clean_timestamp],
                          [sync_scope_properties]
                    FROM  {this.ScopeInfoTableNames.NormalizedName}";

            var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.Connection = connection;
            command.Transaction = transaction;

            return command;
        }

        /// <inheritdoc />
        public override DbCommand GetAllScopeInfoClientsCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText =
                $@"SELECT  [sync_scope_id]
                         , [sync_scope_name]
                         , [sync_scope_hash]
                         , [sync_scope_parameters]
                         , [scope_last_sync_timestamp]
                         , [scope_last_server_sync_timestamp]
                         , [scope_last_sync_duration]
                         , [scope_last_sync]
                         , [sync_scope_errors]
                         , [sync_scope_properties]
                    FROM  [{this.ScopeInfoTableNames.NormalizedName}_client]";

            var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.Connection = connection;
            command.Transaction = transaction;

            return command;
        }

        /// <inheritdoc />
        public override DbCommand GetDeleteScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText = $@"DELETE FROM [{this.ScopeInfoTableNames.NormalizedName}] WHERE [sync_scope_name] = @sync_scope_name";

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

        /// <inheritdoc />
        public override DbCommand GetDeleteScopeInfoClientCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText =
                $@"DELETE FROM [{this.ScopeInfoTableNames.NormalizedName}_client]
                   WHERE [sync_scope_name] = @sync_scope_name and [sync_scope_id] = @sync_scope_id and [sync_scope_hash] = @sync_scope_hash";

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
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_hash";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            return command;
        }

        /// <inheritdoc />
        public override DbCommand GetScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = this.ScopeInfoTableNames.NormalizedName;

            var commandText =
                    $@"SELECT [sync_scope_name], 
                          [sync_scope_schema], 
                          [sync_scope_setup], 
                          [sync_scope_version],
                          [sync_scope_last_clean_timestamp],
                          [sync_scope_properties]
                    FROM  [{tableName}]
                    WHERE [sync_scope_name] = @sync_scope_name";

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

        /// <inheritdoc />
        public override DbCommand GetScopeInfoClientCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableNames.NormalizedName}_client";
            var commandText =
                $@"SELECT    [sync_scope_id]
                           , [sync_scope_name]
                           , [sync_scope_hash]
                           , [sync_scope_parameters]
                           , [scope_last_sync_timestamp]
                           , [scope_last_server_sync_timestamp]
                           , [scope_last_sync_duration]
                           , [scope_last_sync]
                           , [sync_scope_errors]
                           , [sync_scope_properties]
                    FROM  [{tableName}]
                    WHERE [sync_scope_name] = @sync_scope_name and [sync_scope_id] = @sync_scope_id and [sync_scope_hash] = @sync_scope_hash";

            var command = connection.CreateCommand();
            command.Transaction = transaction;

            command.CommandText = commandText;

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            var p0 = command.CreateParameter();
            p0.ParameterName = "@sync_scope_id";
            p0.DbType = DbType.Guid;
            command.Parameters.Add(p0);

            var p1 = command.CreateParameter();
            p1.ParameterName = "@sync_scope_hash";
            p1.DbType = DbType.String;
            p1.Size = 100;
            command.Parameters.Add(p1);

            return command;
        }

        /// <inheritdoc />
        public override DbCommand GetInsertScopeInfoCommand(DbConnection connection, DbTransaction transaction)
            => this.GetSaveScopeInfoCommand(false, connection, transaction);

        /// <inheritdoc />
        public override DbCommand GetInsertScopeInfoClientCommand(DbConnection connection, DbTransaction transaction)
            => this.GetSaveScopeInfoClientCommand(false, connection, transaction);

        /// <inheritdoc />
        public override DbCommand GetUpdateScopeInfoCommand(DbConnection connection, DbTransaction transaction)
            => this.GetSaveScopeInfoCommand(true, connection, transaction);

        /// <inheritdoc />
        public override DbCommand GetUpdateScopeInfoClientCommand(DbConnection connection, DbTransaction transaction)
            => this.GetSaveScopeInfoClientCommand(true, connection, transaction);

        /// <summary>
        /// Gets the save scope info command.
        /// </summary>
        public DbCommand GetSaveScopeInfoCommand(bool exist, DbConnection connection, DbTransaction transaction)
        {
            var tableName = this.ScopeInfoTableNames.NormalizedName;

            var stmtText = new StringBuilder();

            stmtText.AppendLine(exist
                    ? $"UPDATE {tableName} " +
                      $"SET sync_scope_schema=@sync_scope_schema, " +
                      $"sync_scope_setup=@sync_scope_setup, " +
                      $"sync_scope_version=@sync_scope_version, " +
                      $"sync_scope_last_clean_timestamp=@sync_scope_last_clean_timestamp, " +
                      $"sync_scope_properties=@sync_scope_properties " +
                      $"WHERE sync_scope_name=@sync_scope_name;"

                    : $"INSERT INTO {tableName} " +
                      $"(sync_scope_name, sync_scope_schema, sync_scope_setup, sync_scope_version, " +
                      $"sync_scope_last_clean_timestamp, sync_scope_properties) " +
                      $"VALUES " +
                      $"(@sync_scope_name, @sync_scope_schema, @sync_scope_setup, @sync_scope_version, " +
                      $"@sync_scope_last_clean_timestamp, @sync_scope_properties);");

            stmtText.AppendLine(@$"SELECT sync_scope_name
                           , sync_scope_schema
                           , sync_scope_setup
                           , sync_scope_version
                           , sync_scope_last_clean_timestamp
                           , sync_scope_properties
                    FROM  {tableName}
                    WHERE sync_scope_name=@sync_scope_name;");

            var command = new SqliteCommand(stmtText.ToString(), (SqliteConnection)connection, (SqliteTransaction)transaction);

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

        /// <summary>
        /// Gets the save scope info client command.
        /// </summary>
        public DbCommand GetSaveScopeInfoClientCommand(bool exist, DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableNames.NormalizedName}_client";

            var stmtText = new StringBuilder();

            stmtText.AppendLine(exist
                    ? $"UPDATE {tableName} " +
                      $"SET scope_last_sync_timestamp=@scope_last_sync_timestamp, " +
                      $"scope_last_server_sync_timestamp=@scope_last_server_sync_timestamp, " +
                      $"scope_last_sync=@scope_last_sync, " +
                      $"scope_last_sync_duration=@scope_last_sync_duration, " +
                      $"sync_scope_properties=@sync_scope_properties,  " +
                      $"sync_scope_errors=@sync_scope_errors,  " +
                      $"sync_scope_parameters=@sync_scope_parameters  " +
                      $"WHERE sync_scope_id=@sync_scope_id and sync_scope_name=@sync_scope_name and sync_scope_hash=@sync_scope_hash;"

                    : $"INSERT INTO {tableName} " +
                      $"(sync_scope_name, sync_scope_id, sync_scope_hash, sync_scope_parameters, scope_last_sync_timestamp, scope_last_server_sync_timestamp, " +
                      $"scope_last_sync, scope_last_sync_duration, sync_scope_errors, sync_scope_properties) " +
                      $"VALUES " +
                      $"(@sync_scope_name, @sync_scope_id, @sync_scope_hash, @sync_scope_parameters, @scope_last_sync_timestamp, @scope_last_server_sync_timestamp, " +
                      $"@scope_last_sync, @scope_last_sync_duration, @sync_scope_errors, @sync_scope_properties);");

            stmtText.AppendLine(@$"SELECT sync_scope_id
                           , sync_scope_name
                           , sync_scope_hash
                           , sync_scope_parameters
                           , scope_last_sync_timestamp
                           , scope_last_server_sync_timestamp
                           , scope_last_sync
                           , scope_last_sync_duration
                           , sync_scope_errors
                           , sync_scope_properties
                    FROM  {tableName}
                    WHERE sync_scope_name=@sync_scope_name and sync_scope_id=@sync_scope_id and sync_scope_hash=@sync_scope_hash;");

            var command = new SqliteCommand(stmtText.ToString(), (SqliteConnection)connection, (SqliteTransaction)transaction);

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

        /// <inheritdoc />
        public override DbCommand GetDropScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = this.ScopeInfoTableNames.NormalizedName;

            var commandText = $"DROP TABLE {tableName}";

            var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.Connection = connection;
            command.Transaction = transaction;

            return command;
        }

        /// <inheritdoc />
        public override DbCommand GetDropScopeInfoClientTableCommand(DbConnection connection, DbTransaction transaction)
        {

            var commandText = $"DROP TABLE {this.ScopeInfoTableNames.NormalizedName}_client";

            var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.Connection = connection;
            command.Transaction = transaction;

            return command;
        }
    }
}