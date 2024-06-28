using Dotmim.Sync.Builders;
using Microsoft.Data.Sqlite;
using System.Data;
using System.Data.Common;
using System.Text;

namespace Dotmim.Sync.Sqlite
{
    public class SqliteScopeBuilder : DbScopeBuilder
    {
        public SqliteScopeBuilder(string scopeInfoTableName) : base(scopeInfoTableName)
        {
        }

        public override DbCommand GetLocalTimestampCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText = $"Select {SqliteObjectNames.TimestampValue}";
            var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.Connection = connection;
            command.Transaction = transaction;
            return command;
        }

        //Exists Scope Table
        // ------------------------------

        public override DbCommand GetExistsScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = this.ScopeInfoTableName.Unquoted().Normalized().ToString();

            var commandText = $@"SELECT count(*) FROM sqlite_master WHERE type='table' AND name='{tableName}'";
            var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.Connection = connection;
            command.Transaction = transaction;
            return command;
        }
        public override DbCommand GetExistsScopeInfoClientTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_client";
            var commandText = $@"SELECT count(*) FROM sqlite_master WHERE type='table' AND name='{tableName}'";
            var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.Connection = connection;
            command.Transaction = transaction;
            return command;
        }


        //Exists Scope Info
        // ------------------------------
        public override DbCommand GetExistsScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = this.ScopeInfoTableName.Unquoted().Normalized().ToString();

            var commandText = $@"Select count(*) from {tableName} where sync_scope_name = @sync_scope_name";

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
        public override DbCommand GetExistsScopeInfoClientCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_client";


            var command = connection.CreateCommand();
            command.CommandText = $@"Select count(*) from [{tableName}] 
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


        // Create Table
        // ------------------------------
        public override DbCommand GetCreateScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{ScopeInfoTableName.Unquoted().Normalized().ToString()}";

            var commandText =
                    $@"CREATE TABLE [{tableName}](
                        sync_scope_name text NOT NULL,
                        sync_scope_schema text NULL,
                        sync_scope_setup text NULL,
                        sync_scope_version text NULL,
                        sync_scope_last_clean_timestamp integer NULL,
                        sync_scope_properties text NULL,
                        CONSTRAINT PKey_{tableName} PRIMARY KEY(sync_scope_name))";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandText;
            return command;

        }
        public override DbCommand GetCreateScopeInfoClientTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{ScopeInfoTableName.Unquoted().Normalized().ToString()}_client";

            var commandText =
                    $@"CREATE TABLE [{tableName}](
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
                        CONSTRAINT PKey_{tableName} PRIMARY KEY(sync_scope_id, sync_scope_name, sync_scope_hash))";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandText;
            return command;

        }


        // Get all scopes
        // ------------------------------

        public override DbCommand GetAllScopeInfosCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = this.ScopeInfoTableName.Unquoted().Normalized().ToString();

            var commandText =
                $@"SELECT [sync_scope_name], 
                          [sync_scope_schema], 
                          [sync_scope_setup], 
                          [sync_scope_version],
                          [sync_scope_last_clean_timestamp],
                          [sync_scope_properties]
                    FROM  {tableName}";

            var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.Connection = connection;
            command.Transaction = transaction;

            return command;
        }
        public override DbCommand GetAllScopeInfoClientsCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_client";
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
                    FROM  [{tableName}]";

            var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.Connection = connection;
            command.Transaction = transaction;

            return command;
        }


        // Delete scope
        // ------------------------------
        public override DbCommand GetDeleteScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = this.ScopeInfoTableName.Unquoted().Normalized().ToString();

            var commandText = $@"DELETE FROM [{tableName}] WHERE [sync_scope_name] = @sync_scope_name";

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
        public override DbCommand GetDeleteScopeInfoClientCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_client";
            var commandText =
                $@"DELETE FROM [{tableName}]
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


        // Get scope
        // ------------------------------
        public override DbCommand GetScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = this.ScopeInfoTableName.Unquoted().Normalized().ToString();

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
        public override DbCommand GetScopeInfoClientCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_client";
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


        // Insert Scope
        // -------------------------------
        public override DbCommand GetInsertScopeInfoCommand(DbConnection connection, DbTransaction transaction) => GetSaveScopeInfoCommand(false, connection, transaction);
        public override DbCommand GetInsertScopeInfoClientCommand(DbConnection connection, DbTransaction transaction) => GetSaveScopeInfoClientCommand(false, connection, transaction);


        // Update Scope
        // -------------------------------
        public override DbCommand GetUpdateScopeInfoCommand(DbConnection connection, DbTransaction transaction) => GetSaveScopeInfoCommand(true, connection, transaction);
        public override DbCommand GetUpdateScopeInfoClientCommand(DbConnection connection, DbTransaction transaction) => GetSaveScopeInfoClientCommand(true, connection, transaction);

        public DbCommand GetSaveScopeInfoCommand(bool exist, DbConnection connection, DbTransaction transaction)
        {
            var tableName = this.ScopeInfoTableName.Unquoted().Normalized().ToString();

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

        public DbCommand GetSaveScopeInfoClientCommand(bool exist, DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_client";

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

        // Drop Scope table
        // -------------------------------

        public override DbCommand GetDropScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = this.ScopeInfoTableName.Unquoted().Normalized().ToString();

            var commandText = $"DROP TABLE {tableName}";

            var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.Connection = connection;
            command.Transaction = transaction;

            return command;
        }
        public override DbCommand GetDropScopeInfoClientTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText = $"DROP TABLE {ScopeInfoTableName.Unquoted().ToString()}_client";

            var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.Connection = connection;
            command.Transaction = transaction;

            return command;
        }



    }
}
