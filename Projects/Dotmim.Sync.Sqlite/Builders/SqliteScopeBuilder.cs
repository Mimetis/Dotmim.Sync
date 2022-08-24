using Dotmim.Sync.Builders;
using Microsoft.Data.Sqlite;
using Newtonsoft.Json;
using System;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

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
            var commandText = $@"SELECT count(*) FROM sqlite_master WHERE type='table' AND name='{ScopeInfoTableName.Unquoted().ToString()}'";
            var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.Connection = connection;
            command.Transaction = transaction;
            return command;
        }
        public override DbCommand GetExistsScopeInfoClientTableCommand(DbConnection connection, DbTransaction transaction) => null;
        public override DbCommand GetExistsServerScopeInfoTableCommand(DbConnection connection, DbTransaction transaction) => null;


        //Exists Scope Info
        // ------------------------------
        public override DbCommand GetExistsScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText = $@"Select count(*) from {ScopeInfoTableName.Unquoted().ToString()} where sync_scope_name = @sync_scope_name";

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
        public override DbCommand GetExistsScopeInfoClientCommand(DbConnection connection, DbTransaction transaction) => null;
        public override DbCommand GetExistsServerScopeInfoCommand(DbConnection connection, DbTransaction transaction) => null;


        // Create Table
        // ------------------------------
        public override DbCommand GetCreateScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{ScopeInfoTableName.Unquoted().Normalized().ToString()}";


            var commandText =
                    $@"CREATE TABLE [{tableName}](
                        sync_scope_id blob NOT NULL,
                        sync_scope_name text NOT NULL,
                        sync_scope_schema text NULL,
                        sync_scope_setup text NULL,
                        sync_scope_version text NULL,
                        scope_last_server_sync_timestamp integer NULL,
                        scope_last_sync_timestamp integer NULL,
                        scope_last_sync_duration integer NULL,
                        scope_last_sync datetime NULL,
                        CONSTRAINT PK_{tableName} PRIMARY KEY(sync_scope_id, sync_scope_name))";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandText;
            return command;

        }
        public override DbCommand GetCreateScopeInfoClientTableCommand(DbConnection connection, DbTransaction transaction) => null;
        public override DbCommand GetCreateServerScopeInfoTableCommand(DbConnection connection, DbTransaction transaction) => null;


        // Get all scopes
        // ------------------------------

        public override DbCommand GetAllScopeInfosCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText = $@"SELECT sync_scope_id, sync_scope_name, sync_scope_schema, sync_scope_setup, sync_scope_version,
                               scope_last_sync, scope_last_server_sync_timestamp, scope_last_sync_timestamp, scope_last_sync_duration
                               FROM  {ScopeInfoTableName.Unquoted().ToString()}";

            var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.Connection = connection;
            command.Transaction = transaction;

            return command;
        }
        public override DbCommand GetAllServerScopesInfoCommand(DbConnection connection, DbTransaction transaction) => null;
        public override DbCommand GetAllScopeInfoClientsCommand(DbConnection connection, DbTransaction transaction) => null;


        // Delete scope
        // ------------------------------
        public override DbCommand GetDeleteScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText = $@"DELETE FROM  {ScopeInfoTableName.Unquoted().ToString()}
                               WHERE [sync_scope_name] = @sync_scope_name";

            var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.Connection = connection;
            command.Transaction = transaction;

            var p0 = command.CreateParameter();
            p0.ParameterName = "@sync_scope_name";
            p0.DbType = DbType.String;
            p0.Size = 100;
            command.Parameters.Add(p0);
            return command;
        }
        public override DbCommand GetDeleteServerScopeInfoCommand(DbConnection connection, DbTransaction transaction) => null;
        public override DbCommand GetDeleteScopeInfoClientCommand(DbConnection connection, DbTransaction transaction) => null;


        // Get scope
        // ------------------------------
        public override DbCommand GetScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText = $@"SELECT sync_scope_id, sync_scope_name, sync_scope_schema, sync_scope_setup, sync_scope_version,
                               scope_last_sync, scope_last_server_sync_timestamp, scope_last_sync_timestamp, scope_last_sync_duration
                               FROM  {ScopeInfoTableName.Unquoted().ToString()}
                               WHERE [sync_scope_name] = @sync_scope_name";

            var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.Connection = connection;
            command.Transaction = transaction;

            var p0 = command.CreateParameter();
            p0.ParameterName = "@sync_scope_name";
            p0.DbType = DbType.String;
            p0.Size = 100;
            command.Parameters.Add(p0);
            return command;
        }
        public override DbCommand GetServerScopeInfoCommand(DbConnection connection, DbTransaction transaction) => null;
        public override DbCommand GetScopeInfoClientCommand(DbConnection connection, DbTransaction transaction) => null;


        // Insert Scope
        // -------------------------------
        public override DbCommand GetInsertScopeInfoCommand(DbConnection connection, DbTransaction transaction) => GetSaveScopeInfoCommand(false, connection, transaction);
        public override DbCommand GetInsertScopeInfoClientCommand(DbConnection connection, DbTransaction transaction) => null;
        public override DbCommand GetInsertServerScopeInfoCommand(DbConnection connection, DbTransaction transaction) => null;


        // Update Scope
        // -------------------------------
        public override DbCommand GetUpdateScopeInfoCommand(DbConnection connection, DbTransaction transaction) => GetSaveScopeInfoCommand(true, connection, transaction);
        public override DbCommand GetUpdateServerScopeInfoCommand(DbConnection connection, DbTransaction transaction) => null;
        public override DbCommand GetUpdateScopeInfoClientCommand(DbConnection connection, DbTransaction transaction) => null;

        public DbCommand GetSaveScopeInfoCommand(bool exist, DbConnection connection, DbTransaction transaction)
        {
            var stmtText = new StringBuilder();

            stmtText.AppendLine(exist
                    ? $"Update {ScopeInfoTableName.Unquoted().ToString()} set sync_scope_schema=@sync_scope_schema, sync_scope_setup=@sync_scope_setup, sync_scope_version=@sync_scope_version, scope_last_sync=@scope_last_sync, scope_last_server_sync_timestamp=@scope_last_server_sync_timestamp,  scope_last_sync_timestamp=@scope_last_sync_timestamp, scope_last_sync_duration=@scope_last_sync_duration where sync_scope_id=@sync_scope_id and sync_scope_name=@sync_scope_name;"
                    : $"Insert into {ScopeInfoTableName.Unquoted().ToString()} (sync_scope_name, sync_scope_schema, sync_scope_setup, sync_scope_version, scope_last_sync, scope_last_sync_duration, scope_last_server_sync_timestamp, scope_last_sync_timestamp, sync_scope_id) values (@sync_scope_name, @sync_scope_schema, @sync_scope_setup, @sync_scope_version, @scope_last_sync, @scope_last_sync_duration, @scope_last_server_sync_timestamp, @scope_last_sync_timestamp, @sync_scope_id);");


            stmtText.AppendLine(@$"SELECT sync_scope_id
                           , sync_scope_name
                           , sync_scope_schema
                           , sync_scope_setup
                           , sync_scope_version
                           , scope_last_sync
                           , scope_last_server_sync_timestamp
                           , scope_last_sync_timestamp
                           , scope_last_sync_duration
                    FROM  {ScopeInfoTableName.Unquoted().ToString()}
                    WHERE sync_scope_name=@sync_scope_name and sync_scope_id=@sync_scope_id");

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
            p.ParameterName = "@scope_last_sync";
            p.DbType = DbType.DateTime;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@scope_last_server_sync_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@scope_last_sync_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@scope_last_sync_duration";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_id";
            p.DbType = DbType.String;
            p.Size = -1;
            command.Parameters.Add(p);

            return command;

        }


        // Drop Scope table
        // -------------------------------

        public override DbCommand GetDropScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText = $"DROP Table {ScopeInfoTableName.Unquoted().ToString()}";

            var command = connection.CreateCommand();
            command.CommandText = commandText;
            command.Connection = connection;
            command.Transaction = transaction;

            return command;
        }
        public override DbCommand GetDropScopeInfoClientTableCommand(DbConnection connection, DbTransaction transaction) => null;
        public override DbCommand GetDropServerScopeInfoTableCommand(DbConnection connection, DbTransaction transaction) => null;


    }
}
