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


        public Task<DbCommand> GetAllClientScopesCommandAsync(string scopeName, DbConnection connection, DbTransaction transaction)
        {
            var commandText =
                    $@"SELECT sync_scope_id
                           , sync_scope_name
                           , sync_scope_schema
                           , sync_scope_setup
                           , sync_scope_version
                           , scope_last_sync
                           , scope_last_server_sync_timestamp
                           , scope_last_sync_timestamp
                           , scope_last_sync_duration
                    FROM  {ScopeInfoTableName.Unquoted().ToString()}
                    WHERE sync_scope_name = @sync_scope_name";

            var command = connection.CreateCommand();

            command.CommandText = commandText;

            command.Connection = connection;
            if (transaction != null)
                command.Transaction = transaction;

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.Value = scopeName;
            p.DbType = DbType.String;
            command.Parameters.Add(p);


            return Task.FromResult(command);
        }

        public override Task<DbCommand> GetAllScopesCommandAsync(DbScopeType scopeType, string scopeName, DbConnection connection, DbTransaction transaction)
        {
            if (scopeType != DbScopeType.Client)
                return Task.FromResult<DbCommand>(null);

            return GetAllClientScopesCommandAsync(scopeName, connection, transaction);
        }



        public override Task<DbCommand> GetCreateScopeInfoTableCommandAsync(DbScopeType scopeType, DbConnection connection, DbTransaction transaction)
        {
            if (scopeType != DbScopeType.Client)
                return Task.FromResult<DbCommand>(null);

            var commandText =
                    $@"CREATE TABLE {ScopeInfoTableName.Quoted().ToString()}(
                        sync_scope_id blob NOT NULL PRIMARY KEY,
	                    sync_scope_name text NOT NULL,
	                    sync_scope_schema text NULL,
	                    sync_scope_setup text NULL,
	                    sync_scope_version text NULL,
                        scope_last_server_sync_timestamp integer NULL,
                        scope_last_sync_timestamp integer NULL,
                        scope_last_sync_duration integer NULL,
                        scope_last_sync datetime NULL
                        )";

            var command = connection.CreateCommand();

            if (transaction != null)
                command.Transaction = transaction;

            command.CommandText = commandText;

            return Task.FromResult(command);
        }

        public override Task<DbCommand> GetDropScopeInfoTableCommandAsync(DbScopeType scopeType, DbConnection connection, DbTransaction transaction)
        {
            if (scopeType != DbScopeType.Client)
                return Task.FromResult<DbCommand>(null);

            var commandText = $"DROP Table {ScopeInfoTableName.Unquoted().ToString()}";

            var command = connection.CreateCommand();

            command.CommandText = commandText;

            command.Connection = connection;
            if (transaction != null)
                command.Transaction = transaction;

            return Task.FromResult(command);
        }
        public override Task<DbCommand> GetExistsScopeInfoTableCommandAsync(DbScopeType scopeType, DbConnection connection, DbTransaction transaction)
        {
            if (scopeType != DbScopeType.Client)
                return Task.FromResult<DbCommand>(null);

            var commandText = $@"SELECT count(*) FROM sqlite_master WHERE type='table' AND name='{ScopeInfoTableName.Unquoted().ToString()}'";

            var command = connection.CreateCommand();

            command.CommandText = commandText;

            command.Connection = connection;
            if (transaction != null)
                command.Transaction = transaction;

            return Task.FromResult(command);

        }
        public override Task<DbCommand> GetLocalTimestampCommandAsync(DbConnection connection, DbTransaction transaction)
        {

            var commandText = $"Select {SqliteObjectNames.TimestampValue}";

            var command = connection.CreateCommand();

            command.CommandText = commandText;

            command.Connection = connection;
            if (transaction != null)
                command.Transaction = transaction;

            return Task.FromResult(command);
        }


        public override async Task<DbCommand> GetSaveScopeInfoCommandAsync(DbScopeType scopeType, object scopeInfoObject, DbConnection connection, DbTransaction transaction)
        {
            if (scopeType != DbScopeType.Client)
                return null;

            var scopeInfo = scopeInfoObject as ScopeInfo;

            var commandText = $@"Select count(*) from {ScopeInfoTableName.Unquoted().ToString()} where sync_scope_id = @sync_scope_id";

            bool exist;

            using (var scommand = new SqliteCommand(commandText, (SqliteConnection)connection, (SqliteTransaction)transaction))
            {
                var p0 = scommand.CreateParameter();
                p0.ParameterName = "@sync_scope_id";
                p0.Value = scopeInfo.Id.ToString();
                p0.DbType = DbType.String;
                scommand.Parameters.Add(p0);

                exist = ((long)await scommand.ExecuteScalarAsync().ConfigureAwait(false)) > 0;
            }

            var stmtText = new StringBuilder();

            stmtText.AppendLine(exist
                    ? $"Update {ScopeInfoTableName.Unquoted().ToString()} set sync_scope_name=@sync_scope_name, sync_scope_schema=@sync_scope_schema, sync_scope_setup=@sync_scope_setup, sync_scope_version=@sync_scope_version, scope_last_sync=@scope_last_sync, scope_last_server_sync_timestamp=@scope_last_server_sync_timestamp,  scope_last_sync_timestamp=@scope_last_sync_timestamp, scope_last_sync_duration=@scope_last_sync_duration where sync_scope_id=@sync_scope_id;"
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
                    WHERE sync_scope_name = @sync_scope_name");

            var command = new SqliteCommand(stmtText.ToString(), (SqliteConnection)connection, (SqliteTransaction)transaction);

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.Value = scopeInfo.Name;
            p.DbType = DbType.String;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_schema";
            p.Value = scopeInfo.Schema == null ? DBNull.Value : (object)JsonConvert.SerializeObject(scopeInfo.Schema);
            p.DbType = DbType.String;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_setup";
            p.Value = scopeInfo.Setup == null ? DBNull.Value : (object)JsonConvert.SerializeObject(scopeInfo.Setup);
            p.DbType = DbType.String;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_version";
            p.Value = string.IsNullOrEmpty(scopeInfo.Version) ? DBNull.Value : (object)scopeInfo.Version;
            p.DbType = DbType.String;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@scope_last_sync";
            p.Value = scopeInfo.LastSync.HasValue ? (object)scopeInfo.LastSync.Value : DBNull.Value;
            p.DbType = DbType.DateTime;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@scope_last_server_sync_timestamp";
            p.Value = scopeInfo.LastServerSyncTimestamp;
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@scope_last_sync_timestamp";
            p.Value = scopeInfo.LastSyncTimestamp;
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@scope_last_sync_duration";
            p.Value = scopeInfo.LastSyncDuration;
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_id";
            p.Value = scopeInfo.Id.ToString();
            p.DbType = DbType.String;
            command.Parameters.Add(p);

            return command;

        }
    }
}
