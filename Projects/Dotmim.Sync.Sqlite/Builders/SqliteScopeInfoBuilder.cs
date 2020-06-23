using Dotmim.Sync.Builders;

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Microsoft.Data.Sqlite;
using System.Diagnostics;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace Dotmim.Sync.Sqlite
{
    public class SqliteScopeInfoBuilder : IDbScopeInfoBuilder
    {
        private readonly ParserName scopeTableName;

        public SqliteScopeInfoBuilder(string scopeTableName)
        {
            this.scopeTableName = ParserName.Parse(scopeTableName);
        }

        public async Task CreateClientScopeInfoTableAsync(DbConnection connection, DbTransaction transaction)
        {
            var commandText =
                    $@"CREATE TABLE {scopeTableName.Quoted().ToString()}(
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

            using (var command = new SqliteCommand(commandText, (SqliteConnection)connection, (SqliteTransaction)transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }

        public Task CreateServerHistoryScopeInfoTableAsync(DbConnection connection, DbTransaction transaction) => throw new NotImplementedException();

        public Task CreateServerScopeInfoTableAsync(DbConnection connection, DbTransaction transaction) => throw new NotImplementedException();

        public async Task DropClientScopeInfoTableAsync(DbConnection connection, DbTransaction transaction)
        {
            var commandText = $"DROP Table {scopeTableName.Unquoted().ToString()}";

            using (var command = new SqliteCommand(commandText, (SqliteConnection)connection, (SqliteTransaction)transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }

        public Task DropServerHistoryScopeInfoTableAsync(DbConnection connection, DbTransaction transaction) => throw new NotImplementedException();

        public Task DropServerScopeInfoTableAsync(DbConnection connection, DbTransaction transaction) => throw new NotImplementedException();

        public async Task<List<ScopeInfo>> GetAllClientScopesAsync(string scopeName, DbConnection connection, DbTransaction transaction)
        {
            List<ScopeInfo> scopes = new List<ScopeInfo>();
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
                    FROM  {scopeTableName.Unquoted().ToString()}
                    WHERE sync_scope_name = @sync_scope_name";

            using (var command = new SqliteCommand(commandText, (SqliteConnection)connection, (SqliteTransaction)transaction))
            {
                var p = command.CreateParameter();
                p.ParameterName = "@sync_scope_name";
                p.Value = scopeName;
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                using (DbDataReader reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    if (reader.HasRows)
                    {
                        // read only the first one
                        while (reader.Read())
                        {
                            var scopeInfo = new ScopeInfo();
                            scopeInfo.Name = reader["sync_scope_name"] as String;
                            scopeInfo.Schema = reader["sync_scope_schema"] == DBNull.Value ? null : JsonConvert.DeserializeObject<SyncSet>((string)reader["sync_scope_schema"]);
                            scopeInfo.Setup = reader["sync_scope_setup"] == DBNull.Value ? null : JsonConvert.DeserializeObject<SyncSetup>((string)reader["sync_scope_setup"]);
                            scopeInfo.Version = reader["sync_scope_version"] as String;
                            scopeInfo.Id = reader.GetGuid(reader.GetOrdinal("sync_scope_id"));
                            scopeInfo.LastSync = reader["scope_last_sync"] != DBNull.Value
                                            ? (DateTime?)reader.GetDateTime(reader.GetOrdinal("scope_last_sync"))
                                            : null;
                            scopeInfo.LastServerSyncTimestamp = reader["scope_last_server_sync_timestamp"] != DBNull.Value ? reader.GetInt64(reader.GetOrdinal("scope_last_server_sync_timestamp")) : 0L;
                            scopeInfo.LastSyncTimestamp = reader["scope_last_sync_timestamp"] != DBNull.Value ? reader.GetInt64(reader.GetOrdinal("scope_last_sync_timestamp")) : 0L;
                            scopeInfo.LastSyncDuration = reader["scope_last_sync_duration"] != DBNull.Value ? reader.GetInt64(reader.GetOrdinal("scope_last_sync_duration")) : 0L;
                            scopes.Add(scopeInfo);
                        }
                    }
                }

                return scopes;
            }
        }

        public Task<List<ServerHistoryScopeInfo>> GetAllServerHistoryScopesAsync(string scopeName, DbConnection connection, DbTransaction transaction) => throw new NotImplementedException();

        public Task<List<ServerScopeInfo>> GetAllServerScopesAsync(string scopeName, DbConnection connection, DbTransaction transaction) => throw new NotImplementedException();

        public async Task<long> GetLocalTimestampAsync(DbConnection connection, DbTransaction transaction)
        {

            var commandText = $"Select {SqliteObjectNames.TimestampValue}";

            using (var command = new SqliteCommand(commandText, (SqliteConnection)connection, (SqliteTransaction)transaction))
            {
                long result = Convert.ToInt64(await command.ExecuteScalarAsync().ConfigureAwait(false));
                return result;
            }
        }

        public async Task<ScopeInfo> InsertOrUpdateClientScopeInfoAsync(ScopeInfo scopeInfo, DbConnection connection, DbTransaction transaction)
        {
            var commandText = $@"Select count(*) from {scopeTableName.Unquoted().ToString()} where sync_scope_id = @sync_scope_id";

            bool exist;

            using (var command = new SqliteCommand(commandText, (SqliteConnection)connection, (SqliteTransaction)transaction))
            {
                var p = command.CreateParameter();
                p.ParameterName = "@sync_scope_id";
                p.Value = scopeInfo.Id.ToString();
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                exist = ((long)await command.ExecuteScalarAsync().ConfigureAwait(false)) > 0;
            }

            string stmtText = exist
                    ? $"Update {scopeTableName.Unquoted().ToString()} set sync_scope_name=@sync_scope_name, sync_scope_schema=@sync_scope_schema, sync_scope_setup=@sync_scope_setup, sync_scope_version=@sync_scope_version, scope_last_sync=@scope_last_sync, scope_last_server_sync_timestamp=@scope_last_server_sync_timestamp,  scope_last_sync_timestamp=@scope_last_sync_timestamp, scope_last_sync_duration=@scope_last_sync_duration where sync_scope_id=@sync_scope_id"
                    : $"Insert into {scopeTableName.Unquoted().ToString()} (sync_scope_name, sync_scope_schema, sync_scope_setup, sync_scope_version, scope_last_sync, scope_last_sync_duration, scope_last_server_sync_timestamp, scope_last_sync_timestamp, sync_scope_id) values (@sync_scope_name, @sync_scope_schema, @sync_scope_setup, @sync_scope_version, @scope_last_sync, @scope_last_sync_duration, @scope_last_server_sync_timestamp, @scope_last_sync_timestamp, @sync_scope_id)";

            using (var command = new SqliteCommand(stmtText, (SqliteConnection)connection, (SqliteTransaction)transaction))
            {
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

                using (DbDataReader reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {

                            scopeInfo.Name = reader["sync_scope_name"] as string;
                            scopeInfo.Schema = reader["sync_scope_schema"] == DBNull.Value ? null : JsonConvert.DeserializeObject<SyncSet>((string)reader["sync_scope_schema"]);
                            scopeInfo.Setup = reader["sync_scope_setup"] == DBNull.Value ? null : JsonConvert.DeserializeObject<SyncSetup>((string)reader["sync_scope_setup"]);
                            scopeInfo.Version = reader["sync_scope_version"] as string;
                            scopeInfo.Id = reader.GetGuid(reader.GetOrdinal("sync_scope_id"));
                            scopeInfo.LastSync = reader["scope_last_sync"] != DBNull.Value
                                        ? (DateTime?)reader.GetDateTime(reader.GetOrdinal("scope_last_sync"))
                                        : null;
                            scopeInfo.LastSyncTimestamp = reader["scope_last_sync_timestamp"] != DBNull.Value ? reader.GetInt64(reader.GetOrdinal("scope_last_sync_timestamp")) : 0L;
                            scopeInfo.LastServerSyncTimestamp = reader["scope_last_server_sync_timestamp"] != DBNull.Value ? reader.GetInt64(reader.GetOrdinal("scope_last_server_sync_timestamp")) : 0L;
                            scopeInfo.LastSyncDuration = reader["scope_last_sync_duration"] != DBNull.Value ? reader.GetInt64(reader.GetOrdinal("scope_last_sync_duration")) : 0L;
                        }
                    }
                }

                return scopeInfo;
            }
        }

        public Task<ServerHistoryScopeInfo> InsertOrUpdateServerHistoryScopeInfoAsync(ServerHistoryScopeInfo serverHistoryScopeInfo, DbConnection connection, DbTransaction transaction) => throw new NotImplementedException();

        public Task<ServerScopeInfo> InsertOrUpdateServerScopeInfoAsync(ServerScopeInfo serverScopeInfo, DbConnection connection, DbTransaction transaction) => throw new NotImplementedException();

        public async Task<bool> NeedToCreateClientScopeInfoTableAsync(DbConnection connection, DbTransaction transaction)
        {

            var commandText =
                $@"SELECT count(*) FROM sqlite_master WHERE type='table' AND name='{scopeTableName.Unquoted().ToString()}'";

            using (var command = new SqliteCommand(commandText, (SqliteConnection)connection, (SqliteTransaction)transaction))
            {
                return ((long)await command.ExecuteScalarAsync()) != 1;
            }
        }

        public Task<bool> NeedToCreateServerHistoryScopeInfoTableAsync(DbConnection connection, DbTransaction transaction) => throw new NotImplementedException();

        public Task<bool> NeedToCreateServerScopeInfoTableAsync(DbConnection connection, DbTransaction transaction) => throw new NotImplementedException();
    }
}
