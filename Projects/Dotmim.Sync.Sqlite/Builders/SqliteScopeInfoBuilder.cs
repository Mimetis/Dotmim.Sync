using Dotmim.Sync.Builders;
using Dotmim.Sync.Log;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Microsoft.Data.Sqlite;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Dotmim.Sync.Sqlite
{
    public class SqliteScopeInfoBuilder : IDbScopeInfoBuilder
    {
        private readonly ParserName scopeTableName;
        private readonly SqliteConnection connection;
        private readonly SqliteTransaction transaction;

        public SqliteScopeInfoBuilder(string scopeTableName, DbConnection connection, DbTransaction transaction = null)
        {
            this.connection = connection as SqliteConnection;
            this.transaction = transaction as SqliteTransaction;
            this.scopeTableName = ParserName.Parse(scopeTableName);
        }

        public async Task CreateClientScopeInfoTableAsync()
        {
            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                command.CommandText =
                    $@"CREATE TABLE {scopeTableName.Quoted()}(
                        sync_scope_id blob NOT NULL PRIMARY KEY,
	                    sync_scope_name text NOT NULL,
	                    sync_scope_schema text NULL,
	                    sync_scope_version text NULL,
                        scope_last_server_sync_timestamp integer NULL,
                        scope_last_sync_timestamp integer NULL,
                        scope_last_sync_duration integer NULL,
                        scope_last_sync datetime NULL
                        )";

                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during CreateTableScope : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

                if (command != null)
                    command.Dispose();
            }
        }

        public Task CreateServerHistoryScopeInfoTableAsync() => throw new NotImplementedException();

        public Task CreateServerScopeInfoTableAsync() => throw new NotImplementedException();

        public async Task DropClientScopeInfoTableAsync()
        {
            var command = connection.CreateCommand();

            if (transaction != null)
                command.Transaction = transaction;

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                command.CommandText = $"DROP Table {scopeTableName.Unquoted().ToString()}";

                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during DropScopeInfoTable : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

                if (command != null)
                    command.Dispose();
            }
        }

        public Task DropServerHistoryScopeInfoTableAsync() => throw new NotImplementedException();

        public Task DropServerScopeInfoTableAsync() => throw new NotImplementedException();

        public async Task<List<ScopeInfo>> GetAllClientScopesAsync(string scopeName)
        {
            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;

            bool alreadyOpened = connection.State == ConnectionState.Open;

            List<ScopeInfo> scopes = new List<ScopeInfo>();
            try
            {
                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                command.CommandText =
                    $@"SELECT sync_scope_id
                           , sync_scope_name
                           , sync_scope_schema
                           , sync_scope_version
                           , scope_last_sync
                           , scope_last_server_sync_timestamp
                           , scope_last_sync_timestamp
                           , scope_last_sync_duration
                    FROM  {scopeTableName.Unquoted()}
                    WHERE sync_scope_name = @sync_scope_name";

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
                            scopeInfo.Schema = reader["sync_scope_schema"] as String;
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
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during GetAllScopes : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

                if (command != null)
                    command.Dispose();
            }
        }

        public Task<List<ServerScopeInfo>> GetAllServerScopesAsync(string scopeName) => throw new NotImplementedException();

        public async Task<long> GetLocalTimestampAsync()
        {
            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;

            bool alreadyOpened = connection.State == ConnectionState.Open;
            try
            {
                command.CommandText = $"Select {SqliteObjectNames.TimestampValue}";

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                    long result = Convert.ToInt64(await command.ExecuteScalarAsync().ConfigureAwait(false));

                return result;
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during GetLocalTimestamp : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

                if (command != null)
                    command.Dispose();
            }
        }

        public async Task<ScopeInfo> InsertOrUpdateClientScopeInfoAsync(ScopeInfo scopeInfo)
        {
            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                command.CommandText = $@"Select count(*) from {scopeTableName.Unquoted().ToString()} where sync_scope_id = @sync_scope_id";

                var p = command.CreateParameter();
                p.ParameterName = "@sync_scope_id";
                p.Value = scopeInfo.Id.ToString();
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                var exist = ((long)await command.ExecuteScalarAsync().ConfigureAwait(false)) > 0;

                string stmtText = exist
                    ? $"Update {scopeTableName.Unquoted().ToString()} set sync_scope_name=@sync_scope_name, sync_scope_schema=@sync_scope_schema, sync_scope_version=@sync_scope_version, scope_last_sync=@scope_last_sync, scope_last_server_sync_timestamp=@scope_last_server_sync_timestamp,  scope_last_sync_timestamp=@scope_last_sync_timestamp, scope_last_sync_duration=@scope_last_sync_duration where sync_scope_id=@sync_scope_id"
                    : $"Insert into {scopeTableName.Unquoted().ToString()} (sync_scope_name, sync_scope_schema, sync_scope_version, scope_last_sync, scope_last_sync_duration, scope_last_server_sync_timestamp, scope_last_sync_timestamp, sync_scope_id) values (@sync_scope_name, @sync_scope_schema, @sync_scope_version, @scope_last_sync, @scope_last_sync_duration, @scope_last_server_sync_timestamp, @scope_last_sync_timestamp, @sync_scope_id)";

                command = connection.CreateCommand();
                command.CommandText = stmtText;

                p = command.CreateParameter();
                p.ParameterName = "@sync_scope_name";
                p.Value = scopeInfo.Name;
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                p = command.CreateParameter();
                p.ParameterName = "@sync_scope_schema";
                p.Value = string.IsNullOrEmpty(scopeInfo.Schema) ? DBNull.Value : (object)scopeInfo.Schema;
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
                            scopeInfo.Schema = reader["sync_scope_schema"] as string;
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
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during InsertOrUpdateClientScopeInfoAsync : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

                if (command != null)
                    command.Dispose();
            }
        }

        public async Task<ServerHistoryScopeInfo> InsertOrUpdateServerHistoryScopeInfoAsync(ServerHistoryScopeInfo serverHistoryScopeInfo) 
            => throw new NotImplementedException();

        public async Task<ServerScopeInfo> InsertOrUpdateServerScopeInfoAsync(ServerScopeInfo serverScopeInfo) 
            => throw new NotImplementedException();

        public async Task<bool> NeedToCreateClientScopeInfoTableAsync()
        {
            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                command.CommandText =
                    $@"SELECT count(*) FROM sqlite_master WHERE type='table' AND name='{scopeTableName.Unquoted().ToString()}'";

                return ((long)await command.ExecuteScalarAsync()) != 1;

            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during NeedToCreateClientScopeInfoTableAsync command : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

                if (command != null)
                    command.Dispose();
            }
        }

        public async Task<bool> NeedToCreateServerHistoryScopeInfoTableAsync() 
            => throw new NotImplementedException();

        public async Task<bool> NeedToCreateServerScopeInfoTableAsync() 
            => throw new NotImplementedException();
    }
}
