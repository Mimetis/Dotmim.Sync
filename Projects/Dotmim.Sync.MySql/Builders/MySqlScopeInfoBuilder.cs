using Dotmim.Sync.Builders;

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using MySql.Data.MySqlClient;
using System.Diagnostics;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace Dotmim.Sync.MySql
{
    public class MySqlScopeInfoBuilder : IDbScopeInfoBuilder
    {
        private readonly ParserName scopeTableName;
        private readonly MySqlConnection connection;
        private readonly MySqlTransaction transaction;

        public MySqlScopeInfoBuilder(string scopeTableName, DbConnection connection, DbTransaction transaction = null)
        {
            this.connection = connection as MySqlConnection;
            this.transaction = transaction as MySqlTransaction;
            this.scopeTableName = ParserName.Parse(scopeTableName, "`");
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
                    $@"CREATE TABLE {scopeTableName.Quoted().ToString()}(
                        sync_scope_id varchar(36) NOT NULL,
	                    sync_scope_name varchar(100) NOT NULL,
	                    sync_scope_schema longtext NULL,
	                    sync_scope_setup longtext NULL,
	                    sync_scope_version varchar(10) NULL,
                        scope_last_sync datetime NULL,
                        scope_last_server_sync_timestamp bigint NULL,
                        scope_last_sync_timestamp bigint NULL,
                        scope_last_sync_duration bigint NULL,
                        PRIMARY KEY (sync_scope_id)
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

        public async Task CreateServerHistoryScopeInfoTableAsync()
        {
            var command = connection.CreateCommand();

            if (transaction != null)
                command.Transaction = transaction;

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                var tableName = $"{scopeTableName.Unquoted().Normalized().ToString()}_history";

                command.CommandText =
                    $@"CREATE TABLE `{tableName}`(
                        sync_scope_id varchar(36) NOT NULL,
                        sync_scope_name varchar(100) NOT NULL,
                        scope_last_sync_timestamp bigint NULL,
                        scope_last_sync_duration bigint NULL,
                        scope_last_sync datetime NULL,
                        PRIMARY KEY (sync_scope_id)
                        )";

                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during CreateServerHistoryScopeInfoTableAsync : {ex}");
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

        public async Task CreateServerScopeInfoTableAsync()
        {
            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                var tableName = $"{scopeTableName.Unquoted().Normalized().ToString()}_server";

                command.CommandText =
                    $@"CREATE TABLE `{tableName}` (
	                    sync_scope_name varchar(100) NOT NULL,
	                    sync_scope_schema longtext NULL,
	                    sync_scope_setup longtext NULL,
	                    sync_scope_version varchar(10) NULL,
                        sync_scope_last_clean_timestamp bigint NULL,
                        PRIMARY KEY (sync_scope_name)
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

                command.CommandText = $"drop table if exists {scopeTableName.Quoted().ToString()}";

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

        public async Task DropServerHistoryScopeInfoTableAsync()
        {
            var command = connection.CreateCommand();

            if (transaction != null)
                command.Transaction = transaction;

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                var tableName = $"{scopeTableName.Unquoted().Normalized().ToString()}_history";

                command.CommandText = $"drop table if exists `{tableName}`";

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

        public async Task DropServerScopeInfoTableAsync()
        {
            var command = connection.CreateCommand();

            if (transaction != null)
                command.Transaction = transaction;

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                var tableName = $"{scopeTableName.Unquoted().Normalized().ToString()}_server";

                command.CommandText = $"drop table if exists `{tableName}`";

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
                           , sync_scope_setup
                           , sync_scope_version
                           , scope_last_sync
                           , scope_last_server_sync_timestamp
                           , scope_last_sync_timestamp
                           , scope_last_sync_duration
                    FROM  {scopeTableName.Quoted().ToString()}
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
                            scopeInfo.Schema = reader["sync_scope_schema"] == DBNull.Value ? null : JsonConvert.DeserializeObject<SyncSet>((string)reader["sync_scope_schema"]);
                            scopeInfo.Setup = reader["sync_scope_setup"] == DBNull.Value ? null : JsonConvert.DeserializeObject<SyncSetup>((string)reader["sync_scope_setup"]);
                            scopeInfo.Version = reader["sync_scope_version"] as string;
                            scopeInfo.Id = SyncTypeConverter.TryConvertTo<Guid>(reader["sync_scope_id"]);
                            scopeInfo.LastSync = reader["scope_last_sync"] != DBNull.Value ? (DateTime?)reader["scope_last_sync"] : null;
                            scopeInfo.LastSyncDuration = reader["scope_last_sync_duration"] != DBNull.Value ? (long)reader["scope_last_sync_duration"] : 0L;
                            scopeInfo.LastServerSyncTimestamp = reader["scope_last_server_sync_timestamp"] != DBNull.Value ? (long)reader["scope_last_server_sync_timestamp"] : 0L;
                            scopeInfo.LastSyncTimestamp = reader["scope_last_sync_timestamp"] != DBNull.Value ? (long)reader["scope_last_sync_timestamp"] : 0L;
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

        public async Task<List<ServerScopeInfo>> GetAllServerScopesAsync(string scopeName)
        {
            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;

            bool alreadyOpened = connection.State == ConnectionState.Open;

            var tableName = $"{scopeTableName.Unquoted().Normalized().ToString()}_server";

            List<ServerScopeInfo> scopes = new List<ServerScopeInfo>();
            try
            {
                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                command.CommandText =
                    $@"SELECT sync_scope_name
                           , sync_scope_schema
                           , sync_scope_setup
                           , sync_scope_version
                           , sync_scope_last_clean_timestamp                    
                    FROM  `{tableName}`
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
                            var scopeInfo = new ServerScopeInfo();
                            scopeInfo.Name = reader["sync_scope_name"] as String;
                            scopeInfo.Schema = reader["sync_scope_schema"] == DBNull.Value ? null : JsonConvert.DeserializeObject<SyncSet>((string)reader["sync_scope_schema"]);
                            scopeInfo.Setup = reader["sync_scope_setup"] == DBNull.Value ? null : JsonConvert.DeserializeObject<SyncSetup>((string)reader["sync_scope_setup"]);
                            scopeInfo.Version = reader["sync_scope_version"] as string;
                            scopeInfo.LastCleanupTimestamp = reader["sync_scope_last_clean_timestamp"] != DBNull.Value ? (long)reader["sync_scope_last_clean_timestamp"] : 0L;
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

        public async Task<List<ServerHistoryScopeInfo>> GetAllServerHistoryScopesAsync(string scopeName)
        {
            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;

            bool alreadyOpened = connection.State == ConnectionState.Open;

            var tableName = $"{scopeTableName.Unquoted().Normalized().ToString()}_history";

            List<ServerHistoryScopeInfo> scopes = new List<ServerHistoryScopeInfo>();
            try
            {
                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                command.CommandText =
                    $@"SELECT  sync_scope_id
                           , sync_scope_name
                           , scope_last_sync_timestamp
                           , scope_last_sync_duration
                           , scope_last_sync           
                    FROM  `{tableName}`
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
                            var serverScopeInfo = new ServerHistoryScopeInfo();
                            serverScopeInfo.Id = SyncTypeConverter.TryConvertTo<Guid>(reader["sync_scope_id"]);
                            serverScopeInfo.Name = reader["sync_scope_name"] as string;
                            serverScopeInfo.LastSync = reader["scope_last_sync"] != DBNull.Value ? (DateTime?)reader["scope_last_sync"] : null;
                            serverScopeInfo.LastSyncDuration = reader["scope_last_sync_duration"] != DBNull.Value ? (long)reader["scope_last_sync_duration"] : 0;
                            serverScopeInfo.LastSyncTimestamp = reader["scope_last_sync_timestamp"] != DBNull.Value ? (long)reader["scope_last_sync_timestamp"] : 0;
                            scopes.Add(serverScopeInfo);
                        }
                    }
                }

                return scopes;
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during GetAllServerHistoryScopes : {ex}");
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

        public async Task<long> GetLocalTimestampAsync()
        {
            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;

            bool alreadyOpened = connection.State == ConnectionState.Open;
            try
            {
                command.CommandText = $"Select {MySqlObjectNames.TimestampValue}";

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
            bool alreadyOpened = connection.State == ConnectionState.Open;
            bool exist;
            try
            {
                using (var command = connection.CreateCommand())
                {
                    if (transaction != null)
                        command.Transaction = transaction;

                    if (!alreadyOpened)
                        await connection.OpenAsync().ConfigureAwait(false);

                    command.CommandText = $@"Select count(*) from {scopeTableName.Quoted().ToString()} where sync_scope_id = @sync_scope_id";

                    var p = command.CreateParameter();
                    p.ParameterName = "@sync_scope_id";
                    p.Value = scopeInfo.Id.ToString();
                    p.DbType = DbType.String;
                    command.Parameters.Add(p);

                    exist = ((long)await command.ExecuteScalarAsync().ConfigureAwait(false)) > 0;
                }

                string stmtText = exist
                    ? $"Update {scopeTableName.Quoted().ToString()} set sync_scope_name=@sync_scope_name, sync_scope_schema=@sync_scope_schema,  sync_scope_setup=@sync_scope_setup, sync_scope_version=@sync_scope_version, scope_last_sync=@scope_last_sync, scope_last_server_sync_timestamp=@scope_last_server_sync_timestamp, scope_last_sync_timestamp=@scope_last_sync_timestamp, scope_last_sync_duration=@scope_last_sync_duration  where sync_scope_id=@sync_scope_id"
                    : $"Insert into {scopeTableName.Quoted().ToString()} (sync_scope_name, sync_scope_schema, sync_scope_setup, sync_scope_version, scope_last_sync, sync_scope_id, scope_last_server_sync_timestamp, scope_last_sync_timestamp, scope_last_sync_duration) values (@sync_scope_name, @sync_scope_schema, @sync_scope_setup, @sync_scope_version, @scope_last_sync, @sync_scope_id, @scope_last_server_sync_timestamp, @scope_last_sync_timestamp, @scope_last_sync_duration)";

                using (var command = connection.CreateCommand())
                {
                    if (transaction != null)
                        command.Transaction = transaction;

                    command.CommandText = stmtText;

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
                    p.Value = scopeInfo.Version;
                    p.DbType = DbType.String;
                    command.Parameters.Add(p);

                    p = command.CreateParameter();
                    p.ParameterName = "@scope_last_sync";
                    p.Value = scopeInfo.LastSync.HasValue ? (object)scopeInfo.LastSync.Value : DBNull.Value;
                    p.DbType = DbType.DateTime;
                    command.Parameters.Add(p);

                    p = command.CreateParameter();
                    p.ParameterName = "@scope_last_sync_timestamp";
                    p.Value = scopeInfo.LastSyncTimestamp;
                    p.DbType = DbType.Int64;
                    command.Parameters.Add(p);

                    p = command.CreateParameter();
                    p.ParameterName = "@scope_last_server_sync_timestamp";
                    p.Value = scopeInfo.LastServerSyncTimestamp;
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

                    using (DbDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                scopeInfo.Name = reader["sync_scope_name"] as string;
                                scopeInfo.Schema = reader["sync_scope_schema"] == DBNull.Value ? null : JsonConvert.DeserializeObject<SyncSet>((string)reader["sync_scope_schema"]);
                                scopeInfo.Setup = reader["sync_scope_setup"] == DBNull.Value ? null : JsonConvert.DeserializeObject<SyncSetup>((string)reader["sync_scope_setup"]);
                                scopeInfo.Version = reader["sync_scope_version"] as string;
                                scopeInfo.Id = SyncTypeConverter.TryConvertTo<Guid>(reader["sync_scope_id"]);
                                scopeInfo.LastSyncDuration = reader["scope_last_sync_duration"] != DBNull.Value ? (long)reader["scope_last_sync_duration"] : 0L;
                                scopeInfo.LastServerSyncTimestamp = reader["scope_last_server_sync_timestamp"] != DBNull.Value ? (long)reader["scope_last_server_sync_timestamp"] : 0L;
                                scopeInfo.LastSyncTimestamp = reader["scope_last_sync_timestamp"] != DBNull.Value ? (long)reader["scope_last_sync_timestamp"] : 0L;
                                scopeInfo.LastSync = reader["scope_last_sync"] != DBNull.Value ? (DateTime?)reader["scope_last_sync"] : null;
                            }
                        }
                    }

                    return scopeInfo;
                }
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
            }
        }

        public async Task<ServerHistoryScopeInfo> InsertOrUpdateServerHistoryScopeInfoAsync(ServerHistoryScopeInfo serverHistoryScopeInfo)
        {
            bool alreadyOpened = connection.State == ConnectionState.Open;
            bool exist;
            var tableName = $"{scopeTableName.Unquoted().Normalized().ToString()}_history";

            try
            {
                using (var command = connection.CreateCommand())
                {
                    if (transaction != null)
                        command.Transaction = transaction;

                    if (!alreadyOpened)
                        await connection.OpenAsync().ConfigureAwait(false);

                    command.CommandText = $@"Select count(*) from `{tableName}` where sync_scope_id = @sync_scope_id";

                    var p = command.CreateParameter();
                    p.ParameterName = "@sync_scope_id";
                    p.Value = serverHistoryScopeInfo.Id;
                    p.DbType = DbType.Guid;
                    command.Parameters.Add(p);

                    exist = ((long)await command.ExecuteScalarAsync().ConfigureAwait(false)) > 0;
                }

                string stmtText = exist
                    ? $"Update `{tableName}` set sync_scope_name=@sync_scope_name, scope_last_sync_timestamp=@scope_last_sync_timestamp, scope_last_sync=@scope_last_sync, scope_last_sync_duration=@scope_last_sync_duration where sync_scope_id=@sync_scope_id"
                    : $"Insert into `{tableName}` (sync_scope_id, sync_scope_name, scope_last_sync_timestamp, scope_last_sync, scope_last_sync_duration) values (@sync_scope_id, @sync_scope_name, @scope_last_sync_timestamp, @scope_last_sync, @scope_last_sync_duration)";

                using (var command = connection.CreateCommand())
                {
                    if (transaction != null)
                        command.Transaction = transaction;

                    command.CommandText = stmtText;

                    var p = command.CreateParameter();
                    p.ParameterName = "@sync_scope_name";
                    p.Value = serverHistoryScopeInfo.Name;
                    p.DbType = DbType.String;
                    command.Parameters.Add(p);

                    p = command.CreateParameter();
                    p.ParameterName = "@scope_last_sync_timestamp";
                    p.Value = serverHistoryScopeInfo.LastSyncTimestamp;
                    p.DbType = DbType.Int64;
                    command.Parameters.Add(p);

                    p = command.CreateParameter();
                    p.ParameterName = "@scope_last_sync";
                    p.Value = serverHistoryScopeInfo.LastSync.HasValue ? (object)serverHistoryScopeInfo.LastSync.Value : DBNull.Value;
                    p.DbType = DbType.DateTime;
                    command.Parameters.Add(p);

                    p = command.CreateParameter();
                    p.ParameterName = "@scope_last_sync_duration";
                    p.Value = serverHistoryScopeInfo.LastSyncDuration;
                    p.DbType = DbType.Int64;
                    command.Parameters.Add(p);

                    p = command.CreateParameter();
                    p.ParameterName = "@sync_scope_id";
                    p.Value = serverHistoryScopeInfo.Id;
                    p.DbType = DbType.Guid;
                    command.Parameters.Add(p);

                    using (DbDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                serverHistoryScopeInfo.Id = SyncTypeConverter.TryConvertTo<Guid>(reader["sync_scope_id"]);
                                serverHistoryScopeInfo.Name = reader["sync_scope_name"] as string;
                                serverHistoryScopeInfo.LastSyncDuration = reader["scope_last_sync_duration"] != DBNull.Value ? (long)reader["scope_last_sync_duration"] : 0L;
                                serverHistoryScopeInfo.LastSyncTimestamp = reader["scope_last_sync_timestamp"] != DBNull.Value ? (long)reader["scope_last_sync_timestamp"] : 0L;
                                serverHistoryScopeInfo.LastSync = reader["scope_last_sync"] != DBNull.Value ? (DateTime?)reader["scope_last_sync"] : null;
                            }
                        }
                    }

                    return serverHistoryScopeInfo;
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during InsertOrUpdateServerHistoryScopeInfoAsync : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();
            }
        }

        public async Task<ServerScopeInfo> InsertOrUpdateServerScopeInfoAsync(ServerScopeInfo serverScopeInfo)
        {
            bool alreadyOpened = connection.State == ConnectionState.Open;
            bool exist;
            var tableName = $"{scopeTableName.Unquoted().Normalized().ToString()}_server";

            try
            {
                using (var command = connection.CreateCommand())
                {
                    if (transaction != null)
                        command.Transaction = transaction;

                    if (!alreadyOpened)
                        await connection.OpenAsync().ConfigureAwait(false);

                    command.CommandText = $@"Select count(*) from `{tableName}` where sync_scope_name = @sync_scope_name";

                    var p = command.CreateParameter();
                    p.ParameterName = "@sync_scope_name";
                    p.Value = serverScopeInfo.Name;
                    p.DbType = DbType.String;
                    command.Parameters.Add(p);

                    exist = ((long)await command.ExecuteScalarAsync().ConfigureAwait(false)) > 0;
                }

                string stmtText = exist
                    ? $"Update `{tableName}` set sync_scope_schema=@sync_scope_schema, sync_scope_setup=@sync_scope_setup, sync_scope_version=@sync_scope_version, sync_scope_last_clean_timestamp=@sync_scope_last_clean_timestamp where sync_scope_name=@sync_scope_name"
                    : $"Insert into `{tableName}` (sync_scope_name, sync_scope_schema, sync_scope_setup, sync_scope_version, sync_scope_last_clean_timestamp) values (@sync_scope_name, @sync_scope_schema, @sync_scope_setup, @sync_scope_version, @sync_scope_last_clean_timestamp)";

                using (var command = connection.CreateCommand())
                {
                    if (transaction != null)
                        command.Transaction = transaction;

                    command.CommandText = stmtText;

                    var p = command.CreateParameter();
                    p.ParameterName = "@sync_scope_name";
                    p.Value = serverScopeInfo.Name;
                    p.DbType = DbType.String;
                    command.Parameters.Add(p);

                    p = command.CreateParameter();
                    p.ParameterName = "@sync_scope_schema";
                    p.Value = serverScopeInfo.Schema == null ? DBNull.Value : (object)JsonConvert.SerializeObject(serverScopeInfo.Schema);
                    p.DbType = DbType.String;
                    command.Parameters.Add(p);

                    p = command.CreateParameter();
                    p.ParameterName = "@sync_scope_setup";
                    p.Value = serverScopeInfo.Setup == null ? DBNull.Value : (object)JsonConvert.SerializeObject(serverScopeInfo.Setup);
                    p.DbType = DbType.String;
                    command.Parameters.Add(p);

                    p = command.CreateParameter();
                    p.ParameterName = "@sync_scope_version";
                    p.Value = serverScopeInfo.Version;
                    p.DbType = DbType.String;
                    command.Parameters.Add(p);

                    p = command.CreateParameter();
                    p.ParameterName = "@sync_scope_last_clean_timestamp";
                    p.Value = serverScopeInfo.LastCleanupTimestamp;
                    p.DbType = DbType.Int64;
                    command.Parameters.Add(p);

                    using (DbDataReader reader = command.ExecuteReader())
                    {
                        if (reader.HasRows)
                        {
                            while (reader.Read())
                            {
                                serverScopeInfo.Name = reader["sync_scope_name"] as string;
                                serverScopeInfo.Schema = reader["sync_scope_schema"] == DBNull.Value ? null : JsonConvert.DeserializeObject<SyncSet>((string)reader["sync_scope_schema"]);
                                serverScopeInfo.Setup = reader["sync_scope_setup"] == DBNull.Value ? null : JsonConvert.DeserializeObject<SyncSetup>((string)reader["sync_scope_setup"]);
                                serverScopeInfo.Version = reader["sync_scope_version"] as string;
                                serverScopeInfo.LastCleanupTimestamp = reader["sync_scope_last_clean_timestamp"] != DBNull.Value ? (long)reader["sync_scope_last_clean_timestamp"] : 0L;
                            }
                        }
                    }

                    return serverScopeInfo;
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during InsertOrUpdateServerScopeInfoAsync : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();
            }
        }

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

                command.CommandText = $"select count(*) from information_schema.TABLES where TABLE_NAME = '{scopeTableName.Unquoted().ToString()}' and TABLE_SCHEMA = schema() and TABLE_TYPE = 'BASE TABLE'";

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
        {
            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                var tableName = $"{scopeTableName.Unquoted().Normalized().ToString()}_history";

                command.CommandText = $"select count(*) from information_schema.TABLES where TABLE_NAME = '{tableName}' and TABLE_SCHEMA = schema() and TABLE_TYPE = 'BASE TABLE'";

                return ((long)await command.ExecuteScalarAsync()) != 1;

            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during NeedToCreateServerHistoryScopeInfoTableAsync command : {ex}");
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

        public async Task<bool> NeedToCreateServerScopeInfoTableAsync()
        {
            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                var tableName = $"{scopeTableName.Unquoted().Normalized().ToString()}_server";

                command.CommandText = $"select count(*) from information_schema.TABLES where TABLE_NAME = '{tableName}' and TABLE_SCHEMA = schema() and TABLE_TYPE = 'BASE TABLE'";

                return ((long)await command.ExecuteScalarAsync()) != 1;

            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during NeedToCreateServerHistoryScopeInfoTableAsync command : {ex}");
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
    }
}
