using Dotmim.Sync.Builders;
using Dotmim.Sync.Log;
using Dotmim.Sync.SqlServer.Manager;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using System.Diagnostics;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace Dotmim.Sync.SqlServer.Scope
{
    public class SqlScopeInfoBuilder : IDbScopeInfoBuilder
    {
        protected readonly ParserName scopeTableName;
        protected readonly SqlConnection connection;
        protected readonly SqlTransaction transaction;

        public SqlScopeInfoBuilder(string scopeTableName, DbConnection connection, DbTransaction transaction = null)
        {
            this.connection = connection as SqlConnection;
            this.transaction = transaction as SqlTransaction;
            this.scopeTableName = ParserName.Parse(scopeTableName);
        }

        public virtual async Task CreateClientScopeInfoTableAsync()
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
                    $@"CREATE TABLE [dbo].{scopeTableName.Quoted()}(
                        [sync_scope_id] [uniqueidentifier] NOT NULL,
	                    [sync_scope_name] [nvarchar](100) NOT NULL,
	                    [sync_scope_schema] [nvarchar](max) NULL,
	                    [sync_scope_setup] [nvarchar](max) NULL,
	                    [sync_scope_version] [nvarchar](10) NULL,
                        [scope_last_server_sync_timestamp] [bigint] NULL,
                        [scope_last_sync_timestamp] [bigint] NULL,
                        [scope_last_sync_duration] [bigint] NULL,
                        [scope_last_sync] [datetime] NULL
                        CONSTRAINT [PK_{scopeTableName.Unquoted().Normalized()}] PRIMARY KEY CLUSTERED ([sync_scope_id] ASC)
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

        public virtual async Task DropClientScopeInfoTableAsync()
        {
            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                command.CommandText = $"DROP Table [dbo].{scopeTableName.Quoted()}";

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

        public virtual async Task CreateServerHistoryScopeInfoTableAsync()
        {
            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                var tableName = $"{scopeTableName.Unquoted().Normalized()}_history";

                command.CommandText =
                    $@"CREATE TABLE [dbo].[{tableName}](
                        [sync_scope_id] [uniqueidentifier] NOT NULL,
	                    [sync_scope_name] [nvarchar](100) NOT NULL,
                        [scope_last_sync_timestamp] [bigint] NULL,
                        [scope_last_sync_duration] [bigint] NULL,
                        [scope_last_sync] [datetime] NULL
                        CONSTRAINT [PK_{tableName}] PRIMARY KEY CLUSTERED ([sync_scope_id] ASC)
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

        public virtual async Task DropServerHistoryScopeInfoTableAsync()
        {
            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                var tableName = $"{scopeTableName.Unquoted().Normalized()}_history";

                command.CommandText = $"DROP Table [dbo].[{tableName}]";

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

        public virtual async Task CreateServerScopeInfoTableAsync()
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
                    $@"CREATE TABLE [dbo].[{tableName}] (
	                    [sync_scope_name] [nvarchar](100) NOT NULL,
	                    [sync_scope_schema] [nvarchar](max) NULL,
	                    [sync_scope_setup] [nvarchar](max) NULL,
	                    [sync_scope_version] [nvarchar](10) NULL,
                        [sync_scope_last_clean_timestamp] [bigint] NULL,
                        CONSTRAINT [PK_{scopeTableName.Unquoted().Normalized()}_server] PRIMARY KEY CLUSTERED ([sync_scope_name] ASC)
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

        public virtual async Task DropServerScopeInfoTableAsync()
        {
            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                var tableName = $"{scopeTableName.Unquoted().Normalized()}_server";

                command.CommandText = $"DROP Table [dbo].[{tableName}]";

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

        public virtual async Task<List<ScopeInfo>> GetAllClientScopesAsync(string scopeName)
        {
            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;

            bool alreadyOpened = connection.State == ConnectionState.Open;

            var scopes = new List<ScopeInfo>();
            try
            {
                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                command.CommandText =
                    $@"SELECT [sync_scope_id]
                           , [sync_scope_name]
                           , [sync_scope_schema]
                           , [sync_scope_setup]
                           , [sync_scope_version]
                           , [scope_last_sync]
                           , [scope_last_server_sync_timestamp]
                           , [scope_last_sync_timestamp]
                           , [scope_last_sync_duration]
                    FROM  {scopeTableName.Quoted().ToString()}
                    WHERE [sync_scope_name] = @sync_scope_name";

                var p = command.CreateParameter();
                p.ParameterName = "@sync_scope_name";
                p.Value = scopeName;
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    if (reader.HasRows)
                    {
                        // read only the first one
                        while (reader.Read())
                        {
                            var scopeInfo = new ScopeInfo();
                            scopeInfo.Name = reader["sync_scope_name"] as string;
                            scopeInfo.Schema = reader["sync_scope_schema"] == DBNull.Value ? null : JsonConvert.DeserializeObject<SyncSet>((string)reader["sync_scope_schema"]);
                            scopeInfo.Setup = reader["sync_scope_setup"] == DBNull.Value ? null : JsonConvert.DeserializeObject<SyncSetup>((string)reader["sync_scope_setup"]); 
                            scopeInfo.Version = reader["sync_scope_version"] as string;
                            scopeInfo.Id = (Guid)reader["sync_scope_id"];
                            scopeInfo.LastSync = reader["scope_last_sync"] != DBNull.Value ? (DateTime?)reader["scope_last_sync"] : null;
                            scopeInfo.LastServerSyncTimestamp = reader["scope_last_server_sync_timestamp"] != DBNull.Value ? (long)reader["scope_last_server_sync_timestamp"] : 0;
                            scopeInfo.LastSyncTimestamp = reader["scope_last_sync_timestamp"] != DBNull.Value ? (long)reader["scope_last_sync_timestamp"] : 0;
                            scopeInfo.LastSyncDuration = reader["scope_last_sync_duration"] != DBNull.Value ? (long)reader["scope_last_sync_duration"] : 0;
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

        public virtual async Task<long> GetLocalTimestampAsync()
        {
            using (var command = connection.CreateCommand())
            {
                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (transaction != null)
                    command.Transaction = transaction;

                // UPDATE Nov 2019 : We don't use min_active_rowversion anymore, since we are in a transaction
                // and we still need the last row version, so check back to @@DBTS
                command.CommandText = "SELECT @sync_new_timestamp = @@DBTS";
                DbParameter p = command.CreateParameter();
                p.ParameterName = "@sync_new_timestamp";
                p.DbType = DbType.Int64;
                p.Direction = ParameterDirection.Output;
                command.Parameters.Add(p);

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                await command.ExecuteNonQueryAsync().ConfigureAwait(false);

                var outputParameter = SqlManager.GetParameter(command, "sync_new_timestamp");

                if (outputParameter == null)
                    return 0L;

                long.TryParse(outputParameter.Value.ToString(), out long result);

                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

                return result;
            }
        }

        public virtual async Task<ScopeInfo> InsertOrUpdateClientScopeInfoAsync(ScopeInfo scopeInfo)
        {
            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                command.CommandText = $@"
                    MERGE {scopeTableName.Quoted()} AS [base] 
                    USING (
                               SELECT  @sync_scope_id AS sync_scope_id,  
	                                   @sync_scope_name AS sync_scope_name,  
	                                   @sync_scope_schema AS sync_scope_schema,  
	                                   @sync_scope_setup AS sync_scope_setup,  
	                                   @sync_scope_version AS sync_scope_version,  
                                       @scope_last_sync AS scope_last_sync,
                                       @scope_last_sync_timestamp AS scope_last_sync_timestamp,
                                       @scope_last_server_sync_timestamp AS scope_last_server_sync_timestamp,
                                       @scope_last_sync_duration AS scope_last_sync_duration
                           ) AS [changes] 
                    ON [base].[sync_scope_id] = [changes].[sync_scope_id]
                    WHEN NOT MATCHED THEN
	                    INSERT ([sync_scope_name], [sync_scope_schema], [sync_scope_setup], [sync_scope_version], [sync_scope_id], [scope_last_sync], [scope_last_sync_timestamp],           [scope_last_server_sync_timestamp],           [scope_last_sync_duration])
	                    VALUES ([changes].[sync_scope_name], [changes].[sync_scope_schema], [changes].[sync_scope_setup], [changes].[sync_scope_version], [changes].[sync_scope_id], [changes].[scope_last_sync],  [changes].[scope_last_sync_timestamp], [changes].[scope_last_server_sync_timestamp], [changes].[scope_last_sync_duration])
                    WHEN MATCHED THEN
	                    UPDATE SET [sync_scope_name] = [changes].[sync_scope_name], 
                                   [sync_scope_schema] = [changes].[sync_scope_schema], 
                                   [sync_scope_setup] = [changes].[sync_scope_setup], 
                                   [sync_scope_version] = [changes].[sync_scope_version], 
                                   [scope_last_sync] = [changes].[scope_last_sync],
                                   [scope_last_sync_timestamp] = [changes].[scope_last_sync_timestamp],
                                   [scope_last_server_sync_timestamp] = [changes].[scope_last_server_sync_timestamp],
                                   [scope_last_sync_duration] = [changes].[scope_last_sync_duration]
                    OUTPUT  INSERTED.[sync_scope_name], 
                            INSERTED.[sync_scope_schema], 
                            INSERTED.[sync_scope_setup], 
                            INSERTED.[sync_scope_version], 
                            INSERTED.[sync_scope_id], 
                            INSERTED.[scope_last_sync],
                            INSERTED.[scope_last_sync_timestamp],
                            INSERTED.[scope_last_server_sync_timestamp],
                            INSERTED.[scope_last_sync_duration];
                ";

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
                p.Value = scopeInfo.Setup ==null  ? DBNull.Value : (object)JsonConvert.SerializeObject(scopeInfo.Setup);
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                p = command.CreateParameter();
                p.ParameterName = "@sync_scope_version";
                p.Value = string.IsNullOrEmpty(scopeInfo.Version) ? DBNull.Value : (object)scopeInfo.Version;
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                p = command.CreateParameter();
                p.ParameterName = "@sync_scope_id";
                p.Value = scopeInfo.Id;
                p.DbType = DbType.Guid;
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


                using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            scopeInfo.Name = reader["sync_scope_name"] as string;
                            scopeInfo.Schema = reader["sync_scope_schema"] == DBNull.Value ? null : JsonConvert.DeserializeObject<SyncSet>((string)reader["sync_scope_schema"]);
                            scopeInfo.Setup = reader["sync_scope_setup"] == DBNull.Value ? null : JsonConvert.DeserializeObject<SyncSetup>((string)reader["sync_scope_setup"]);
                            scopeInfo.Version = reader["sync_scope_Version"] as string;
                            scopeInfo.Id = (Guid)reader["sync_scope_id"];
                            scopeInfo.LastSync = reader["scope_last_sync"] != DBNull.Value ? (DateTime?)reader["scope_last_sync"] : null;
                            scopeInfo.LastSyncTimestamp = reader["scope_last_sync_timestamp"] != DBNull.Value ? (long)reader["scope_last_sync_timestamp"] : 0;
                            scopeInfo.LastServerSyncTimestamp = reader["scope_last_server_sync_timestamp"] != DBNull.Value ? (long)reader["scope_last_server_sync_timestamp"] : 0;
                            scopeInfo.LastSyncDuration = reader["scope_last_sync_duration"] != DBNull.Value ? (long)reader["scope_last_sync_duration"] : 0;
                        }
                    }
                }

                return scopeInfo;
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

        public virtual async Task<ServerScopeInfo> InsertOrUpdateServerScopeInfoAsync(ServerScopeInfo serverScopeInfo)
        {
            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                var tableName = $"{scopeTableName.Unquoted().Normalized()}_server";

                command.CommandText = $@"
                    MERGE {tableName} AS [base] 
                    USING (
                               SELECT  @sync_scope_name AS sync_scope_name,  
	                                   @sync_scope_schema AS sync_scope_schema,  
	                                   @sync_scope_setup AS sync_scope_setup,  
	                                   @sync_scope_version AS sync_scope_version,  
                                       @sync_scope_last_clean_timestamp AS sync_scope_last_clean_timestamp
                           ) AS [changes] 
                    ON [base].[sync_scope_name] = [changes].[sync_scope_name]
                    WHEN NOT MATCHED THEN
	                    INSERT ([sync_scope_name], [sync_scope_schema], [sync_scope_setup], [sync_scope_version], [sync_scope_last_clean_timestamp])
	                    VALUES ([changes].[sync_scope_name], [changes].[sync_scope_schema], [changes].[sync_scope_setup], [changes].[sync_scope_version], [changes].[sync_scope_last_clean_timestamp])
                    WHEN MATCHED THEN
	                    UPDATE SET [sync_scope_name] = [changes].[sync_scope_name], 
                                   [sync_scope_schema] = [changes].[sync_scope_schema], 
                                   [sync_scope_setup] = [changes].[sync_scope_setup], 
                                   [sync_scope_version] = [changes].[sync_scope_version], 
                                   [sync_scope_last_clean_timestamp] = [changes].[sync_scope_last_clean_timestamp]
                    OUTPUT  INSERTED.[sync_scope_name], 
                            INSERTED.[sync_scope_schema], 
                            INSERTED.[sync_scope_setup], 
                            INSERTED.[sync_scope_version], 
                            INSERTED.[sync_scope_last_clean_timestamp];
                ";

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
                p.Value = string.IsNullOrEmpty(serverScopeInfo.Version) ? DBNull.Value : (object)serverScopeInfo.Version;
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                p = command.CreateParameter();
                p.ParameterName = "@sync_scope_last_clean_timestamp";
                p.Value = serverScopeInfo.LastCleanupTimestamp;
                p.DbType = DbType.Int64;
                command.Parameters.Add(p);


                using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            serverScopeInfo.Name = reader["sync_scope_name"] as string;
                            serverScopeInfo.Schema = reader["sync_scope_schema"] == DBNull.Value ? null : JsonConvert.DeserializeObject<SyncSet>((string)reader["sync_scope_schema"]);
                            serverScopeInfo.Setup = reader["sync_scope_setup"] == DBNull.Value ? null : JsonConvert.DeserializeObject<SyncSetup>((string)reader["sync_scope_setup"]);
                            serverScopeInfo.Version = reader["sync_scope_version"] as string;
                            serverScopeInfo.LastCleanupTimestamp = reader["sync_scope_last_clean_timestamp"] != DBNull.Value ? (long)reader["sync_scope_last_clean_timestamp"] : 0;
                        }
                    }
                }

                return serverScopeInfo;
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

        public virtual async Task<ServerHistoryScopeInfo> InsertOrUpdateServerHistoryScopeInfoAsync(ServerHistoryScopeInfo serverHistoryScopeInfo)
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

                command.CommandText = $@"
                    MERGE [{tableName}] AS [base] 
                    USING (
                               SELECT  @sync_scope_id AS sync_scope_id,  
	                                   @sync_scope_name AS sync_scope_name,  
                                       @scope_last_sync_timestamp AS scope_last_sync_timestamp,
                                       @scope_last_sync_duration AS scope_last_sync_duration,
                                       @scope_last_sync AS scope_last_sync
                           ) AS [changes] 
                    ON [base].[sync_scope_id] = [changes].[sync_scope_id]
                    WHEN NOT MATCHED THEN
	                    INSERT ([sync_scope_name], [sync_scope_id], [scope_last_sync_timestamp], [scope_last_sync], [scope_last_sync_duration])
	                    VALUES ([changes].[sync_scope_name], [changes].[sync_scope_id], [changes].[scope_last_sync_timestamp],[changes].[scope_last_sync], [changes].[scope_last_sync_duration])
                    WHEN MATCHED THEN
	                    UPDATE SET [sync_scope_name] = [changes].[sync_scope_name], 
                                   [scope_last_sync_timestamp] = [changes].[scope_last_sync_timestamp],
                                   [scope_last_sync] = [changes].[scope_last_sync],
                                   [scope_last_sync_duration] = [changes].[scope_last_sync_duration]
                    OUTPUT  INSERTED.[sync_scope_name], 
                            INSERTED.[sync_scope_id], 
                            INSERTED.[scope_last_sync_timestamp],
                            INSERTED.[scope_last_sync],
                            INSERTED.[scope_last_sync_duration];
                ";

                var p = command.CreateParameter();
                p.ParameterName = "@sync_scope_name";
                p.Value = serverHistoryScopeInfo.Name;
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                p = command.CreateParameter();
                p.ParameterName = "@sync_scope_id";
                p.Value = serverHistoryScopeInfo.Id;
                p.DbType = DbType.Guid;
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

                using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            serverHistoryScopeInfo.Name = reader["sync_scope_name"] as String;
                            serverHistoryScopeInfo.Id = (Guid)reader["sync_scope_id"];
                            serverHistoryScopeInfo.LastSyncTimestamp = reader["scope_last_sync_timestamp"] != DBNull.Value ? (long)reader["scope_last_sync_timestamp"] : 0;
                            serverHistoryScopeInfo.LastSync = reader["scope_last_sync"] != DBNull.Value ? (DateTime?)reader["scope_last_sync"] : null;
                            serverHistoryScopeInfo.LastSyncDuration = reader["scope_last_sync_duration"] != DBNull.Value ? (long)reader["scope_last_sync_duration"] : 0;
                        }
                    }
                }

                return serverHistoryScopeInfo;
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

        public virtual async Task<bool> NeedToCreateClientScopeInfoTableAsync()
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
                    $@"IF EXISTS (SELECT t.name FROM sys.tables t 
                            JOIN sys.schemas s ON s.schema_id = t.schema_id 
                            WHERE t.name = N'{this.scopeTableName.Unquoted()}')
                     SELECT 1 
                     ELSE
                     SELECT 0";

                var result = await command.ExecuteScalarAsync().ConfigureAwait(false);

                return (int)result != 1;
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during NeedToCreateScopeInfoTable command : {ex}");
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

        public virtual async Task<bool> NeedToCreateServerHistoryScopeInfoTableAsync()
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
                    $@"IF EXISTS (SELECT t.name FROM sys.tables t 
                            JOIN sys.schemas s ON s.schema_id = t.schema_id 
                            WHERE t.name = N'{tableName}')
                     SELECT 1 
                     ELSE
                     SELECT 0";

                var result = await command.ExecuteScalarAsync().ConfigureAwait(false);

                return (int)result != 1;

            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during NeedToCreateServerHistoryScopeInfoTable command : {ex}");
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

        public virtual async Task<bool> NeedToCreateServerScopeInfoTableAsync()
        {
            using (var command = connection.CreateCommand())
            {
                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (transaction != null)
                    command.Transaction = transaction;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                var tableName = $"{scopeTableName.Unquoted().Normalized()}_server";

                command.CommandText =
                    $@"IF EXISTS (SELECT t.name FROM sys.tables t 
                            JOIN sys.schemas s ON s.schema_id = t.schema_id 
                            WHERE t.name = N'{tableName}')
                     SELECT 1 
                     ELSE
                     SELECT 0";

                var result = await command.ExecuteScalarAsync().ConfigureAwait(false);

                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

                return (int)result != 1;
            }

        }

        public async Task<List<ServerScopeInfo>> GetAllServerScopesAsync(string scopeName)
        {
            using (var command = connection.CreateCommand())
            {
                var tableName = $"{scopeTableName.Unquoted().Normalized()}_server";

                if (transaction != null)
                    command.Transaction = transaction;

                bool alreadyOpened = connection.State == ConnectionState.Open;

                var scopes = new List<ServerScopeInfo>();

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                command.CommandText =
                    $@"SELECT [sync_scope_name]
                           , [sync_scope_schema]
                           , [sync_scope_setup]
                           , [sync_scope_version]
                           , [sync_scope_last_clean_timestamp]
                    FROM  [{tableName}]
                    WHERE [sync_scope_name] = @sync_scope_name";

                var p = command.CreateParameter();
                p.ParameterName = "@sync_scope_name";
                p.Value = scopeName;
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    if (reader.HasRows)
                    {
                        // read only the first one
                        while (reader.Read())
                        {
                            var serverScopeInfo = new ServerScopeInfo();
                            serverScopeInfo.Name = reader["sync_scope_name"] as string;
                            serverScopeInfo.Schema = reader["sync_scope_schema"] == DBNull.Value ? null : JsonConvert.DeserializeObject<SyncSet>((string)reader["sync_scope_schema"]);
                            serverScopeInfo.Setup = reader["sync_scope_setup"] == DBNull.Value ? null : JsonConvert.DeserializeObject<SyncSetup>((string)reader["sync_scope_setup"]);
                            serverScopeInfo.Version = reader["sync_scope_version"] as string;
                            serverScopeInfo.LastCleanupTimestamp = reader["sync_scope_last_clean_timestamp"] != DBNull.Value ? (long)reader["sync_scope_last_clean_timestamp"] : 0;
                            scopes.Add(serverScopeInfo);
                        }
                    }
                }

                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

                return scopes;
            }

        }

        public async Task<List<ServerHistoryScopeInfo>> GetAllServerHistoryScopesAsync(string scopeName)
        {
            using (var command = connection.CreateCommand())
            {
                var tableName = $"{scopeTableName.Unquoted().Normalized()}_history";

                if (transaction != null)
                    command.Transaction = transaction;

                bool alreadyOpened = connection.State == ConnectionState.Open;

                var scopes = new List<ServerHistoryScopeInfo>();

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                command.CommandText =
                    $@"SELECT [sync_scope_id]
                           , [sync_scope_name]
                           , [scope_last_sync_timestamp]
                           , [scope_last_sync_duration]
                           , [scope_last_sync]
                    FROM  [{tableName}]
                    WHERE [sync_scope_name] = @sync_scope_name";

                var p = command.CreateParameter();
                p.ParameterName = "@sync_scope_name";
                p.Value = scopeName;
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    if (reader.HasRows)
                    {
                        // read only the first one
                        while (reader.Read())
                        {
                            var serverScopeInfo = new ServerHistoryScopeInfo();
                            serverScopeInfo.Id = (Guid)reader["sync_scope_id"];
                            serverScopeInfo.Name = reader["sync_scope_name"] as string;
                            serverScopeInfo.LastSync = reader["scope_last_sync"] != DBNull.Value ? (DateTime?)reader["scope_last_sync"] : null;
                            serverScopeInfo.LastSyncDuration = reader["scope_last_sync_duration"] != DBNull.Value ? (long)reader["scope_last_sync_duration"] : 0;
                            serverScopeInfo.LastSyncTimestamp = reader["scope_last_sync_timestamp"] != DBNull.Value ? (long)reader["scope_last_sync_timestamp"] : 0;
                            scopes.Add(serverScopeInfo);
                        }
                    }
                }

                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

                return scopes;
            }

        }
    }
}
