using Dotmim.Sync.Builders;

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using MySql.Data.MySqlClient;
using System.Diagnostics;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.Text;

namespace Dotmim.Sync.MySql
{
    public class MySqlScopeInfoBuilder : DbScopeBuilder
    {
        public MySqlScopeInfoBuilder(string scopeTableName) : base(scopeTableName)
        {
            base.ScopeInfoTableName = ParserName.Parse(scopeTableName, "`");
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
                    FROM  {this.ScopeInfoTableName.Quoted().ToString()}
                    WHERE sync_scope_name = @sync_scope_name";

            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;

            command.CommandText = commandText;

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.Value = scopeName;
            p.DbType = DbType.String;
            command.Parameters.Add(p);

            return Task.FromResult(command);

        }

        public Task<DbCommand> GetAllServerHistoryScopesCommandAsync(string scopeName, DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_history";
            List<ServerHistoryScopeInfo> scopes = new List<ServerHistoryScopeInfo>();

            var commandText =
                    $@"SELECT  sync_scope_id
                           , sync_scope_name
                           , scope_last_sync_timestamp
                           , scope_last_sync_duration
                           , scope_last_sync           
                    FROM  `{tableName}`
                    WHERE sync_scope_name = @sync_scope_name";



            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;

            command.CommandText = commandText;

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.Value = scopeName;
            p.DbType = DbType.String;
            command.Parameters.Add(p);

            return Task.FromResult(command);

        }

        public Task<DbCommand> GetAllServerScopesCommandAsync(string scopeName, DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_server";

            var commandText =
                $@"SELECT sync_scope_name
                           , sync_scope_schema
                           , sync_scope_setup
                           , sync_scope_version
                           , sync_scope_last_clean_timestamp                    
                    FROM  `{tableName}`
                    WHERE sync_scope_name = @sync_scope_name";

            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;

            command.CommandText = commandText;

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.Value = scopeName;
            p.DbType = DbType.String;
            command.Parameters.Add(p);

            return Task.FromResult(command);
        }

        public override Task<DbCommand> GetAllScopesCommandAsync(DbScopeType scopeType, string scopeName, DbConnection connection, DbTransaction transaction)
            => scopeType switch
            {
                DbScopeType.Server => GetAllServerScopesCommandAsync(scopeName, connection, transaction),
                DbScopeType.ServerHistory => GetAllServerHistoryScopesCommandAsync(scopeName, connection, transaction),
                _ => GetAllClientScopesCommandAsync(scopeName, connection, transaction)
            };


        public Task<DbCommand> GetCreateClientScopeInfoTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {

            var commandText =
                $@"CREATE TABLE IF NOT EXISTS {this.ScopeInfoTableName.Quoted().ToString()}(
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

            var command = connection.CreateCommand();

            if (transaction != null)
                command.Transaction = transaction;

            command.CommandText = commandText;

            return Task.FromResult(command);

        }

        public Task<DbCommand> GetCreateServerHistoryScopeInfoTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {

            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_history";

            var commandText =
                $@"CREATE TABLE IF NOT EXISTS `{tableName}`(
                        sync_scope_id varchar(36) NOT NULL,
                        sync_scope_name varchar(100) NOT NULL,
                        scope_last_sync_timestamp bigint NULL,
                        scope_last_sync_duration bigint NULL,
                        scope_last_sync datetime NULL,
                        PRIMARY KEY (sync_scope_id)
                        )";

            var command = connection.CreateCommand();

            if (transaction != null)
                command.Transaction = transaction;

            command.CommandText = commandText;

            return Task.FromResult(command);


        }

        public Task<DbCommand> GetCreateServerScopeInfoTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_server";

            var commandText =
                $@"CREATE TABLE IF NOT EXISTS `{tableName}` (
	                    sync_scope_name varchar(100) NOT NULL,
	                    sync_scope_schema longtext NULL,
	                    sync_scope_setup longtext NULL,
	                    sync_scope_version varchar(10) NULL,
                        sync_scope_last_clean_timestamp bigint NULL,
                        PRIMARY KEY (sync_scope_name)
                        )";
            var command = connection.CreateCommand();

            if (transaction != null)
                command.Transaction = transaction;

            command.CommandText = commandText;

            return Task.FromResult(command);
        }

        public override Task<DbCommand> GetCreateScopeInfoTableCommandAsync(DbScopeType scopeType, DbConnection connection, DbTransaction transaction)
        => scopeType switch
        {
            DbScopeType.Server => GetCreateServerScopeInfoTableCommandAsync(connection, transaction),
            DbScopeType.ServerHistory => GetCreateServerHistoryScopeInfoTableCommandAsync(connection, transaction),
            _ => GetCreateClientScopeInfoTableCommandAsync(connection, transaction)
        };

        public override Task<DbCommand> GetDropScopeInfoTableCommandAsync(DbScopeType scopeType, DbConnection connection, DbTransaction transaction)
        {
            var tableName = scopeType switch
            {
                DbScopeType.Server => $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_server",
                DbScopeType.ServerHistory => $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_history",
                _ => this.ScopeInfoTableName.Unquoted().Normalized().ToString(),
            };

            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;

            command.CommandText = $"DROP TABLE `{tableName}`";

            return Task.FromResult(command);
        }

        public override Task<DbCommand> GetExistsScopeInfoTableCommandAsync(DbScopeType scopeType, DbConnection connection, DbTransaction transaction)
        {
            var tableName = scopeType switch
            {
                DbScopeType.Server => $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_server",
                DbScopeType.ServerHistory => $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_history",
                _ => this.ScopeInfoTableName.Unquoted().ToString(),
            };

            var command = connection.CreateCommand();

            if (transaction != null)
                command.Transaction = transaction;

            command.CommandText = $"select count(*) from information_schema.TABLES where TABLE_NAME = '{tableName}' and TABLE_SCHEMA = schema() and TABLE_TYPE = 'BASE TABLE'";

            return Task.FromResult(command);
        }

        public override Task<DbCommand> GetLocalTimestampCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var commandText = $"Select {MySqlObjectNames.TimestampValue}";

            var command = connection.CreateCommand();

            command.Connection = connection;
            if (transaction != null)
                command.Transaction = transaction;
            command.CommandText = commandText;

            return Task.FromResult(command);
        }


        public override Task<DbCommand> GetSaveScopeInfoCommandAsync(DbScopeType scopeType, object scopeInfo, DbConnection connection, DbTransaction transaction)
            => scopeInfo switch
            {
                ScopeInfo si => GetSaveClientScopeInfoCommandAsync(si, connection, transaction),
                ServerHistoryScopeInfo shsi => GetSaveServerHistoryScopeInfoCommandAsync(shsi, connection, transaction),
                ServerScopeInfo ssi => GetSaveServerScopeInfoCommandAsync(ssi, connection, transaction),
                _ => throw new NotImplementedException($"Can't save this DbScopeType {scopeType}")
            };

        public async Task<DbCommand> GetSaveClientScopeInfoCommandAsync(ScopeInfo scopeInfo, DbConnection connection, DbTransaction transaction)
        {
            bool exist;
            var commandText = $@"Select count(*) from {this.ScopeInfoTableName.Quoted().ToString()} where sync_scope_id = @sync_scope_id";

            using (var existCommand = new MySqlCommand(commandText, (MySqlConnection)connection, (MySqlTransaction)transaction))
            {
                var p0 = existCommand.CreateParameter();
                p0.ParameterName = "@sync_scope_id";
                p0.Value = scopeInfo.Id.ToString();
                p0.DbType = DbType.String;
                existCommand.Parameters.Add(p0);

                exist = ((long)await existCommand.ExecuteScalarAsync().ConfigureAwait(false)) > 0;
            }

            var stmtText = new StringBuilder(exist
                ? $"Update {this.ScopeInfoTableName.Quoted().ToString()} set sync_scope_name=@sync_scope_name, sync_scope_schema=@sync_scope_schema,  sync_scope_setup=@sync_scope_setup, sync_scope_version=@sync_scope_version, scope_last_sync=@scope_last_sync, scope_last_server_sync_timestamp=@scope_last_server_sync_timestamp, scope_last_sync_timestamp=@scope_last_sync_timestamp, scope_last_sync_duration=@scope_last_sync_duration  where sync_scope_id=@sync_scope_id;"
                : $"Insert into {this.ScopeInfoTableName.Quoted().ToString()} (sync_scope_name, sync_scope_schema, sync_scope_setup, sync_scope_version, scope_last_sync, sync_scope_id, scope_last_server_sync_timestamp, scope_last_sync_timestamp, scope_last_sync_duration) values (@sync_scope_name, @sync_scope_schema, @sync_scope_setup, @sync_scope_version, @scope_last_sync, @sync_scope_id, @scope_last_server_sync_timestamp, @scope_last_sync_timestamp, @scope_last_sync_duration);");

            stmtText.AppendLine();

            stmtText.AppendLine($@"SELECT sync_scope_id, sync_scope_name, sync_scope_schema, sync_scope_setup, 
                             sync_scope_version, scope_last_sync, scope_last_server_sync_timestamp, scope_last_sync_timestamp, scope_last_sync_duration
                             FROM  {this.ScopeInfoTableName.Quoted().ToString()} WHERE sync_scope_id = @sync_scope_id;");

            var command = connection.CreateCommand();

            command.Connection = connection;
            if (transaction != null)
                command.Transaction = transaction;

            command.CommandText = stmtText.ToString();

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

            return command;

        }

        public async Task<DbCommand> GetSaveServerHistoryScopeInfoCommandAsync(ServerHistoryScopeInfo serverHistoryScopeInfo, DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_history";
            var commandText = $@"Select count(*) from `{tableName}` where sync_scope_id = @sync_scope_id";

            bool exist;

            using (var existCommand = new MySqlCommand(commandText, (MySqlConnection)connection, (MySqlTransaction)transaction))
            {
                var p0 = existCommand.CreateParameter();
                p0.ParameterName = "@sync_scope_id";
                p0.Value = serverHistoryScopeInfo.Id;
                p0.DbType = DbType.Guid;
                existCommand.Parameters.Add(p0);

                exist = ((long)await existCommand.ExecuteScalarAsync().ConfigureAwait(false)) > 0;
            }

            var stmtText = new StringBuilder(exist
                ? $"Update `{tableName}` set sync_scope_name=@sync_scope_name, scope_last_sync_timestamp=@scope_last_sync_timestamp, scope_last_sync=@scope_last_sync, scope_last_sync_duration=@scope_last_sync_duration where sync_scope_id=@sync_scope_id;"
                : $"Insert into `{tableName}` (sync_scope_id, sync_scope_name, scope_last_sync_timestamp, scope_last_sync, scope_last_sync_duration) values (@sync_scope_id, @sync_scope_name, @scope_last_sync_timestamp, @scope_last_sync, @scope_last_sync_duration);");

            stmtText.AppendLine();

            stmtText.AppendLine($@"SELECT  sync_scope_id, sync_scope_name, scope_last_sync_timestamp, scope_last_sync_duration, scope_last_sync           
                                   FROM `{tableName}` WHERE sync_scope_id = @sync_scope_id;");



            var command = connection.CreateCommand();

            command.Connection = connection;
            if (transaction != null)
                command.Transaction = transaction;

            command.CommandText = stmtText.ToString();

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

            return command;
        }

        public async Task<DbCommand> GetSaveServerScopeInfoCommandAsync(ServerScopeInfo serverScopeInfo, DbConnection connection, DbTransaction transaction)
        {
            bool exist;
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_server";
            var commandText = $@"Select count(*) from `{tableName}` where sync_scope_name = @sync_scope_name";

            using (var existCommand = new MySqlCommand(commandText, (MySqlConnection)connection, (MySqlTransaction)transaction))
            {

                var p0 = existCommand.CreateParameter();
                p0.ParameterName = "@sync_scope_name";
                p0.Value = serverScopeInfo.Name;
                p0.DbType = DbType.String;
                existCommand.Parameters.Add(p0);

                exist = ((long)await existCommand.ExecuteScalarAsync().ConfigureAwait(false)) > 0;
            }

            var stmtText =new StringBuilder(exist
                ? $"UPDATE `{tableName}` set sync_scope_schema=@sync_scope_schema, sync_scope_setup=@sync_scope_setup, sync_scope_version=@sync_scope_version, sync_scope_last_clean_timestamp=@sync_scope_last_clean_timestamp where sync_scope_name=@sync_scope_name;"
                : $"INSERT INTO `{tableName}` (sync_scope_name, sync_scope_schema, sync_scope_setup, sync_scope_version, sync_scope_last_clean_timestamp) values (@sync_scope_name, @sync_scope_schema, @sync_scope_setup, @sync_scope_version, @sync_scope_last_clean_timestamp);");
            
            stmtText.AppendLine();
            
            stmtText.AppendLine($@"SELECT sync_scope_name, sync_scope_schema, sync_scope_setup, 
                                sync_scope_version, sync_scope_last_clean_timestamp                    
                                FROM  `{tableName}`WHERE sync_scope_name = @sync_scope_name;");

            var command = connection.CreateCommand();

            command.Connection = connection;
            if (transaction != null)
                command.Transaction = transaction;

            command.CommandText = stmtText.ToString();

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

            return command;
        }


    }
}
