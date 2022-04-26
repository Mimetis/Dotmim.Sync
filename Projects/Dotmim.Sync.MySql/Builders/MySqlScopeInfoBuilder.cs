using Dotmim.Sync.Builders;

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
#if NET5_0 || NET6_0|| NETCOREAPP3_1
using MySqlConnector;
#elif NETSTANDARD 
using MySql.Data.MySqlClient;
#endif
using System.Diagnostics;
using System.Threading.Tasks;
using Newtonsoft.Json;
using System.Text;

#if MARIADB
namespace Dotmim.Sync.MariaDB.Builders
#elif MYSQL
namespace Dotmim.Sync.MySql.Builders
#endif
{
    public class MySqlScopeInfoBuilder : DbScopeBuilder
    {

        public MySqlScopeInfoBuilder(string scopeTableName) : base(scopeTableName)
        {
            base.ScopeInfoTableName = ParserName.Parse(scopeTableName, "`");
        }


        public DbCommand GetClientScopeCommand(DbConnection connection, DbTransaction transaction)
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
                    FROM  {this.ScopeInfoTableName.Quoted().ToString()}";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandText;
            return command;

        }

        public DbCommand GetAllServerHistoryScopesCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_history";

            var commandText =
                    $@"SELECT  sync_scope_id
                           , sync_scope_name
                           , scope_last_sync_timestamp
                           , scope_last_sync_duration
                           , scope_last_sync           
                    FROM  `{tableName}`
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

        public DbCommand GetAllServerScopesCommand(DbConnection connection, DbTransaction transaction)
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
            command.Transaction = transaction;

            command.CommandText = commandText;

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            return command;
        }

        public override DbCommand GetAllScopesCommand(DbScopeType scopeType, DbConnection connection, DbTransaction transaction)
            => scopeType switch
            {
                DbScopeType.Server => GetAllServerScopesCommand(connection, transaction),
                DbScopeType.ServerHistory => GetAllServerHistoryScopesCommand(connection, transaction),
                _ => GetClientScopeCommand(connection, transaction)
            };


        public DbCommand GetCreateClientScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
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

            command.Transaction = transaction;

            command.CommandText = commandText;

            return command;

        }

        public DbCommand GetCreateServerHistoryScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
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
            command.Transaction = transaction;

            command.CommandText = commandText;

            return command;


        }

        public DbCommand GetCreateServerScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
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
            command.Transaction = transaction;

            command.CommandText = commandText;

            return command;
        }

        public override DbCommand GetCreateScopeInfoTableCommand(DbScopeType scopeType, DbConnection connection, DbTransaction transaction)
        => scopeType switch
        {
            DbScopeType.Server => GetCreateServerScopeInfoTableCommand(connection, transaction),
            DbScopeType.ServerHistory => GetCreateServerHistoryScopeInfoTableCommand(connection, transaction),
            _ => GetCreateClientScopeInfoTableCommand(connection, transaction)
        };

        public override DbCommand GetDropScopeInfoTableCommand(DbScopeType scopeType, DbConnection connection, DbTransaction transaction)
        {
            var tableName = scopeType switch
            {
                DbScopeType.Server => $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_server",
                DbScopeType.ServerHistory => $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_history",
                _ => this.ScopeInfoTableName.Unquoted().Normalized().ToString(),
            };

            var command = connection.CreateCommand();
            command.Transaction = transaction;

            command.CommandText = $"DROP TABLE `{tableName}`";

            return command;
        }

        public override DbCommand GetExistsScopeInfoTableCommand(DbScopeType scopeType, DbConnection connection, DbTransaction transaction)
        {
            var tableName = scopeType switch
            {
                DbScopeType.Server => $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_server",
                DbScopeType.ServerHistory => $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_history",
                _ => this.ScopeInfoTableName.Unquoted().ToString(),
            };

            var command = connection.CreateCommand();
            command.Transaction = transaction;

            command.CommandText = $"select count(*) from information_schema.TABLES where TABLE_NAME = '{tableName}' and TABLE_SCHEMA = schema() and TABLE_TYPE = 'BASE TABLE'";

            return command;
        }

        public override DbCommand GetLocalTimestampCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText = $"Select {MySqlObjectNames.TimestampValue}";

            var command = connection.CreateCommand();

            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = commandText;

            return command;
        }


        public override DbCommand GetUpdateScopeInfoCommand(DbScopeType scopeType, DbConnection connection, DbTransaction transaction)
            => scopeType switch
            {
                DbScopeType.Client => GetSaveClientScopeInfoCommand(true, connection, transaction),
                DbScopeType.ServerHistory => GetSaveServerHistoryScopeInfoCommand(true, connection, transaction),
                DbScopeType.Server => GetSaveServerScopeInfoCommand(true, connection, transaction),
                _ => throw new NotImplementedException($"Can't get Update command from this DbScopeType {scopeType}")
            };

        public override DbCommand GetInsertScopeInfoCommand(DbScopeType scopeType, DbConnection connection, DbTransaction transaction)
            => scopeType switch
            {
                DbScopeType.Client => GetSaveClientScopeInfoCommand(false, connection, transaction),
                DbScopeType.ServerHistory => GetSaveServerHistoryScopeInfoCommand(false, connection, transaction),
                DbScopeType.Server => GetSaveServerScopeInfoCommand(false, connection, transaction),
                _ => throw new NotImplementedException($"Can't get Insert command from this DbScopeType {scopeType}")
            };


        public override DbCommand GetExistsScopeInfoCommand(DbScopeType scopeType, DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();
            command.Transaction = transaction;

            var tableName = scopeType switch
            {
                DbScopeType.Client => $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}",
                DbScopeType.Server => $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_server",
                DbScopeType.ServerHistory => $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_history",
                _ => throw new NotImplementedException($"Can't get scope name from this DbScopeType {scopeType}")
            };

            if (scopeType == DbScopeType.Server)
                command.CommandText = $@"Select count(*) from `{tableName}` where sync_scope_name = @sync_scope_id";
            else
                command.CommandText = $@"Select count(*) from `{tableName}` where sync_scope_id = @sync_scope_id";


            var p0 = command.CreateParameter();
            p0.ParameterName = "@sync_scope_id";
            p0.DbType = DbType.String;
            p0.Size = -1;
            command.Parameters.Add(p0);

            return command;
        }

        public DbCommand GetSaveClientScopeInfoCommand(bool exist, DbConnection connection, DbTransaction transaction)
        {
            var stmtText = new StringBuilder(exist
                ? $"Update {this.ScopeInfoTableName.Quoted().ToString()} set sync_scope_name=@sync_scope_name, sync_scope_schema=@sync_scope_schema,  sync_scope_setup=@sync_scope_setup, sync_scope_version=@sync_scope_version, scope_last_sync=@scope_last_sync, scope_last_server_sync_timestamp=@scope_last_server_sync_timestamp, scope_last_sync_timestamp=@scope_last_sync_timestamp, scope_last_sync_duration=@scope_last_sync_duration  where sync_scope_id=@sync_scope_id;"
                : $"Insert into {this.ScopeInfoTableName.Quoted().ToString()} (sync_scope_name, sync_scope_schema, sync_scope_setup, sync_scope_version, scope_last_sync, sync_scope_id, scope_last_server_sync_timestamp, scope_last_sync_timestamp, scope_last_sync_duration) values (@sync_scope_name, @sync_scope_schema, @sync_scope_setup, @sync_scope_version, @scope_last_sync, @sync_scope_id, @scope_last_server_sync_timestamp, @scope_last_sync_timestamp, @scope_last_sync_duration);");

            stmtText.AppendLine();

            stmtText.AppendLine($@"SELECT sync_scope_id, sync_scope_name, sync_scope_schema, sync_scope_setup, 
                             sync_scope_version, scope_last_sync, scope_last_server_sync_timestamp, scope_last_sync_timestamp, scope_last_sync_duration
                             FROM  {this.ScopeInfoTableName.Quoted().ToString()} WHERE sync_scope_id = @sync_scope_id;");

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
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_setup";
            p.DbType = DbType.String;
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
            p.ParameterName = "@scope_last_sync_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@scope_last_server_sync_timestamp";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@scope_last_sync_duration";
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_id";
            p.DbType = DbType.String;
            p.Size = 36;
            command.Parameters.Add(p);

            return command;

        }

        public DbCommand GetSaveServerHistoryScopeInfoCommand(bool exist, DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_history";

            var stmtText = new StringBuilder(exist
                ? $"Update `{tableName}` set sync_scope_name=@sync_scope_name, scope_last_sync_timestamp=@scope_last_sync_timestamp, scope_last_sync=@scope_last_sync, scope_last_sync_duration=@scope_last_sync_duration where sync_scope_id=@sync_scope_id;"
                : $"Insert into `{tableName}` (sync_scope_id, sync_scope_name, scope_last_sync_timestamp, scope_last_sync, scope_last_sync_duration) values (@sync_scope_id, @sync_scope_name, @scope_last_sync_timestamp, @scope_last_sync, @scope_last_sync_duration);");

            stmtText.AppendLine();

            stmtText.AppendLine($@"SELECT  sync_scope_id, sync_scope_name, scope_last_sync_timestamp, scope_last_sync_duration, scope_last_sync           
                                   FROM `{tableName}` WHERE sync_scope_id = @sync_scope_id;");

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
            p.ParameterName = "@scope_last_sync_timestamp";
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
            p.ParameterName = "@sync_scope_id";
            p.DbType = DbType.Guid;
            p.Size = 36;
            command.Parameters.Add(p);

            return command;
        }

        public DbCommand GetSaveServerScopeInfoCommand(bool exist, DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_server";

            var stmtText = new StringBuilder(exist
                ? $"UPDATE `{tableName}` set sync_scope_schema=@sync_scope_schema, sync_scope_setup=@sync_scope_setup, sync_scope_version=@sync_scope_version, sync_scope_last_clean_timestamp=@sync_scope_last_clean_timestamp where sync_scope_name=@sync_scope_name;"
                : $"INSERT INTO `{tableName}` (sync_scope_name, sync_scope_schema, sync_scope_setup, sync_scope_version, sync_scope_last_clean_timestamp) values (@sync_scope_name, @sync_scope_schema, @sync_scope_setup, @sync_scope_version, @sync_scope_last_clean_timestamp);");

            stmtText.AppendLine();

            stmtText.AppendLine($@"SELECT sync_scope_name, sync_scope_schema, sync_scope_setup, 
                                sync_scope_version, sync_scope_last_clean_timestamp                    
                                FROM  `{tableName}`WHERE sync_scope_name = @sync_scope_name;");

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
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_setup";
            p.DbType = DbType.String;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_version";
            p.DbType = DbType.String;
            p.Size = 10;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_last_clean_timestamp";
            //p.Value = serverScopeInfo.LastCleanupTimestamp;
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            return command;
        }


    }
}
