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


        public override DbCommand GetLocalTimestampCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText = $"Select {MySqlObjectNames.TimestampValue}";

            var command = connection.CreateCommand();

            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = commandText;

            return command;
        }


        //Exists Scope Table
        // ------------------------------

        public override DbCommand GetExistsClientScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = this.ScopeInfoTableName.Unquoted().ToString();
            var command = connection.CreateCommand();
            command.Transaction = transaction;

            command.CommandText = $"select count(*) from information_schema.TABLES where TABLE_NAME = '{tableName}' and TABLE_SCHEMA = schema() and TABLE_TYPE = 'BASE TABLE'";

            return command;
        }
        public override DbCommand GetExistsServerScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_server";
            var command = connection.CreateCommand();
            command.Transaction = transaction;

            command.CommandText = $"select count(*) from information_schema.TABLES where TABLE_NAME = '{tableName}' and TABLE_SCHEMA = schema() and TABLE_TYPE = 'BASE TABLE'";

            return command;
        }
        public override DbCommand GetExistsServerHistoryScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_history";
            var command = connection.CreateCommand();
            command.Transaction = transaction;

            command.CommandText = $"select count(*) from information_schema.TABLES where TABLE_NAME = '{tableName}' and TABLE_SCHEMA = schema() and TABLE_TYPE = 'BASE TABLE'";

            return command;
        }
        // ------------------------------


        // Create Table
        // ------------------------------
        public override DbCommand GetCreateClientScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
        {
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
                         PRIMARY KEY (sync_scope_id, sync_scope_name)
                         )";

                var command = connection.CreateCommand();
                command.Transaction = transaction;
                command.CommandText = commandText;
                return command;

            }
        }
        public override DbCommand GetCreateServerScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
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
        public override DbCommand GetCreateServerHistoryScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_history";

            var commandText =
                $@"CREATE TABLE IF NOT EXISTS `{tableName}`(
                        sync_scope_id varchar(36) NOT NULL,
                        sync_scope_name varchar(100) NOT NULL,
                        scope_last_sync_timestamp bigint NULL,
                        scope_last_sync_duration bigint NULL,
                        scope_last_sync datetime NULL,
                        PRIMARY KEY (sync_scope_id, sync_scope_name)
                        )";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandText;
            return command;
        }
        // ------------------------------


        // Get all scopes
        // ------------------------------
        public override DbCommand GetAllClientScopesInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText =
                $@"SELECT sync_scope_id, sync_scope_name, sync_scope_schema, sync_scope_setup, sync_scope_version
                        , scope_last_sync, scope_last_server_sync_timestamp, scope_last_sync_timestamp, scope_last_sync_duration
                        FROM  {this.ScopeInfoTableName.Quoted().ToString()}";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandText;

            return command;
        }
        public override DbCommand GetAllServerScopesInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_server";

            var commandText =
                $@"SELECT sync_scope_name
                           , sync_scope_schema
                           , sync_scope_setup
                           , sync_scope_version
                           , sync_scope_last_clean_timestamp                    
                    FROM  `{tableName}`";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandText;

            return command;
        }
        public override DbCommand GetAllServerHistoriesScopesInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_history";

            var commandText =
                    $@"SELECT  sync_scope_id
                           , sync_scope_name
                           , scope_last_sync_timestamp
                           , scope_last_sync_duration
                           , scope_last_sync           
                    FROM  `{tableName}`";
            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandText;

            return command;

        }
        // ------------------------------

        // Get scope
        // ------------------------------
        public override DbCommand GetClientScopeInfoCommand(DbConnection connection, DbTransaction transaction)
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
            command.Transaction = transaction;
            command.CommandText = commandText;

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            return command;

        }
        public override DbCommand GetServerScopeInfoCommand(DbConnection connection, DbTransaction transaction)
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
        public override DbCommand GetServerHistoryScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_history";

            var commandText =
                    $@"SELECT  sync_scope_id
                           , sync_scope_name
                           , scope_last_sync_timestamp
                           , scope_last_sync_duration
                           , scope_last_sync           
                    FROM  `{tableName}`
                    WHERE sync_scope_name = @sync_scope_name and sync_scope_id = @sync_scope_id";

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
            p.DbType = DbType.String;
            p.Size = 36;
            command.Parameters.Add(p);

            return command;

        }
        // ------------------------------

        // Insert Scope
        // ------------------------------
        public override DbCommand GetInsertClientScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = this.ScopeInfoTableName.Quoted().ToString();
            var stmtText = new StringBuilder(
                $"Insert into {tableName} (sync_scope_name, sync_scope_schema, sync_scope_setup, sync_scope_version, scope_last_sync, sync_scope_id, scope_last_server_sync_timestamp, scope_last_sync_timestamp, scope_last_sync_duration) values (@sync_scope_name, @sync_scope_schema, @sync_scope_setup, @sync_scope_version, @scope_last_sync, @sync_scope_id, @scope_last_server_sync_timestamp, @scope_last_sync_timestamp, @scope_last_sync_duration);");

            stmtText.AppendLine();

            stmtText.AppendLine($@"SELECT sync_scope_id, sync_scope_name, sync_scope_schema, sync_scope_setup, 
                             sync_scope_version, scope_last_sync, scope_last_server_sync_timestamp, scope_last_sync_timestamp, scope_last_sync_duration
                             FROM  {this.ScopeInfoTableName.Quoted().ToString()} WHERE sync_scope_id = @sync_scope_id and sync_scope_name=@sync_scope_name;");

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
        public override DbCommand GetInsertServerScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_server";

            var stmtText = new StringBuilder(
                $"INSERT INTO `{tableName}` (sync_scope_name, sync_scope_schema, sync_scope_setup, sync_scope_version, sync_scope_last_clean_timestamp) values (@sync_scope_name, @sync_scope_schema, @sync_scope_setup, @sync_scope_version, @sync_scope_last_clean_timestamp);");

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
        public override DbCommand GetInsertServerHistoryScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_history";

            var stmtText = new StringBuilder(
                $"Insert into `{tableName}` (sync_scope_id, sync_scope_name, scope_last_sync_timestamp, scope_last_sync, scope_last_sync_duration) values (@sync_scope_id, @sync_scope_name, @scope_last_sync_timestamp, @scope_last_sync, @scope_last_sync_duration);");

            stmtText.AppendLine();

            stmtText.AppendLine($@"SELECT  sync_scope_id, sync_scope_name, scope_last_sync_timestamp, scope_last_sync_duration, scope_last_sync           
                                   FROM `{tableName}` WHERE sync_scope_id = @sync_scope_id and sync_scope_name=@sync_scope_name;");

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
        // ------------------------------

        // Update Scope
        // ------------------------------
        public override DbCommand GetUpdateClientScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = this.ScopeInfoTableName.Quoted().ToString();
            var stmtText = new StringBuilder(
                $"Update {tableName} set sync_scope_schema=@sync_scope_schema, sync_scope_setup=@sync_scope_setup, sync_scope_version=@sync_scope_version, scope_last_sync=@scope_last_sync, scope_last_server_sync_timestamp=@scope_last_server_sync_timestamp, scope_last_sync_timestamp=@scope_last_sync_timestamp, scope_last_sync_duration=@scope_last_sync_duration where sync_scope_id=@sync_scope_id and sync_scope_name=@sync_scope_name;");

            stmtText.AppendLine();

            stmtText.AppendLine($@"SELECT sync_scope_id, sync_scope_name, sync_scope_schema, sync_scope_setup, 
                             sync_scope_version, scope_last_sync, scope_last_server_sync_timestamp, scope_last_sync_timestamp, scope_last_sync_duration
                             FROM  {this.ScopeInfoTableName.Quoted().ToString()} WHERE sync_scope_id = @sync_scope_id and sync_scope_name=@sync_scope_name;");

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
        public override DbCommand GetUpdateServerScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {

            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_server";

            var stmtText = new StringBuilder(
                $"UPDATE `{tableName}` set sync_scope_schema=@sync_scope_schema, sync_scope_setup=@sync_scope_setup, sync_scope_version=@sync_scope_version, sync_scope_last_clean_timestamp=@sync_scope_last_clean_timestamp where sync_scope_name=@sync_scope_name;");

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
        public override DbCommand GetUpdateServerHistoryScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_history";

            var stmtText = new StringBuilder(
                 $"Update `{tableName}` set scope_last_sync_timestamp=@scope_last_sync_timestamp, scope_last_sync=@scope_last_sync, scope_last_sync_duration=@scope_last_sync_duration where sync_scope_id=@sync_scope_id and sync_scope_name=@sync_scope_name;");

            stmtText.AppendLine();

            stmtText.AppendLine($@"SELECT  sync_scope_id, sync_scope_name, scope_last_sync_timestamp, scope_last_sync_duration, scope_last_sync           
                                   FROM `{tableName}` WHERE sync_scope_id = @sync_scope_id and sync_scope_name=@sync_scope_name;");

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
        // ------------------------------

        // Delete scope
        // ------------------------------
        public override DbCommand GetDeleteClientScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var stmtText = $"Delete From {this.ScopeInfoTableName.Quoted().ToString()} where sync_scope_id=@sync_scope_id and sync_scope_name=@sync_scope_name;";

            var command = connection.CreateCommand();

            command.Connection = connection;
            command.Transaction = transaction;

            command.CommandText = stmtText;

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_id";
            p.DbType = DbType.String;
            p.Size = 36;
            command.Parameters.Add(p);

            return command;
        }
        public override DbCommand GetDeleteServerScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_server";

            var stmtText = $"Delete From `{tableName}` where sync_scope_name=@sync_scope_name;";

            var command = connection.CreateCommand();

            command.Connection = connection;
            command.Transaction = transaction;

            command.CommandText = stmtText;

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            return command;
        }
        public override DbCommand GetDeleteServerHistoryScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_history";

            var stmtText = $"Delete From `{tableName}` where sync_scope_id=@sync_scope_id and sync_scope_name=@sync_scope_name;";

            var command = connection.CreateCommand();

            command.Connection = connection;
            command.Transaction = transaction;

            command.CommandText = stmtText;

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_id";
            p.DbType = DbType.String;
            p.Size = 36;
            command.Parameters.Add(p);

            return command;
        }
        // ------------------------------

        // Drop Scope table
        // ------------------------------
        public override DbCommand GetDropClientScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = $@"Drop Table`{tableName}`;";

            return command;
        }
        public override DbCommand GetDropServerScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_server";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = $@"Drop Table`{tableName}`;";

            return command;
        }
        public override DbCommand GetDropServerHistoryScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_history";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = $@"Drop Table`{tableName}`;";

            return command;
        }
        // ------------------------------

        // Exist Client Scope
        // ------------------------------
        public override DbCommand GetExistsClientScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}";
            
            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = $@"Select count(*) from `{tableName}` where sync_scope_name=@sync_scope_name;";

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            return command;
        }
        public override DbCommand GetExistsServerScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_server";
            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = $@"Select count(*) from `{tableName}` where sync_scope_name=@sync_scope_name;";

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            return command;
        }
        public override DbCommand GetExistsServerHistoryScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();
            command.Transaction = transaction;

            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_history";
            command.CommandText = $@"Select count(*) from `{tableName}` where sync_scope_name=@sync_scope_name and sync_scope_id=@sync_scope_id";

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_id";
            p.DbType = DbType.String;
            p.Size = 36;
            command.Parameters.Add(p);

            return command;
        }

    }
}
