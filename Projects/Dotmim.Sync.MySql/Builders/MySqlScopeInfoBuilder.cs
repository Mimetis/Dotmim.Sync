using Dotmim.Sync.Builders;

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
#if NET5_0 || NET6_0 || NET7_0 || NETCOREAPP3_1
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

        public override DbCommand GetExistsScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = this.ScopeInfoTableName.Unquoted().Normalized().ToString();
            var command = connection.CreateCommand();
            command.Transaction = transaction;

            command.CommandText = $"select count(*) from information_schema.TABLES where TABLE_NAME = '{tableName}' and TABLE_SCHEMA = schema() and TABLE_TYPE = 'BASE TABLE'";

            return command;
        }
        public override DbCommand GetExistsScopeInfoClientTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_client";
            var command = connection.CreateCommand();
            command.Transaction = transaction;

            command.CommandText = $"select count(*) from information_schema.TABLES where TABLE_NAME = '{tableName}' and TABLE_SCHEMA = schema() and TABLE_TYPE = 'BASE TABLE'";

            return command;
        }
        // ------------------------------


        // Create Table
        // ------------------------------
        public override DbCommand GetCreateScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}";
            var commandText =
                    $@"CREATE TABLE IF NOT EXISTS `{tableName}`(
                         sync_scope_name varchar(100) NOT NULL,
                         sync_scope_schema longtext NULL,
                         sync_scope_setup longtext NULL,
                         sync_scope_version varchar(10) NULL,
                         sync_scope_last_clean_timestamp bigint NULL,
                         sync_scope_properties longtext NULL,
                         PRIMARY KEY (sync_scope_name)
                         )";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandText;
            return command;
        }
        public override DbCommand GetCreateScopeInfoClientTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_client";

            var commandText =
                $@"CREATE TABLE IF NOT EXISTS `{tableName}`(
                        sync_scope_id varchar(36) NOT NULL,
                        sync_scope_name varchar(100) NOT NULL,
                        sync_scope_hash varchar(100) NOT NULL,
                        sync_scope_parameters longtext NULL,
                        scope_last_sync_timestamp bigint NULL,
                        scope_last_server_sync_timestamp bigint NULL,
                        scope_last_sync_duration bigint NULL,
                        scope_last_sync datetime NULL,
                        sync_scope_errors longtext NULL,
                        sync_scope_properties longtext NULL,
                        PRIMARY KEY (sync_scope_id, sync_scope_name, sync_scope_hash)
                        )";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandText;
            return command;
        }
        // ------------------------------


        // Get all scopes
        // ------------------------------
        public override DbCommand GetAllScopeInfosCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = this.ScopeInfoTableName.Unquoted().Normalized().ToString();

            var commandText =
                $@"SELECT sync_scope_name, 
                          sync_scope_schema, 
                          sync_scope_setup, 
                          sync_scope_version,
                          sync_scope_last_clean_timestamp,
                          sync_scope_properties
                        FROM `{tableName}`";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandText;

            return command;
        }
        public override DbCommand GetAllScopeInfoClientsCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_client";

            var commandText =  $@"SELECT  sync_scope_id
                         , sync_scope_name
                         , sync_scope_hash
                         , sync_scope_parameters
                         , scope_last_sync_timestamp
                         , scope_last_server_sync_timestamp
                         , scope_last_sync_duration
                         , scope_last_sync
                         , sync_scope_errors
                         , sync_scope_properties
                    FROM `{tableName}`";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandText;

            return command;

        }
        // ------------------------------

        // Get scope
        // ------------------------------
        public override DbCommand GetScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = this.ScopeInfoTableName.Unquoted().Normalized().ToString();
         
            var commandText =
                $@"SELECT sync_scope_name, 
                          sync_scope_schema, 
                          sync_scope_setup, 
                          sync_scope_version,
                          sync_scope_last_clean_timestamp,
                          sync_scope_properties
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
      
        public override DbCommand GetScopeInfoClientCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_client";

            var commandText =
                    $@"SELECT sync_scope_id
                         , sync_scope_name
                         , sync_scope_hash
                         , sync_scope_parameters
                         , scope_last_sync_timestamp
                         , scope_last_server_sync_timestamp
                         , scope_last_sync_duration
                         , scope_last_sync
                         , sync_scope_errors
                         , sync_scope_properties
                    FROM  `{tableName}`
                    WHERE sync_scope_name = @sync_scope_name and sync_scope_id = @sync_scope_id and sync_scope_hash = @sync_scope_hash";

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

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_hash";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            return command;

        }
        // ------------------------------

        // Insert Scope
        // ------------------------------
        public override DbCommand GetInsertScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = this.ScopeInfoTableName.Unquoted().Normalized().ToString();

            var stmtText = new StringBuilder(
                $"INSERT INTO `{tableName}` " +
                      $"(sync_scope_name, sync_scope_schema, sync_scope_setup, sync_scope_version, " +
                      $"sync_scope_last_clean_timestamp, sync_scope_properties) " +
                      $"VALUES " +
                      $"(@sync_scope_name, @sync_scope_schema, @sync_scope_setup, @sync_scope_version, " +
                      $"@sync_scope_last_clean_timestamp, @sync_scope_properties);");

            stmtText.AppendLine();

            stmtText.AppendLine($"SELECT sync_scope_name, sync_scope_schema, sync_scope_setup, sync_scope_version, " +
                $"sync_scope_last_clean_timestamp, sync_scope_properties " +
                $"FROM `{tableName}` " +
                $"WHERE sync_scope_name=@sync_scope_name;");

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
            p.DbType = DbType.DateTime;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_properties";
            p.DbType = DbType.String;
            p.Size = -1;
            command.Parameters.Add(p);

            return command;
        }
        public override DbCommand GetInsertScopeInfoClientCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_client";

            var stmtText = new StringBuilder(
                $"INSERT INTO `{tableName}` " +
                      $"(sync_scope_name, sync_scope_id, sync_scope_hash, sync_scope_parameters, scope_last_sync_timestamp, scope_last_server_sync_timestamp, " +
                      $"scope_last_sync, scope_last_sync_duration, sync_scope_errors, sync_scope_properties) " +
                      $"VALUES " +
                      $"(@sync_scope_name, @sync_scope_id, @sync_scope_hash, @sync_scope_parameters, @scope_last_sync_timestamp, @scope_last_server_sync_timestamp, " +
                      $"@scope_last_sync, @scope_last_sync_duration, @sync_scope_errors, @sync_scope_properties);");

            stmtText.AppendLine();

            stmtText.AppendLine($"SELECT sync_scope_id, sync_scope_name, sync_scope_hash, " +
                $"sync_scope_parameters, scope_last_sync_timestamp, scope_last_server_sync_timestamp, " +
                $"scope_last_sync, scope_last_sync_duration, sync_scope_errors, sync_scope_properties " +
                $"FROM `{tableName}` " +
                $"WHERE sync_scope_id=@sync_scope_id AND sync_scope_name=@sync_scope_name AND sync_scope_hash=@sync_scope_hash;");

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
        // ------------------------------

        // Update Scope
        // ------------------------------
        public override DbCommand GetUpdateScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = this.ScopeInfoTableName.Unquoted().Normalized().ToString();

            var stmtText = new StringBuilder(
                $"UPDATE `{tableName}` " +
                $"SET sync_scope_schema=@sync_scope_schema, " +
                $"sync_scope_setup=@sync_scope_setup, " +
                $"sync_scope_version=@sync_scope_version, " +
                $"sync_scope_last_clean_timestamp=@sync_scope_last_clean_timestamp, " +
                $"sync_scope_properties=@sync_scope_properties " +
                $"WHERE sync_scope_name=@sync_scope_name;");
            stmtText.AppendLine();
            stmtText.AppendLine();
            stmtText.AppendLine(
                $"SELECT sync_scope_name, sync_scope_schema, sync_scope_setup, sync_scope_version, " +
                $"sync_scope_last_clean_timestamp, sync_scope_properties " +
                $"FROM `{tableName}` " +
                $"WHERE sync_scope_name=@sync_scope_name;");

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
        public override DbCommand GetUpdateScopeInfoClientCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_client";

            var stmtText = new StringBuilder(
                $"UPDATE `{tableName}` " +
                $"SET scope_last_sync_timestamp=@scope_last_sync_timestamp, " +
                $"scope_last_server_sync_timestamp=@scope_last_server_sync_timestamp, " +
                $"scope_last_sync=@scope_last_sync, " +
                $"scope_last_sync_duration=@scope_last_sync_duration, " +
                $"sync_scope_properties=@sync_scope_properties,  " +
                $"sync_scope_errors=@sync_scope_errors,  " +
                $"sync_scope_parameters=@sync_scope_parameters  " +
                $"WHERE sync_scope_id=@sync_scope_id and sync_scope_name=@sync_scope_name and sync_scope_hash=@sync_scope_hash;");

            stmtText.AppendLine();

            stmtText.AppendLine(
                $"SELECT sync_scope_id, sync_scope_name, sync_scope_hash, sync_scope_parameters, " +
                $"scope_last_sync_timestamp, scope_last_server_sync_timestamp, scope_last_sync, " +
                $"scope_last_sync_duration, sync_scope_errors, sync_scope_properties " +
                $"FROM  `{tableName}` " +
                $"WHERE sync_scope_name=@sync_scope_name and sync_scope_id=@sync_scope_id and sync_scope_hash=@sync_scope_hash; ");

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
        // ------------------------------

        // Delete scope
        // ------------------------------
        public override DbCommand GetDeleteScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {

            var tableName = this.ScopeInfoTableName.Unquoted().Normalized().ToString();

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
      
        public override DbCommand GetDeleteScopeInfoClientCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_client";

            var stmtText = $"Delete From `{tableName}` where sync_scope_id=@sync_scope_id and sync_scope_name=@sync_scope_name and sync_scope_hash=@sync_scope_hash;";

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
            p.ParameterName = "@sync_scope_hash";
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
        public override DbCommand GetDropScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = $@"Drop Table`{tableName}`;";

            return command;
        }
        public override DbCommand GetDropScopeInfoClientTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_client";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = $@"Drop Table`{tableName}`;";

            return command;
        }
        // ------------------------------

        // Exist Client Scope
        // ------------------------------
        public override DbCommand GetExistsScopeInfoCommand(DbConnection connection, DbTransaction transaction)
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
        public override DbCommand GetExistsScopeInfoClientCommand(DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();
            command.Transaction = transaction;

            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_client";
            command.CommandText = $@"Select count(*) from `{tableName}` where sync_scope_name=@sync_scope_name and sync_scope_id=@sync_scope_id and sync_scope_hash=@sync_scope_hash";

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
            
            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_hash";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            return command;
        }

    }
}
