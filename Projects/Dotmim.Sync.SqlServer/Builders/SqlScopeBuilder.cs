using Dotmim.Sync.Builders;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync.SqlServer.Scope
{
    public class SqlScopeBuilder : DbScopeBuilder
    {
        public SqlScopeBuilder(string scopeInfoTableName) : base(scopeInfoTableName)
        {
        }

        public override DbCommand GetLocalTimestampCommand(DbConnection connection, DbTransaction transaction)
        {
            // UPDATE Nov 2019 : We don't use min_active_rowversion anymore, since we are in a transaction
            // and we still need the last row version "during the transaction", so check back to @@DBTS

            var command = connection.CreateCommand();
            command.Transaction = transaction;

            command.CommandText = "SELECT CONVERT(bigint, @@DBTS) as lastTimestamp";

            DbParameter p = command.CreateParameter();
            p.ParameterName = "@sync_new_timestamp";
            p.DbType = DbType.Int64;
            p.Direction = ParameterDirection.Output;
            command.Parameters.Add(p);

            return command;
        }

        //Exists Scope Table
        // ------------------------------
        public override DbCommand GetExistsClientScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = this.ScopeInfoTableName.Unquoted().Normalized().ToString();
            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = $@"IF EXISTS (SELECT t.name FROM sys.tables t WHERE t.name = N'{tableName}') SELECT 1 ELSE SELECT 0";
            return command;
        }
        public override DbCommand GetExistsServerScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_server";
            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = $@"IF EXISTS (SELECT t.name FROM sys.tables t WHERE t.name = N'{tableName}') SELECT 1 ELSE SELECT 0";
            return command;
        }
        public override DbCommand GetExistsServerHistoryScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_history";
            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = $@"IF EXISTS (SELECT t.name FROM sys.tables t WHERE t.name = N'{tableName}') SELECT 1 ELSE SELECT 0";
            return command;
        }
        // ------------------------------


        // Create Table
        // ------------------------------
        public override DbCommand GetCreateClientScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText =
            $@"CREATE TABLE [dbo].{this.ScopeInfoTableName.Quoted().ToString()}(
                                [sync_scope_id] [uniqueidentifier] NOT NULL,
	                            [sync_scope_name] [nvarchar](100) NOT NULL,
	                            [sync_scope_schema] [nvarchar](max) NULL,
	                            [sync_scope_setup] [nvarchar](max) NULL,
	                            [sync_scope_version] [nvarchar](10) NULL,
                                [scope_last_server_sync_timestamp] [bigint] NULL,
                                [scope_last_sync_timestamp] [bigint] NULL,
                                [scope_last_sync_duration] [bigint] NULL,
                                [scope_last_sync] [datetime] NULL
                                CONSTRAINT [PK_{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}] 
                                    PRIMARY KEY CLUSTERED ([sync_scope_id] ASC, [sync_scope_name] ASC)
                                )";
            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandText;
            return command;
        }
        public override DbCommand GetCreateServerScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_server";

            var commandText =
                $@"CREATE TABLE [dbo].[{tableName}] (
	                    [sync_scope_name] [nvarchar](100) NOT NULL,
	                    [sync_scope_schema] [nvarchar](max) NULL,
	                    [sync_scope_setup] [nvarchar](max) NULL,
	                    [sync_scope_version] [nvarchar](10) NULL,
                        [sync_scope_last_clean_timestamp] [bigint] NULL,
                        CONSTRAINT [PK_{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_server] PRIMARY KEY CLUSTERED ([sync_scope_name] ASC)
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
                $@"CREATE TABLE [dbo].[{tableName}](
                        [sync_scope_id] [uniqueidentifier] NOT NULL,
	                    [sync_scope_name] [nvarchar](100) NOT NULL,
                        [scope_last_sync_timestamp] [bigint] NULL,
                        [scope_last_sync_duration] [bigint] NULL,
                        [scope_last_sync] [datetime] NULL
                        CONSTRAINT [PK_{tableName}] PRIMARY KEY CLUSTERED ([sync_scope_id] ASC, [sync_scope_name] ASC)
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
                $@"SELECT [sync_scope_id], [sync_scope_name], [sync_scope_schema], [sync_scope_setup], [sync_scope_version], 
                          [scope_last_sync], [scope_last_server_sync_timestamp], [scope_last_sync_timestamp], [scope_last_sync_duration]
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
                $@"SELECT [sync_scope_name]
                           , [sync_scope_schema]
                           , [sync_scope_setup]
                           , [sync_scope_version]
                           , [sync_scope_last_clean_timestamp]
                    FROM  [{tableName}]";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandText;

            return command;
        }
        public override DbCommand GetAllServerHistoriesScopesInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_history";
            var commandText =
                $@"SELECT [sync_scope_id]
                           , [sync_scope_name]
                           , [scope_last_sync_timestamp]
                           , [scope_last_sync_duration]
                           , [scope_last_sync]
                    FROM  [{tableName}]";


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
                    $@"SELECT [sync_scope_id]
                           , [sync_scope_name]
                           , [sync_scope_schema]
                           , [sync_scope_setup]
                           , [sync_scope_version]
                           , [scope_last_sync]
                           , [scope_last_server_sync_timestamp]
                           , [scope_last_sync_timestamp]
                           , [scope_last_sync_duration]
                    FROM  {this.ScopeInfoTableName.Quoted().ToString()}
                    WHERE [sync_scope_name] = @sync_scope_name" ;

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
                $@"SELECT [sync_scope_name]
                           , [sync_scope_schema]
                           , [sync_scope_setup]
                           , [sync_scope_version]
                           , [sync_scope_last_clean_timestamp]
                    FROM  [{tableName}]
                    WHERE [sync_scope_name] = @sync_scope_name";

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
                $@"SELECT [sync_scope_id]
                           , [sync_scope_name]
                           , [scope_last_sync_timestamp]
                           , [scope_last_sync_duration]
                           , [scope_last_sync]
                    FROM  [{tableName}]
                    WHERE [sync_scope_name] = @sync_scope_name and [sync_scope_id] = @sync_scope_id";


            var command = connection.CreateCommand();
            command.Transaction = transaction;

            command.CommandText = commandText;

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            var p0 = command.CreateParameter();
            p0.ParameterName = "@sync_scope_id";
            p0.DbType = DbType.String;
            p0.Size = -1;
            command.Parameters.Add(p0);

            return command;
        }
        // ------------------------------

        // Insert Scope
        // ------------------------------
        public override DbCommand GetInsertClientScopeInfoCommand(DbConnection connection, DbTransaction transaction) 
            => GetUpdateClientScopeInfoCommand(connection, transaction);
        public override DbCommand GetInsertServerScopeInfoCommand(DbConnection connection, DbTransaction transaction)
            => GetUpdateServerScopeInfoCommand(connection, transaction);
        public override DbCommand GetInsertServerHistoryScopeInfoCommand(DbConnection connection, DbTransaction transaction)
            => GetUpdateServerHistoryScopeInfoCommand(connection, transaction);
        // ------------------------------

        // Update Scope
        // ------------------------------
        public override DbCommand GetUpdateClientScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText = $@"
                    MERGE {this.ScopeInfoTableName.Quoted().ToString()} AS [base] 
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
                    ON [base].[sync_scope_id] = [changes].[sync_scope_id] and [base].[sync_scope_name] = [changes].[sync_scope_name]
                    WHEN NOT MATCHED THEN
	                    INSERT ([sync_scope_name], [sync_scope_schema], [sync_scope_setup], [sync_scope_version], [sync_scope_id], [scope_last_sync], [scope_last_sync_timestamp],  [scope_last_server_sync_timestamp], [scope_last_sync_duration])
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
                            INSERTED.[scope_last_sync_duration];";


            var command = connection.CreateCommand();
            command.Transaction = transaction;

            command.CommandText = commandText;

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
            p.ParameterName = "@sync_scope_id";
            p.DbType = DbType.Guid;
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

            return command;

        }
        public override DbCommand GetUpdateServerScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_server";

            var commandText = $@"
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
                            INSERTED.[sync_scope_last_clean_timestamp];";


            var command = connection.CreateCommand();
            command.Transaction = transaction;

            command.CommandText = commandText;

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            //p.Value = serverScopeInfo.Name;
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_schema";
            //p.Value = serverScopeInfo.Schema == null ? DBNull.Value : (object)JsonConvert.SerializeObject(serverScopeInfo.Schema);
            p.DbType = DbType.String;
            p.Size = int.MaxValue;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_setup";
            //p.Value = serverScopeInfo.Setup == null ? DBNull.Value : (object)JsonConvert.SerializeObject(serverScopeInfo.Setup);
            p.DbType = DbType.String;
            p.Size = int.MaxValue;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_version";
            //p.Value = string.IsNullOrEmpty(serverScopeInfo.Version) ? DBNull.Value : (object)serverScopeInfo.Version;
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

            var commandText = $@"
                    MERGE [{tableName}] AS [base] 
                    USING (
                               SELECT  @sync_scope_id AS sync_scope_id,  
	                                   @sync_scope_name AS sync_scope_name,  
                                       @scope_last_sync_timestamp AS scope_last_sync_timestamp,
                                       @scope_last_sync_duration AS scope_last_sync_duration,
                                       @scope_last_sync AS scope_last_sync
                           ) AS [changes] 
                    ON [base].[sync_scope_id] = [changes].[sync_scope_id] and [base].[sync_scope_name] = [changes].[sync_scope_name]
                    WHEN NOT MATCHED THEN
	                    INSERT ([sync_scope_name], [sync_scope_id], [scope_last_sync_timestamp], [scope_last_sync], [scope_last_sync_duration])
	                    VALUES ([changes].[sync_scope_name], [changes].[sync_scope_id], [changes].[scope_last_sync_timestamp],[changes].[scope_last_sync], [changes].[scope_last_sync_duration])
                    WHEN MATCHED THEN
	                    UPDATE SET [scope_last_sync_timestamp] = [changes].[scope_last_sync_timestamp],
                                   [scope_last_sync] = [changes].[scope_last_sync],
                                   [scope_last_sync_duration] = [changes].[scope_last_sync_duration]
                    OUTPUT  INSERTED.[sync_scope_name], 
                            INSERTED.[sync_scope_id], 
                            INSERTED.[scope_last_sync_timestamp],
                            INSERTED.[scope_last_sync],
                            INSERTED.[scope_last_sync_duration];";

            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;

            command.CommandText = commandText;

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            //p.Value = serverHistoryScopeInfo.Name;
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@sync_scope_id";
            //p.Value = serverHistoryScopeInfo.Id;
            p.DbType = DbType.Guid;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@scope_last_sync_timestamp";
            //p.Value = serverHistoryScopeInfo.LastSyncTimestamp;
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@scope_last_sync";
            //p.Value = serverHistoryScopeInfo.LastSync.HasValue ? (object)serverHistoryScopeInfo.LastSync.Value : DBNull.Value;
            p.DbType = DbType.DateTime;
            command.Parameters.Add(p);

            p = command.CreateParameter();
            p.ParameterName = "@scope_last_sync_duration";
            //p.Value = serverHistoryScopeInfo.LastSyncDuration;
            p.DbType = DbType.Int64;
            command.Parameters.Add(p);

            return command;

        }

        // ------------------------------

        // Drop Scope table
        // ------------------------------
        public override DbCommand GetDropClientScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = this.ScopeInfoTableName.Unquoted().ToString();
            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = $"DROP Table [dbo].[{tableName}]";
            return command;
        }
        public override DbCommand GetDropServerScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_server";
            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = $"DROP Table [dbo].[{tableName}]";
            return command;
        }
        public override DbCommand GetDropServerHistoryScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_history";
            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = $"DROP Table [dbo].[{tableName}]";
            return command;
        }
        // ------------------------------

        // Exist Client Scope
        // ------------------------------
        public override DbCommand GetExistsClientScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();

            command.Transaction = transaction;

            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}";

            command.CommandText = $@"Select count(*) from [{tableName}] where sync_scope_name = @sync_scope_name;";

            var p1 = command.CreateParameter();
            p1.ParameterName = "@sync_scope_name";
            p1.DbType = DbType.String;
            p1.Size = 100;
            command.Parameters.Add(p1);

            return command;

        }
        public override DbCommand GetExistsServerScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();

            command.Transaction = transaction;

            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_server";

            command.CommandText = $@"Select count(*) from [{tableName}] where sync_scope_name = @sync_scope_name;";

            var p1 = command.CreateParameter();
            p1.ParameterName = "@sync_scope_name";
            p1.DbType = DbType.String;
            p1.Size = 100;
            command.Parameters.Add(p1);

            return command;

        }
        public override DbCommand GetExistsServerHistoryScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();

            command.Transaction = transaction;

            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_history";

            command.CommandText = $@"Select count(*) from [{tableName}] where sync_scope_id = @sync_scope_id and sync_scope_name = @sync_scope_name;";

            var p0 = command.CreateParameter();
            p0.ParameterName = "@sync_scope_id";
            p0.DbType = DbType.String;
            p0.Size = -1;
            command.Parameters.Add(p0);

            var p1 = command.CreateParameter();
            p1.ParameterName = "@sync_scope_name";
            p1.DbType = DbType.String;
            p1.Size = 100;
            command.Parameters.Add(p1);

            return command;
        }
        // ------------------------------

    }
}
