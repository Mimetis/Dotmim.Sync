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

        public DbCommand GetAllClientScopesCommand(DbConnection connection, DbTransaction transaction)
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
                    WHERE [sync_scope_name] = @sync_scope_name";

            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;

            command.CommandText = commandText;

            var p = command.CreateParameter();
            p.ParameterName = "@sync_scope_name";
            p.DbType = DbType.String;
            p.Size = 100;
            command.Parameters.Add(p);

            return command;
        }

        public DbCommand GetAllServerHistoryScopesCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_history";
            var commandText =
                $@"SELECT [sync_scope_id]
                           , [sync_scope_name]
                           , [scope_last_sync_timestamp]
                           , [scope_last_sync_duration]
                           , [scope_last_sync]
                    FROM  [{tableName}]
                    WHERE [sync_scope_name] = @sync_scope_name";


            var command = connection.CreateCommand();
            if (transaction != null)
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
                $@"SELECT [sync_scope_name]
                           , [sync_scope_schema]
                           , [sync_scope_setup]
                           , [sync_scope_version]
                           , [sync_scope_last_clean_timestamp]
                    FROM  [{tableName}]
                    WHERE [sync_scope_name] = @sync_scope_name";

            var command = connection.CreateCommand();
            if (transaction != null)
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
            _ => GetAllClientScopesCommand(connection, transaction)
        };

        public DbCommand GetCreateClientScopeInfoTableCommandAsync(DbConnection connection, DbTransaction transaction)
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
                        CONSTRAINT [PK_{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}] PRIMARY KEY CLUSTERED ([sync_scope_id] ASC)
                        )";
            var command = connection.CreateCommand();

            if (transaction != null)
                command.Transaction = transaction;

            command.CommandText = commandText;

            return command;
        }

        public DbCommand GetCreateServerHistoryScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_history";

            var commandText =
                $@"CREATE TABLE [dbo].[{tableName}](
                        [sync_scope_id] [uniqueidentifier] NOT NULL,
	                    [sync_scope_name] [nvarchar](100) NOT NULL,
                        [scope_last_sync_timestamp] [bigint] NULL,
                        [scope_last_sync_duration] [bigint] NULL,
                        [scope_last_sync] [datetime] NULL
                        CONSTRAINT [PK_{tableName}] PRIMARY KEY CLUSTERED ([sync_scope_id] ASC)
                        )";

            var command = connection.CreateCommand();

            if (transaction != null)
                command.Transaction = transaction;

            command.CommandText = commandText;

            return command;

        }
        public DbCommand GetCreateServerScopeInfoTableCommand(DbConnection connection, DbTransaction transaction)
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

            if (transaction != null)
                command.Transaction = transaction;

            command.CommandText = commandText;

            return command;

        }

        public override DbCommand GetCreateScopeInfoTableCommand(DbScopeType scopeType, DbConnection connection, DbTransaction transaction)
        => scopeType switch
        {
            DbScopeType.Server => GetCreateServerScopeInfoTableCommand(connection, transaction),
            DbScopeType.ServerHistory => GetCreateServerHistoryScopeInfoTableCommand(connection, transaction),
            _ => GetCreateClientScopeInfoTableCommandAsync(connection, transaction)
        };


        public override DbCommand GetDropScopeInfoTableCommand(DbScopeType scopeType, DbConnection connection, DbTransaction transaction)
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

            command.CommandText = $"DROP Table [dbo].[{tableName}]";

            return command;
        }

        public override DbCommand GetExistsScopeInfoTableCommand(DbScopeType scopeType, DbConnection connection, DbTransaction transaction)
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

            command.CommandText = $@"IF EXISTS (SELECT t.name FROM sys.tables t WHERE t.name = N'{tableName}') SELECT 1 ELSE SELECT 0"; ;

            return command;
        }

        public override DbCommand GetLocalTimestampCommand(DbConnection connection, DbTransaction transaction)
        {
            // UPDATE Nov 2019 : We don't use min_active_rowversion anymore, since we are in a transaction
            // and we still need the last row version "during the transaction", so check back to @@DBTS

            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;

            command.CommandText = "SELECT CONVERT(bigint, @@DBTS) as lastTimestamp";

            DbParameter p = command.CreateParameter();
            p.ParameterName = "@sync_new_timestamp";
            p.DbType = DbType.Int64;
            p.Direction = ParameterDirection.Output;
            command.Parameters.Add(p);

            return command;
        }

        public override DbCommand GetExistsScopeInfoCommand(DbScopeType scopeType, DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();

            if (transaction != null)
                command.Transaction = transaction;

            var tableName = scopeType switch
            {
                DbScopeType.Client => $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}",
                DbScopeType.Server => $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_server",
                DbScopeType.ServerHistory => $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_history",
                _ => throw new NotImplementedException($"Can't get scope name from this DbScopeType {scopeType}")
            };

            if (scopeType == DbScopeType.Server)
                command.CommandText = $@"Select count(*) from [{tableName}] where sync_scope_name = @sync_scope_id";
            else
                command.CommandText = $@"Select count(*) from [{tableName}] where sync_scope_id = @sync_scope_id";

            var p0 = command.CreateParameter();
            p0.ParameterName = "@sync_scope_id";
            p0.DbType = DbType.String;
            p0.Size = -1;
            command.Parameters.Add(p0);

            return command;
        }
        public override DbCommand GetUpdateScopeInfoCommand(DbScopeType scopeType, DbConnection connection, DbTransaction transaction)
            => GetInsertScopeInfoCommand(scopeType, connection, transaction);

        public override DbCommand GetInsertScopeInfoCommand(DbScopeType scopeType, DbConnection connection, DbTransaction transaction)
            => scopeType switch
            {
                DbScopeType.Client => GetSaveClientScopeInfoCommand(connection, transaction),
                DbScopeType.ServerHistory => GetSaveServerHistoryScopeInfoCommand(connection, transaction),
                DbScopeType.Server => GetSaveServerScopeInfoCommand(connection, transaction),
                _ => throw new NotImplementedException($"Can't save this DbScopeType {scopeType}")
            };

        public DbCommand GetSaveClientScopeInfoCommand(DbConnection connection, DbTransaction transaction)
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
                            INSERTED.[scope_last_sync_duration];";


            var command = connection.CreateCommand();
            if (transaction != null)
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
        public DbCommand GetSaveServerHistoryScopeInfoCommand(DbConnection connection, DbTransaction transaction)
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
        public DbCommand GetSaveServerScopeInfoCommand(DbConnection connection, DbTransaction transaction)
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
            if (transaction != null)
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
    }
}
