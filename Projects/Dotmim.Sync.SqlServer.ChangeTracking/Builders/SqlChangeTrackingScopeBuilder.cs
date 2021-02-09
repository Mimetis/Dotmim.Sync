using Dotmim.Sync.Builders;
using Dotmim.Sync.SqlServer.Scope;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.SqlServer.ChangeTracking.Builders
{
    public class SqlChangeTrackingScopeBuilder : SqlScopeBuilder
    {
        public SqlChangeTrackingScopeBuilder(string scopeInfoTableName) : base(scopeInfoTableName)
        {
        }

        public override DbCommand GetLocalTimestampCommand(DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();
            command.Transaction = transaction;

            command.CommandText = "SELECT CHANGE_TRACKING_CURRENT_VERSION()";

            return command;
        }

        public override DbCommand GetInsertScopeInfoCommand(DbScopeType scopeType, DbConnection connection, DbTransaction transaction)
            => this.GetUpdateScopeInfoCommand(scopeType, connection, transaction);

        public override DbCommand GetUpdateScopeInfoCommand(DbScopeType scopeType, DbConnection connection, DbTransaction transaction)
            => scopeType switch
            {
                DbScopeType.Client => GetSaveClientScopeInfoCommand(connection, transaction),
                DbScopeType.ServerHistory => GetSaveServerHistoryScopeInfoCommand(connection, transaction),
                DbScopeType.Server => GetSaveServerScopeInfoCommandForTrackingChange(connection, transaction),
                _ => throw new NotImplementedException($"Can't save this DbScopeType {scopeType}")
            };


        public DbCommand GetSaveServerScopeInfoCommandForTrackingChange(DbConnection connection, DbTransaction transaction)
        {
            var tableName = $"{this.ScopeInfoTableName.Unquoted().Normalized().ToString()}_server";
            var commandText = $@"
                    Declare @minVersion int;
                    Select @minVersion = MIN(CHANGE_TRACKING_MIN_VALID_VERSION(T.object_id)) from sys.tables T where CHANGE_TRACKING_MIN_VALID_VERSION(T.object_id) is not null;

                    MERGE {tableName} AS [base] 
                    USING (
                               SELECT  @sync_scope_name AS sync_scope_name,  
	                                   @sync_scope_schema AS sync_scope_schema,  
	                                   @sync_scope_setup AS sync_scope_setup,  
	                                   @sync_scope_version AS sync_scope_version
                           ) AS [changes] 
                    ON [base].[sync_scope_name] = [changes].[sync_scope_name]
                    WHEN NOT MATCHED THEN
	                    INSERT ([sync_scope_name], [sync_scope_schema], [sync_scope_setup], [sync_scope_version], [sync_scope_last_clean_timestamp])
	                    VALUES ([changes].[sync_scope_name], [changes].[sync_scope_schema], [changes].[sync_scope_setup], [changes].[sync_scope_version], @minVersion)
                    WHEN MATCHED THEN
	                    UPDATE SET [sync_scope_name] = [changes].[sync_scope_name], 
                                   [sync_scope_schema] = [changes].[sync_scope_schema], 
                                   [sync_scope_setup] = [changes].[sync_scope_setup], 
                                   [sync_scope_version] = [changes].[sync_scope_version], 
                                   [sync_scope_last_clean_timestamp] = @minVersion
                    OUTPUT  INSERTED.[sync_scope_name], 
                            INSERTED.[sync_scope_schema], 
                            INSERTED.[sync_scope_setup], 
                            INSERTED.[sync_scope_version], 
                            INSERTED.[sync_scope_last_clean_timestamp];
                ";

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

            return command;
        }
    }
}
