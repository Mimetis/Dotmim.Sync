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

        public override Task<DbCommand> GetLocalTimestampCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;

            command.CommandText = "SELECT CHANGE_TRACKING_CURRENT_VERSION()";

            return Task.FromResult(command);
        }

        public override Task<DbCommand> GetUpsertScopeInfoCommandAsync(DbScopeType scopeType, object scopeInfo, DbConnection connection, DbTransaction transaction)
        => scopeInfo switch
        {
            ScopeInfo si => GetUpsertClientScopeInfoCommandAsync(si, connection, transaction),
            ServerHistoryScopeInfo shsi => GetUpsertServerHistoryScopeInfoCommandAsync(shsi, connection, transaction),
            ServerScopeInfo ssi => GetUpsertServerScopeInfoCommandForTrackingChangeAsync(ssi, connection, transaction),
            _ => throw new NotImplementedException($"Can't upsert this DbScopeType {scopeType}")
        };


        public Task<DbCommand> GetUpsertServerScopeInfoCommandForTrackingChangeAsync(ServerScopeInfo serverScopeInfo, DbConnection connection, DbTransaction transaction)
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
            if (transaction != null)
                command.Transaction = transaction;

            command.CommandText = commandText;

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

            //p = command.CreateParameter();
            //p.ParameterName = "@sync_scope_last_clean_timestamp";
            //p.Value = serverScopeInfo.LastCleanupTimestamp;
            //p.DbType = DbType.Int64;
            //command.Parameters.Add(p);

            return Task.FromResult(command);
        }
    }
}
