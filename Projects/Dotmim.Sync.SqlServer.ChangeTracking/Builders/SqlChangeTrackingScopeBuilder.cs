using Dotmim.Sync.SqlServer.Scope;
using System.Data;
using System.Data.Common;

namespace Dotmim.Sync.SqlServer.ChangeTracking.Builders
{
    /// <inheritdoc />
    public class SqlChangeTrackingScopeBuilder : SqlScopeBuilder
    {
        /// <inheritdoc />
        public SqlChangeTrackingScopeBuilder(string scopeInfoTableName)
            : base(scopeInfoTableName)
        {
        }

        /// <inheritdoc />
        public override DbCommand GetLocalTimestampCommand(DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = "SELECT CHANGE_TRACKING_CURRENT_VERSION()";
            return command;
        }

        /// <inheritdoc />
        public override DbCommand GetUpdateScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText = $@"
                    DECLARE @minVersion int;
                    SELECT @minVersion = MIN(CHANGE_TRACKING_MIN_VALID_VERSION(T.object_id)) 
                    FROM sys.tables T 
                    WHERE CHANGE_TRACKING_MIN_VALID_VERSION(T.object_id) is not null;

                    MERGE [{this.ScopeInfoTableNames.NormalizedFullName}] AS [base] 
                    USING (
                               SELECT  @sync_scope_name AS sync_scope_name,  
	                                   @sync_scope_schema AS sync_scope_schema,  
	                                   @sync_scope_setup AS sync_scope_setup,  
	                                   @sync_scope_version AS sync_scope_version,
                                       @sync_scope_properties as sync_scope_properties
                           ) AS [changes] 
                    ON [base].[sync_scope_name] = [changes].[sync_scope_name]
                    WHEN NOT MATCHED THEN
	                    INSERT ([sync_scope_name], [sync_scope_schema], [sync_scope_setup], [sync_scope_version], [sync_scope_last_clean_timestamp], [sync_scope_properties])
	                    VALUES ([changes].[sync_scope_name], [changes].[sync_scope_schema], [changes].[sync_scope_setup], [changes].[sync_scope_version], @minVersion, [changes].[sync_scope_properties])
                    WHEN MATCHED THEN
	                    UPDATE SET [sync_scope_name] = [changes].[sync_scope_name], 
                                   [sync_scope_schema] = [changes].[sync_scope_schema], 
                                   [sync_scope_setup] = [changes].[sync_scope_setup], 
                                   [sync_scope_version] = [changes].[sync_scope_version],
                                   [sync_scope_last_clean_timestamp] = @minVersion,
                                   [sync_scope_properties] = [changes].[sync_scope_properties]
                    OUTPUT  INSERTED.[sync_scope_name], 
                            INSERTED.[sync_scope_schema], 
                            INSERTED.[sync_scope_setup], 
                            INSERTED.[sync_scope_version],
                            INSERTED.[sync_scope_last_clean_timestamp],
                            INSERTED.[sync_scope_properties];";

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
            p.ParameterName = "@sync_scope_properties";
            p.DbType = DbType.String;
            p.Size = -1;
            command.Parameters.Add(p);

            return command;
        }
    }
}