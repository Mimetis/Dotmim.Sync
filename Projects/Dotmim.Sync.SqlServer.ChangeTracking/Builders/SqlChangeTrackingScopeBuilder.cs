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
        public override DbCommand GetUpdateScopeInfoClientCommand(DbConnection connection, DbTransaction transaction)
        {

            var commandText = $@"
                    
                    -- Need to update the last clean up in scope info as it's done automatically by SQL Server
                    -- Here is a good place as this update is called at the end of any sync to update the current client
                    
                    IF EXISTS (SELECT t.name FROM sys.tables t WHERE t.name = N'{this.ScopeInfoTableNames.Name}')
                    BEGIN
                        DECLARE @maxVersion bigint;
                        SELECT @maxVersion = MAX(CHANGE_TRACKING_MIN_VALID_VERSION(T.object_id)) 
                        FROM sys.tables T 
                        WHERE CHANGE_TRACKING_MIN_VALID_VERSION(T.object_id) is not null;
                        
                        UPDATE {this.ScopeInfoTableNames.QuotedFullName} WITH (READCOMMITTED) SET sync_scope_last_clean_timestamp = @maxVersion;
                    END 

                    MERGE {this.ScopeInfoClientTableNames.QuotedFullName} WITH (READCOMMITTED) AS [base] 
                    USING (
                               SELECT  @sync_scope_id AS sync_scope_id,  
	                                   @sync_scope_name AS sync_scope_name,  
	                                   @sync_scope_hash AS sync_scope_hash,  
	                                   @sync_scope_parameters AS sync_scope_parameters,  
                                       @scope_last_sync_timestamp AS scope_last_sync_timestamp,
                                       @scope_last_server_sync_timestamp AS scope_last_server_sync_timestamp,
                                       @scope_last_sync_duration AS scope_last_sync_duration,
                                       @scope_last_sync AS scope_last_sync,
                                       @sync_scope_errors AS sync_scope_errors,
                                       @sync_scope_properties AS sync_scope_properties
                           ) AS [changes] 
                    ON [base].[sync_scope_id] = [changes].[sync_scope_id] and [base].[sync_scope_name] = [changes].[sync_scope_name] and [base].[sync_scope_hash] = [changes].[sync_scope_hash]
                    WHEN NOT MATCHED THEN
	                    INSERT ([sync_scope_name], [sync_scope_id], [sync_scope_hash], [sync_scope_parameters], [scope_last_sync_timestamp],  [scope_last_server_sync_timestamp], [scope_last_sync], [scope_last_sync_duration], [sync_scope_errors], [sync_scope_properties])
	                    VALUES ([changes].[sync_scope_name], [changes].[sync_scope_id], [changes].[sync_scope_hash], [changes].[sync_scope_parameters], [changes].[scope_last_sync_timestamp], [changes].[scope_last_server_sync_timestamp], [changes].[scope_last_sync], [changes].[scope_last_sync_duration], [changes].[sync_scope_errors], [changes].[sync_scope_properties])
                    WHEN MATCHED THEN
	                    UPDATE SET [scope_last_sync_timestamp] = [changes].[scope_last_sync_timestamp],
                                   [scope_last_server_sync_timestamp] = [changes].[scope_last_server_sync_timestamp],
                                   [scope_last_sync] = [changes].[scope_last_sync],
                                   [scope_last_sync_duration] = [changes].[scope_last_sync_duration],
                                   [sync_scope_errors] = [changes].[sync_scope_errors],
                                   [sync_scope_properties] = [changes].[sync_scope_properties]
                    OUTPUT  INSERTED.[sync_scope_name], 
                            INSERTED.[sync_scope_id], 
                            INSERTED.[sync_scope_hash], 
                            INSERTED.[sync_scope_parameters], 
                            INSERTED.[scope_last_sync_timestamp],
                            INSERTED.[scope_last_server_sync_timestamp],
                            INSERTED.[scope_last_sync],
                            INSERTED.[scope_last_sync_duration],
                            INSERTED.[sync_scope_errors],
                            INSERTED.[sync_scope_properties]; ";

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

        /// <inheritdoc />
        public override DbCommand GetUpdateScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandText = $@"
                    DECLARE @maxVersion bigint;
                    SELECT @maxVersion = MAX(CHANGE_TRACKING_MIN_VALID_VERSION(T.object_id)) 
                    FROM sys.tables T 
                    WHERE CHANGE_TRACKING_MIN_VALID_VERSION(T.object_id) is not null;

                    MERGE {this.ScopeInfoTableNames.QuotedFullName} WITH (READCOMMITTED) AS [base] 
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
	                    VALUES ([changes].[sync_scope_name], [changes].[sync_scope_schema], [changes].[sync_scope_setup], [changes].[sync_scope_version], @maxVersion, [changes].[sync_scope_properties])
                    WHEN MATCHED THEN
	                    UPDATE SET [sync_scope_name] = [changes].[sync_scope_name], 
                                   [sync_scope_schema] = [changes].[sync_scope_schema], 
                                   [sync_scope_setup] = [changes].[sync_scope_setup], 
                                   [sync_scope_version] = [changes].[sync_scope_version],
                                   [sync_scope_last_clean_timestamp] = @maxVersion,
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

        /// <inheritdoc />
        public override DbCommand GetAllScopeInfosCommand(DbConnection connection, DbTransaction transaction)
        {

            // The last clean timestamp is the max version of the change tracking.
            // This value is maintained by SQL Server itself
            var commandText =
                $@" DECLARE @maxVersion bigint;
                    SELECT @maxVersion = MAX(CHANGE_TRACKING_MIN_VALID_VERSION(T.object_id)) 
                    FROM sys.tables T 
                    WHERE CHANGE_TRACKING_MIN_VALID_VERSION(T.object_id) is not null;

                    SELECT [sync_scope_name], 
                          [sync_scope_schema], 
                          [sync_scope_setup], 
                          [sync_scope_version],
                          @maxVersion as [sync_scope_last_clean_timestamp],
                          [sync_scope_properties]
                    FROM  {this.ScopeInfoTableNames.QuotedFullName}";

            var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = commandText;

            return command;
        }

        /// <inheritdoc />
        public override DbCommand GetScopeInfoCommand(DbConnection connection, DbTransaction transaction)
        {

            // The last clean timestamp is the max version of the change tracking.
            // This value is maintained by SQL Server itself
            var commandText =
                    $@"
                    DECLARE @maxVersion bigint;
                    SELECT @maxVersion = MAX(CHANGE_TRACKING_MIN_VALID_VERSION(T.object_id)) 
                    FROM sys.tables T 
                    WHERE CHANGE_TRACKING_MIN_VALID_VERSION(T.object_id) is not null;

                    SELECT [sync_scope_name], 
                          [sync_scope_schema], 
                          [sync_scope_setup], 
                          [sync_scope_version],
                          @maxVersion as [sync_scope_last_clean_timestamp],
                          [sync_scope_properties]
                    FROM  {this.ScopeInfoTableNames.QuotedFullName}
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
    }
}