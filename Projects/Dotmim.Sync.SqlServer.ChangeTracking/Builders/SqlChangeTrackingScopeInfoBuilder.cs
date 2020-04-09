using Dotmim.Sync.SqlServer.Manager;
using Dotmim.Sync.SqlServer.Scope;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.SqlServer.ChangeTracking.Builders
{
    public class SqlChangeTrackingScopeInfoBuilder : SqlScopeInfoBuilder
    {
        public SqlChangeTrackingScopeInfoBuilder(string scopeTableName, DbConnection connection, DbTransaction transaction = null) 
            : base(scopeTableName, connection, transaction)
        {
        }

        public override async Task<long> GetLocalTimestampAsync()
        {
            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;

            bool alreadyOpened = connection.State == ConnectionState.Open;
            try
            {
                command.CommandText = "SELECT @sync_new_timestamp = CHANGE_TRACKING_CURRENT_VERSION();";
                DbParameter p = command.CreateParameter();
                p.ParameterName = "@sync_new_timestamp";
                p.DbType = DbType.Int64;
                p.Direction = ParameterDirection.Output;
                command.Parameters.Add(p);

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                await command.ExecuteNonQueryAsync().ConfigureAwait(false);

                var outputParameter = SqlManager.GetParameter(command, "sync_new_timestamp");

                if (outputParameter == null)
                    return 0L;

                long result = 0L;

                long.TryParse(outputParameter.Value.ToString(), out result);

                command.Dispose();

                return Math.Max(result, 0);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during GetLocalTimestamp : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

                if (command != null)
                    command.Dispose();
            }
        }


        public override async Task<ServerScopeInfo> InsertOrUpdateServerScopeInfoAsync(ServerScopeInfo serverScopeInfo)
        {
            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                var tableName = $"{scopeTableName.Unquoted().Normalized()}_server";

                command.CommandText = $@"
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

                var p = command.CreateParameter();
                p.ParameterName = "@sync_scope_name";
                p.Value = serverScopeInfo.Name;
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                p = command.CreateParameter();
                p.ParameterName = "@sync_scope_schema";
                p.Value = string.IsNullOrEmpty(serverScopeInfo.Schema) ? DBNull.Value : (object)serverScopeInfo.Schema;
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                p = command.CreateParameter();
                p.ParameterName = "@sync_scope_setup";
                p.Value = string.IsNullOrEmpty(serverScopeInfo.Setup) ? DBNull.Value : (object)serverScopeInfo.Setup;
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


                using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            serverScopeInfo.Name = reader["sync_scope_name"] as string;
                            serverScopeInfo.Schema = reader["sync_scope_schema"] as string;
                            serverScopeInfo.Setup = reader["sync_scope_setup"] as string;
                            serverScopeInfo.Version = reader["sync_scope_version"] as string;
                            serverScopeInfo.LastCleanupTimestamp = reader["sync_scope_last_clean_timestamp"] != DBNull.Value ? (long)reader["sync_scope_last_clean_timestamp"] : 0;
                        }
                    }
                }

                return serverScopeInfo;
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during CreateTableScope : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

                if (command != null)
                    command.Dispose();
            }
        }

    }
}
