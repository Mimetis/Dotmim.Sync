using Dotmim.Sync.Core.Scope;
using System;
using System.Collections.Generic;
using System.Text;
using System.Data.Common;
using Dotmim.Sync.SqlServer.Manager;
using System.Data.SqlClient;
using System.Data;
using Dotmim.Sync.Core.Log;

namespace Dotmim.Sync.SqlServer.Scope
{
    public class SqlScopeInfoBuilder : IDbScopeInfoBuilder
    {


        private SqlConnection connection;
        private SqlTransaction transaction;

        public SqlScopeInfoBuilder(DbConnection connection, DbTransaction transaction = null)
        {
            this.connection = connection as SqlConnection;
            this.transaction = transaction as SqlTransaction;
        }



        public void CreateScopeInfoTable()
        {
            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                command.CommandText =
                    @"CREATE TABLE [dbo].[scope_info](
                        [sync_scope_id] [uniqueidentifier] NOT NULL,
	                    [sync_scope_name] [nvarchar](100) NOT NULL,
	                    [scope_timestamp] [timestamp] NULL,
                        [scope_is_local] [bit] NOT NULL DEFAULT(0) -- , [scope_database_created] [bit] NOT NULL DEFAULT(0)
                        CONSTRAINT [PK_scope_info] PRIMARY KEY CLUSTERED ([sync_scope_id] ASC)
                        )";
                command.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                Logger.Current.Error($"Error during CreateTableScope : {ex}");
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


        public List<ScopeInfo> GetAllScopes(string scopeName)
        {
            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;

            bool alreadyOpened = connection.State == ConnectionState.Open;

            List<ScopeInfo> scopes = new List<ScopeInfo>();
            try
            {
                if (!alreadyOpened)
                    connection.Open();

                command.CommandText =
                    @"SELECT [sync_scope_id]
                           , [sync_scope_name]
                           , [scope_timestamp]
                           , [scope_is_local] -- , [scope_database_created]
                    FROM  [scope_info] 
                    WHERE [sync_scope_name] = @sync_scope_name";

                var p = command.CreateParameter();
                p.ParameterName = "@sync_scope_name";
                p.Value = scopeName;
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                using (DbDataReader reader = command.ExecuteReader())
                {
                    // read only the first one
                    while (reader.Read())
                    {
                        ScopeInfo scopeInfo = new ScopeInfo();
                        scopeInfo.Name = reader["sync_scope_name"] as String;
                        scopeInfo.Id = (Guid)reader["sync_scope_id"];
                        scopeInfo.LastTimestamp = SqlManager.ParseTimestamp(reader["scope_timestamp"]);
                        //scopeInfo.IsDatabaseCreated = (bool)reader["scope_database_created"];
                        scopeInfo.IsLocal = (bool)reader["scope_is_local"];
                        scopes.Add(scopeInfo);
                    }
                }

                return scopes;
            }
            catch (Exception ex)
            {
                Logger.Current.Error($"Error during GetAllScopes : {ex}");
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

        public long GetLocalTimestamp()
        {
            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;

            bool alreadyOpened = connection.State == ConnectionState.Open;
            try
            {
                command.CommandText = "SELECT @sync_new_timestamp = min_active_rowversion() - 1";
                DbParameter p = command.CreateParameter();
                p.ParameterName = "@sync_new_timestamp";
                p.DbType = DbType.Int64;
                p.Direction = ParameterDirection.Output;
                command.Parameters.Add(p);

                if (!alreadyOpened)
                    connection.Open();

                command.ExecuteNonQuery();

                var outputParameter = SqlManager.GetParameter(command, "sync_new_timestamp");

                if (outputParameter == null)
                    return 0L;

                long result = 0L;

                long.TryParse(outputParameter.Value.ToString(), out result);

                return result;
            }
            catch (Exception ex)
            {
                Logger.Current.Error($"Error during GetLocalTimestamp : {ex}");
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

        public ScopeInfo InsertOrUpdateScopeInfo(ScopeInfo scopeInfo)
        {
            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                command.CommandText = @"
                    MERGE [scope_info] AS [base] 
                    USING (
	                           SELECT  @sync_scope_name AS sync_scope_name,  
                                       @sync_scope_id AS sync_scope_id,  
                                       @scope_is_local as scope_is_local --,@scope_database_created AS scope_database_created
                           ) AS [changes] 
                    ON [base].[sync_scope_name] = [changes].[sync_scope_name] AND [base].[sync_scope_id] = [changes].[sync_scope_id]
                    WHEN NOT MATCHED THEN
	                    INSERT ([sync_scope_name], [sync_scope_id], [scope_is_local]) --, [scope_database_created])
	                    VALUES ([changes].[sync_scope_name], [changes].[sync_scope_id], [changes].[scope_is_local]) --, [changes].[scope_database_created])
                    WHEN MATCHED THEN
	                    UPDATE SET [sync_scope_name] = [changes].[sync_scope_name], 
                                   [sync_scope_id] = [changes].[sync_scope_id], 
                                   [scope_is_local] = [changes].[scope_is_local] --, [scope_database_created] = [changes].[scope_database_created]
                    OUTPUT  INSERTED.[sync_scope_name], 
                            INSERTED.[sync_scope_id], 
                            INSERTED.[scope_timestamp], 
                            INSERTED.[scope_is_local]; --,INSERTED.[scope_database_created];
                ";

                var p = command.CreateParameter();
                p.ParameterName = "@sync_scope_name";
                p.Value = scopeInfo.Name;
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                p = command.CreateParameter();
                p.ParameterName = "@sync_scope_id";
                p.Value = scopeInfo.Id;
                p.DbType = DbType.Guid;
                command.Parameters.Add(p);

                p = command.CreateParameter();
                p.ParameterName = "@scope_is_local";
                p.Value = scopeInfo.IsLocal;
                p.DbType = DbType.Boolean;
                command.Parameters.Add(p);

                //p = command.CreateParameter();
                //p.ParameterName = "@scope_database_created";
                //p.Value = scopeInfo.IsDatabaseCreated;
                //p.DbType = DbType.Boolean;
                //command.Parameters.Add(p);
             
                
                using (DbDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        scopeInfo.Name = reader["sync_scope_name"] as String;
                        scopeInfo.Id = (Guid)reader["sync_scope_Id"];
                        scopeInfo.LastTimestamp = SqlManager.ParseTimestamp(reader["scope_timestamp"]);
                        scopeInfo.IsLocal = (bool)reader["scope_is_local"];
                        //scopeInfo.IsDatabaseCreated = (bool)reader["scope_database_created"];
                    }
                }

                return scopeInfo;
            }
            catch (Exception ex)
            {
                Logger.Current.Error($"Error during CreateTableScope : {ex}");
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


        public bool NeedToCreateScopeInfoTable()
        {
            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                command.CommandText =
                    @"IF EXISTS (SELECT t.name FROM sys.tables t 
                            JOIN sys.schemas s ON s.schema_id = t.schema_id 
                            WHERE t.name = N'scope_info')
                     SELECT 1 
                     ELSE
                     SELECT 0";

                return (int)command.ExecuteScalar() != 1;

            }
            catch (Exception ex)
            {
                Logger.Current.Error($"Error during NeedToCreateScopeInfoTable command : {ex}");
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
