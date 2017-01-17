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
	                    [sync_scope_name] [nvarchar](100) NOT NULL,
	                    [scope_timestamp] [timestamp] NULL,
	                    [scope_database_created] [bit] NOT NULL DEFAULT(0)
                        CONSTRAINT [PK_scope_info] PRIMARY KEY CLUSTERED ([sync_scope_name] ASC)
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


        public List<ScopeInfo> GetAllScopes()
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
                    @"SELECT [sync_scope_name]
                           ,[scope_timestamp]
                           ,[scope_database_created]
                    FROM  [scope_info]";

                using (DbDataReader reader = command.ExecuteReader())
                {

                    // read only the first one
                    while (reader.Read())
                    {
                        ScopeInfo scopeInfo = new ScopeInfo();
                        scopeInfo.Name = reader["sync_scope_name"] as String;
                        scopeInfo.LastTimestamp = SqlManager.ParseTimestamp(reader["scope_timestamp"]);
                        scopeInfo.IsDatabaseCreated = (bool)reader["scope_database_created"];
                        scopes.Add(scopeInfo);
                    }
                }

                return scopes;
            }
            catch (Exception ex)
            {
                Logger.Current.Error($"Error during ReadScopeInfo : {ex}");
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

        public ScopeInfo InsertOrUpdateScopeInfo(string scopeName, bool? isDatabaseCreated = null)
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
                    MERGE [scope_info] AS [base] USING
                    (
	                    SELECT T.[sync_scope_name],
		                    CASE WHEN @scope_database_created IS NULL 
		                    THEN S.[scope_database_created] 
		                    ELSE T.[scope_database_created] 
		                    END AS [scope_database_created]
	                    FROM [scope_info] S
	                    RIGHT JOIN (SELECT @sync_scope_name AS sync_scope_name, @scope_database_created AS scope_database_created) AS T ON T.sync_scope_name = S.sync_scope_name
	                    WHERE T.[sync_scope_name] = @sync_scope_name
                    ) 
                    AS [changes] ON [base].[sync_scope_name] = [changes].[sync_scope_name]
                    WHEN NOT MATCHED THEN
	                    INSERT ([sync_scope_name])
	                    VALUES ([changes].[sync_scope_name])
                    WHEN MATCHED THEN
	                    UPDATE SET [sync_scope_name] = [changes].[sync_scope_name], [scope_database_created] = [changes].[scope_database_created]
                    OUTPUT INSERTED.[sync_scope_name], INSERTED.[scope_timestamp], INSERTED.[scope_database_created];
                ";

                var p = command.CreateParameter();
                p.ParameterName = "@sync_scope_name";
                p.Value = scopeName;
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                p = command.CreateParameter();
                p.ParameterName = "@scope_database_created";
                p.Value = isDatabaseCreated.HasValue ? (object)isDatabaseCreated : DBNull.Value ;
                p.DbType = DbType.Boolean;
                command.Parameters.Add(p);
             

                ScopeInfo scopeInfo = new ScopeInfo();
                using (DbDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        scopeInfo.Name = reader["sync_scope_name"] as String;
                        scopeInfo.LastTimestamp = SqlManager.ParseTimestamp(reader["scope_timestamp"]);
                        scopeInfo.IsDatabaseCreated = (bool)reader["scope_database_created"];
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

        public ScopeInfo ReadFirstScopeInfo()
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
                    @"SELECT [sync_scope_name]
                           ,[scope_timestamp]
                           ,[scope_database_created]
                    FROM  [scope_info]";

                using (DbDataReader reader = command.ExecuteReader())
                {
                    // read only the first one
                    if (reader.Read())
                    {
                        ScopeInfo scopeInfo = new ScopeInfo();
                        scopeInfo.Name = reader["sync_scope_name"] as String;
                        scopeInfo.LastTimestamp = SqlManager.ParseTimestamp(reader["scope_timestamp"]);
                        scopeInfo.IsDatabaseCreated = (bool)reader["scope_database_created"];
                        //scopeInfo.UserComment = reader["scope_user_comment"] as string;
                        return scopeInfo;
                    }
                }

                return null;
            }
            catch (Exception ex)
            {
                Logger.Current.Error($"Error during ReadScopeInfo : {ex}");
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


        public ScopeInfo ReadScopeInfo(string scopeName)
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
                    @"SELECT [sync_scope_name]
                           ,[scope_timestamp]
                           ,[scope_database_created]
                  FROM  [scope_info]
                  WHERE [sync_scope_name] = @sync_scope_name";

                var p = command.CreateParameter();
                p.ParameterName = "@sync_scope_name";
                p.Value = scopeName;
                command.Parameters.Add(p);

                ScopeInfo scopeInfo = new ScopeInfo();
                using (DbDataReader reader = command.ExecuteReader())
                {
                    if (!reader.HasRows)
                        return null;

                    while (reader.Read())
                    {
                        scopeInfo.Name = reader["sync_scope_name"] as String;
                        scopeInfo.LastTimestamp = SqlManager.ParseTimestamp(reader["scope_timestamp"]);
                        scopeInfo.IsDatabaseCreated = (bool)reader["scope_database_created"];
                        //scopeInfo.UserComment = reader["scope_user_comment"] as string;
                    }
                }
                return scopeInfo;
            }
            catch (Exception ex)
            {
                Logger.Current.Error($"Error during ReadScopeInfo : {ex}");
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
