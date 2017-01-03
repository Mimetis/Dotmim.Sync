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
	                    [scope_config_id] [uniqueidentifier] NULL,
	                    [scope_user_comment] [nvarchar](max) NULL,
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
                           ,[scope_config_id]
                           ,[scope_user_comment]
                    FROM  [scope_info]";

                using (DbDataReader reader = command.ExecuteReader())
                {

                    // read only the first one
                    while (reader.Read())
                    {
                        ScopeInfo scopeInfo = new ScopeInfo();
                        scopeInfo.Name = reader["sync_scope_name"] as String;
                        scopeInfo.LastTimestamp = SqlManager.ParseTimestamp(reader["scope_timestamp"]);
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

        public ScopeInfo InsertOrUpdateScopeInfo(string scopeName, Guid? configId = default(Guid?), string comment = null)
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
                    IF (SELECT count(*) FROM [scope_info] WHERE [sync_scope_name] = @sync_scope_name) > 0
                    BEGIN
                     UPDATE [scope_info]     
                     SET [sync_scope_name] = @sync_scope_name
                     WHERE [sync_scope_name] = @sync_scope_name;
                     SELECT [sync_scope_name] ,[scope_timestamp] ,[scope_config_id], [scope_user_comment]
                     FROM  [scope_info]
                     WHERE [sync_scope_name] = @sync_scope_name;                    END
                    ELSE
                    BEGIN
                     INSERT INTO [scope_info] ([sync_scope_name] , [scope_config_id], [scope_user_comment]) 
                     VALUES (@sync_scope_name, @scope_config_id, @scope_user_comment);
                     SELECT [sync_scope_name], [scope_timestamp] ,[scope_config_id] ,[scope_user_comment]
                     FROM  [scope_info]
                     WHERE [sync_scope_name] = @sync_scope_name
                    END";

                var p = command.CreateParameter();
                p.ParameterName = "@sync_scope_name";
                p.Value = scopeName;
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                p = command.CreateParameter();
                p.ParameterName = "@scope_config_id";
                p.Value = !configId.HasValue ? (object)DBNull.Value : configId.Value;
                p.DbType = DbType.Guid;
                command.Parameters.Add(p);

                p = command.CreateParameter();
                p.ParameterName = "@scope_user_comment";
                p.Value = string.IsNullOrEmpty(comment) ? (object)DBNull.Value : comment;
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                ScopeInfo scopeInfo = new ScopeInfo();
                using (DbDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        scopeInfo.Name = reader["sync_scope_name"] as String;
                        scopeInfo.LastTimestamp = SqlManager.ParseTimestamp(reader["scope_timestamp"]);
                        //scopeInfo.ConfigId = reader["scope_config_id"] == DBNull.Value ? Guid.Empty : (Guid)reader["scope_config_id"];
                        //scopeInfo.UserComment = reader["scope_user_comment"] as string;
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
                           ,[scope_config_id]
                           ,[scope_user_comment]
                    FROM  [scope_info]";

                using (DbDataReader reader = command.ExecuteReader())
                {
                    // read only the first one
                    if (reader.Read())
                    {
                        ScopeInfo scopeInfo = new ScopeInfo();
                        scopeInfo.Name = reader["sync_scope_name"] as String;
                        scopeInfo.LastTimestamp = SqlManager.ParseTimestamp(reader["scope_timestamp"]);
                        //scopeInfo.ConfigId = reader["scope_config_id"] == DBNull.Value ? Guid.Empty : (Guid)reader["scope_config_id"];
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
                           ,[scope_config_id]
                           ,[scope_user_comment]
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
                        //scopeInfo.ConfigId = reader["scope_config_id"] == DBNull.Value ? Guid.Empty : (Guid)reader["scope_config_id"];
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
