using Dotmim.Sync.Core.Common;
using Dotmim.Sync.Core.Log;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Core.Scope
{
    internal class ScopeFactory : IDisposable
    {
        bool alreadyOpened;
        public DbConnection Connection { get; private set; }
        public DbTransaction Transaction { get; private set; }

        public ScopeFactory(DbConnection connection, DbTransaction transaction = null)
        {
            this.Connection = connection;
            this.Transaction = transaction;
            this.alreadyOpened = this.Connection.State == ConnectionState.Open;
        }

        //-----------------------------------------------------------------------
        // Scope Config
        //-----------------------------------------------------------------------

        public ScopeConfig ReadScopeConfig(Guid configId)
        {
            var command = this.Connection.CreateCommand();
            try
            {
                if (!alreadyOpened)
                    this.Connection.Open();

                if (this.Transaction != null)
                    command.Transaction = this.Transaction;

                command.CommandText = GetScopeConfigSelectCommand();

                var p = command.CreateParameter();
                p.ParameterName = "@config_id";
                p.Value = configId;
                command.Parameters.Add(p);

                ScopeConfig scopeConfig = new ScopeConfig();

                using (DbDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        scopeConfig.ConfigId = (Guid)reader["config_id"];
                        scopeConfig.ConfigData = reader["config_data"] as string;
                        scopeConfig.ConfigStatus = reader["scope_status"] as string;
                    }
                }

                return scopeConfig;
            }
            catch (Exception ex)
            {
                Logger.Current.Error($"Error during ReadScope : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && this.Connection.State != ConnectionState.Closed)
                    this.Connection.Close();

                if (command != null)
                    command.Dispose();
            }
        }
        public void CreateScopeConfigTable()
        {
            var command = this.Connection.CreateCommand();
            if (this.Transaction != null)
                command.Transaction = this.Transaction;

            try
            {
                if (!alreadyOpened)
                    this.Connection.Open();

                command.CommandText = GetScopeConfigCreateTableCommand();
                command.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                Logger.Current.Error($"Error during CreateTableScope : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && this.Connection.State != ConnectionState.Closed)
                    this.Connection.Close();

                if (command != null)
                    command.Dispose();
            }
        }
        public void UpdateScopeConfig(ScopeConfig config)
        {
            var command = this.Connection.CreateCommand();
            if (this.Transaction != null)
                command.Transaction = this.Transaction;


            try
            {
                if (!alreadyOpened)
                    this.Connection.Open();

                command.CommandText = GetScopeConfigUpdateCommand();

                var p = command.CreateParameter();
                p.ParameterName = "@config_data";
                p.Value = config.ConfigData;
                p.DbType = DbType.Xml;
                command.Parameters.Add(p);

                p = command.CreateParameter();
                p.ParameterName = "@scope_status";
                p.DbType = DbType.String;
                p.Value = config.ConfigStatus;
                command.Parameters.Add(p);

                p = command.CreateParameter();
                p.ParameterName = "@config_id";
                p.DbType = DbType.Guid;
                p.Value = config.ConfigId;
                command.Parameters.Add(p);

                command.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                Logger.Current.Error($"Error during UpdateScopeConfig : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && this.Connection.State != ConnectionState.Closed)
                    this.Connection.Close();

                if (command != null)
                    command.Dispose();
            }
        }
        public void DeleteScopeConfig(Guid configId)
        {
            var command = this.Connection.CreateCommand();
            if (this.Transaction != null)
                command.Transaction = this.Transaction;


            try
            {
                if (!alreadyOpened)
                    this.Connection.Open();

                command.CommandText = GetScopeConfigDeleteCommand();

                var p = command.CreateParameter();
                p.ParameterName = "@config_id";
                p.DbType = DbType.Guid;
                p.Value = configId;
                command.Parameters.Add(p);

                command.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                Logger.Current.Error($"Error during DeleteScopeConfig : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && this.Connection.State != ConnectionState.Closed)
                    this.Connection.Close();

                if (command != null)
                    command.Dispose();
            }
        }

        internal void InsertScopeConfig(ScopeConfig config)
        {
            var command = this.Connection.CreateCommand();
            if (this.Transaction != null)
                command.Transaction = this.Transaction;


            try
            {
                if (!alreadyOpened)
                    this.Connection.Open();

                command.CommandText = GetScopeConfigInsertCommand();

                var p = command.CreateParameter();
                p.ParameterName = "@config_data";
                p.Value = config.ConfigData;
                p.DbType = DbType.Xml;
                command.Parameters.Add(p);

                p = command.CreateParameter();
                p.ParameterName = "@scope_status";
                p.DbType = DbType.String;
                p.Value = config.ConfigStatus;
                command.Parameters.Add(p);

                p = command.CreateParameter();
                p.ParameterName = "@config_id";
                p.DbType = DbType.Guid;
                p.Value = config.ConfigId;
                command.Parameters.Add(p);

                command.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                Logger.Current.Error($"Error during UpdateScopeConfig : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && this.Connection.State != ConnectionState.Closed)
                    this.Connection.Close();

                if (command != null)
                    command.Dispose();
            }
        }

        public bool IsScopeConfigProvisionned(string scopeName)
        {
            var command = this.Connection.CreateCommand();
            if (this.Transaction != null)
                command.Transaction = this.Transaction;

            try
            {
                if (!alreadyOpened)
                    this.Connection.Open();

                command.CommandText = GetScopeConfigExistCommand();

                var p = command.CreateParameter();
                p.ParameterName = "@sync_scope_name";
                p.DbType = DbType.String;
                p.Value = scopeName;
                command.Parameters.Add(p);

                return (int)command.ExecuteScalar() == 1;

            }
            catch (Exception ex)
            {
                Logger.Current.Error($"Error during IsScopeConfigProvisionned : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && this.Connection.State != ConnectionState.Closed)
                    this.Connection.Close();

                if (command != null)
                    command.Dispose();
            }
        }
        public bool IsScopeExist(string scopeName)
        {
            var command = this.Connection.CreateCommand();
            if (this.Transaction != null)
                command.Transaction = this.Transaction;

            try
            {
                if (!alreadyOpened)
                    this.Connection.Open();

                command.CommandText = GetScopeExistCommand();

                var p = command.CreateParameter();
                p.ParameterName = "@sync_scope_name";
                p.DbType = DbType.String;
                p.Value = scopeName;
                command.Parameters.Add(p);

                return (int)command.ExecuteScalar() == 1;

            }
            catch (Exception ex)
            {
                Logger.Current.Error($"Error during IsScopeExist command : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && this.Connection.State != ConnectionState.Closed)
                    this.Connection.Close();

                if (command != null)
                    command.Dispose();
            }
        }

        //-----------------------------------------------------------------------
        // Scope Info
        //-----------------------------------------------------------------------


        public ScopeInfo ReadFirstScopeInfo()
        {
            var command = this.Connection.CreateCommand();
            if (this.Transaction != null)
                command.Transaction = this.Transaction;

            try
            {
                if (!alreadyOpened)
                    this.Connection.Open();

                command.CommandText = GetScopesInfoSelectCommand();

                using (DbDataReader reader = command.ExecuteReader())
                {
                    // read only the first one
                    if (reader.Read())
                    {
                        ScopeInfo scopeInfo = new ScopeInfo();
                        scopeInfo.Name = reader["sync_scope_name"] as String;
                        scopeInfo.LastTimestamp = DbHelper.ParseTimestamp(reader["scope_timestamp"]);
                        scopeInfo.ConfigId = reader["scope_config_id"] == DBNull.Value ? Guid.Empty : (Guid)reader["scope_config_id"];
                        scopeInfo.UserComment = reader["scope_user_comment"] as string;
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
                if (!alreadyOpened && this.Connection.State != ConnectionState.Closed)
                    this.Connection.Close();

                if (command != null)
                    command.Dispose();
            }

        }

        public ScopeInfo ReadScopeInfo(string scopeName)
        {
            var command = this.Connection.CreateCommand();
            if (this.Transaction != null)
                command.Transaction = this.Transaction;

            try
            {


                if (!alreadyOpened)
                    this.Connection.Open();

                command.CommandText = GetScopeInfoSelectCommand();
                var p = command.CreateParameter();
                p.ParameterName = "@sync_scope_name";
                p.Value = scopeName;
                command.Parameters.Add(p);

                ScopeInfo scopeInfo = new ScopeInfo();
                using (DbDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        scopeInfo.Name = reader["sync_scope_name"] as String;
                        scopeInfo.LastTimestamp = DbHelper.ParseTimestamp(reader["scope_timestamp"]);
                        scopeInfo.ConfigId = reader["scope_config_id"] == DBNull.Value ? Guid.Empty : (Guid)reader["scope_config_id"];
                        scopeInfo.UserComment = reader["scope_user_comment"] as string;
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
                if (!alreadyOpened && this.Connection.State != ConnectionState.Closed)
                    this.Connection.Close();

                if (command != null)
                    command.Dispose();
            }

        }

        /// <summary>
        /// Insert a new scope for a new remote client
        /// </summary>
        public ScopeInfo InsertScopeInfo(string scopeName, Guid configId, string comment = null)
        {
            var command = this.Connection.CreateCommand();
            if (this.Transaction != null)
                command.Transaction = this.Transaction;

            try
            {
                if (!alreadyOpened)
                    this.Connection.Open();

                command.CommandText = GetScopeInfoInsertCommand();

                var p = command.CreateParameter();
                p.ParameterName = "@sync_scope_name";
                p.Value = scopeName;
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                p = command.CreateParameter();
                p.ParameterName = "@scope_config_id";
                p.Value = configId;
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
                        scopeInfo.LastTimestamp = DbHelper.ParseTimestamp(reader["scope_timestamp"]);
                        scopeInfo.ConfigId = reader["scope_config_id"] == DBNull.Value ? Guid.Empty : (Guid)reader["scope_config_id"];
                        scopeInfo.UserComment = reader["scope_user_comment"] as string;
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
                if (!alreadyOpened && this.Connection.State != ConnectionState.Closed)
                    this.Connection.Close();

                if (command != null)
                    command.Dispose();
            }
        }

        /// <summary>
        /// Update a scope info
        /// </summary>
        public ScopeInfo UpdateScopeInfo(string scopeName)
        {
            var command = this.Connection.CreateCommand();
            if (this.Transaction != null)
                command.Transaction = this.Transaction;

            try
            {
                if (!alreadyOpened)
                    this.Connection.Open();

                command.CommandText = GetScopeInfoUpdateCommand();

                var p = command.CreateParameter();
                p.ParameterName = "@sync_scope_name";
                p.Value = scopeName;
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                ScopeInfo scopeInfo = new ScopeInfo();
                using (DbDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        scopeInfo.Name = reader["sync_scope_name"] as String;
                        scopeInfo.LastTimestamp = DbHelper.ParseTimestamp(reader["scope_timestamp"]);
                        scopeInfo.ConfigId = reader["scope_config_id"] == DBNull.Value ? Guid.Empty : (Guid)reader["scope_config_id"];
                        scopeInfo.UserComment = reader["scope_user_comment"] as string;
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
                if (!alreadyOpened && this.Connection.State != ConnectionState.Closed)
                    this.Connection.Close();

                if (command != null)
                    command.Dispose();
            }
        }


        //-----------------------------------------------------------------------
        // Timestamp
        //-----------------------------------------------------------------------


        public long GetLocalTimestamp()
        {

            var command = this.Connection.CreateCommand();
            if (this.Transaction != null)
                command.Transaction = this.Transaction;

            try
            {
                command.CommandText = "SELECT @sync_new_timestamp = min_active_rowversion() - 1";
                DbParameter p = command.CreateParameter();
                p.ParameterName = "@sync_new_timestamp";
                p.DbType = DbType.Int64;
                p.Direction = ParameterDirection.Output;
                command.Parameters.Add(p);

                if (!alreadyOpened)
                    this.Connection.Open();

                command.ExecuteNonQuery();

                var outputParameter = DbHelper.GetParameter(command, "sync_new_timestamp");

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
                if (!alreadyOpened && this.Connection.State != ConnectionState.Closed)
                    this.Connection.Close();

                if (command != null)
                    command.Dispose();
            }
        }

        public long GetTimestampForConflictResolution()
        {
            return GetLocalTimestamp() + (long)1;
        }


        //-----------------------------------------------------------------------
        // Scope Template
        //-----------------------------------------------------------------------
        public bool IsScopeTemplateExist(string templateName)
        {
            var command = this.Connection.CreateCommand();
            if (this.Transaction != null)
                command.Transaction = this.Transaction;

            try
            {
                if (!alreadyOpened)
                    this.Connection.Open();

                command.CommandText = GetScopeTemplateExistCommand();

                var p = command.CreateParameter();
                p.ParameterName = "@template_name";
                p.DbType = DbType.String;
                p.Value = templateName;
                command.Parameters.Add(p);

                return (int)command.ExecuteScalar() == 1;

            }
            catch (Exception ex)
            {
                Logger.Current.Error($"Error during IsScopeConfigProvisionned : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && this.Connection.State != ConnectionState.Closed)
                    this.Connection.Close();

                if (command != null)
                    command.Dispose();
            }
        }


        // --------------------------------------------------------------------
        // Dispose
        // --------------------------------------------------------------------

        /// <summary>
        /// Releases all resources used by the <see cref="T:Microsoft.Synchronization.Data.DbSyncBatchInfo" />.
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Releases the unmanaged resources used 
        /// by the <see cref="T:Microsoft.Synchronization.Data.DbSyncBatchInfo" /> and optionally releases the managed resources.
        /// </summary>
        protected virtual void Dispose(bool cleanup)
        {
            if (alreadyOpened)
                return;

            if (this.Transaction != null)
            {
                this.Transaction.Dispose();
            }
            if (this.Connection != null && this.Connection.State != ConnectionState.Closed)
            {
                this.Connection.Close();
            }

        }

        string GetScopeConfigInsertCommand()
        {
            return @"INSERT INTO [scope_config] ([config_id], [config_data], [scope_status]) 
                     VALUES (@config_id, @config_data, @scope_status)";
        }

        string GetScopeConfigUpdateCommand()
        {
            return @"UPDATE [scope_config]
                        SET [config_data] = @config_data
                           ,[scope_status] = @scope_status
                      WHERE [config_id] = @config_id";
        }
        string GetScopeConfigExistCommand()
        {
            return @"IF EXISTS (SELECT t.name FROM sys.tables t 
                            JOIN sys.schemas s ON s.schema_id = t.schema_id 
                            WHERE t.name = N'scope_config')
                     BEGIN
                        IF EXISTS (SELECT si.[sync_scope_name]
                                    FROM [scope_info] si INNER JOIN [scope_config] sc on si.[scope_config_id] = sc.[config_id]
                                    where si.[sync_scope_name] = @sync_scope_name)
                        SELECT 1 
                        ELSE
                        SELECT 0
                     END
                     ELSE SELECT 0";
        }
        string GetScopeConfigCreateTableCommand()
        {

            return @"IF NOT EXISTS (SELECT t.name FROM sys.tables t 
                               JOIN sys.schemas s ON s.schema_id = t.schema_id 
                               WHERE t.name = N'scope_config')
                               BEGIN
                                CREATE TABLE [scope_config](
	                            [config_id] [uniqueidentifier] NOT NULL,
	                            [config_data] [xml] NOT NULL,
	                            [scope_status] [char](1) NULL,
                                CONSTRAINT [PK_scope_config] PRIMARY KEY CLUSTERED ( [config_id] ASC )
                               END";

        }
        string GetScopeConfigSelectCommand()
        {
            return @"SELECT config_id, config_data, scope_status FROM scope_config WHERE config_id = @config_id";
        }
        string GetScopeConfigDeleteCommand()
        {
            return @"DELETE FROM scope_config WHERE config_id = @config_id";
        }
        string GetScopeInfoSelectCommand()
        {
            return @"SELECT [sync_scope_name]
                           ,[scope_timestamp]
                           ,[scope_config_id]
                           ,[scope_user_comment]
                  FROM  [scope_info]
                  WHERE [sync_scope_name] = @sync_scope_name";
        }

        string GetScopesInfoSelectCommand()
        {
            return @"SELECT [sync_scope_name]
                           ,[scope_timestamp]
                           ,[scope_config_id]
                           ,[scope_user_comment]
                  FROM  [scope_info]";
        }
        string GetScopeTemplateExistCommand()
        {
            return @"IF EXISTS (SELECT t.name FROM sys.tables t 
                            JOIN sys.schemas s ON s.schema_id = t.schema_id 
                            WHERE t.name = N'scope_templates')
                     BEGIN
                        IF (SELECT count(*)
                                    FROM [scope_templates]
                                    where [template_name] = @template_name) > 0
                        SELECT 1 
                        ELSE
                        SELECT 0
                     END
                     ELSE SELECT 0";
        }
        string GetScopeExistCommand()
        {
            return @"IF EXISTS (SELECT t.name FROM sys.tables t 
                            JOIN sys.schemas s ON s.schema_id = t.schema_id 
                            WHERE t.name = N'scope_info')
                     BEGIN
                        IF (SELECT count(*)
                                    FROM [scope_info]
                                    where [sync_scope_name] = @sync_scope_name) > 0
                        SELECT 1 
                        ELSE
                        SELECT 0
                     END
                     ELSE SELECT 0";
        }

        string GetScopeInfoInsertCommand()
        {

            return @"INSERT INTO [scope_info] ([sync_scope_name] , [scope_config_id], [scope_user_comment]) 
                     VALUES (@sync_scope_name, @scope_config_id, @scope_user_comment);
                     SELECT [sync_scope_name], [scope_timestamp] ,[scope_config_id] ,[scope_user_comment]
                     FROM  [scope_info]
                     WHERE [sync_scope_name] = @sync_scope_name";
        }


        string GetScopeInfoUpdateCommand()
        {

            return @"UPDATE [scope_info] 
                     SET [sync_scope_name] = @sync_scope_name
                     WHERE [sync_scope_name] = @sync_scope_name;
                     SELECT [sync_scope_name] ,[scope_timestamp] ,[scope_config_id], [scope_user_comment]
                     FROM  [scope_info]
                     WHERE [sync_scope_name] = @sync_scope_name;";
        }
    }
}
