using Dotmim.Sync.Core.Scope;
using System;
using System.Collections.Generic;
using System.Text;
using System.Data.Common;
using System.Data.SqlClient;
using System.Data;
using Dotmim.Sync.Core.Log;

namespace Dotmim.Sync.SqlServer.Scope
{
    public class SqlScopeConfigBuilder : IDbScopeConfigBuilder
    {

        //private SqlConnection connection;
        //private SqlTransaction transaction;

        //public SqlScopeConfigBuilder(DbConnection connection, DbTransaction transaction = null) 
        //{
        //    this.connection = connection as SqlConnection;
        //    this.transaction = transaction as SqlTransaction;
        //}

        //public void CreateScopeConfigTable()
        //{
        //    var command = connection.CreateCommand();

        //    if (transaction != null)
        //        command.Transaction = transaction;

        //    bool alreadyOpened = connection.State == ConnectionState.Open;
        //    try
        //    {
        //        if (!alreadyOpened)
        //            connection.Open();

        //        command.CommandText = GetScopeConfigCreateTableCommand();
        //        command.ExecuteNonQuery();
        //    }
        //    catch (Exception ex)
        //    {
        //        Logger.Current.Error($"Error during CreateTableScope : {ex}");
        //        throw;
        //    }
        //    finally
        //    {
        //        if (!alreadyOpened && connection.State != ConnectionState.Closed)
        //            connection.Close();

        //        if (command != null)
        //            command.Dispose();
        //    }
        //}

        //public void DeleteScopeConfig(Guid configId)
        //{
        //    var command = connection.CreateCommand();
        //    if (transaction != null)
        //        command.Transaction = transaction;

        //    bool alreadyOpened = connection.State == ConnectionState.Open;

        //    try
        //    {
        //        if (!alreadyOpened)
        //            connection.Open();

        //        command.CommandText = GetScopeConfigDeleteCommand();

        //        var p = command.CreateParameter();
        //        p.ParameterName = "@config_id";
        //        p.DbType = DbType.Guid;
        //        p.Value = configId;
        //        command.Parameters.Add(p);

        //        command.ExecuteNonQuery();
        //    }
        //    catch (Exception ex)
        //    {
        //        Logger.Current.Error($"Error during DeleteScopeConfig : {ex}");
        //        throw;
        //    }
        //    finally
        //    {
        //        if (!alreadyOpened && connection.State != ConnectionState.Closed)
        //            connection.Close();

        //        if (command != null)
        //            command.Dispose();
        //    }
        //}

        //public void InsertScopeConfig(ScopeConfig config)
        //{
        //    var command = connection.CreateCommand();
        //    if (transaction != null)
        //        command.Transaction = transaction;

        //    bool alreadyOpened = connection.State == ConnectionState.Open;

        //    try
        //    {
        //        if (!alreadyOpened)
        //            connection.Open();

        //        command.CommandText = GetScopeConfigInsertCommand();

        //        var p = command.CreateParameter();
        //        p.ParameterName = "@config_data";
        //        p.Value = config.ConfigData;
        //        p.DbType = DbType.Xml;
        //        command.Parameters.Add(p);

        //        p = command.CreateParameter();
        //        p.ParameterName = "@scope_status";
        //        p.DbType = DbType.String;
        //        p.Value = config.ConfigStatus;
        //        command.Parameters.Add(p);

        //        p = command.CreateParameter();
        //        p.ParameterName = "@config_id";
        //        p.DbType = DbType.Guid;
        //        p.Value = config.ConfigId;
        //        command.Parameters.Add(p);

        //        command.ExecuteNonQuery();
        //    }
        //    catch (Exception ex)
        //    {
        //        Logger.Current.Error($"Error during UpdateScopeConfig : {ex}");
        //        throw;
        //    }
        //    finally
        //    {
        //        if (!alreadyOpened && connection.State != ConnectionState.Closed)
        //            connection.Close();

        //        if (command != null)
        //            command.Dispose();
        //    }
        //}

        //public bool NeedProvisionScopeConfig(string scopeName)
        //{
        //    var command = connection.CreateCommand();
        //    if (transaction != null)
        //        command.Transaction = transaction;

        //    bool alreadyOpened = connection.State == ConnectionState.Open;

        //    try
        //    {
        //        if (!alreadyOpened)
        //            connection.Open();

        //        command.CommandText = GetScopeConfigExistCommand();

        //        var p = command.CreateParameter();
        //        p.ParameterName = "@sync_scope_id";
        //        p.DbType = DbType.String;
        //        p.Value = scopeName;
        //        command.Parameters.Add(p);

        //        return (int)command.ExecuteScalar() == 1;

        //    }
        //    catch (Exception ex)
        //    {
        //        Logger.Current.Error($"Error during IsScopeConfigProvisionned : {ex}");
        //        throw;
        //    }
        //    finally
        //    {
        //        if (!alreadyOpened && connection.State != ConnectionState.Closed)
        //           connection.Close();

        //        if (command != null)
        //            command.Dispose();
        //    }
        //}

        //public ScopeConfig ReadScopeConfig(Guid configId)
        //{
        //    var command = connection.CreateCommand();
        //    bool alreadyOpened = connection.State == ConnectionState.Open;
        //    try
        //    {
        //        if (!alreadyOpened)
        //            connection.Open();

        //        if (transaction != null)
        //            command.Transaction = transaction;

        //        command.CommandText = GetScopeConfigSelectCommand();

        //        var p = command.CreateParameter();
        //        p.ParameterName = "@config_id";
        //        p.Value = configId;
        //        command.Parameters.Add(p);

        //        ScopeConfig scopeConfig = new ScopeConfig();

        //        using (DbDataReader reader = command.ExecuteReader())
        //        {
        //            while (reader.Read())
        //            {
        //                scopeConfig.ConfigId = (Guid)reader["config_id"];
        //                scopeConfig.ConfigData = reader["config_data"] as string;
        //                scopeConfig.ConfigStatus = reader["scope_status"] as string;
        //            }
        //        }

        //        return scopeConfig;
        //    }
        //    catch (Exception ex)
        //    {
        //        Logger.Current.Error($"Error during ReadScope : {ex}");
        //        throw;
        //    }
        //    finally
        //    {
        //        if (!alreadyOpened && connection.State != ConnectionState.Closed)
        //            connection.Close();

        //        if (command != null)
        //            command.Dispose();
        //    }
        //}

        //public void UpdateScopeConfig(ScopeConfig config)
        //{
        //    var command = connection.CreateCommand();
        //    if (transaction != null)
        //        command.Transaction = transaction;

        //    bool alreadyOpened =connection.State == ConnectionState.Open;

        //    try
        //    {
        //        if (!alreadyOpened)
        //            connection.Open();

        //        command.CommandText = GetScopeConfigUpdateCommand();

        //        var p = command.CreateParameter();
        //        p.ParameterName = "@config_data";
        //        p.Value = config.ConfigData;
        //        p.DbType = DbType.Xml;
        //        command.Parameters.Add(p);

        //        p = command.CreateParameter();
        //        p.ParameterName = "@scope_status";
        //        p.DbType = DbType.String;
        //        p.Value = config.ConfigStatus;
        //        command.Parameters.Add(p);

        //        p = command.CreateParameter();
        //        p.ParameterName = "@config_id";
        //        p.DbType = DbType.Guid;
        //        p.Value = config.ConfigId;
        //        command.Parameters.Add(p);

        //        command.ExecuteNonQuery();
        //    }
        //    catch (Exception ex)
        //    {
        //        Logger.Current.Error($"Error during UpdateScopeConfig : {ex}");
        //        throw;
        //    }
        //    finally
        //    {
        //        if (!alreadyOpened && connection.State != ConnectionState.Closed)
        //            connection.Close();

        //        if (command != null)
        //            command.Dispose();
        //    }
        //}


        //string GetScopeConfigInsertCommand()
        //{
        //    return @"INSERT INTO [scope_config] ([config_id], [config_data], [scope_status]) 
        //             VALUES (@config_id, @config_data, @scope_status)";
        //}
        //string GetScopeConfigUpdateCommand()
        //{
        //    return @"UPDATE [scope_config]
        //                SET [config_data] = @config_data
        //                   ,[scope_status] = @scope_status
        //              WHERE [config_id] = @config_id";
        //}
        //string GetScopeConfigExistCommand()
        //{
        //    return @"IF EXISTS (SELECT t.name FROM sys.tables t 
        //                    JOIN sys.schemas s ON s.schema_id = t.schema_id 
        //                    WHERE t.name = N'scope_config')
        //             BEGIN
        //                IF EXISTS (SELECT si.[sync_scope_id]
        //                            FROM [scope_info] si INNER JOIN [scope_config] sc on si.[scope_config_id] = sc.[config_id]
        //                            where si.[sync_scope_id] = @sync_scope_id)
        //                SELECT 1 
        //                ELSE
        //                SELECT 0
        //             END
        //             ELSE SELECT 0";
        //}
        //string GetScopeConfigCreateTableCommand()
        //{

        //    return @"IF NOT EXISTS (SELECT t.name FROM sys.tables t 
        //                       JOIN sys.schemas s ON s.schema_id = t.schema_id 
        //                       WHERE t.name = N'scope_config')
        //                       BEGIN
        //                        CREATE TABLE [scope_config](
	       //                     [config_id] [uniqueidentifier] NOT NULL,
	       //                     [config_data] [xml] NOT NULL,
	       //                     [scope_status] [char](1) NULL,
        //                        CONSTRAINT [PK_scope_config] PRIMARY KEY CLUSTERED ( [config_id] ASC )
        //                       END";

        //}
        //string GetScopeConfigSelectCommand()
        //{
        //    return @"SELECT config_id, config_data, scope_status FROM scope_config WHERE config_id = @config_id";
        //}
        //string GetScopeConfigDeleteCommand()
        //{
        //    return @"DELETE FROM scope_config WHERE config_id = @config_id";
        //}

    }
}
