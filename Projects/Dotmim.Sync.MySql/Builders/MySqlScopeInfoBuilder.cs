using Dotmim.Sync.Builders;
using Dotmim.Sync.Log;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using MySql.Data.MySqlClient;
using System.Diagnostics;

namespace Dotmim.Sync.MySql
{
    public class MySqlScopeInfoBuilder : IDbScopeInfoBuilder
    {
        private readonly ObjectNameParser scopeTableName;
        private readonly MySqlConnection connection;
        private readonly MySqlTransaction transaction;

        public MySqlScopeInfoBuilder(string scopeTableName, DbConnection connection, DbTransaction transaction = null)
        {
            this.connection = connection as MySqlConnection;
            this.transaction = transaction as MySqlTransaction;
            this.scopeTableName = new ObjectNameParser(scopeTableName, "`", "`");
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
                    $@"CREATE TABLE {scopeTableName.ObjectNameNormalized}(
                        sync_scope_id varchar(36) NOT NULL,
	                    sync_scope_name varchar(100) NOT NULL,
	                    scope_timestamp bigint NULL,
                        scope_is_local int NOT NULL DEFAULT 0, 
                        scope_last_sync datetime NULL,
                        scope_last_sync_timestamp bigint NULL,
                        scope_last_sync_duration bigint NULL,
                        PRIMARY KEY (sync_scope_id)
                        )";
                command.ExecuteNonQuery();
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

        public void DropScopeInfoTable()
        {
            var command = connection.CreateCommand();

            if (transaction != null)
                command.Transaction = transaction;

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                command.CommandText = $"drop table if exists {scopeTableName.ObjectNameNormalized}";

                command.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during DropScopeInfoTable : {ex}");
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
                    $@"SELECT sync_scope_id
                           , sync_scope_name
                           , scope_timestamp
                           , scope_is_local
                           , scope_last_sync
                           , scope_last_sync_timestamp
                           , scope_last_sync_duration
                    FROM  {scopeTableName.ObjectNameNormalized}
                    WHERE sync_scope_name = @sync_scope_name";

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
                        scopeInfo.Id = new Guid((String)reader["sync_scope_id"]);
                        scopeInfo.Timestamp = MySqlManager.ParseTimestamp(reader["scope_timestamp"]);
                        scopeInfo.LastSync = reader["scope_last_sync"] != DBNull.Value ? (DateTime?)reader["scope_last_sync"] : null;
                        scopeInfo.LastSyncDuration = reader["scope_last_sync_duration"] != DBNull.Value ? (long)reader["scope_last_sync_duration"] : 0L;
                        scopeInfo.LastSyncTimestamp = reader["scope_last_sync_timestamp"] != DBNull.Value ? (long)reader["scope_last_sync_timestamp"] : 0L;
                        scopeInfo.IsLocal = reader.GetBoolean(reader.GetOrdinal("scope_is_local"));
                        scopes.Add(scopeInfo);
                    }
                }

                return scopes;
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during GetAllScopes : {ex}");
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
                command.CommandText = $"Select {MySqlObjectNames.TimestampValue}";

                if (!alreadyOpened)
                    connection.Open();

                long result = Convert.ToInt64(command.ExecuteScalar());

                return result;
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

        public ScopeInfo InsertOrUpdateScopeInfo(ScopeInfo scopeInfo)
        {
            bool alreadyOpened = connection.State == ConnectionState.Open;
            bool exist;
            try
            {
                using (var command = connection.CreateCommand())
                {
                    if (transaction != null)
                        command.Transaction = transaction;


                    if (!alreadyOpened)
                        connection.Open();

                    command.CommandText = $@"Select count(*) from {scopeTableName.ObjectNameNormalized} where sync_scope_id = @sync_scope_id";

                    var p = command.CreateParameter();
                    p.ParameterName = "@sync_scope_id";
                    p.Value = scopeInfo.Id.ToString();
                    p.DbType = DbType.String;
                    command.Parameters.Add(p);

                    exist = (long)command.ExecuteScalar() > 0;

                }

                string stmtText = exist
                    ? $"Update {scopeTableName.ObjectNameNormalized} set sync_scope_name=@sync_scope_name, scope_timestamp={MySqlObjectNames.TimestampValue}, scope_is_local=@scope_is_local, scope_last_sync=@scope_last_sync, scope_last_sync_timestamp=@scope_last_sync_timestamp, scope_last_sync_duration=@scope_last_sync_duration  where sync_scope_id=@sync_scope_id"
                    : $"Insert into {scopeTableName.ObjectNameNormalized} (sync_scope_name, scope_timestamp, scope_is_local, scope_last_sync, sync_scope_id, scope_last_sync_timestamp, scope_last_sync_duration) values (@sync_scope_name, {MySqlObjectNames.TimestampValue}, @scope_is_local, @scope_last_sync, @sync_scope_id, @scope_last_sync_timestamp, @scope_last_sync_duration)";

                using (var command = connection.CreateCommand())
                {
                    if (transaction != null)
                        command.Transaction = transaction;

                    command.CommandText = stmtText;

                    var p = command.CreateParameter();
                    p.ParameterName = "@sync_scope_name";
                    p.Value = scopeInfo.Name;
                    p.DbType = DbType.String;
                    command.Parameters.Add(p);

                    p = command.CreateParameter();
                    p.ParameterName = "@scope_is_local";
                    p.Value = scopeInfo.IsLocal;
                    p.DbType = DbType.Boolean;
                    command.Parameters.Add(p);

                    p = command.CreateParameter();
                    p.ParameterName = "@scope_last_sync";
                    p.Value = scopeInfo.LastSync.HasValue ? (object)scopeInfo.LastSync.Value : DBNull.Value;
                    p.DbType = DbType.DateTime;
                    command.Parameters.Add(p);

                    p = command.CreateParameter();
                    p.ParameterName = "@scope_last_sync_timestamp";
                    p.Value = scopeInfo.LastSyncTimestamp;
                    p.DbType = DbType.Int64;
                    command.Parameters.Add(p);

                    p = command.CreateParameter();
                    p.ParameterName = "@scope_last_sync_duration";
                    p.Value = scopeInfo.LastSyncDuration;
                    p.DbType = DbType.Int64;
                    command.Parameters.Add(p);

                    p = command.CreateParameter();
                    p.ParameterName = "@sync_scope_id";
                    p.Value = scopeInfo.Id.ToString();
                    p.DbType = DbType.String;
                    command.Parameters.Add(p);

                    using (DbDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            scopeInfo.Name = reader["sync_scope_name"] as String;
                            scopeInfo.Id = new Guid((string)reader["sync_scope_id"]);
                            scopeInfo.Timestamp = MySqlManager.ParseTimestamp(reader["scope_timestamp"]);
                            scopeInfo.IsLocal = (bool)reader["scope_is_local"];
                            scopeInfo.LastSyncDuration = reader["scope_last_sync_duration"] != DBNull.Value ? (long)reader["scope_last_sync_duration"] : 0L;
                            scopeInfo.LastSyncTimestamp = reader["scope_last_sync_timestamp"] != DBNull.Value ? (long)reader["scope_last_sync_timestamp"] : 0L;
                            scopeInfo.LastSync = reader["scope_last_sync"] != DBNull.Value ? (DateTime?)reader["scope_last_sync"] : null;
                        }
                    }

                    return scopeInfo;
                }
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

                command.CommandText = $"select count(*) from information_schema.TABLES where TABLE_NAME = '{scopeTableName.ObjectNameNormalized}' and TABLE_SCHEMA = schema() and TABLE_TYPE = 'BASE TABLE'";

                return (long)command.ExecuteScalar() != 1;

            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during NeedToCreateScopeInfoTable command : {ex}");
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
