using Dotmim.Sync.Core.Scope;
using System;
using System.Collections.Generic;
using System.Text;
using System.Data.Common;
using System.Data;
using Dotmim.Sync.Core.Log;
using System.Data.SQLite;


namespace Dotmim.Sync.SQLite
{
    public class SQLiteScopeInfoBuilder : IDbScopeInfoBuilder
    {


        private SQLiteConnection connection;
        private SQLiteTransaction transaction;

        public SQLiteScopeInfoBuilder(DbConnection connection, DbTransaction transaction = null)
        {
            this.connection = connection as SQLiteConnection;
            this.transaction = transaction as SQLiteTransaction;
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
                    @"CREATE TABLE scope_info(
                        sync_scope_id varchar(36) NOT NULL PRIMARY KEY,
	                    sync_scope_name varchar(100) NOT NULL,
	                    scope_timestamp bigint NULL,
                        scope_is_local integer NOT NULL DEFAULT(0), 
                        scope_last_sync datetime NULL
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
                    @"SELECT sync_scope_id
                           , sync_scope_name
                           , scope_timestamp
                           , scope_is_local
                           , scope_last_sync
                    FROM  scope_info
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
                        scopeInfo.LastTimestamp = SQLiteManager.ParseTimestamp(reader["scope_timestamp"]);
                        scopeInfo.LastSync = reader["scope_last_sync"] != DBNull.Value ? (DateTime?)reader["scope_last_sync"] : null;
                        scopeInfo.IsLocal = reader.GetBoolean(reader.GetOrdinal("scope_is_local"));
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
                command.CommandText = "Select strftime('%s', datetime('now', 'utc'))";

                if (!alreadyOpened)
                    connection.Open();

                long result = (long)command.ExecuteScalar() ;

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

                command.CommandText = @"Select count(*) from scope_info where sync_scope_id = @sync_scope_id";

                var p = command.CreateParameter();
                p.ParameterName = "@sync_scope_id";
                p.Value = scopeInfo.Id.ToString();
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                var exist = (long)command.ExecuteScalar() > 0;

                string stmtText = exist
                    ? "Update scope_info set sync_scope_name=@sync_scope_name, scope_timestamp=strftime('%s', datetime('now', 'utc')), scope_is_local=@scope_is_local, scope_last_sync=@scope_last_sync where sync_scope_id=@sync_scope_id"
                    : "Insert into scope_info (sync_scope_name, scope_timestamp, scope_is_local, scope_last_sync, sync_scope_id) values (@sync_scope_name, strftime('%s', datetime('now', 'utc')), @scope_is_local, @scope_last_sync, @sync_scope_id)";

                command = connection.CreateCommand();
                command.CommandText = stmtText;

                p = command.CreateParameter();
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
                p.ParameterName = "@sync_scope_id";
                p.Value = scopeInfo.Id.ToString();
                p.DbType = DbType.String;
                command.Parameters.Add(p);

                using (DbDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        scopeInfo.Name = reader["sync_scope_name"] as String;
                        scopeInfo.Id = new Guid((string)reader["sync_scope_Id"]);
                        scopeInfo.LastTimestamp = SQLiteManager.ParseTimestamp(reader["scope_timestamp"]);
                        scopeInfo.IsLocal = (bool)reader["scope_is_local"];
                        scopeInfo.LastSync = reader["scope_last_sync"] != DBNull.Value ? (DateTime?)reader["scope_last_sync"] : null;
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
                    @"SELECT count(*) FROM sqlite_master WHERE type='table' AND name='scope_info'";

                return (long)command.ExecuteScalar() != 1;

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
