using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.OracleClient;
using System.Diagnostics;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Oracle.Manager;

namespace Dotmim.Sync.Oracle.Scope
{
    internal class OracleScopeInfoBuilder : IDbScopeInfoBuilder
    {
        private OracleConnection connection;
        private OracleTransaction transaction;

        public OracleScopeInfoBuilder(DbConnection connection, DbTransaction transaction)
        {
            this.connection = connection as OracleConnection;
            this.transaction = transaction as OracleTransaction;
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
                    @"CREATE TABLE scope_info (
                        sync_scope_id VARCHAR2(200) NOT NULL,
	                    sync_scope_name VARCHAR2(100) NOT NULL,
	                    scope_timestamp NUMBER(20) NULL,
                        scope_is_local number(1, 0) DEFAULT 0 NOT NULL , 
                        scope_last_sync DATE NULL,
                        CONSTRAINT PK_scope_info PRIMARY KEY (sync_scope_id)
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

                command.CommandText = "DROP TABLE scope_info";

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
                        scopeInfo.Id = Guid.Parse(reader["sync_scope_id"] as String);
                        scopeInfo.LastTimestamp = OracleManager.ParseTimestamp(reader["scope_timestamp"]);
                        scopeInfo.LastSync = reader["scope_last_sync"] != DBNull.Value ? (DateTime?)reader["scope_last_sync"] : null;
                        scopeInfo.IsLocal = (bool)reader["scope_is_local"];
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
                command.CommandText = "select to_number(to_char(systimestamp, 'YYYYMMDDHH24MISSFF3')) as currentts from dual;";

                if (!alreadyOpened)
                    connection.Open();

                long result = 0L;
                using (DbDataReader reader = command.ExecuteReader())
                {
                    // read only the first one
                    if (reader.Read())
                        long.TryParse(reader["currentts"] as string, out result);
                }
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
            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                command.CommandText = @"
                    MERGE INTO scope_info as base
                    USING (
                               SELECT  @sync_scope_id AS sync_scope_id,  
	                                   @sync_scope_name AS sync_scope_name,  
                                       @scope_is_local as scope_is_local,
                                       @scope_last_sync AS scope_last_sync
                           ) AS changes
                    ON base.sync_scope_id = changes.sync_scope_id
                    WHEN NOT MATCHED THEN
	                    INSERT (sync_scope_name, sync_scope_id, scope_is_local, scope_last_sync)
	                    VALUES (changes.sync_scope_name, changes.sync_scope_id, changes.scope_is_local, changes.scope_last_sync)
                    WHEN MATCHED THEN
	                    UPDATE SET sync_scope_name = changes.sync_scope_name, 
                                   scope_is_local = changes.scope_is_local, 
                                   scope_last_sync = changes.scope_last_sync
                    OUTPUT  INSERTED.sync_scope_name, 
                            INSERTED.sync_scope_id, 
                            INSERTED.scope_timestamp, 
                            INSERTED.scope_is_local,
                            INSERTED.scope_last_sync;
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

                p = command.CreateParameter();
                p.ParameterName = "@scope_last_sync";
                p.Value = scopeInfo.LastSync.HasValue ? (object)scopeInfo.LastSync.Value : DBNull.Value;
                p.DbType = DbType.DateTime;
                command.Parameters.Add(p);


                using (DbDataReader reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        scopeInfo.Name = reader["sync_scope_name"] as String;
                        scopeInfo.Id = (Guid)reader["sync_scope_id"];
                        scopeInfo.LastTimestamp = OracleManager.ParseTimestamp(reader["scope_timestamp"]);
                        scopeInfo.IsLocal = (bool)reader["scope_is_local"];
                        scopeInfo.LastSync = reader["scope_last_sync"] != DBNull.Value ? (DateTime?)reader["scope_last_sync"] : null;
                    }
                }

                return scopeInfo;
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

                command.CommandText = @"SELECT COUNT(1) FROM dba_users WHERE username = 'scope_info';";

                return (int)command.ExecuteScalar() != 1;
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