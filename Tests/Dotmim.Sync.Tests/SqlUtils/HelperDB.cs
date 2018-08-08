using Microsoft.Data.Sqlite;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;

namespace Dotmim.Sync.Test.SqlUtils
{
    public class HelperDB
    {
        public static String GetDatabaseConnectionString(string dbName) =>
            System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(System.Runtime.InteropServices.OSPlatform
                    .Windows) ?
            $@"Data Source=(localdb)\MSSQLLocalDB; Initial Catalog={dbName}; Integrated Security=true;" :
            $@"Server=localhost; Database={dbName}; User=sa; Password=QWE123qwe";

        public static string GetMySqlDatabaseConnectionString(string dbName) =>
            $@"Server=127.0.0.1; Port=3306; Database={dbName}; Uid=root; Pwd=azerty31$;";
        /// <summary>
        /// Generate a database
        /// </summary>
        public void CreateDatabase(string dbName, bool recreateDb = true)
        {
            SqlConnection masterConnection = null;
            SqlCommand cmdDb = null;
            masterConnection = new SqlConnection(GetDatabaseConnectionString("master"));

            masterConnection.Open();
            cmdDb = new SqlCommand(GetCreationDBScript(dbName, recreateDb), masterConnection);
            cmdDb.ExecuteNonQuery();
            masterConnection.Close();
        }


        public void CreateMySqlDatabase(string dbName)
        {
            MySqlConnection sysConnection = null;
            MySqlCommand cmdDb = null;
            sysConnection = new MySqlConnection(HelperDB.GetMySqlDatabaseConnectionString("sys"));

            sysConnection.Open();
            cmdDb = new MySqlCommand($"Drop schema if exists  {dbName};", sysConnection);
            cmdDb.ExecuteNonQuery();
            cmdDb = new MySqlCommand($"create schema {dbName};", sysConnection);
            cmdDb.ExecuteNonQuery();
            sysConnection.Close();

        }

        public void DropMySqlDatabase(string dbName)
        {
            MySqlConnection sysConnection = null;
            MySqlCommand cmdDb = null;
            sysConnection = new MySqlConnection(HelperDB.GetMySqlDatabaseConnectionString("sys"));

            sysConnection.Open();
            cmdDb = new MySqlCommand($"drop database if exists {dbName};", sysConnection);
            cmdDb.ExecuteNonQuery();
            sysConnection.Close();

        }


        public void ExecuteMySqlScript(string dbName, string script)
        {
            MySqlConnection connection = null;
            MySqlCommand cmdDb = null;
            connection = new MySqlConnection(GetMySqlDatabaseConnectionString(dbName));

            connection.Open();
            cmdDb = new MySqlCommand(script, connection);
            cmdDb.ExecuteNonQuery();
            connection.Close();
        }
        /// <summary>
        /// Delete a database
        /// </summary>
        public void DeleteDatabase(string dbName)
        {
            SqlConnection masterConnection = null;
            SqlCommand cmdDb = null;
            masterConnection = new SqlConnection(GetDatabaseConnectionString("master"));

            masterConnection.Open();
            cmdDb = new SqlCommand(GetDeleteDatabaseScript(dbName), masterConnection);
            cmdDb.ExecuteNonQuery();
            masterConnection.Close();
        }

        public void ExecuteScript(string dbName, string script)
        {
            SqlConnection connection = null;
            SqlCommand cmdDb = null;
            connection = new SqlConnection(GetDatabaseConnectionString(dbName));

            connection.Open();
            cmdDb = new SqlCommand(script, connection);
            cmdDb.ExecuteNonQuery();
            connection.Close();
        }

        public void ExecuteSqliteScript(string dbName, string script)
        {
            SqliteConnection connection = null;
            SqliteCommand cmdDb = null;
            connection = new SqliteConnection(dbName);

            connection.Open();
            cmdDb = new SqliteCommand(script, connection);
            cmdDb.ExecuteNonQuery();
            connection.Close();
        }


        public void RestoreDatabase(string dbName, string filePath)
        {
            var isWindows =
                System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(System.Runtime.InteropServices.OSPlatform
                    .Windows);

            SqlConnection connection = new SqlConnection(GetDatabaseConnectionString("master"));

            string dataFile = null, logFile = null;

            connection.Open();
            if (!isWindows)
            {
                using (SqlCommand cmd = new SqlCommand($@"RESTORE FILELISTONLY from DISK = N'{filePath}'", connection))
                {
                    using (var rd = cmd.ExecuteReader())
                    {
                        while (rd.Read())
                        {
                            if (rd["Type"].ToString() == "D")
                                dataFile = rd["LogicalName"].ToString();
                            else if (rd["Type"].ToString() == "L")
                                logFile = rd["LogicalName"].ToString();
                        }
                    }
                }
            }
            
            var restoreClause =
                System.Runtime.InteropServices.RuntimeInformation.IsOSPlatform(System.Runtime.InteropServices.OSPlatform.Windows) ?
                    $@"RESTORE DATABASE [{dbName}] FROM  DISK = N'{filePath}' WITH  RESTRICTED_USER, REPLACE
                       ALTER DATABASE [{dbName}] SET MULTI_USER" :
                    $@"RESTORE DATABASE [{dbName}]
            FROM DISK = N'{filePath}'
            WITH
                MOVE '{dataFile}' TO '/var/opt/mssql/data/{dbName}.mdf',
                MOVE '{logFile}' TO '/var/opt/mssql/data/{dbName}.ldf'"; 
            var script = $@"
                if (exists (select * from sys.databases where name = '{dbName}'))
                begin                
                    ALTER DATABASE [{dbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
                end
                {restoreClause}";

            SqlCommand cmdDb = null;

            cmdDb = new SqlCommand(script, connection);
            cmdDb.ExecuteNonQuery();
            connection.Close();

        }


        /// <summary>
        /// Gets the Create or Re-create a database script text
        /// </summary>
        private string GetCreationDBScript(string dbName, Boolean recreateDb = true)
        {
            if (recreateDb)
                return $@"if (exists (Select * from sys.databases where name = '{dbName}'))
                    begin
	                    alter database [{dbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
	                    drop database {dbName}
                    end
                    Create database {dbName}";
            else
                return $@"if not (exists (Select * from sys.databases where name = '{dbName}')) 
                          Create database {dbName}";

        }

        private string GetDeleteDatabaseScript(string dbName)
        {
            return $@"if (exists (Select * from sys.databases where name = '{dbName}'))
                    begin
	                    alter database [{dbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
	                    drop database {dbName}
                    end";
        }



        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }
}
