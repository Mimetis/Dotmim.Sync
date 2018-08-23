using Microsoft.Data.Sqlite;
using MySql.Data.MySqlClient;
using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;

namespace Dotmim.Sync.Test.SqlUtils
{
    public class HelperDB
    {  
        /// <summary>
        /// Returns the database server to be used in the untittests - note that this is the connection to appveyor SQL Server 2016 instance!
        /// see: https://www.appveyor.com/docs/services-databases/#mysql
        /// </summary>
        /// <param name="dbName"></param>
        /// <returns></returns>
        public static String GetDatabaseConnectionString(string dbName)
        {

            // check if we are running on appveyor or not
            string isOnAppVeyor = Environment.GetEnvironmentVariable("APPVEYOR");

            if (!String.IsNullOrEmpty(isOnAppVeyor) && isOnAppVeyor.ToLowerInvariant() == "true")
                return $@"Server=(local)\SQL2016;Database={dbName};UID=sa;PWD=Password12!";
            else
                return $@"Data Source=(localdb)\MSSQLLocalDB;Initial Catalog={dbName};Integrated Security=true;";

        }
        /// <summary>
        /// Returns the database server to be used in the untittests - note that this is the connection to appveyor MySQL 5.7 x64 instance!
        /// see: https://www.appveyor.com/docs/services-databases/#mysql
        /// </summary>
        /// <param name="dbName"></param>
        /// <returns></returns>
        public static string GetMySqlDatabaseConnectionString(string dbName)
        {
            // check if we are running on appveyor or not
            string isOnAppVeyor = Environment.GetEnvironmentVariable("APPVEYOR");

            if (!String.IsNullOrEmpty(isOnAppVeyor) && isOnAppVeyor.ToLowerInvariant() == "true")
                return $@"Server=127.0.0.1; Port=3306; Database={dbName}; Uid=root; Pwd=Password12!";
            else
                return $@"Server=127.0.0.1; Port=3306; Database={dbName}; Uid=root; Pwd=azerty31$;";

        }

        /// <summary>
        /// Generate a database
        /// </summary>
        public void CreateDatabase(string dbName, bool recreateDb = true)
        {
            using (var masterConnection = new SqlConnection(GetDatabaseConnectionString("master")))
            {
                masterConnection.Open();
                var cmdDb = new SqlCommand(GetCreationDBScript(dbName, recreateDb), masterConnection);
                cmdDb.ExecuteNonQuery();
                masterConnection.Close();
            }
        }


        public void CreateMySqlDatabase(string dbName)
        {
            using (var sysConnection = new MySqlConnection(HelperDB.GetMySqlDatabaseConnectionString("sys")))
            {
                sysConnection.Open();
                var cmdDb = new MySqlCommand($"Drop schema if exists  {dbName};", sysConnection);
                cmdDb.ExecuteNonQuery();
                cmdDb = new MySqlCommand($"create schema {dbName};", sysConnection);
                cmdDb.ExecuteNonQuery();
                sysConnection.Close();
            }
        }

        public void DropMySqlDatabase(string dbName)
        {
            using (var sysConnection = new MySqlConnection(HelperDB.GetMySqlDatabaseConnectionString("sys")))
            {
                sysConnection.Open();
                var cmdDb = new MySqlCommand($"drop database if exists {dbName};", sysConnection);
                cmdDb.ExecuteNonQuery();
                sysConnection.Close();
            }
        }


        public void ExecuteMySqlScript(string dbName, string script)
        {
            using (var connection = new MySqlConnection(GetMySqlDatabaseConnectionString(dbName)))
            {
                connection.Open();
                var cmdDb = new MySqlCommand(script, connection);
                cmdDb.ExecuteNonQuery();
                connection.Close();
            }
        }
        /// <summary>
        /// Delete a database
        /// </summary>
        public void DeleteDatabase(string dbName)
        {
            using (var masterConnection = new SqlConnection(GetDatabaseConnectionString("master")))
            {
                masterConnection.Open();
                var cmdDb = new SqlCommand(GetDeleteDatabaseScript(dbName), masterConnection);
                cmdDb.ExecuteNonQuery();
                masterConnection.Close();
            }
        }

        public void ExecuteScript(string dbName, string script)
        {
            using (var connection = new SqlConnection(GetDatabaseConnectionString(dbName)))
            {
                connection.Open();
                var cmdDb = new SqlCommand(script, connection);
                cmdDb.ExecuteNonQuery();
                connection.Close();
            }
        }

        public void ExecuteSqliteScript(string dbName, string script)
        {
            using (var connection = new SqliteConnection(dbName))
            {
                connection.Open();
                var cmdDb = new SqliteCommand(script, connection);
                cmdDb.ExecuteNonQuery();
                connection.Close();
            }
        }


        public void RestoreDatabase(string dbName, string filePath)
        {
            var dataName = Path.GetFileNameWithoutExtension(dbName) + ".mdf";
            var logName = Path.GetFileNameWithoutExtension(dbName) + ".ldf";
            var script = $@"
                if (exists (select * from sys.databases where name = '{dbName}'))
                    begin                
                        ALTER DATABASE [{dbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
                    End
                else
                    begin
                        CREATE DATABASE [{dbName}]
                    end

                -- the backup contains the full path to the database files
                -- in order to be able to restore them on different developer machines
                -- we retrieve the default data path from the server
                -- and use it in RESTORE with the MOVE option
                declare @databaseFolder as nvarchar(256);
                set @databaseFolder = Convert(nvarchar(256), (SELECT ServerProperty(N'InstanceDefaultDataPath') AS default_file));

                declare @dataFile as nvarchar(256);
                declare @logFile as nvarchar(256);
                set @dataFile =@databaseFolder + '{dataName}';
                set @logFile =@databaseFolder + '{logName}';

                RESTORE DATABASE [{dbName}] FROM  DISK = N'{filePath}' WITH  RESTRICTED_USER, REPLACE,
                    MOVE '{dbName}' TO @dataFile,
                    MOVE '{dbName}_log' TO @logFile;
                ALTER DATABASE [{dbName}] SET MULTI_USER";


            using (var connection = new SqlConnection(GetDatabaseConnectionString("master")))
            {
                if(connection.State == ConnectionState.Closed)
                    connection.Open();

                var cmdDb = new SqlCommand(script, connection);
                cmdDb.ExecuteNonQuery();
                connection.Close();
            }
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
