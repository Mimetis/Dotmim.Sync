using Dotmim.Sync.Tests;
using Dotmim.Sync.Tests.Core;
using Microsoft.Data.Sqlite;
using MySql.Data.MySqlClient;
using System;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;

namespace Dotmim.Sync.Tests
{
    public static class HelperDB
    {

        public static Func<String, String> GetSqlConnectionString;


        /// <summary>
        /// Get the Sqlite file path (ie: /Dir/mydatabase.db)
        /// </summary>
        public static string GetSqliteFilePath(string dbName)
        {
            return Path.Combine(Directory.GetCurrentDirectory(), $"{dbName}.db");

        }

        /// <summary>
        /// Gets the connection string used to open a sqlite database
        /// </summary>
        public static string GetSqliteDatabaseConnectionString(string dbName)
        {
            var builder = new SqliteConnectionStringBuilder { DataSource = GetSqliteFilePath(dbName) };

            return builder.ConnectionString;
        }


        /// <summary>
        /// Get connection string, depending on providerType (and will call the good get connectionstring method)
        /// </summary>
        public static string GetConnectionString(ProviderType providerType, string dbName)
        {
            switch (providerType)
            {
                case ProviderType.Sql:
                    return Setup.GetSqlDatabaseConnectionString(dbName);
                case ProviderType.MySql:
                    return Setup.GetMySqlDatabaseConnectionString(dbName);
                case ProviderType.Sqlite:
                    return GetSqliteDatabaseConnectionString(dbName);
            }

            // default 
            return GetSqliteDatabaseConnectionString(dbName);
        }

        /// <summary>
        /// Create a database, depending the Provider type
        /// </summary>
        public static void CreateDatabase(ProviderType providerType, string dbName, bool recreateDb = true)
        {
            switch (providerType)
            {
                case ProviderType.Sql:
                    CreateSqlServerDatabase(dbName, recreateDb);
                    break;
                case ProviderType.MySql:
                    CreateMySqlDatabase(dbName, recreateDb);
                    break;
            }

            // default 
            CreateSqlServerDatabase(dbName, recreateDb);
        }

        /// <summary>
        /// Create a new Sql Server database
        /// </summary>
        public static void CreateSqlServerDatabase(string dbName, bool recreateDb = true)
        {
            using (var masterConnection = new SqlConnection(Setup.GetSqlDatabaseConnectionString("master")))
            {
                masterConnection.Open();
                var cmdDb = new SqlCommand(GetSqlCreationScript(dbName, recreateDb), masterConnection);
                cmdDb.ExecuteNonQuery();
                masterConnection.Close();
            }
        }

        /// <summary>
        /// Create a new MySql Server database
        /// </summary>
        public static void CreateMySqlDatabase(string dbName, bool recreateDb = true)
        {
            using (var sysConnection = new MySqlConnection(Setup.GetMySqlDatabaseConnectionString("sys")))
            {
                sysConnection.Open();
                if (recreateDb)
                {
                    var cmdDrop = new MySqlCommand($"Drop schema if exists  {dbName};", sysConnection);
                    cmdDrop.ExecuteNonQuery();
                }

                var cmdDb = new MySqlCommand($"create schema {dbName};", sysConnection);
                cmdDb.ExecuteNonQuery();
                sysConnection.Close();
            }
        }


        /// <summary>
        /// Drop a database, depending the Provider type
        /// </summary>
        public static void DropDatabase(ProviderType providerType, string dbName)
        {
            switch (providerType)
            {
                case ProviderType.Sql:
                    DropSqlDatabase(dbName);
                    break;
                case ProviderType.MySql:
                    DropMySqlDatabase(dbName);
                    break;
                case ProviderType.Sqlite:
                    DropSqliteDatabase(dbName);
                    break;
            }

            // default 
            DropSqlDatabase(dbName);
        }

        /// <summary>
        /// Drop a mysql database
        /// </summary>
        public static void DropMySqlDatabase(string dbName)
        {
            using (var sysConnection = new MySqlConnection(Setup.GetMySqlDatabaseConnectionString("sys")))
            {
                sysConnection.Open();
                var cmdDb = new MySqlCommand($"drop database if exists {dbName};", sysConnection);
                cmdDb.ExecuteNonQuery();
                sysConnection.Close();
            }
        }

        /// <summary>
        /// Drop a sqlite database
        /// </summary>
        public static void DropSqliteDatabase(string dbName)
        {
            string filePath=null;
            try
            {
                GC.Collect();
                GC.WaitForPendingFinalizers();
                filePath = GetSqliteFilePath(dbName);

                if (File.Exists(filePath))
                    File.Delete(filePath);

            }
            catch (Exception)
            {

                Debug.WriteLine($"Sqlite file seems loked. ({filePath})");
            }
        }

        /// <summary>
        /// Delete a database
        /// </summary>
        public static void DropSqlDatabase(string dbName)
        {
            using (var masterConnection = new SqlConnection(Setup.GetSqlDatabaseConnectionString("master")))
            {
                masterConnection.Open();
                var cmdDb = new SqlCommand(GetSqlDropDatabaseScript(dbName), masterConnection);
                cmdDb.ExecuteNonQuery();
                masterConnection.Close();
            }
        }


        public static void ExecuteMySqlScript(string dbName, string script)
        {
            using (var connection = new MySqlConnection(Setup.GetMySqlDatabaseConnectionString(dbName)))
            {
                connection.Open();
                var cmdDb = new MySqlCommand(script, connection);
                cmdDb.ExecuteNonQuery();
                connection.Close();
            }
        }
        public static void ExecuteSqlScript(string dbName, string script)
        {
            using (var connection = new SqlConnection(Setup.GetSqlDatabaseConnectionString(dbName)))
            {
                connection.Open();

                //split the script on "GO" commands
                string[] splitter = new string[] { "\r\nGO\r\n" };
                string[] commandTexts = script.Split(splitter, StringSplitOptions.RemoveEmptyEntries);

                foreach (string commandText in commandTexts)
                {
                    var cmdDb = new SqlCommand(commandText, connection);
                    cmdDb.ExecuteNonQuery();
                }
                connection.Close();
            }
        }
        public static void ExecuteSqliteScript(string dbName, string script)
        {
            using (var connection = new SqliteConnection(dbName))
            {
                connection.Open();
                var cmdDb = new SqliteCommand(script, connection);
                cmdDb.ExecuteNonQuery();
                connection.Close();
            }
        }

        /// <summary>
        /// Restore a sql backup file
        /// </summary>
        public static void RestoreSqlDatabase(string dbName, string filePath)
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


            using (var connection = new SqlConnection(Setup.GetSqlDatabaseConnectionString("master")))
            {
                if (connection.State == ConnectionState.Closed)
                    connection.Open();

                var cmdDb = new SqlCommand(script, connection);
                cmdDb.ExecuteNonQuery();
                connection.Close();
            }
        }

        /// <summary>
        /// Gets the Create or Re-create a database script text
        /// </summary>
        private static string GetSqlCreationScript(string dbName, Boolean recreateDb = true)
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

        /// <summary>
        /// Gets the drop sql database script
        /// </summary>
        private static string GetSqlDropDatabaseScript(string dbName)
        {
            return $@"if (exists (Select * from sys.databases where name = '{dbName}'))
                    begin
	                    alter database [{dbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
	                    drop database {dbName}
                    end";
        }

    }
}
