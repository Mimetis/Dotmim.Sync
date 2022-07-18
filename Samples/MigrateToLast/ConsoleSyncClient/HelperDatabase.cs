using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using MySqlConnector;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WebSyncClient
{
    public static class HelperDatabase
    {

        public static string GetRandomName(string pref = default)
        {
            var str1 = Path.GetRandomFileName().Replace(".", "").ToLowerInvariant();
            return $"{pref}{str1}";
        }

        /// <summary>
        /// Get the Sqlite file path (ie: /Dir/mydatabase.db)
        /// </summary>
        public static string GetSqliteFilePath(string dbName)
        {
            var fi = new FileInfo(dbName);

            if (string.IsNullOrEmpty(fi.Extension))
                dbName = $"{dbName}.db";

            return Path.Combine(Directory.GetCurrentDirectory(), dbName);

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
        public static string GetConnectionString(this IConfiguration configuration, ProviderType providerType, string dbName)
        {
            string con = "";
            switch (providerType)
            {
                case ProviderType.Sql:
                    con = string.Format(configuration.GetConnectionString("SqlConnection"), dbName);
                    break;
                case ProviderType.MySql:
                    con = string.Format(configuration.GetConnectionString("MySqlConnection"), dbName);
                    break;
                case ProviderType.MariaDB:
                    con = string.Format(configuration.GetConnectionString("MariaDBConnection"), dbName);
                    break;
                case ProviderType.Sqlite:
                    con = GetSqliteDatabaseConnectionString(dbName);
                    break;
            }

            // default 
            return con;
        }


        /// <summary>
        /// Drop a database, depending the Provider type
        /// </summary>
        [DebuggerStepThrough]
        public static void DropDatabase(this IConfiguration configuration, ProviderType providerType, string dbName)
        {
            try
            {
                switch (providerType)
                {
                    case ProviderType.Sql:
                        DropSqlDatabase(GetConnectionString(configuration, ProviderType.Sql, "master"), dbName);
                        break;
                    case ProviderType.MySql:
                        DropMySqlDatabase(GetConnectionString(configuration, ProviderType.MySql, "information_schema"), dbName);
                        break;
                    case ProviderType.MariaDB:
                        DropMariaDBDatabase(GetConnectionString(configuration, ProviderType.MariaDB, "information_schema"), dbName);
                        break;
                    case ProviderType.Sqlite:
                        DropSqliteDatabase(dbName);
                        break;
                }
            }
            catch (Exception) { }
            finally
            {

            }
        }

        /// <summary>
        /// Drop a mysql database
        /// </summary>
        private static void DropMySqlDatabase(string connectionString, string dbName)
        {
            using var sysConnection = new MySqlConnection(connectionString);
            sysConnection.Open();

            using (var cmdDb = new MySqlCommand($"drop database if exists {dbName};", sysConnection))
                cmdDb.ExecuteNonQuery();

            sysConnection.Close();
        }


        /// <summary>
        /// Drop a MariaDB database
        /// </summary>
        private static void DropMariaDBDatabase(string connectionString, string dbName)
        {
            using var sysConnection = new MySqlConnection(connectionString);
            sysConnection.Open();

            using (var cmdDb = new MySqlCommand($"drop database if exists {dbName};", sysConnection))
                cmdDb.ExecuteNonQuery();

            sysConnection.Close();
        }

        /// <summary>
        /// Drop a sqlite database
        /// </summary>
        [DebuggerStepThrough]
        private static void DropSqliteDatabase(string dbName)
        {
            string filePath = null;
            try
            {
                SqliteConnection.ClearAllPools();
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
            finally { }

        }

        /// <summary>
        /// Delete a database
        /// </summary>
        private static void DropSqlDatabase(string connectionString, string dbName)
        {
            using var masterConnection = new SqlConnection(connectionString);
            try
            {
                masterConnection.Open();

                using (var cmdDb = new SqlCommand(GetSqlDropDatabaseScript(dbName), masterConnection))
                    cmdDb.ExecuteNonQuery();

                masterConnection.Close();

            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.Message);
                throw;
            }
        }


        public static Task ExecuteScriptAsync(IConfiguration configuration, ProviderType providerType, string dbName, string script)
        {
            switch (providerType)
            {
                case ProviderType.MySql:
                    return ExecuteMySqlScriptAsync(GetConnectionString(configuration, ProviderType.MySql, dbName), script);
                case ProviderType.MariaDB:
                    return ExecuteMariaDBScriptAsync(GetConnectionString(configuration, ProviderType.MariaDB, dbName), script);
                case ProviderType.Sqlite:
                    return ExecuteSqliteScriptAsync(GetConnectionString(configuration, ProviderType.Sqlite, dbName), script);
                case ProviderType.Sql:
                default:
                    return ExecuteSqlScriptAsync(GetConnectionString(configuration, ProviderType.Sql, dbName), script);
            }
        }

        private static async Task ExecuteMariaDBScriptAsync(string connectionString, string script)
        {

            using var connection = new MySqlConnection(connectionString);
            connection.Open();

            using (var cmdDb = new MySqlCommand(script, connection))
                await cmdDb.ExecuteNonQueryAsync();

            connection.Close();
        }
        private static async Task ExecuteMySqlScriptAsync(string connectionString, string script)
        {
            using var connection = new MySqlConnection(connectionString);
            connection.Open();

            using (var cmdDb = new MySqlCommand(script, connection))
                await cmdDb.ExecuteNonQueryAsync();

            connection.Close();
        }
        private static async Task ExecuteSqlScriptAsync(string connectionString, string script)
        {
            using var connection = new SqlConnection(connectionString);
            connection.Open();

            //split the script on "GO" commands
            string[] splitter = new string[] { "\r\nGO\r\n" };
            string[] commandTexts = script.Split(splitter, StringSplitOptions.RemoveEmptyEntries);

            foreach (string commandText in commandTexts)
            {
                using var cmdDb = new SqlCommand(commandText, connection);
                await cmdDb.ExecuteNonQueryAsync();
            }
            connection.Close();
        }
        public static async Task ExecuteSqliteScriptAsync(string dbName, string script)
        {
            using var connection = new SqliteConnection(GetSqliteDatabaseConnectionString(dbName));
            connection.Open();
            using (var cmdDb = new SqliteCommand(script, connection))
            {
                await cmdDb.ExecuteNonQueryAsync();
            }
            connection.Close();
        }

        /// <summary>
        /// Gets the drop sql database script
        /// </summary>
        public static string GetSqlDropDatabaseScript(string dbName)
        {
            return $@"if (exists (Select * from sys.databases where name = '{dbName}'))
            begin
	            alter database [{dbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	            drop database {dbName};
            end";
        }

    }

    [Flags]
    public enum ProviderType
    {
        Sql = 0x1,
        MySql = 0x2,
        Sqlite = 0x40,
        MariaDB = 0x80,

    }
}
