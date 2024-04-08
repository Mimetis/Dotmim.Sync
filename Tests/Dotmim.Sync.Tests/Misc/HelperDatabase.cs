using Dotmim.Sync.Tests.Core;
using Microsoft.Data.Sqlite;
#if NET6_0 || NET8_0 
using MySqlConnector;
#elif NETCOREAPP3_1
using MySql.Data.MySqlClient;
#endif

using System;
using System.Data;
using Microsoft.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using System.Xml.Linq;
using Npgsql;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.MySql;
using Dotmim.Sync.MariaDB;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.PostgreSql;
using Dotmim.Sync.Tests.Fixtures;
using Dotmim.Sync.Tests.Models;
using Microsoft.Extensions.Configuration;
using System.Collections.Concurrent;
using System.Threading;

namespace Dotmim.Sync.Tests.Misc
{
    public static class HelperDatabase
    {
        private static IConfigurationRoot configuration;

        static HelperDatabase()
        {
            configuration = new ConfigurationBuilder()
              .AddJsonFile("appsettings.json", false, true)
              .AddJsonFile("appsettings.local.json", true, true)
              .Build();

        }

        /// <summary>
        /// Returns the database connection string for Sql
        /// </summary>
        internal static string GetSqlDatabaseConnectionString(string dbName)
        {
            var cstring = string.Format(configuration.GetSection("ConnectionStrings")["SqlConnection"], dbName);

            var builder = new SqlConnectionStringBuilder(cstring);

            if (Setup.IsOnAzureDev)
            {
                builder.IntegratedSecurity = false;
                builder.DataSource = @"localhost";
                builder.UserID = "sa";
                builder.Password = "Password12!";
                builder.TrustServerCertificate = true;
            }
            return builder.ToString();
        }

        /// <summary>
        /// Returns the database connection string for Azure Sql
        /// </summary>
        internal static string GetSqlAzureDatabaseConnectionString(string dbName) =>
            string.Format(configuration.GetSection("ConnectionStrings")["AzureSqlConnection"], dbName);

        /// <summary>
        /// Returns the database connection string for MySql
        /// </summary>
        internal static string GetMySqlDatabaseConnectionString(string dbName)
        {
            var cstring = string.Format(configuration.GetSection("ConnectionStrings")["MySqlConnection"], dbName);

            var builder = new MySqlConnectionStringBuilder(cstring)
            {
                UseAffectedRows = false,
                AllowUserVariables = true
            };

            if (Setup.IsOnAzureDev)
            {
                builder.Port = 3307;
                builder.UserID = "root";
                builder.Password = "Password12!";
            }

            var cn = builder.ToString();
            return cn;
        }


        /// <summary>
        /// Returns the database connection string for MySql
        /// </summary>
        internal static string GetMariaDBDatabaseConnectionString(string dbName)
        {
            var cstring = string.Format(configuration.GetSection("ConnectionStrings")["MariaDBConnection"], dbName);

            var builder = new MySqlConnectionStringBuilder(cstring)
            {
                UseAffectedRows = false,
                AllowUserVariables = true
            };

            if (Setup.IsOnAzureDev)
            {
                builder.Port = 3308;
                builder.UserID = "root";
                builder.Password = "Password12!";
            }

            var cn = builder.ToString();
            return cn;
        }

        /// <summary>
        /// Returns the database connection string for MySql
        /// </summary>
        internal static string GetPostgresDatabaseConnectionString(string dbName)
        {
            var cstring = string.Format(configuration.GetSection("ConnectionStrings")["NpgsqlConnection"], dbName);

            var builder = new NpgsqlConnectionStringBuilder(cstring);

            if (Setup.IsOnAzureDev)
            {
                builder.Port = 5432;
                builder.Username = "postgres";
                builder.Password = "Password12!";
            }

            var cn = builder.ToString();
            return cn;
        }

        public static ConcurrentDictionary<string, string> names = new ConcurrentDictionary<string, string>();

        public static string GetRandomName(string pref = default)
        {
            string newGeneratedRandomName =$"{pref}{Path.GetRandomFileName().Replace(".", "").ToLowerInvariant()}";
            
            while (names.TryGetValue(newGeneratedRandomName, out var existsDbName))
            {
                Console.WriteLine($"Database {existsDbName} already exists. try another name");
                Thread.Sleep(1000);
                newGeneratedRandomName = $"{pref}{Path.GetRandomFileName().Replace(".", "").ToLowerInvariant()}";
            }


            names.TryAdd(newGeneratedRandomName, newGeneratedRandomName);
            
            return newGeneratedRandomName;
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
        public static string GetConnectionString(ProviderType providerType, string dbName)
        {
            var con = "";
            switch (providerType)
            {
                case ProviderType.Sql:
                    con = GetSqlDatabaseConnectionString(dbName);
                    break;
                case ProviderType.MySql:
                    con = GetMySqlDatabaseConnectionString(dbName);
                    break;
                case ProviderType.MariaDB:
                    con = GetMariaDBDatabaseConnectionString(dbName);
                    break;
                case ProviderType.Sqlite:
                    con = GetSqliteDatabaseConnectionString(dbName);
                    break;
                case ProviderType.Postgres:
                    con = GetPostgresDatabaseConnectionString(dbName);
                    break;
            }

            // default 
            return con;
        }


        public static (ProviderType ProviderType, string DatabaseName) GetDatabaseType(CoreProvider coreProvider)
        {
            var dbName = coreProvider.GetDatabaseName();

            return coreProvider switch
            {
                SqlSyncProvider _ => (ProviderType.Sql, dbName),
                MySqlSyncProvider _ => (ProviderType.MySql, dbName),
                MariaDBSyncProvider _ => (ProviderType.MariaDB, dbName),
                SqliteSyncProvider _ => (ProviderType.Sqlite, dbName),
                NpgsqlSyncProvider _ => (ProviderType.Postgres, dbName),
                _ => (ProviderType.Sql, dbName),
            };
        }

        public static CoreProvider GetSyncProvider(ProviderType providerType, string dbName, bool useFallbackSchema = false)
        {

            CoreProvider provider = providerType switch
            {
                ProviderType.Sql => new SqlSyncProvider(GetSqlDatabaseConnectionString(dbName)),
                ProviderType.MySql => new MySqlSyncProvider(GetMySqlDatabaseConnectionString(dbName)),
                ProviderType.MariaDB => new MariaDBSyncProvider(GetMariaDBDatabaseConnectionString(dbName)),
                ProviderType.Sqlite => new SqliteSyncProvider(GetSqliteDatabaseConnectionString(dbName)),
                ProviderType.Postgres => new NpgsqlSyncProvider(GetPostgresDatabaseConnectionString(dbName)),
                _ => null,
            };

            // Can't drop postgres sql databases on azure devops for ... some reasons...
            provider.UseShouldDropDatabase(providerType != ProviderType.Postgres);

            if (useFallbackSchema)
                provider.UseFallbackSchema(true);

            return provider;
        }

        public static void ClearAllPools()
        {
            SqlConnection.ClearAllPools();
            MySqlConnection.ClearAllPools();
            SqliteConnection.ClearAllPools();
            NpgsqlConnection.ClearAllPools();

        }

        public static void ClearPool(ProviderType providerType)
        {
            switch (providerType)
            {
                case ProviderType.Sql:
                    SqlConnection.ClearAllPools();
                    break;
                case ProviderType.MySql:
                    MySqlConnection.ClearAllPools();
                    break;
                case ProviderType.MariaDB:
                    MySqlConnection.ClearAllPools();
                    break;
                case ProviderType.Sqlite:
                    SqliteConnection.ClearAllPools();
                    break;
                case ProviderType.Postgres:
                    NpgsqlConnection.ClearAllPools();
                    break;
                default:
                    break;
            }
        }

        /// <summary>
        /// Create a database, depending the Provider type
        /// </summary>
        public static async Task CreateDatabaseAsync(ProviderType providerType, string dbName, bool recreateDb = true)
        {
            switch (providerType)
            {
                case ProviderType.Sql:
                    await CreateSqlServerDatabaseAsync(dbName, recreateDb);
                    break;
                case ProviderType.MySql:
                    await CreateMySqlDatabaseAsync(dbName, recreateDb);
                    break;
                case ProviderType.MariaDB:
                    await CreateMariaDBDatabaseAsync(dbName, recreateDb);
                    break;
                case ProviderType.Postgres:
                    await CreatePostgresDatabaseAsync(dbName, recreateDb);
                    break;
                case ProviderType.Sqlite:
                    await Task.CompletedTask;
                    break;
                default:
                    throw new Exception($"Provider type {providerType} is not existing;");
            }

            if (providerType == ProviderType.Sql)
                await ActivateChangeTracking(dbName);

        }

        /// <summary>
        /// Create a new Sql Server database
        /// </summary>
        public static async Task CreateSqlServerDatabaseAsync(string dbName, bool recreateDb = true)
        {
            var onRetry = new Func<Exception, int, TimeSpan, object, Task>((ex, cpt, ts, arg) =>
            {
                Console.WriteLine($"Creating SQL Server database failed when connecting to master ({ex.Message}). Wating {ts.Milliseconds}. Try number {cpt}");
                return Task.CompletedTask;
            });

            var policy = SyncPolicy.WaitAndRetry(3, TimeSpan.FromMilliseconds(500), null, onRetry);

            await policy.ExecuteAsync(async () =>
            {
                using var masterConnection = new SqlConnection(GetSqlDatabaseConnectionString("master"));
                masterConnection.Open();

                using (var cmdDb = new SqlCommand(GetSqlCreationScript(dbName, recreateDb), masterConnection))
                    await cmdDb.ExecuteNonQueryAsync();

                masterConnection.Close();
            });
        }


        public static async Task ActivateChangeTracking(string dbName)
        {
            var c = new SqlConnection(GetSqlDatabaseConnectionString(dbName));

            // Check if we are using change tracking and it's enabled on the source
            var isChangeTrackingEnabled = await SqlManagementUtils.IsChangeTrackingEnabledAsync(c, null).ConfigureAwait(false);

            if (isChangeTrackingEnabled)
                return;

            using var masterConnection = new SqlConnection(GetSqlDatabaseConnectionString("master"));

            var script = $"ALTER DATABASE {dbName} SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)";


            masterConnection.Open();

            using (var cmdCT = new SqlCommand(script, masterConnection))
                await cmdCT.ExecuteNonQueryAsync();

            masterConnection.Close();
        }
        /// <summary>
        /// Create a new MySql Server database
        /// </summary>
        private static async Task CreateMySqlDatabaseAsync(string dbName, bool recreateDb = true)
        {
            var onRetry = new Func<Exception, int, TimeSpan, object, Task>((ex, cpt, ts, arg) =>
            {
                Console.WriteLine($"Creating MySql database failed when connecting to information_schema ({ex.Message}). Wating {ts.Milliseconds}. Try number {cpt}");
                return Task.CompletedTask;
            });

            var policy = SyncPolicy.WaitAndRetry(3, TimeSpan.FromMilliseconds(500), null, onRetry);

            await policy.ExecuteAsync(async () =>
            {
                using var sysConnection = new MySqlConnection(GetMySqlDatabaseConnectionString("information_schema"));
                sysConnection.Open();

                if (recreateDb)
                {
                    using var cmdDrop = new MySqlCommand($"Drop schema if exists  {dbName};", sysConnection);
                    await cmdDrop.ExecuteNonQueryAsync();
                }

                using (var cmdDb = new MySqlCommand($"create schema {dbName};", sysConnection))
                    cmdDb.ExecuteNonQuery();

                sysConnection.Close();
            });
        }

        /// <summary>
        /// Create a new MySql Server database
        /// </summary>
        private static async Task CreateMariaDBDatabaseAsync(string dbName, bool recreateDb = true)
        {
            var onRetry = new Func<Exception, int, TimeSpan, object, Task>((ex, cpt, ts, arg) =>
            {
                Console.WriteLine($"Creating MariaDB database failed when connecting to information_schema ({ex.Message}). Wating {ts.Milliseconds}. Try number {cpt}");
                return Task.CompletedTask;
            });

            var policy = SyncPolicy.WaitAndRetry(3, TimeSpan.FromMilliseconds(500), null, onRetry);

            await policy.ExecuteAsync(async () =>
            {
                using var sysConnection = new MySqlConnection(GetMariaDBDatabaseConnectionString("information_schema"));
                sysConnection.Open();

                if (recreateDb)
                {
                    using var cmdDrop = new MySqlCommand($"Drop schema if exists  {dbName};", sysConnection);
                    await cmdDrop.ExecuteNonQueryAsync();
                }

                using (var cmdDb = new MySqlCommand($"create schema {dbName};", sysConnection))
                    cmdDb.ExecuteNonQuery();

                sysConnection.Close();
            });
        }

        /// <summary>
        /// Create a new Postgres database
        /// </summary>
        private static async Task CreatePostgresDatabaseAsync(string dbName, bool recreateDb = true)
        {
            var onRetry = new Func<Exception, int, TimeSpan, object, Task>((ex, cpt, ts, arg) =>
            {
                Console.WriteLine($"Creating Postgres database failed when connecting to information_schema ({ex.Message}). Wating {ts.Milliseconds}. Try number {cpt}");
                return Task.CompletedTask;
            });

            var policy = SyncPolicy.WaitAndRetry(3, TimeSpan.FromMilliseconds(500), null, onRetry);

            await policy.ExecuteAsync(async () =>
            {
                using var sysConnection = new NpgsqlConnection(GetPostgresDatabaseConnectionString("postgres"));
                sysConnection.Open();

                if (recreateDb)
                {
                    using var cmdDrop = new NpgsqlCommand($"Drop database if exists  {dbName};", sysConnection);
                    await cmdDrop.ExecuteNonQueryAsync();
                }

                using (var cmdDb = new NpgsqlCommand($"create database {dbName};", sysConnection))
                    cmdDb.ExecuteNonQuery();

                sysConnection.Close();
            });
        }

        /// <summary>
        /// Drop a database, depending the Provider type
        /// </summary>
        [DebuggerStepThrough]
        public static void DropDatabase(ProviderType providerType, string dbName)
        {
            try
            {
                switch (providerType)
                {
                    case ProviderType.Sql:
                        DropSqlDatabase(dbName);
                        break;
                    case ProviderType.MySql:
                        DropMySqlDatabase(dbName);
                        break;
                    case ProviderType.MariaDB:
                        DropMariaDBDatabase(dbName);
                        break;
                    case ProviderType.Sqlite:
                        DropSqliteDatabase(dbName);
                        break;
                    case ProviderType.Postgres:
                        DropPostgresDatabase(dbName);
                        break;
                }
                Debug.WriteLine($"- Database {providerType} {dbName} dropped");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error trying to drop database {dbName} of type {providerType}: {ex.Message}");
            }


        }

        /// <summary>
        /// Drop a database, depending the Provider type
        /// </summary>
        [DebuggerStepThrough]
        public static void TruncateTable(ProviderType providerType, string dbName, string tableName, string schemaName)
        {
            try
            {
                switch (providerType)
                {
                    case ProviderType.Sql:
                        TruncateSqlTable(dbName, tableName, schemaName);
                        break;
                    case ProviderType.MySql:
                        TruncateMySqlTable(dbName, tableName);
                        break;
                    case ProviderType.MariaDB:
                        TruncateMariaDbTable(dbName, tableName);
                        break;
                    case ProviderType.Sqlite:
                        TruncateSqliteTable(dbName, tableName);
                        break;
                    case ProviderType.Postgres:
                        TruncatePostgresTable(dbName, tableName, schemaName);
                        break;
                }
                Debug.WriteLine($"- Database {providerType} {dbName} dropped");
            }
            catch (Exception ex)
            {
                Debug.WriteLine(ex.Message);
            }
        }



        /// <summary>
        /// Drop a database, depending the Provider type
        /// </summary>
        [DebuggerStepThrough]
        public static bool ExistsDatabase(ProviderType providerType, string dbName)
        {
            switch (providerType)
            {
                case ProviderType.Sql:
                    return ExistsSqlDatabase(dbName);
                case ProviderType.MySql:
                    return ExistsMySqlDatabase(dbName);
                case ProviderType.MariaDB:
                    return ExistsMariaDbDatabase(dbName);
                case ProviderType.Sqlite:
                    return ExistsSqliteDatabase(dbName);
                case ProviderType.Postgres:
                    return ExistsPostgresDatabase(dbName);
            }

            return false;
        }


        /// <summary>
        /// Drop a mysql database
        /// </summary>
        private static void DropMySqlDatabase(string dbName)
        {
            using var sysConnection = new MySqlConnection(GetMySqlDatabaseConnectionString("information_schema"));
            sysConnection.Open();

            using (var cmdDb = new MySqlCommand($"drop database if exists {dbName};", sysConnection))
                cmdDb.ExecuteNonQuery();

            sysConnection.Close();
        }

        private static void TruncateMySqlTable(string dbName, string tableName)
        {
            using var connection = new MySqlConnection(GetMySqlDatabaseConnectionString(dbName));
            connection.Open();

            using (var cmdDb = new MySqlCommand($"SET FOREIGN_KEY_CHECKS=0; DELETE FROM `{tableName}`;SET FOREIGN_KEY_CHECKS=1;", connection))
                cmdDb.ExecuteNonQuery();

            connection.Close();
        }

        /// <summary>
        /// Check if a mysql database exists
        /// </summary>
        /// <param name="dbName"></param>
        private static bool ExistsMySqlDatabase(string dbName)
        {
            using var sysConnection = new MySqlConnection(GetMySqlDatabaseConnectionString("information_schema"));
            sysConnection.Open();
            using var cmdDb = new MySqlCommand($"SELECT 1 FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{dbName}';", sysConnection);
            var exists = cmdDb.ExecuteScalar();
            sysConnection.Close();

            return exists != null && exists != DBNull.Value && (long)exists == 1;
        }

        /// <summary>
        /// Drop a Postgres database
        /// </summary>
        private static void DropPostgresDatabase(string dbName)
        {
            using var sysConnection = new NpgsqlConnection(GetPostgresDatabaseConnectionString("postgres"));
            sysConnection.Open();

            using (var cmdDb = new NpgsqlCommand($"" +
                $"SELECT pg_terminate_backend(pg_stat_activity.pid) " +
                $"FROM pg_stat_activity " +
                $"WHERE pg_stat_activity.datname = '{dbName}' " +
                $"AND pid <> pg_backend_pid();" +
                $"DROP DATABASE IF EXISTS {dbName};", sysConnection))
                cmdDb.ExecuteNonQuery();

            sysConnection.Close();
        }

        private static void TruncatePostgresTable(string dbName, string tableName, string schemaName)
        {

            schemaName = string.IsNullOrEmpty(schemaName) ? "public" : schemaName;

            using var connection = new NpgsqlConnection(GetPostgresDatabaseConnectionString(dbName));
            connection.Open();

            using (var cmdDb = new NpgsqlCommand($"ALTER TABLE \"{schemaName}\".\"{tableName}\" DISABLE TRIGGER ALL; DELETE FROM \"{schemaName}\".\"{tableName}\"; ALTER TABLE \"{schemaName}\".\"{tableName}\" ENABLE TRIGGER ALL; ", connection))
                cmdDb.ExecuteNonQuery();

            connection.Close();
        }

        /// <summary>
        /// Check if a Postgres database exists
        /// </summary>
        private static bool ExistsPostgresDatabase(string dbName)
        {
            using var sysConnection = new NpgsqlConnection(GetPostgresDatabaseConnectionString("postgres"));
            sysConnection.Open();

            using var cmdDb = new NpgsqlCommand($"SELECT 1 FROM pg_database WHERE datname='{dbName}'");

            var exists = cmdDb.ExecuteScalar();
            sysConnection.Close();

            return exists != null && exists != DBNull.Value && (long)exists == 1;
        }

        /// <summary>
        /// Drop a MariaDB database
        /// </summary>
        private static void DropMariaDBDatabase(string dbName)
        {
            using var sysConnection = new MySqlConnection(GetMariaDBDatabaseConnectionString("information_schema"));
            sysConnection.Open();

            using (var cmdDb = new MySqlCommand($"drop database if exists {dbName};", sysConnection))
                cmdDb.ExecuteNonQuery();

            sysConnection.Close();
        }

        private static void TruncateMariaDbTable(string dbName, string tableName)
        {
            using var connection = new MySqlConnection(GetMariaDBDatabaseConnectionString(dbName));
            connection.Open();

            using (var cmdDb = new MySqlCommand($"DELETE FROM `{tableName}`;", connection))
                cmdDb.ExecuteNonQuery();

            connection.Close();
        }

        /// <summary>
        /// Check if a mariadb database exists
        /// </summary>
        /// <param name="dbName"></param>
        private static bool ExistsMariaDbDatabase(string dbName)
        {
            using var sysConnection = new MySqlConnection(GetMySqlDatabaseConnectionString("information_schema"));
            sysConnection.Open();
            using var cmdDb = new MySqlCommand($"SELECT 1 FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{dbName}';", sysConnection);
            var exists = cmdDb.ExecuteScalar();
            sysConnection.Close();

            return exists != null && exists != DBNull.Value && (long)exists == 1;
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
                Console.WriteLine($"Sqlite file seems loked. ({filePath})");
            }
            finally { }

        }

        private static void TruncateSqliteTable(string dbName, string tableName)
        {
            using var connection = new SqliteConnection(GetSqliteDatabaseConnectionString(dbName));
            connection.Open();

            using (var cmdDb = new SqliteCommand($"DELETE FROM [{tableName}];", connection))
                cmdDb.ExecuteNonQuery();

            connection.Close();
        }

        /// <summary>
        /// Drop a sqlite database
        /// </summary>
        [DebuggerStepThrough]
        private static bool ExistsSqliteDatabase(string dbName)
        {
            var filePath = GetSqliteFilePath(dbName);
            return File.Exists(filePath);
        }

        /// <summary>
        /// Delete a database
        /// </summary>
        private static void DropSqlDatabase(string dbName)
        {
            using var masterConnection = new SqlConnection(GetSqlDatabaseConnectionString("master"));
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
        private static void TruncateSqlTable(string dbName, string tableName, string schemaName)
        {
            using var connection = new SqlConnection(GetSqlDatabaseConnectionString(dbName));
            connection.Open();

            schemaName = string.IsNullOrEmpty(schemaName) ? "dbo" : schemaName;

            using (var cmdDb = new SqlCommand($"DELETE FROM [{schemaName}].[{tableName}];", connection))
                cmdDb.ExecuteNonQuery();

            connection.Close();
        }

        /// <summary>
        /// Delete a database
        /// </summary>
        private static bool ExistsSqlDatabase(string dbName)
        {
            using var masterConnection = new SqlConnection(GetSqlDatabaseConnectionString("master"));
            masterConnection.Open();

            using var cmdDb = new SqlCommand($"Select * from sys.databases where name = '{dbName}'", masterConnection);

            var exists = cmdDb.ExecuteScalar();

            masterConnection.Close();
            return exists != null && exists != DBNull.Value && (long)exists == 1;
        }

        public static Task ExecuteScriptAsync(ProviderType providerType, string dbName, string script)
        {
            switch (providerType)
            {
                case ProviderType.MySql:
                    return ExecuteMySqlScriptAsync(dbName, script);
                case ProviderType.MariaDB:
                    return ExecuteMariaDBScriptAsync(dbName, script);
                case ProviderType.Sqlite:
                    return ExecuteSqliteScriptAsync(dbName, script);
                case ProviderType.Postgres:
                    return ExecutePostgreSqlScriptAsync(dbName, script);
                case ProviderType.Sql:
                default:
                    return ExecuteSqlScriptAsync(dbName, script);
            }
        }

        private static async Task ExecuteMariaDBScriptAsync(string dbName, string script)
        {
            using var connection = new MySqlConnection(GetMariaDBDatabaseConnectionString(dbName));
            connection.Open();

            using (var cmdDb = new MySqlCommand(script, connection))
                await cmdDb.ExecuteNonQueryAsync();

            connection.Close();
        }
        private static async Task ExecuteMySqlScriptAsync(string dbName, string script)
        {
            using var connection = new MySqlConnection(GetMySqlDatabaseConnectionString(dbName));
            connection.Open();

            using (var cmdDb = new MySqlCommand(script, connection))
                await cmdDb.ExecuteNonQueryAsync();

            connection.Close();
        }
        private static async Task ExecuteSqlScriptAsync(string dbName, string script)
        {
            using var connection = new SqlConnection(GetSqlDatabaseConnectionString(dbName));
            connection.Open();

            //split the script on "GO" commands
            var splitter = new string[] { "\r\nGO\r\n" };
            var commandTexts = script.Split(splitter, StringSplitOptions.RemoveEmptyEntries);

            foreach (var commandText in commandTexts)
            {
                using var cmdDb = new SqlCommand(commandText, connection);
                await cmdDb.ExecuteNonQueryAsync();
            }
            connection.Close();
        }
        private static async Task ExecuteSqliteScriptAsync(string dbName, string script)
        {
            using var connection = new SqliteConnection(GetSqliteDatabaseConnectionString(dbName));
            connection.Open();
            using (var cmdDb = new SqliteCommand(script, connection))
                await cmdDb.ExecuteNonQueryAsync();
            connection.Close();
        }
        private static async Task ExecutePostgreSqlScriptAsync(string dbName, string script)
        {
            using var connection = new NpgsqlConnection(GetPostgresDatabaseConnectionString(dbName));
            connection.Open();
            using (var cmdDb = new NpgsqlCommand(script, connection))
                await cmdDb.ExecuteNonQueryAsync();
            connection.Close();
        }

        /// <summary>
        /// Backup a SQL Server database
        /// </summary>
        public static void BackupDatabase(string dbName)
        {
            if (!Directory.Exists(Path.Combine(Path.GetTempPath(), "Backup")))
                Directory.CreateDirectory(Path.Combine(Path.GetTempPath(), "Backup"));

            var localDatabasePath = Path.Combine(Path.GetTempPath(), "Backup", $"{dbName}.bak");

            var formatMediaName = $"DatabaseToolkitBackup_{dbName}";

            using var connection = new SqlConnection(GetSqlDatabaseConnectionString(dbName));

            var sql = @$"BACKUP DATABASE [{dbName}] TO DISK = N'{localDatabasePath}' WITH NAME = '{formatMediaName}'";

            connection.Open();

            using (var command = new SqlCommand(sql, connection))
            {
                command.CommandType = CommandType.Text;
                command.ExecuteNonQuery();
            }

            connection.Close();

        }

        /// <summary>
        /// Restore a sql backup file
        /// </summary>
        public static void RestoreSqlDatabase(string dbName)
        {
            var localDatabasePath = Path.Combine(Path.GetTempPath(), "Backup", $"{dbName}.bak");

            var script = $@"
                if (exists (select * from sys.databases where name = '{dbName}'))
                begin                
                    ALTER DATABASE [{dbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
                End

                RESTORE DATABASE [{dbName}] FROM  DISK = N'{localDatabasePath}' WITH  RESTRICTED_USER, NOUNLOAD, REPLACE

                ALTER DATABASE [{dbName}] SET MULTI_USER";


            using var connection = new SqlConnection(GetSqlDatabaseConnectionString("master"));
            connection.Open();

            using var cmdDb = new SqlCommand(script, connection);

            cmdDb.ExecuteNonQuery();

            connection.Close();
        }

        /// <summary>
        /// Restore a sql backup file from a server to client
        /// </summary>
        public static void RestoreSqlDatabase(string fromDbName, string toDbName)
        {
            var dataName = Path.GetFileNameWithoutExtension(toDbName) + ".mdf";
            var logName = Path.GetFileNameWithoutExtension(toDbName) + ".ldf";

            var localDatabaseBackupPath = Path.Combine(Path.GetTempPath(), "Backup", $"{fromDbName}.bak");


            var script = $@"
                if (exists (select * from sys.databases where name = '{toDbName}'))
                    begin                
                        ALTER DATABASE [{toDbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
                    End
                else
                    begin
                        CREATE DATABASE [{toDbName}]
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

                RESTORE DATABASE [{toDbName}] FROM  DISK = N'{localDatabaseBackupPath}' WITH  RESTRICTED_USER, REPLACE,
                    MOVE '{fromDbName}' TO @dataFile,
                    MOVE '{fromDbName}_log' TO @logFile;
                ALTER DATABASE [{toDbName}] SET MULTI_USER";


            using var connection = new SqlConnection(GetSqlDatabaseConnectionString("master"));
            connection.Open();

            using (var cmdDb = new SqlCommand(script, connection))
                cmdDb.ExecuteNonQuery();

            connection.Close();
        }

        /// <summary>
        /// Gets the Create or Re-create a database script text
        /// </summary>
        private static string GetSqlCreationScript(string dbName, bool recreateDb = true)
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
	            alter database [{dbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	            drop database {dbName};
            end";

            //return $@"EXEC msdb.dbo.sp_delete_database_backuphistory @database_name = N'{dbName}'; " +
            //         $"alter database [{dbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; " + 
            //         $"drop database {dbName};";
        }

    }
}
