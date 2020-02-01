using Dotmim.Sync.Tests.Core;
using MySql.Data.MySqlClient;
using System;
using System.Data.SqlClient;
using System.Runtime.InteropServices;

namespace Dotmim.Sync.Tests
{
    /// <summary>
    /// Setup class is all you need to setup connection string, tables and client enabled for your provider tests
    /// </summary>
    public class Setup
    {
        static Setup()
        {
        }

        /// <summary>
        /// Returns the database server to be used in the untittests - note that this is the connection to appveyor SQL Server 2016 instance!
        /// see: https://www.appveyor.com/docs/services-databases/#mysql
        /// </summary>
        internal static string GetSqlDatabaseConnectionString(string dbName)
        {
            // check if we are running localy on windows or linux
            bool isWindowsRuntime = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);

            string userId = "sa";
            string password = "Password12!";

            var builder = new SqlConnectionStringBuilder
            {
                UserID = userId,
                Password = password,
                DataSource = "localhost",
                InitialCatalog = dbName,
                PersistSecurityInfo = false,
                MultipleActiveResultSets = false,
                TrustServerCertificate = false,
                ConnectTimeout = 30,
                IntegratedSecurity = false,
                ConnectRetryCount = 4,
                ConnectRetryInterval = 4
            };


            if (IsOnAppVeyor)
            {
                builder.DataSource = @"(local)\SQL2016";
            }
            else if (IsOnAzureDev)
            {
                builder.DataSource = @"localhost";
            }
            else if (isWindowsRuntime)
            {
                builder.DataSource = @"(localdb)\MSSQLLocalDB";
                builder.IntegratedSecurity = true;
            }

            return builder.ToString();

        }

        internal static string GetSqlAzureDatabaseConnectionString(string dbName)
        {
            string userId = "";
            string password = "";

            var builder = new SqlConnectionStringBuilder
            {
                UserID = userId,
                Password = password,
                DataSource = "tcp:spertus.database.windows.net",
                InitialCatalog = dbName,
                PersistSecurityInfo = false,
                MultipleActiveResultSets = false,
                TrustServerCertificate = false,
                ConnectTimeout = 30
            };

            return builder.ToString();
        }

        /// <summary>
        /// Returns the database server to be used in the untittests - note that this is the connection to appveyor MySQL 5.7 x64 instance!
        /// see: https://www.appveyor.com/docs/services-databases/#mysql
        /// </summary>
        internal static string GetMySqlDatabaseConnectionString(string dbName)
        {

            var builder = new MySqlConnectionStringBuilder
            {
                Server = "127.0.0.1",
                Port = 3306,
                UserID = "root",
                Password = "Password12!",
                Database = dbName
            };

            if (IsOnAppVeyor)
                builder.Port = 3306;
            else if (IsOnAzureDev)
                builder.Port = 3307;
            else
                builder.Port = 3307;

            var cn = builder.ToString();
            return cn;
        }

        /// <summary>
        /// Gets if the tests are running on AppVeyor
        /// </summary>
        internal static bool IsOnAppVeyor
        {
            get
            {
                // check if we are running on appveyor or not
                string isOnAppVeyor = Environment.GetEnvironmentVariable("APPVEYOR");
                return !string.IsNullOrEmpty(isOnAppVeyor) && isOnAppVeyor.ToLowerInvariant() == "true";
            }
        }

        /// <summary>
        /// Gets if the tests are running on Azure Dev
        /// </summary>
        internal static bool IsOnAzureDev
        {
            get
            {
                // check if we are running on appveyor or not
                string isOnAzureDev = Environment.GetEnvironmentVariable("AZUREDEV");
                return !string.IsNullOrEmpty(isOnAzureDev) && isOnAzureDev.ToLowerInvariant() == "true";
            }
        }

    }
}
