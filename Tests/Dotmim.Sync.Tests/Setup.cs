using Dotmim.Sync.Tests.Core;
#if NET5_0 || NET6_0 || NET7_0 || NETCOREAPP3_1
using MySqlConnector;
#elif NETCOREAPP2_1
using MySql.Data.MySqlClient;
#endif

using System;
using Microsoft.Data.SqlClient;
using System.Runtime.InteropServices;
using Microsoft.Extensions.Configuration;

namespace Dotmim.Sync.Tests
{
    /// <summary>
    /// Setup class is all you need to setup connection string, tables and client enabled for your provider tests
    /// </summary>
    public class Setup
    {
        private static IConfigurationRoot configuration;

        static Setup()
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

            if (IsOnAzureDev)
            {
                builder.IntegratedSecurity = false;
                builder.DataSource = @"localhost";
                builder.UserID = "sa";
                builder.Password = "Password12!";
                builder.TrustServerCertificate = true;
            }
            Console.WriteLine(builder.ToString());
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

            var builder = new MySqlConnectionStringBuilder(cstring);

            if (IsOnAzureDev)
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

            var builder = new MySqlConnectionStringBuilder(cstring);

            if (IsOnAzureDev)
            {
                builder.Port = 3308;
                builder.UserID = "root";
                builder.Password = "Password12!";
            }

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
