using Dotmim.Sync.Tests.Core;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
using System.Linq;

namespace Dotmim.Sync.Tests
{
    /// <summary>
    /// Setup class is all you need to setup connection string, tables and client enabled for your provider tests
    /// </summary>
    public class Setup
    {

        /// <summary>
        /// Configure a provider fixture
        /// </summary>
        internal static void OnConfiguring<T>(ProviderFixture<T> providerFixture) where T : CoreProvider
        {

            // Set tables to be used for your provider
            var sqlTables = new string[]
            {
                "SalesLT.ProductCategory", "SalesLT.ProductModel", "SalesLT.Product", "Customer", "Address", "CustomerAddress",
                "SalesLT.SalesOrderHeader", "SalesLT.SalesOrderDetail", "dbo.Sql", "Posts", "Tags", "PostTag"
            };

            var mySqlTables = new string[]
            {
                "productcategory", "productmodel", "product", "customer", "address","customeraddress",
                "salesorderheader", "salesorderdetail", "sql", "posts", "tags", "posttag"
            };

            // 1) Add database name
            providerFixture.AddDatabaseName(ProviderType.Sql, "SqlAdventureWorks");
            providerFixture.AddDatabaseName(ProviderType.MySql, "mysqladventureworks");

            // 2) Add tables
            providerFixture.AddTables(ProviderType.Sql, sqlTables);
            providerFixture.AddTables(ProviderType.MySql, mySqlTables);

            // 3) Add runs

            // Enable the test to run on TCP / HTTP and on various client
            // Example : EnableClientType((ProviderType.Sql, NetworkType.Tcp), ProviderType.Sql | ProviderType.MySql | ProviderType.Sqlite)
            // 1st arg : (NetworkType.Tcp, ProviderType.Sql) : For a server provider SQL and on TCP
            // 2nd arg : ProviderType.Sql | ProviderType.MySql | ProviderType.Sqlite : Enable tests on clients of type Sql, MySql and Sqlite

            // SQL Server provider

            providerFixture.AddRun((ProviderType.Sql, NetworkType.Tcp),
                    ProviderType.Sql |
                    ProviderType.Sqlite);

            providerFixture.AddRun((ProviderType.Sql, NetworkType.Http),
                    ProviderType.MySql |
                    ProviderType.Sqlite);

            // My SQL (disable http to go faster on app veyor)
            providerFixture.AddRun((ProviderType.MySql, NetworkType.Tcp),
                    ProviderType.MySql |
                    ProviderType.Sqlite);

            providerFixture.AddRun((ProviderType.MySql, NetworkType.Http),
                    ProviderType.Sql |
                    ProviderType.Sqlite);
        }

        /// <summary>
        /// Returns the database server to be used in the untittests - note that this is the connection to appveyor SQL Server 2016 instance!
        /// see: https://www.appveyor.com/docs/services-databases/#mysql
        /// </summary>
        internal static String GetSqlDatabaseConnectionString(string dbName)
        {
            // check if we are running localy on windows or linux
            bool isWindowsRuntime = RuntimeInformation.IsOSPlatform(OSPlatform.Windows);

            if (IsOnAppVeyor)
                return $@"Server=(local)\SQL2016;Database={dbName};UID=sa;PWD=Password12!";
            else if (IsOnAzureDev)
                return $@"Data Source=localhost;Initial Catalog={dbName};User Id=SA;Password=Password12!";
            else if (isWindowsRuntime)
                return $@"Data Source=(localdb)\MSSQLLocalDB;Initial Catalog={dbName};Integrated Security=true;";
            else
                return $@"Data Source=localhost; Database={dbName}; User=sa; Password=Password12!";
        }

        /// <summary>
        /// Returns the database server to be used in the untittests - note that this is the connection to appveyor MySQL 5.7 x64 instance!
        /// see: https://www.appveyor.com/docs/services-databases/#mysql
        /// </summary>
        internal static string GetMySqlDatabaseConnectionString(string dbName)
        {
            var cs = "";
            if (IsOnAppVeyor)
                cs = $@"Server=127.0.0.1; Port=3306; Database={dbName}; Uid=root; Pwd=Password12!";
            else if (IsOnAzureDev)
                cs = $@"Server={Environment.GetEnvironmentVariable("MYSQLIP")}; Port=3306; Database={dbName}; Uid=root; Pwd=Password12!";
            else
                cs = $@"Server=127.0.0.1; Port=3306; Database={dbName}; Uid=root; Pwd=azerty31$;";

            return cs;
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
                return !String.IsNullOrEmpty(isOnAppVeyor) && isOnAppVeyor.ToLowerInvariant() == "true";
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
                return !String.IsNullOrEmpty(isOnAzureDev) && isOnAzureDev.ToLowerInvariant() == "true";
            }
        }

    }
}
