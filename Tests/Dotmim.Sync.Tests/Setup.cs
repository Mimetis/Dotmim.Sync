using Dotmim.Sync.Data;
using Dotmim.Sync.Filter;
using Dotmim.Sync.Tests.Core;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
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
                "SalesLT.ProductCategory", "SalesLT.ProductModel", "SalesLT.Product", "Employee", "Customer", "Address", "CustomerAddress", "EmployeeAddress",
                "SalesLT.SalesOrderHeader", "SalesLT.SalesOrderDetail", "dbo.Sql", "Posts", "Tags", "PostTag"
            };

            var mySqlTables = new string[]
            {
                "ProductCategory", "ProductModel", "Product", "Customer", "Address", "CustomerAddress",
                "SalesOrderHeader", "SalesOrderDetail", "Sql", "Posts", "Tags", "PostTag"
            };

            // 1) Add database name
            providerFixture.AddDatabaseName(ProviderType.Sql, "SqlAdventureWorks");
            providerFixture.AddDatabaseName(ProviderType.MySql, "mysqladventureworks");

            // 2) Add tables
            providerFixture.AddTables(ProviderType.Sql, sqlTables);
            providerFixture.AddTables(ProviderType.MySql, mySqlTables);


            //// 3) Add filters
            //providerFixture.AddFilter(ProviderType.Sql,
            //    new FilterClause("Employee", "EmployeeID"));
            //providerFixture.AddFilter(ProviderType.Sql,
            //    new FilterClause("Customer", "EmployeeID"));

            //providerFixture.AddFilterParameter(ProviderType.Sql,
            //    new SyncParameter("Employee", "EmployeeID", 1));

            //providerFixture.AddFilterParameter(ProviderType.Sql,
            //    new SyncParameter("Customer", "EmployeeID", 1));

            // 3) Add runs

            // SQL Server provider

            if (IsOnAzureDev)
            {
                providerFixture.AddRun((ProviderType.Sql, NetworkType.Tcp),
                        ProviderType.Sql | ProviderType.Sqlite);

                providerFixture.AddRun((ProviderType.Sql, NetworkType.Http),
                        ProviderType.MySql |
                        ProviderType.Sqlite);

                // My SQL (disable http to go faster on app veyor)
                providerFixture.AddRun((ProviderType.MySql, NetworkType.Tcp),
                        ProviderType.MySql);

                providerFixture.AddRun((ProviderType.MySql, NetworkType.Http),
                        ProviderType.MySql);
            }
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
                cs = $@"Server=127.0.0.1; Port=3307; Database={dbName}; Uid=root; Pwd=Password12!";
            else
                cs = $@"Server=127.0.0.1; Port=3306; Database={dbName}; Uid=root; Pwd=azerty31$";
            //cs = $@"Server=127.0.0.1; Port=3307; Database={dbName}; Uid=root; Pwd=Password12!";

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
