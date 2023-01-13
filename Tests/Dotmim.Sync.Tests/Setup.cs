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
using Npgsql;
using Dotmim.Sync.Tests.Fixtures;
using Dotmim.Sync.Tests.IntegrationTests;
using Dotmim.Sync.Tests.Misc;
using System.Collections.Generic;
using Xunit.Abstractions;
using System.Xml.Linq;
using Dotmim.Sync.SqlServer;
using Dotmim.Sync.Tests.Models;

namespace Dotmim.Sync.Tests
{
    /// <summary>
    /// Setup class is all you need to setup connection string, tables and client enabled for your provider tests
    /// </summary>
    public class Setup
    {

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
        public static bool IsOnAzureDev
        {
            get
            {
                // check if we are running on appveyor or not
                string isOnAzureDev = Environment.GetEnvironmentVariable("AZUREDEV");
                return !string.IsNullOrEmpty(isOnAzureDev) && isOnAzureDev.ToLowerInvariant() == "true";
            }
        }

    }

    public class SqlServerChangeTrackingTcpTests : TcpTests
    {
        public SqlServerChangeTrackingTcpTests(ITestOutputHelper output, DatabaseServerFixture fixture) : base(output, fixture)
        {
        }

        public override IEnumerable<CoreProvider> GetClientProviders()
        {
            yield return HelperDatabase.GetSyncProvider(ProviderType.Sqlite, "tcp_cli_sqlite", false);
            yield return HelperDatabase.GetSyncProvider(ProviderType.Sql, "tcp_cli_sqlct_adv", true);
        }

        public override CoreProvider GetServerProvider()
        {
            var cstring = HelperDatabase.GetSqlDatabaseConnectionString("tcp_srv_sqlct_adv");
            var provider = new SqlSyncChangeTrackingProvider(cstring);
            provider.UseFallbackSchema(true);
            return provider;
        }
    }

    public class SqlServerTcpTests : TcpTests
    {
        public SqlServerTcpTests(ITestOutputHelper output, DatabaseServerFixture fixture) : base(output, fixture)
        {
        }

        public override IEnumerable<CoreProvider> GetClientProviders()
        {
            yield return HelperDatabase.GetSyncProvider(ProviderType.Sqlite, "tcp_cli_sqlite");
            yield return HelperDatabase.GetSyncProvider(ProviderType.Sql, "tcp_cli_sql_adv", true);
        }

        public override CoreProvider GetServerProvider() => HelperDatabase.GetSyncProvider(ProviderType.Sql, "tcp_srv_sql_adv", true);
    }

    public class PostgresTcpTests : TcpTests
    {
        public PostgresTcpTests(ITestOutputHelper output, DatabaseServerFixture fixture) : base(output, fixture)
        {
        }

        public override IEnumerable<CoreProvider> GetClientProviders()
        {
            yield return HelperDatabase.GetSyncProvider(ProviderType.Sqlite, "tcp_cli_sqlite_adv", false);
            yield return HelperDatabase.GetSyncProvider(ProviderType.Postgres, "tcp_cli_npg_advd", true);
        }

        public override CoreProvider GetServerProvider() => HelperDatabase.GetSyncProvider(ProviderType.Postgres, "tcp_srv_npg_adv", true);
    }



    public class MySqlTcpTests : TcpTests
    {
        public MySqlTcpTests(ITestOutputHelper output, DatabaseServerFixture fixture) : base(output, fixture)
        {
        }

        public override IEnumerable<CoreProvider> GetClientProviders()
        {
            yield return HelperDatabase.GetSyncProvider(ProviderType.Sqlite, "tcp_cli_sqlite_adv");
            yield return HelperDatabase.GetSyncProvider(ProviderType.MySql, "tcp_cli_mysql_adv");
        }

        public override CoreProvider GetServerProvider() => HelperDatabase.GetSyncProvider(ProviderType.MySql, "tcp_srv_mysql_adv");
    }


    public class MariaDBTcpTests : TcpTests
    {
        public MariaDBTcpTests(ITestOutputHelper output, DatabaseServerFixture fixture) : base(output, fixture)
        {
        }

        public override IEnumerable<CoreProvider> GetClientProviders()
        {
            yield return HelperDatabase.GetSyncProvider(ProviderType.Sqlite, "tcp_cli_sqlite");
            yield return HelperDatabase.GetSyncProvider(ProviderType.MariaDB, "tcp_cli_maria_adv");
        }

        public override CoreProvider GetServerProvider() => HelperDatabase.GetSyncProvider(ProviderType.MariaDB, "tcp_srv_maria_adv");
    }

}
