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
using Dotmim.Sync.Tests.UnitTests;

namespace Dotmim.Sync.Tests
{
    /// <summary>
    /// Setup class is all you need to setup connection string, tables and client enabled for your provider tests
    /// </summary>
    public class Setup
    {

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


    public class SqlServerUnitTests : InterceptorsTests
    {
        public SqlServerUnitTests(ITestOutputHelper output, DatabaseServerFixture fixture) : base(output, fixture)
        {
        }

        public override ProviderType ServerProviderType => ProviderType.Sql;

        private string sqlRandomDatabaseName = HelperDatabase.GetRandomName("ut1_sql_");
        public override IEnumerable<CoreProvider> GetClientProviders()
        {
            yield return HelperDatabase.GetSyncProvider(ProviderType.Sql, sqlRandomDatabaseName, false);
        }

    }

    public class SqlServerUnitLocalOrchestratorTests : LocalOrchestratorTests
    {
        public SqlServerUnitLocalOrchestratorTests(ITestOutputHelper output, DatabaseServerFixture fixture) : base(output, fixture)
        {
        }
        public override ProviderType ServerProviderType => ProviderType.Sql;

        private string sqlRandomDatabaseName = HelperDatabase.GetRandomName("ut2_sql_");
        public override IEnumerable<CoreProvider> GetClientProviders()
        {
            yield return HelperDatabase.GetSyncProvider(ProviderType.Sql, sqlRandomDatabaseName, false);
        }
    }

    public class SqlServerUnitRemoteOrchestratorTests : RemoteOrchestratorTests
    {
        public SqlServerUnitRemoteOrchestratorTests(ITestOutputHelper output, DatabaseServerFixture fixture) : base(output, fixture)
        {
        }
        public override ProviderType ServerProviderType => ProviderType.Sql;
        
        private string sqlRandomDatabaseName = HelperDatabase.GetRandomName("ut3_sql_");
        public override IEnumerable<CoreProvider> GetClientProviders()
        {
            yield return HelperDatabase.GetSyncProvider(ProviderType.Sql, sqlRandomDatabaseName, false);
        }

    }

    public class SqlServerChangeTrackingTcpTests : TcpTests
    {
        public SqlServerChangeTrackingTcpTests(ITestOutputHelper output, DatabaseServerFixture fixture) : base(output, fixture)
        {
        }
        public override ProviderType ServerProviderType => ProviderType.Sql;

        private string sqliteRandomDatabaseName = HelperDatabase.GetRandomName("tcpct_sqlite_");
        private string sqlClientRandomDatabaseName = HelperDatabase.GetRandomName("tcpct_sql_");
        private string sqlServerRandomDatabaseName = HelperDatabase.GetRandomName("tcpct_sql_");

        public override IEnumerable<CoreProvider> GetClientProviders()
        {
            yield return HelperDatabase.GetSyncProvider(ProviderType.Sqlite, sqliteRandomDatabaseName, false);
            yield return HelperDatabase.GetSyncProvider(ProviderType.Sql, sqlClientRandomDatabaseName, true);
        }

        public override CoreProvider GetServerProvider()
        {
            var cstring = HelperDatabase.GetSqlDatabaseConnectionString(sqlServerRandomDatabaseName);
            var provider = new SqlSyncChangeTrackingProvider(cstring);
            provider.UseFallbackSchema(true);
            return provider;
        }
    }

    public class SqlServerChangeTrackingTcpFilterTests : TcpFilterTests
    {
        public SqlServerChangeTrackingTcpFilterTests(ITestOutputHelper output, DatabaseServerFixture fixture) : base(output, fixture)
        {
        }

        public override ProviderType ServerProviderType => ProviderType.Sql;

        private string sqliteRandomDatabaseName = HelperDatabase.GetRandomName("tcpctf_sqlite_");
        private string sqlClientRandomDatabaseName = HelperDatabase.GetRandomName("tcpctf_sql_");
        private string sqlServerRandomDatabaseName = HelperDatabase.GetRandomName("tcpctf_sql_");

        public override IEnumerable<CoreProvider> GetClientProviders()
        {
            yield return HelperDatabase.GetSyncProvider(ProviderType.Sqlite, sqliteRandomDatabaseName, false);
            yield return HelperDatabase.GetSyncProvider(ProviderType.Sql, sqlClientRandomDatabaseName, true);
        }

        public override CoreProvider GetServerProvider()
        {
            var cstring = HelperDatabase.GetSqlDatabaseConnectionString(sqlServerRandomDatabaseName);
            var provider = new SqlSyncChangeTrackingProvider(cstring);
            provider.UseFallbackSchema(true);
            return provider;
        }

    }

    public class SqlServerChangeTrackingHttpFilterTests : HttpTests
    {
        public SqlServerChangeTrackingHttpFilterTests(ITestOutputHelper output, DatabaseServerFixture fixture) : base(output, fixture)
        {
        }

        public override ProviderType ServerProviderType => ProviderType.Sql;

        private string sqliteRandomDatabaseName = HelperDatabase.GetRandomName("httpctf_sqlite_");
        private string sqlClientRandomDatabaseName = HelperDatabase.GetRandomName("httpctf_sql_");
        private string sqlServerRandomDatabaseName = HelperDatabase.GetRandomName("httpctf_sql_");

        public override IEnumerable<CoreProvider> GetClientProviders()
        {
            yield return HelperDatabase.GetSyncProvider(ProviderType.Sqlite, sqliteRandomDatabaseName, false);
            yield return HelperDatabase.GetSyncProvider(ProviderType.Sql, sqlClientRandomDatabaseName, true);
        }

        public override CoreProvider GetServerProvider()
        {
            var cstring = HelperDatabase.GetSqlDatabaseConnectionString(sqlServerRandomDatabaseName);
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
        public override ProviderType ServerProviderType => ProviderType.Sql;

        private string sqliteRandomDatabaseName = HelperDatabase.GetRandomName("tcp_sqlite_");
        private string sqlClientRandomDatabaseName = HelperDatabase.GetRandomName("tcp_sql_");

        public override IEnumerable<CoreProvider> GetClientProviders()
        {
            yield return HelperDatabase.GetSyncProvider(ProviderType.Sqlite, sqliteRandomDatabaseName, false);
            yield return HelperDatabase.GetSyncProvider(ProviderType.Sql, sqlClientRandomDatabaseName, true);
        }

    }

    public class SqlServerTcpFilterTests : TcpFilterTests
    {
        public SqlServerTcpFilterTests(ITestOutputHelper output, DatabaseServerFixture fixture) : base(output, fixture)
        {
        }
        public override ProviderType ServerProviderType => ProviderType.Sql;

        private string sqliteRandomDatabaseName = HelperDatabase.GetRandomName("tcpf_sqlite_");
        private string sqlClientRandomDatabaseName = HelperDatabase.GetRandomName("tcpf_sql_");

        public override IEnumerable<CoreProvider> GetClientProviders()
        {
            yield return HelperDatabase.GetSyncProvider(ProviderType.Sqlite, sqliteRandomDatabaseName, false);
            yield return HelperDatabase.GetSyncProvider(ProviderType.Sql, sqlClientRandomDatabaseName, true);
        }

    }

    public class SqlServerHttpTests : HttpTests
    {
        public SqlServerHttpTests(ITestOutputHelper output, DatabaseServerFixture fixture) : base(output, fixture)
        {
        }
        public override ProviderType ServerProviderType => ProviderType.Sql;

        private string sqliteRandomDatabaseName = HelperDatabase.GetRandomName("http_sqlite_");
        private string sqlClientRandomDatabaseName = HelperDatabase.GetRandomName("http_sql_");

        public override IEnumerable<CoreProvider> GetClientProviders()
        {
            yield return HelperDatabase.GetSyncProvider(ProviderType.Sqlite, sqliteRandomDatabaseName, false);
            yield return HelperDatabase.GetSyncProvider(ProviderType.Sql, sqlClientRandomDatabaseName, true);
        }
    }

    public class SqlServerConflictTests : TcpConflictsTests
    {
        public SqlServerConflictTests(ITestOutputHelper output, DatabaseServerFixture fixture) : base(output, fixture)
        {
        }
        public override ProviderType ServerProviderType => ProviderType.Sql;

        private string sqliteRandomDatabaseName = HelperDatabase.GetRandomName("tcpc_sqlite_");
        private string sqlClientRandomDatabaseName = HelperDatabase.GetRandomName("tcpc_sql_");

        public override IEnumerable<CoreProvider> GetClientProviders()
        {
            yield return HelperDatabase.GetSyncProvider(ProviderType.Sqlite, sqliteRandomDatabaseName, false);
            yield return HelperDatabase.GetSyncProvider(ProviderType.Sql, sqlClientRandomDatabaseName, true);
        }
    }

    public class PostgresConflictTests : TcpConflictsTests
    {
        public PostgresConflictTests(ITestOutputHelper output, DatabaseServerFixture fixture) : base(output, fixture)
        {
        }
        public override ProviderType ServerProviderType => ProviderType.Postgres;

        private string sqliteRandomDatabaseName = HelperDatabase.GetRandomName("tcpc_npg_sqlite_");
        private string postgreClientRandomDatabaseName = HelperDatabase.GetRandomName("tcpc_npg_");
        
        public override IEnumerable<CoreProvider> GetClientProviders()
        {
            yield return HelperDatabase.GetSyncProvider(ProviderType.Sqlite, sqliteRandomDatabaseName, false);
            yield return HelperDatabase.GetSyncProvider(ProviderType.Postgres, postgreClientRandomDatabaseName, true);
        }
    }

    public class PostgresTcpTests : TcpTests
    {
        public PostgresTcpTests(ITestOutputHelper output, DatabaseServerFixture fixture) : base(output, fixture)
        {
        }
        public override ProviderType ServerProviderType => ProviderType.Postgres;

        private string sqliteRandomDatabaseName = HelperDatabase.GetRandomName("tcp_npg_sqlite_");
        private string postgreClientRandomDatabaseName = HelperDatabase.GetRandomName("tcp_npg_");

        public override IEnumerable<CoreProvider> GetClientProviders()
        {
            yield return HelperDatabase.GetSyncProvider(ProviderType.Sqlite, sqliteRandomDatabaseName, false);
            yield return HelperDatabase.GetSyncProvider(ProviderType.Postgres, postgreClientRandomDatabaseName, true);
        }
    }

    public class PostgresTcpFilterTests : TcpFilterTests
    {
        public PostgresTcpFilterTests(ITestOutputHelper output, DatabaseServerFixture fixture) : base(output, fixture)
        {
        }
        public override ProviderType ServerProviderType => ProviderType.Postgres;

        private string sqliteRandomDatabaseName = HelperDatabase.GetRandomName("tcfp_npg_sqlite_");
        private string postgreClientRandomDatabaseName = HelperDatabase.GetRandomName("tcpf_npg_");

        public override IEnumerable<CoreProvider> GetClientProviders()
        {
            yield return HelperDatabase.GetSyncProvider(ProviderType.Sqlite, sqliteRandomDatabaseName, false);
            yield return HelperDatabase.GetSyncProvider(ProviderType.Postgres, postgreClientRandomDatabaseName, true);
        }
    }

    public class PostgresHttpTests : HttpTests
    {
        public PostgresHttpTests(ITestOutputHelper output, DatabaseServerFixture fixture) : base(output, fixture)
        {
        }
        public override ProviderType ServerProviderType => ProviderType.Postgres;

        private string sqliteRandomDatabaseName = HelperDatabase.GetRandomName("http_npg_sqlite_");
        private string postgreClientRandomDatabaseName = HelperDatabase.GetRandomName("http_npg_");

        public override IEnumerable<CoreProvider> GetClientProviders()
        {
            yield return HelperDatabase.GetSyncProvider(ProviderType.Sqlite, sqliteRandomDatabaseName, false);
            yield return HelperDatabase.GetSyncProvider(ProviderType.Postgres, postgreClientRandomDatabaseName, true);
        }
    }


    public class MySqlTcpTests : TcpTests
    {
        public MySqlTcpTests(ITestOutputHelper output, DatabaseServerFixture fixture) : base(output, fixture)
        {
        }
        public override ProviderType ServerProviderType => ProviderType.MySql;

        private string sqliteRandomDatabaseName = HelperDatabase.GetRandomName("tcp_mysql_sqlite_");
        private string mysqlClientRandomDatabaseName = HelperDatabase.GetRandomName("tcp_mysql_");

        public override IEnumerable<CoreProvider> GetClientProviders()
        {
            yield return HelperDatabase.GetSyncProvider(ProviderType.Sqlite, sqliteRandomDatabaseName, false);
            yield return HelperDatabase.GetSyncProvider(ProviderType.MySql, mysqlClientRandomDatabaseName, false);
        }
    }

    public class MySqlTcpFilterTests : TcpFilterTests
    {
        public MySqlTcpFilterTests(ITestOutputHelper output, DatabaseServerFixture fixture) : base(output, fixture)
        {
        }

        public override ProviderType ServerProviderType => ProviderType.MySql;

        private string sqliteRandomDatabaseName = HelperDatabase.GetRandomName("tcpf_mysql_sqlite_");
        private string mysqlClientRandomDatabaseName = HelperDatabase.GetRandomName("tcpf_mysql_");

        public override IEnumerable<CoreProvider> GetClientProviders()
        {
            yield return HelperDatabase.GetSyncProvider(ProviderType.Sqlite, sqliteRandomDatabaseName, false);
            yield return HelperDatabase.GetSyncProvider(ProviderType.MySql, mysqlClientRandomDatabaseName, false);
        }
    }

    public class MySqlHttpTests : HttpTests
    {
        public MySqlHttpTests(ITestOutputHelper output, DatabaseServerFixture fixture) : base(output, fixture)
        {
        }

        public override ProviderType ServerProviderType => ProviderType.MySql;

        private string sqliteRandomDatabaseName = HelperDatabase.GetRandomName("http_mysql_sqlite_");
        private string mysqlClientRandomDatabaseName = HelperDatabase.GetRandomName("http_mysql_");

        public override IEnumerable<CoreProvider> GetClientProviders()
        {
            yield return HelperDatabase.GetSyncProvider(ProviderType.Sqlite, sqliteRandomDatabaseName, false);
            yield return HelperDatabase.GetSyncProvider(ProviderType.MySql, mysqlClientRandomDatabaseName, false);
        }
    }

    public class MySqlConflictTests : TcpConflictsTests
    {
        public MySqlConflictTests(ITestOutputHelper output, DatabaseServerFixture fixture) : base(output, fixture)
        {
        }
        public override ProviderType ServerProviderType => ProviderType.MySql;

        private string sqliteRandomDatabaseName = HelperDatabase.GetRandomName("tcpc_mysql_sqlite_");
        private string mysqlClientRandomDatabaseName = HelperDatabase.GetRandomName("tcpc_mysql_");

        public override IEnumerable<CoreProvider> GetClientProviders()
        {
            yield return HelperDatabase.GetSyncProvider(ProviderType.Sqlite, sqliteRandomDatabaseName, false);
            yield return HelperDatabase.GetSyncProvider(ProviderType.MySql, mysqlClientRandomDatabaseName, false);
        }
    }



    public class MariaDBTcpTests : TcpTests
    {
        public MariaDBTcpTests(ITestOutputHelper output, DatabaseServerFixture fixture) : base(output, fixture)
        {
        }
        public override ProviderType ServerProviderType => ProviderType.MariaDB;

        private string sqliteRandomDatabaseName = HelperDatabase.GetRandomName("tcp_maria_sqlite_");
        private string mariaClientRandomDatabaseName = HelperDatabase.GetRandomName("tcp_maria_");

        public override IEnumerable<CoreProvider> GetClientProviders()
        {
            yield return HelperDatabase.GetSyncProvider(ProviderType.Sqlite, sqliteRandomDatabaseName, false);
            yield return HelperDatabase.GetSyncProvider(ProviderType.MariaDB, mariaClientRandomDatabaseName, false);
        }
    }

    public class MariaDBTcpFilterTests : TcpFilterTests
    {
        public MariaDBTcpFilterTests(ITestOutputHelper output, DatabaseServerFixture fixture) : base(output, fixture)
        {
        }

        public override ProviderType ServerProviderType => ProviderType.MariaDB;

        private string sqliteRandomDatabaseName = HelperDatabase.GetRandomName("tcpf_maria_sqlite_");
        private string mariaClientRandomDatabaseName = HelperDatabase.GetRandomName("tcpf_maria_");

        public override IEnumerable<CoreProvider> GetClientProviders()
        {
            yield return HelperDatabase.GetSyncProvider(ProviderType.Sqlite, sqliteRandomDatabaseName, false);
            yield return HelperDatabase.GetSyncProvider(ProviderType.MariaDB, mariaClientRandomDatabaseName, false);
        }
    }

    public class MariaDBHttpTests : TcpTests
    {
        public MariaDBHttpTests(ITestOutputHelper output, DatabaseServerFixture fixture) : base(output, fixture)
        {
        }

        public override ProviderType ServerProviderType => ProviderType.MariaDB;

        private string sqliteRandomDatabaseName = HelperDatabase.GetRandomName("http_maria_sqlite_");
        private string mariaClientRandomDatabaseName = HelperDatabase.GetRandomName("http_maria_");

        public override IEnumerable<CoreProvider> GetClientProviders()
        {
            yield return HelperDatabase.GetSyncProvider(ProviderType.Sqlite, sqliteRandomDatabaseName, false);
            yield return HelperDatabase.GetSyncProvider(ProviderType.MariaDB, mariaClientRandomDatabaseName, false);
        }
    }
}
