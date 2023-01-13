//using Dotmim.Sync.SqlServer;
//using Dotmim.Sync.Tests.Core;
//using Dotmim.Sync.Tests.Models;
//using Microsoft.AspNetCore.Hosting.Server;
//using Microsoft.Data.SqlClient;
//using Microsoft.EntityFrameworkCore;
//using System;
//using System.Collections.Generic;
//using System.Data.Common;
//using System.Diagnostics;
//using System.Linq;
//using System.Security.Cryptography.X509Certificates;
//using System.Text;
//using System.Threading.Tasks;
//using System.Transactions;
//using System.Xml.Linq;
//using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;

//namespace Dotmim.Sync.Tests.Fixtures
//{
//    public class DatabaseServerChangeTrackingFixture<T> : DatabaseServerFixture<T>, IDisposable where T : RelationalFixture
//    {
//        public DatabaseServerChangeTrackingFixture() : base() { }

//        public override List<ProviderType> ClientsType => new List<ProviderType> { ProviderType.Sql, ProviderType.Sqlite, ProviderType.Postgres };

//        public override CoreProvider GetServerProvider() => new SqlSyncProvider(Setup.GetSqlDatabaseConnectionString(ServerDatabaseName));

//    }
//}
