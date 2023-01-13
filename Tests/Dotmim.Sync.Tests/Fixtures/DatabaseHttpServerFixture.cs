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
//using static Microsoft.EntityFrameworkCore.DbLoggerCategory.Database;

//namespace Dotmim.Sync.Tests.Fixtures
//{
//    public class DatabaseHttpServerFixture<T> : DatabaseServerFixture<T>, IDisposable where T : RelationalFixture
//    {
//        public DatabaseHttpServerFixture() : base() { }

//        public override List<ProviderType> ClientsType => new List<ProviderType> { 
//            ProviderType.Sqlite, 
//            typeof(T) == typeof(SqlServerFixtureType) ? ProviderType.Postgres : ProviderType.Sql };
//    }
//}
