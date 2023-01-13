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

//    public class DatabaseServerFixture<T> : IDisposable where T : RelationalFixture
//    {
//        public virtual List<ProviderType> ClientsType => new List<ProviderType> {
//            HelperDatabase.GetProviderType<T>(),
//            ProviderType.Sqlite,
//            typeof(T) == typeof(SqlServerFixtureType) ? ProviderType.Postgres : ProviderType.Sql };

//        public virtual ProviderType ServerProviderType => HelperDatabase.GetProviderType<T>();

//        // SQL Server has schema on server database
//        protected string salesSchema = typeof(T) == typeof(SqlServerFixtureType) || typeof(T) == typeof(PostgresFixtureType) ? "SalesLT." : "";

//        public Stopwatch OverallStopwatch { get; }

//        public virtual string[] Tables => new string[]
//        {
//            $"{salesSchema}ProductCategory", $"{salesSchema}ProductModel", $"{salesSchema}Product",
//            "Employee", "Customer", "Address", "CustomerAddress", "EmployeeAddress",
//            $"{salesSchema}SalesOrderHeader", $"{salesSchema}SalesOrderDetail",
//            "Posts", "Tags", "PostTag",
//            "PricesList", "PricesListCategory", "PricesListDetail", "Log"
//        };

//        public virtual SyncSetup GetSyncSetup() => new SyncSetup(Tables);

//        public virtual bool UseFallbackSchema => typeof(T) == typeof(SqlServerFixtureType) || typeof(T) == typeof(PostgresFixtureType);

//        public string ServerDatabaseName { get; set; }

//        public Dictionary<ProviderType, string> ClientDatabaseNames { get; set; } = new Dictionary<ProviderType, string>();


//        public DatabaseServerFixture()
//        {
//            this.OverallStopwatch = Stopwatch.StartNew();
//            this.ServerDatabaseName = HelperDatabase.GetRandomName("tcp_srv");

//            foreach (var type in this.ClientsType)
//                ClientDatabaseNames.Add(type, HelperDatabase.GetRandomName("tcp_cli"));
//        }

//        /// <summary>
//        /// Get the server provider. Creates database if not exists
//        /// </summary>
//        public virtual CoreProvider GetServerProvider() => HelperDatabase.GetSyncProvider(ServerProviderType, ServerDatabaseName);

//        /// <summary>
//        /// Returns all clients providers. Create database if not exists
//        /// </summary>
//        public virtual IEnumerable<CoreProvider> GetClientProviders()
//        {
//            foreach (var type in this.ClientsType)
//                yield return HelperDatabase.GetSyncProvider(type, ClientDatabaseNames[type]);
//        }


//        public void Dispose()
//        {

//            HelperDatabase.ClearPool(ServerProviderType);

//            foreach (var tvp in ClientDatabaseNames)
//                HelperDatabase.ClearPool(tvp.Key);

//            foreach (var tvp in ClientDatabaseNames)
//                HelperDatabase.DropDatabase(tvp.Key, tvp.Value);

//            HelperDatabase.DropDatabase(ServerProviderType, ServerDatabaseName);

//            this.OverallStopwatch.Stop();

//        }



//    }
//}
