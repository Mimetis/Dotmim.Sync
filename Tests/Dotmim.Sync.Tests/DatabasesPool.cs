//using Dotmim.Sync.Tests.Core;
//using System;
//using System.Collections.Concurrent;
//using System.Collections.Generic;
//using System.Linq;
//using System.Text;

//namespace Dotmim.Sync.Tests
//{
//    public class DatabasesPooling
//    {
//        private static ConcurrentDictionary<string, Lazy<DatabasePool>> pool = new ConcurrentDictionary<string, Lazy<DatabasePool>>();

//        private static DatabasePool CreateDatabasePool(string name, ProviderType providerType)
//        {
//            var database = new DatabasePool
//            {
//                DatabaseName = name,
//                ProviderType = providerType,
//                InUse = false,
//            };

//            return database;
//        }

//        public static Lazy<DatabasePool> GetDatabaseFromPool(string name, ProviderType providerType)
//        {
//            // Try to get the instance
//            string key = $"{name}-{providerType}";

//            var databasePoolRetrived = pool.GetOrAdd(key, k => new Lazy<DatabasePool>(() => CreateDatabasePool(name, providerType)));

//            return databasePoolRetrived;
//        }

//        public static Lazy<DatabasePool> GetADatabaseFromPool(ProviderType providerType)
//        {
//            // Check if any database is created but not in use
//            var lazyDb = pool.FirstOrDefault(kvp => kvp.Value.Value.ProviderType == providerType && kvp.Value.Value.InUse == false);

//            // Get from the pool otherwise create a new one
//            if (lazyDb.Value == default)
//            {
//                var databasePoolRetrived = pool.GetOrAdd(lazyDb.Key, k => new Lazy<DatabasePool>(() =>
//                {
//                    var databaseName = HelperDatabase.GetRandomName("tst");
//                    var poolDatabase = CreateDatabasePool(databaseName, providerType);
//                    return poolDatabase;
//                }));
//            }
//            else
//            {

//            }

//            return databasePoolRetrived;
//        }

//    }



//    public class DatabasePool
//    {
//        public string DatabaseName { get; set; }
//        public ProviderType ProviderType { get; set; }
//        public bool InUse { get; set; }


//    }

//}
