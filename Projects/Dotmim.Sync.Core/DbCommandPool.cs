//using System;
//using System.Collections.Concurrent;
//using System.Collections.Generic;
//using System.Data.Common;
//using System.Text;

//namespace Dotmim.Sync
//{
//    public static class DbCommandPool
//    {

//        // 'GetOrAdd' call on the dictionary is not thread safe and we might end up creating the pipeline more
//        // once. To prevent this Lazy<> is used. In the worst case multiple Lazy<> objects are created for multiple
//        // threads but only one of the objects succeeds 
//        // See https://andrewlock.net/making-getoradd-on-concurrentdictionary-thread-safe-using-lazy/
//        private static ConcurrentDictionary<string, Lazy<DbCommand>> commands = new ConcurrentDictionary<string, Lazy<DbCommand>>();

//        /// <summary>
//        /// Get a DBCommand thanks to the key. If not available, create a new DBCommand and return it
//        /// </summary>
//        public static DbCommand GetPooledCommand(string key, Func<DbCommand> valueFactory)
//        {
//            // Try to get the instance
//            var parserStringRetrieved = commands.GetOrAdd(key, k =>
//                new Lazy<DbCommand>(valueFactory));

//            return parserStringRetrieved.Value;
//        }


//    }
//}
