using Dotmim.Sync.PostgreSql;
using System;

namespace ScriptTesting
{
    internal class Program
    {
        static void Main(string[] args)
        {
            var provider = new NpgsqlSyncProvider();
            provider.ConnectionString = "";


            Console.WriteLine("Hello World!");
        }
    }
}
