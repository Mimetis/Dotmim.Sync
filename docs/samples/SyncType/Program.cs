﻿using Dotmim.Sync;
using Dotmim.Sync.Enumerations;
using Dotmim.Sync.SqlServer;
using System;
using System.Threading.Tasks;

namespace HelloSync
{
    class Program
    {
        public static string GetDatabaseConnectionString(string dbName) =>
            $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog={dbName}; Integrated Security=true;";

        static async Task Main(string[] args)
        {
            await SynchronizeWithReinitializeWithUploadAsync();
        }


        private static async Task SynchronizeAsync()
        {
            // Database script used for this sample : https://github.com/Mimetis/Dotmim.Sync/blob/master/CreateAdventureWorks.sql 

            SqlSyncProvider serverProvider = new SqlSyncProvider(GetDatabaseConnectionString("AdventureWorks"));
            SqlSyncProvider clientProvider = new SqlSyncProvider(GetDatabaseConnectionString("Client"));

            SyncAgent agent = new SyncAgent(clientProvider, serverProvider, new string[] {
                "ProductCategory", "ProductModel", "Product", "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail"});

            Console.WriteLine(await agent.SynchronizeAsync());
            Console.WriteLine("End");
        }

        private static async Task SynchronizeWithReinitializeAsync()
        {
            // Database script used for this sample : https://github.com/Mimetis/Dotmim.Sync/blob/master/CreateAdventureWorks.sql 
            // To illustrate this example, you should make a first call to 'private static async Task SynchronizeAsync()' to have 2 databases synced
            // Then make an update in one of your Client database table, like updating the firstname of one customer
            // Then call this method, and see you client database is reinitialized correctly, but the customer name is lost, and reinitialized with the value from server

            SqlSyncProvider serverProvider = new SqlSyncProvider(GetDatabaseConnectionString("AdventureWorks"));
            SqlSyncProvider clientProvider = new SqlSyncProvider(GetDatabaseConnectionString("Client"));

            SyncAgent agent = new SyncAgent(clientProvider, serverProvider, new string[] {
                "ProductCategory", "ProductModel", "Product", "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail"});

            Console.WriteLine(await agent.SynchronizeAsync(SyncType.Reinitialize));
            Console.WriteLine("End");
        }


        private static async Task SynchronizeWithReinitializeWithUploadAsync()
        {
            // Database script used for this sample : https://github.com/Mimetis/Dotmim.Sync/blob/master/CreateAdventureWorks.sql 
            // To illustrate this example, you should make a first call to 'private static async Task SynchronizeAsync()' to have 2 databases synced
            // Then make an update in one of your Client database table, like updating the firstname of one customer
            // Then call this method, and see you client database is reinitialized correctly, but the customer name has been uploaded to the server, and synced


            SqlSyncProvider serverProvider = new SqlSyncProvider(GetDatabaseConnectionString("AdventureWorks"));
            SqlSyncProvider clientProvider = new SqlSyncProvider(GetDatabaseConnectionString("Client"));

            SyncAgent agent = new SyncAgent(clientProvider, serverProvider, new string[] {
                "ProductCategory", "ProductModel", "Product", "Address", "Customer", "CustomerAddress", "SalesOrderHeader", "SalesOrderDetail"});

            Console.WriteLine(await agent.SynchronizeAsync(SyncType.ReinitializeWithUpload));
            Console.WriteLine("End");
        }
    }
}
