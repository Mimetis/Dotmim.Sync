using Dotmim.Sync;
using Dotmim.Sync.Sqlite;
using Dotmim.Sync.SqlServer;
using System;
using System.Data;
using System.Threading.Tasks;

namespace Filter
{
    class Program
    {
        private static string serverConnectionString = $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog=AdventureWorks;Integrated Security=true;";
        private static string clientConnectionString = $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog=Client;Integrated Security=true;";

        static async Task Main() => await SynchronizeAsync();


        private static async Task SynchronizeAsync()
        {
            // Database script used for this sample : https://github.com/Mimetis/Dotmim.Sync/blob/master/CreateAdventureWorks.sql 

            var serverProvider = new SqlSyncProvider(serverConnectionString);
            var clientProvider = new SqlSyncProvider(clientConnectionString);
            //var clientProvider = new SqliteSyncProvider("advfiltered.db");

            var setup = new SyncSetup(new string[] {"ProductCategory",
                "ProductModel", "Product",
                "Address", "Customer", "CustomerAddress",
                "SalesOrderHeader", "SalesOrderDetail" });

            // ----------------------------------------------------
            // Horizontal Filter: On rows. Removing rows from source
            // ----------------------------------------------------
            // Over all filter : "we Want only customer from specific city and specific postal code"
            // First level table : Address
            // Second level tables : CustomerAddress
            // Third level tables : Customer, SalesOrderHeader
            // Fourth level tables : SalesOrderDetail

            // Create a filter on table Address on City Washington
            // Optional : Sub filter on PostalCode, for testing purpose
            var addressFilter = new SetupFilter("Address");

            // For each filter, you have to provider all the input parameters
            // A parameter could be a parameter mapped to an existing colum : 
            // That way you don't have to specify any type, length and so on ...
            // We can specify if a null value can be passed as parameter value : 
            // That way ALL addresses will be fetched
            // A default value can be passed as well, but works only on SQL Server (MySql is a damn ... thing)
            addressFilter.AddParameter("City", "Address", true);

            // Or a parameter could be a random parameter bound to anything. 
            // In that case, you have to specify everything
            // (This parameter COULD BE bound to a column, like City, 
            //  but for the example, we go for a custom parameter)
            addressFilter.AddParameter("postal", DbType.String, true, null, 20);

            // Then you map each parameter on wich table / column the "where" clause should be applied
            addressFilter.AddWhere("City", "Address", "City");
            addressFilter.AddWhere("PostalCode", "Address", "postal");
            setup.Filters.Add(addressFilter);

            var addressCustomerFilter = new SetupFilter("CustomerAddress");
            addressCustomerFilter.AddParameter("City", "Address", true);
            addressCustomerFilter.AddParameter("postal", DbType.String, true, null, 20);

            // You can join table to go from your table up (or down) to your filter table
            addressCustomerFilter.AddJoin(Join.Left, "Address")
                .On("CustomerAddress", "AddressId", "Address", "AddressId");

            // And then add your where clauses
            addressCustomerFilter.AddWhere("City", "Address", "City");
            addressCustomerFilter.AddWhere("PostalCode", "Address", "postal");
            setup.Filters.Add(addressCustomerFilter);

            var customerFilter = new SetupFilter("Customer");
            customerFilter.AddParameter("City", "Address", true);
            customerFilter.AddParameter("postal", DbType.String, true, null, 20);
            customerFilter.AddJoin(Join.Left, "CustomerAddress")
                .On("CustomerAddress", "CustomerId", "Customer", "CustomerId");
            customerFilter.AddJoin(Join.Left, "Address")
                .On("CustomerAddress", "AddressId", "Address", "AddressId");
            customerFilter.AddWhere("City", "Address", "City");
            customerFilter.AddWhere("PostalCode", "Address", "postal");
            setup.Filters.Add(customerFilter);

            var orderHeaderFilter = new SetupFilter("SalesOrderHeader");
            orderHeaderFilter.AddParameter("City", "Address", true);
            orderHeaderFilter.AddParameter("postal", DbType.String, true, null, 20);
            orderHeaderFilter.AddJoin(Join.Left, "CustomerAddress")
                .On("CustomerAddress", "CustomerId", "SalesOrderHeader", "CustomerId");
            orderHeaderFilter.AddJoin(Join.Left, "Address")
                .On("CustomerAddress", "AddressId", "Address", "AddressId");
            orderHeaderFilter.AddWhere("City", "Address", "City");
            orderHeaderFilter.AddWhere("PostalCode", "Address", "postal");
            setup.Filters.Add(orderHeaderFilter);

            var orderDetailsFilter = new SetupFilter("SalesOrderDetail");
            orderDetailsFilter.AddParameter("City", "Address", true);
            orderDetailsFilter.AddParameter("postal", DbType.String, true, null, 20);
            orderDetailsFilter.AddJoin(Join.Left, "SalesOrderHeader")
                .On("SalesOrderHeader", "SalesOrderID", "SalesOrderDetail", "SalesOrderID");
            orderDetailsFilter.AddJoin(Join.Left, "CustomerAddress")
                .On("CustomerAddress", "CustomerId", "SalesOrderHeader", "CustomerId");
            orderDetailsFilter.AddJoin(Join.Left, "Address")
                .On("CustomerAddress", "AddressId", "Address", "AddressId");
            orderDetailsFilter.AddWhere("City", "Address", "City");
            orderDetailsFilter.AddWhere("PostalCode", "Address", "postal");
            setup.Filters.Add(orderDetailsFilter);

            // Creating an agent that will handle all the process
            var agent = new SyncAgent(clientProvider, serverProvider, setup);

            if (!agent.Parameters.Contains("City"))
                agent.Parameters.Add("City", "Toronto");

            // Because I've specified that "postal" could be null, 
            // I can set the value to DBNull.Value (and the get all postal code in Toronto city)
            if (!agent.Parameters.Contains("postal"))
                agent.Parameters.Add("postal", "M4B 1V5");

            do
            {
                // Launch the sync process
                var s1 = await agent.SynchronizeAsync();
                // Write results
                Console.WriteLine(s1);

            } while (Console.ReadKey().Key != ConsoleKey.Escape);

            Console.WriteLine("End");
        }
    }
}
