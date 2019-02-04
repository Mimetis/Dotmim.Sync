using Dotmim.Sync.Tests.Models;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.SampleConsole
{
    public class DbHelper
    {

        public static string GetDatabaseConnectionString(string dbName) =>
            $"Data Source=(localdb)\\mssqllocaldb; Initial Catalog={dbName}; Integrated Security=true;";


        /// <summary>
        /// create a server database with datas and an empty client database
        /// </summary>
        /// <returns></returns>
        public static async Task EnsureDatabasesAsync(string serverDatabaseName, string clientDatabaseName)
        {
            // Create server database with items
            using (var dbServer = new AdventureWorksContext(GetDatabaseConnectionString(serverDatabaseName), true))
            {
                await dbServer.Database.EnsureDeletedAsync();
                await dbServer.Database.EnsureCreatedAsync();
            }
            // Create an empty client database (with no schema and not datas)
            await CreateDatabaseAsync(clientDatabaseName, true);

        }

        public static async Task DeleteDatabaseAsync(string dbName)
        {
            var masterConnection = new SqlConnection(GetDatabaseConnectionString("master"));
            await masterConnection.OpenAsync();
            var cmdDb = new SqlCommand(GetDeleteDatabaseScript(dbName), masterConnection);
            await cmdDb.ExecuteNonQueryAsync();
            masterConnection.Close();
        }

      

        public static async Task CreateDatabaseAsync(string dbName, bool recreateDb = true)
        {
            var masterConnection = new SqlConnection(GetDatabaseConnectionString("master"));
            await masterConnection.OpenAsync();
            var cmdDb = new SqlCommand(GetCreationDBScript(dbName, recreateDb), masterConnection);
            await cmdDb.ExecuteNonQueryAsync();
            masterConnection.Close();
        }

        private static string GetDeleteDatabaseScript(string dbName) =>
                  $@"if (exists (Select * from sys.databases where name = '{dbName}'))
            begin
	            alter database [{dbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
	            drop database {dbName}
            end";

        private static string GetCreationDBScript(string dbName, bool recreateDb = true)
        {
            if (recreateDb)
                return $@"if (exists (Select * from sys.databases where name = '{dbName}'))
                    begin
	                    alter database [{dbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
	                    drop database {dbName}
                    end
                    Create database {dbName}";
            else
                return $@"if not (exists (Select * from sys.databases where name = '{dbName}')) 
                          Create database {dbName}";

        }

    }
}
