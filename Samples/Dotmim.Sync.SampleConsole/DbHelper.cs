using Dotmim.Sync.Tests.Models;
using MySql.Data.MySqlClient;
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

        public static string GetAzureDatabaseConnectionString(string dbName) =>
            $"Data Source=spertus.database.windows.net; Initial Catalog={dbName}; Integrated Security=false;User Id=spertus;Password=azerty31$;";



        public static string GetMySqlDatabaseConnectionString(string dbName)
        {

            var builder = new MySqlConnectionStringBuilder
            {
                Server = "127.0.0.1",
                Port = 3306,
                UserID = "root",
                Password = "Password12!",
                Database = dbName
            };

            builder.Port = 3307;

            return builder.ToString();
        }


        /// <summary>
        /// create a server database with datas and an empty client database
        /// </summary>
        /// <returns></returns>
        public static async Task EnsureDatabasesAsync(string databaseName, bool useSeeding = true)
        {
            // Create server database with items
            using (var dbServer = new AdventureWorksContext(GetDatabaseConnectionString(databaseName), useSeeding))
            {
                await dbServer.Database.EnsureDeletedAsync();
                await dbServer.Database.EnsureCreatedAsync();
            }
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
