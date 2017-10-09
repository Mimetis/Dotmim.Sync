using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Text;

namespace Dotmim.Sync.FX.Tests.SqlUtils
{
    public class HelperDB
    {
        public static String GetDatabaseConnectionString(string dbName) => $@"Data Source=(localdb)\MSSQLLocalDB; Initial Catalog={dbName}; Integrated Security=true;";

        public static string GetMySqlDatabaseConnectionString(string dbName) => $@"Server=127.0.0.1; Port=3306; Database={dbName}; Uid=root; Pwd=azerty31*;";
        /// <summary>
        /// Generate a database
        /// </summary>
        public void CreateDatabase(string dbName, bool recreateDb = true)
        {
            SqlConnection masterConnection = null;
            SqlCommand cmdDb = null;
            masterConnection = new SqlConnection(GetDatabaseConnectionString("master"));

            masterConnection.Open();
            cmdDb = new SqlCommand(GetCreationDBScript(dbName, recreateDb), masterConnection);
            cmdDb.ExecuteNonQuery();
            masterConnection.Close();
        }

        /// <summary>
        /// Delete a database
        /// </summary>
        public void DeleteDatabase(string dbName)
        {
            SqlConnection masterConnection = null;
            SqlCommand cmdDb = null;
            masterConnection = new SqlConnection(GetDatabaseConnectionString("master"));

            masterConnection.Open();
            cmdDb = new SqlCommand(GetDeleteDatabaseScript(dbName), masterConnection);
            cmdDb.ExecuteNonQuery();
            masterConnection.Close();
        }

        public void ExecuteScript(string dbName, string script)
        {
            SqlConnection connection = null;
            SqlCommand cmdDb = null;
            connection = new SqlConnection(GetDatabaseConnectionString(dbName));

            connection.Open();
            cmdDb = new SqlCommand(script, connection);
            cmdDb.ExecuteNonQuery();
            connection.Close();
        }




        /// <summary>
        /// Gets the Create or Re-create a database script text
        /// </summary>
        private string GetCreationDBScript(string dbName, Boolean recreateDb = true)
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

        private string GetDeleteDatabaseScript(string dbName)
        {
            return $@"if (exists (Select * from sys.databases where name = '{dbName}'))
                    begin
	                    alter database [{dbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
	                    drop database {dbName}
                    end";
        }



        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }
}
