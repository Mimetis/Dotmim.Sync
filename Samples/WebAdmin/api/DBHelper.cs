using Microsoft.Data.SqlClient;

namespace Api;

public interface IDBHelper
{
  Task CreateDatabaseAsync(string dbName, bool recreateDb = true);
  Task DeleteDatabaseAsync(string dbName);
  string GetAzureDatabaseConnectionString(string dbName);
  string GetConnectionString(string connectionStringName);
  string GetDatabaseConnectionString(string dbName);
  string GetMariadbDatabaseConnectionString(string dbName);
  string GetMySqlDatabaseConnectionString(string dbName);
  string GetNpgsqlDatabaseConnectionString(string dbName);
}

public class DBHelper : IDBHelper
{
  private IConfiguration configuration;

  public DBHelper(IConfiguration configuration)
  {
    this.configuration = configuration;
  }

  public string GetConnectionString(string connectionStringName) =>
      configuration.GetSection("ConnectionStrings")[connectionStringName];

  public string GetDatabaseConnectionString(string dbName) =>
      string.Format(configuration.GetSection("ConnectionStrings")["SqlConnection"], dbName);

  public string GetAzureDatabaseConnectionString(string dbName) =>
      string.Format(configuration.GetSection("ConnectionStrings")["AzureSqlConnection"], dbName);

  public string GetMySqlDatabaseConnectionString(string dbName) =>
      string.Format(configuration.GetSection("ConnectionStrings")["MySqlConnection"], dbName);

  public string GetMariadbDatabaseConnectionString(string dbName) =>
      string.Format(configuration.GetSection("ConnectionStrings")["MariadbConnection"], dbName);


  public string GetNpgsqlDatabaseConnectionString(string dbName) =>
      string.Format(configuration.GetSection("ConnectionStrings")["NpgsqlConnection"], dbName);



  public async Task DeleteDatabaseAsync(string dbName)
  {
    var masterConnection = new SqlConnection(GetDatabaseConnectionString("master"));
    await masterConnection.OpenAsync();
    var cmdDb = new SqlCommand(GetDeleteDatabaseScript(dbName), masterConnection);
    await cmdDb.ExecuteNonQueryAsync();
    masterConnection.Close();
  }



  public async Task CreateDatabaseAsync(string dbName, bool recreateDb = true)
  {
    var masterConnection = new SqlConnection(GetDatabaseConnectionString("master"));
    await masterConnection.OpenAsync();
    var cmdDb = new SqlCommand(GetCreationDBScript(dbName, recreateDb), masterConnection);
    await cmdDb.ExecuteNonQueryAsync();
    masterConnection.Close();
  }

  private string GetDeleteDatabaseScript(string dbName) =>
            $@"if (exists (Select * from sys.databases where name = '{dbName}'))
            begin
	            alter database [{dbName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
	            drop database {dbName}
            end";

  private string GetCreationDBScript(string dbName, bool recreateDb = true)
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