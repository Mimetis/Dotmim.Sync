using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using UWPSyncSample.Context;
using Windows.Storage;

namespace UWPSyncSample.Helpers
{
    public class SettingsHelper
    {

        public String this[ConnectionType type]
        {
            get
            {

                if (connectionStrings.TryGetValue(type, out var s1))
                    return s1;


                if (defaultConnectionStrings.TryGetValue(type, out var s2))
                {
                    connectionStrings.Add(type, s2);
                    return s2;
                }

                return null;
            }
            set
            {
                if (connectionStrings.ContainsKey(type))
                    connectionStrings[type] = value;
                else
                    connectionStrings.Add(type, value);
            }
        }

        // Default connection strings
        private Dictionary<ConnectionType, String> defaultConnectionStrings = new Dictionary<ConnectionType, string>();

        // connections strings from local settings
        private Dictionary<ConnectionType, String> connectionStrings = new Dictionary<ConnectionType, string>();

        /// <summary>
        /// Create default values
        /// </summary>
        private void ConstrucDefaultConnectionStrings()
        {
            this.defaultConnectionStrings.Add(ConnectionType.Client_SqlServer,
                @"Data Source=localhost;Initial Catalog=ContosoClient;User Id=sa;Password=Password12!;");

            string dbpath = Path.Combine(ApplicationData.Current.LocalFolder.Path, "contoso.db");
            this.defaultConnectionStrings.Add(ConnectionType.Client_Sqlite, $@"Data Source={dbpath}");

            this.defaultConnectionStrings.Add(ConnectionType.Client_MySql,
                @"Server=127.0.0.1;Port=3306;Database=contosoclient;Uid=root;Pwd=azerty31$;");

            this.defaultConnectionStrings.Add(ConnectionType.Server_SqlServer,
                @"Data Source=localhost;Initial Catalog=Contoso;User Id=sa;Password=Password12!;");

            this.defaultConnectionStrings.Add(ConnectionType.WebProxy,
                @"http://localhost:54347/api/values");
        }

        public SettingsHelper()
        {
            this.ConstrucDefaultConnectionStrings();


            ApplicationDataContainer localSettings = ApplicationData.Current.LocalSettings;
            var serializedCollections = localSettings.Values["connections"] as String;

            // check if we have some connections from settings
            if (String.IsNullOrWhiteSpace(serializedCollections))
                this.connectionStrings = defaultConnectionStrings;
            else
                this.connectionStrings = Deserialize(serializedCollections);

            
        }


        public async Task InitializeDatabasesAsync()
        {
            await this.CreateDatabaseAsync("Contoso", false);
            await this.CreateDatabaseAsync("ContosoClient", false);
            await this.CreateTableOnServerAsync();
        }

        public async Task CreateDatabaseAsync(string dbName, bool recreateDb = true)
        {

            var builder = new SqlConnectionStringBuilder(this.connectionStrings[ConnectionType.Server_SqlServer])
            {
                InitialCatalog = "master"
            };

            var masterConnection = new SqlConnection(builder.ConnectionString);

            masterConnection.Open();
            var cmdDb = new SqlCommand(GetCreationDBScript(dbName, recreateDb), masterConnection);
            await cmdDb.ExecuteNonQueryAsync();
            masterConnection.Close();
        }


        public async Task CreateTableOnServerAsync()
        {

            SqlConnection connection = null;
            SqlCommand cmdDb = null;
            connection = new SqlConnection(this.connectionStrings[ConnectionType.Server_SqlServer]);

            connection.Open();
            cmdDb = new SqlCommand(GetCreationTableDBScript(), connection);
            await cmdDb.ExecuteNonQueryAsync();
            connection.Close();
        }


        private Dictionary<ConnectionType, String> Deserialize(String serializedCollections)
        {
            Dictionary<ConnectionType, String> dictionary = null;
            try
            {

                var connections = serializedCollections.Split("^");
                dictionary = new Dictionary<ConnectionType, string>();
                foreach (var s in connections)
                {
                    ConnectionType connectionType = (ConnectionType)Enum.Parse(typeof(ConnectionType), s.Split("|")[0]);
                    string connectionString = s.Split("|")[1];

                    dictionary.Add(connectionType, connectionString);
                }
            }
            catch (Exception)
            {

                dictionary = defaultConnectionStrings;
            }

            return dictionary;
        }

        private String Serialize(Dictionary<ConnectionType, String> dictionary)
        {

            var lst = dictionary.Select(kvp => $"{kvp.Key.ToString()}|{kvp.Value}").ToList();
            var s = string.Join("^", lst.ToArray());
            return s;
        }


        public void Save()
        {
            ApplicationDataContainer localSettings = ApplicationData.Current.LocalSettings;
            localSettings.Values["connections"] = Serialize(this.connectionStrings);
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


        private string GetCreationTableDBScript()
        {
            return $@"if not (exists (Select * from sys.tables where name = 'Employee'))
                   BEGIN
                    CREATE TABLE [Employee](
	                    [EmployeeId] [uniqueidentifier] NOT NULL,
	                    [FirstName] [nvarchar](50) NOT NULL,
	                    [LastName] [nvarchar](50) NULL,
	                    [ProfilePicture] [varbinary](max) NULL,
	                    [PhoneNumber] [nvarchar](30) NULL,
	                    [HireDate] [datetime] NOT NULL,
	                    [Comments] [nvarchar](max) NULL,
                     CONSTRAINT [PK_Employee] PRIMARY KEY CLUSTERED ([EmployeeId] ASC)
                    )
                   END";
        }
    }
}
