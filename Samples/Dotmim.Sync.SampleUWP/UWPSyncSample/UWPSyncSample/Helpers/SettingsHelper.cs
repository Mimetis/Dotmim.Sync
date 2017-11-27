using System;
using System.Collections.Generic;
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
                @"Data Source=.\SQLEXPRESS;Initial Catalog=ContosoClient;Integrated Security=SSPI;");

            this.defaultConnectionStrings.Add(ConnectionType.Client_Sqlite,
                @"Data Source=contoso.db");

            this.defaultConnectionStrings.Add(ConnectionType.Client_MySql,
                @"Server=127.0.0.1;Port=3306;Database=contosoclient;Uid=root;Pwd=azerty31$;");

            this.defaultConnectionStrings.Add(ConnectionType.Server_SqlServer,
                @"Data Source=.\SQLEXPRESS;Initial Catalog=Contoso;Integrated Security=SSPI;");

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


    }
}
