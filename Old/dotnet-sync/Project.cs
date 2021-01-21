using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Dotmim.Sync.Tools
{
    public class Project
    {

        /// <summary>
        /// Gets or Sets the project name
        /// </summary>
        public String Name { get; set; }

        /// <summary>
        /// Gets or Sets the Server provider used
        /// </summary>
        public Provider ServerProvider { get; set; }

        /// <summary>
        /// Gets or Sets the Client provider used
        /// </summary>
        public Provider ClientProvider { get; set; }

        /// <summary>
        /// Gets or Sets the Table list
        /// </summary>
        public List<Table> Tables { get; set; } = new List<Table>();

        /// <summary>
        /// Gets or Sets the Configuration used
        /// </summary>
        public Configuration Configuration { get; set; } = new Configuration();

      

        public Project()
        {
        }

        /// <summary>
        /// Create the default project
        /// </summary>
        /// <returns></returns>
        public static Project CreateProject(string name)
        {
            return new Project
            {
                Name = name,
                ServerProvider = new Provider
                {
                    ConnectionString = String.Empty,
                    ProviderType = ProviderType.SqlServer,
                    SyncType = SyncType.Server
                },
                ClientProvider = new Provider
                {
                    ConnectionString = String.Empty,
                    ProviderType = ProviderType.SqlServer,
                    SyncType = SyncType.Client
                },
                Configuration = new Configuration
                {
                    BatchDirectory = Path.Combine(Path.GetTempPath(), "DotmimSync"),
                    SerializationFormat = Enumerations.SerializationFormat.Json,
                    UseBulkOperations = true,
                    ConflictResolutionPolicy = Enumerations.ConflictResolutionPolicy.ServerWins
                },
                Tables = new List<Table>()

            };
        }

    }
}
