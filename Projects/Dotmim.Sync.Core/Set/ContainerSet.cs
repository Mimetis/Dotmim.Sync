using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync
{
    [DataContract(Name = "c"), Serializable]
    public class ContainerSet
    {
        /// <summary>
        /// Gets or Sets the name of the data source (database name)
        /// </summary>
        [DataMember(Name = "n", IsRequired = true, Order = 1)]
        public string DataSourceName { get; set; }

        /// <summary>
        /// List of tables
        /// </summary>
        [DataMember(Name = "t", IsRequired = false, EmitDefaultValue = false, Order = 2)]
        public Collection<ContainerTable> Tables { get; set; } = new Collection<ContainerTable>();

        public void Clear()
        {
            foreach (var t in Tables)
                t.Clear();

            Tables.Clear();
        }

        /// <summary>
        /// Check if we have some tables in the container
        /// </summary>
        public bool HasTables => this.Tables?.Count > 0;

        /// <summary>
        /// Check if we have at least one table with one row
        /// </summary>
        public bool HasRows
        {
            get
            {
                if (!HasTables)
                    return false;

                // Check if any of the tables has rows inside
                return this.Tables.Any(t => t.Rows.Count > 0);
            }
        }

        public ContainerSet() { }

        public ContainerSet(string name) => this.DataSourceName = name;
     
    }
}
