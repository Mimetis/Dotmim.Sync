using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Filter
{
    /// <summary>
    /// Encapsulates information sent from the client to the server.
    /// </summary>
    [DataContract(Name = "par"), Serializable]
    public class SyncParameter
    {
        /// <summary>
        /// Gets or sets the name of the column from the table involved in filter.
        /// </summary>
        [DataMember(Name = "cn", IsRequired = true, Order = 1)]
        public string ColumnName { get; set; }

        /// <summary>
        /// Gets or sets the name of the table involved in filter
        /// </summary>
        [DataMember(Name = "tn", IsRequired = true, Order = 2)]
        public string TableName { get; set; }

        /// <summary>
        /// Gets or sets the name of the table schema involved in filter
        /// </summary>
        [DataMember(Name = "sn", IsRequired = false, EmitDefaultValue = false, Order = 3)]
        public string SchemaName { get; set; }

        /// <summary>
        /// Gets or sets the value of the parameter.
        /// </summary>
        [DataMember(Name = "v", IsRequired = true, Order = 4)]
        public Object Value { get; set; }

        /// <summary>
        /// Initializes a new instance of the SyncParameter class by using default values.
        /// </summary>
        public SyncParameter()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="T:Microsoft.Synchronization.Data.SyncParameter" /> class by 
        /// using name and value parameters.
        /// </summary>
        public SyncParameter(string tableName, string columnName, string schemaName, Object value)
        {
            this.ColumnName = columnName;
            this.TableName = tableName;
            this.SchemaName = schemaName;
            this.Value = value;
        }

     
    }
}
