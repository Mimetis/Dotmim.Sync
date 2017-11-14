using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Filter
{
    /// <summary>
    /// Encapsulates information sent from the client to the server.
    /// </summary>
    [Serializable]
    public class SyncParameter
    {
        /// <summary>
        /// Gets or sets the name of the column from the table involved in filter.
        /// </summary>
        public string ColumnName { get; set; }

        /// <summary>
        /// Gets or sets the name of the table involved in filter
        /// </summary>
        public string TableName { get; }

        /// <summary>
        /// Gets or sets the value of the parameter.
        /// </summary>
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
        public SyncParameter(string tableName, string columnName, Object value)
        {
            this.ColumnName = columnName;
            this.TableName = tableName;
            this.Value = value;
        }

     
    }
}
