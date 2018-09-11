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
        public string ParameterName { get; set; }

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
        /// Initializes a new instance of the SyncParameter class by 
        /// using name and value parameters.
        /// </summary>
        public SyncParameter(string parameterName, Object value)
        {
            this.ParameterName = parameterName;
            this.Value = value;
        }

     
    }
}
