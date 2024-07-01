using Dotmim.Sync.Serialization;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// Encapsulates information sent from the client to the server.
    /// </summary>
    [DataContract(Name = "par"), Serializable]
    public class SyncParameter : SyncNamedItem<SyncParameter>
    {
        /// <summary>
        /// Gets or sets the name of the column from the table involved in filter.
        /// </summary>
        [DataMember(Name = "pn", IsRequired = true, Order = 1)]
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets the value of the parameter.
        /// </summary>
        [DataMember(Name = "v", IsRequired = true, Order = 2)]
        public object Value { get; set; }

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
        public SyncParameter(string name, object value)
        {
            this.Name = name;
            this.Value = value;
        }


        public override string ToString() => $"{this.Name}: {this.Value}";

        public override IEnumerable<string> GetAllNamesProperties()
        {
            yield return this.Name;
        }

    }
}
