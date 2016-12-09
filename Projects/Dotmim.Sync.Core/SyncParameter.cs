using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Core
{
    /// <summary>
    /// Encapsulates information sent from the client to the server.
    /// </summary>
    public class SyncParameter
    {
        /// <summary>
        /// Gets or sets the name of the parameter.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets the value of the parameter.
        /// </summary>
        public object Value { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="T:Microsoft.Synchronization.Data.SyncParameter" /> class by 
        /// using default values.</summary>
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

        /// <summary>
        /// Determines whether a <see cref="T:Microsoft.Synchronization.Data.SyncParameter" /> object is equal to the specified object.
        /// </summary>
        public override bool Equals(object obj)
        {
            SyncParameter syncParameter = obj as SyncParameter;
            if (syncParameter == null)
                return false;

            if (String.Equals(this.Name, syncParameter.Name, StringComparison.OrdinalIgnoreCase)) 
                return false;

            return this.Value == syncParameter.Value;
        }

        /// <summary>
        /// Serves as a hash function for a <see cref="T:Microsoft.Synchronization.Data.SyncParameter" />. 
        /// This is suitable for use in hashing algorithms and data structures such as a hash table.
        /// </summary>
        public override int GetHashCode()
        {
            int hashCode = 0;
            if (this.Name != null)
                hashCode = hashCode ^ this.Name.ToUpperInvariant().GetHashCode();

            if (this.Value != null)
                hashCode = hashCode ^ this.Value.GetHashCode();

            return hashCode;
        }


        /// <summary>
        /// Returns a string that represents the <see cref="T:Microsoft.Synchronization.Data.SyncParameter" /> object.
        /// </summary>
        public override string ToString()
        {
            return $"{this.Name} : {this.Value}";
        }
    }
}
