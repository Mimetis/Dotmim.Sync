using System;
using System.Collections.Generic;
using System.Data;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync
{
    /// <summary>
    /// Summary of table changes applied on a source
    /// </summary>
    [DataContract(Name = "tca"), Serializable]
    public class TableChangesApplied
    {
        /// <summary>
        /// ctor for serialization purpose
        /// </summary>
        public TableChangesApplied()
        {
                
        }

        /// <summary>
        /// Gets or sets the name of the table that the DmTableSurrogate object represents.
        /// </summary>
        [DataMember(Name = "tn", IsRequired = true, Order = 1)]
        public string TableName { get; set; }

        /// <summary>
        /// Get or Set the schema used for the DmTableSurrogate
        /// </summary>
        [DataMember(Name = "sn", IsRequired = false, EmitDefaultValue = false, Order = 2)]
        public string SchemaName { get; set; }

        /// <summary>
        /// Gets the RowState of the applied rows
        /// </summary>
        [DataMember(Name = "st", IsRequired = true, Order = 3)]
        public DataRowState State { get; set; }

        /// <summary>
        /// Gets the resolved conflict rows applied count
        /// </summary>
        [DataMember(Name = "rc", IsRequired = true, Order = 4)]
        public int ResolvedConflicts { get; set; }

        /// <summary>
        /// Gets the rows changes applied count. This count contains resolved conflicts count also
        /// </summary>
        [DataMember(Name = "a", IsRequired = true, Order = 5)]
        public int Applied { get; set; }

        /// <summary>
        /// Gets the rows changes failed count
        /// </summary>
        [DataMember(Name = "f", IsRequired = true, Order = 6)]
        public int Failed { get; set; }
    }

}
