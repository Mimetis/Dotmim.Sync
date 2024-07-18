using Dotmim.Sync.Enumerations;
using System;
using System.Runtime.Serialization;

namespace Dotmim.Sync
{
    /// <summary>
    /// Summary of table changes applied on a source.
    /// </summary>
    [DataContract(Name = "tca"), Serializable]
    public class TableChangesApplied
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TableChangesApplied"/> class.
        /// ctor for serialization purpose.
        /// </summary>
        public TableChangesApplied()
        {
        }

        /// <summary>
        /// Gets the name of the table that the DmTableSurrogate object represents.
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            string tn = string.IsNullOrEmpty(this.SchemaName) ? this.TableName : $"{this.SchemaName}.{this.TableName}";
            return $"{tn}: [{this.Applied} applied /{this.ResolvedConflicts} resolved /{this.Failed} failed]";
        }

        /// <summary>
        /// Gets or sets the name of the table that the DmTableSurrogate object represents.
        /// </summary>
        [DataMember(Name = "tn", IsRequired = true, Order = 1)]
        public string TableName { get; set; }

        /// <summary>
        /// Gets or sets get or Set the schema used for the DmTableSurrogate.
        /// </summary>
        [DataMember(Name = "sn", IsRequired = false, EmitDefaultValue = false, Order = 2)]
        public string SchemaName { get; set; }

        /// <summary>
        /// Gets or sets the RowState of the applied rows.
        /// </summary>
        [DataMember(Name = "st", IsRequired = true, Order = 3)]
        public SyncRowState State { get; set; }

        /// <summary>
        /// Gets or sets the resolved conflict rows applied count.
        /// </summary>
        [DataMember(Name = "rc", IsRequired = true, Order = 4)]
        public int ResolvedConflicts { get; set; }

        /// <summary>
        /// Gets or sets the rows changes applied count. This count contains resolved conflicts count also.
        /// </summary>
        [DataMember(Name = "a", IsRequired = true, Order = 5)]
        public int Applied { get; set; }

        /// <summary>
        /// Gets or sets the rows changes failed count.
        /// </summary>
        [DataMember(Name = "f", IsRequired = true, Order = 6)]
        public int Failed { get; set; }

        /// <summary>
        /// Gets or sets the total rows count to apply for all tables (used for progress during sync).
        /// </summary>
        [DataMember(Name = "trc", IsRequired = false, Order = 7)]
        public int TotalRowsCount { get; set; }

        /// <summary>
        /// Gets or sets the total rows count already applied for all tables (used for progress during sync).
        /// </summary>
        [DataMember(Name = "tac", IsRequired = false, Order = 8)]
        public int TotalAppliedCount { get; set; }
    }
}