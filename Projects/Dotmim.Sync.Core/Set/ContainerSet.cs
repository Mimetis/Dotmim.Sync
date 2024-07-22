using System;
using System.Collections.ObjectModel;
using System.Linq;
using System.Runtime.Serialization;

namespace Dotmim.Sync
{

    /// <summary>
    /// ContainerSet is a collection of tables and rows to be sent over the wire.
    /// </summary>
    [DataContract(Name = "c"), Serializable]
    public class ContainerSet
    {
        /// <summary>
        /// Gets or sets list of tables.
        /// </summary>
        [DataMember(Name = "t", IsRequired = false, EmitDefaultValue = false, Order = 2)]
        public Collection<ContainerTable> Tables { get; set; } = [];

        /// <summary>
        /// Gets a value indicating whether check if we have some tables in the container.
        /// </summary>
        public bool HasTables => this.Tables?.Count > 0;

        /// <summary>
        /// Gets a value indicating whether check if we have at least one table with one row.
        /// </summary>
        public bool HasRows
        {
            get
            {
                if (!this.HasTables)
                    return false;

                // Check if any of the tables has rows inside
                return this.Tables.Any(t => t.Rows.Count > 0);
            }
        }

        /// <summary>
        /// Getting the container rows count.
        /// </summary>
        public int RowsCount()
        {
            if (!this.HasTables)
                return 0;

            return this.Tables.Sum(t => t.Rows.Count);
        }

        /// <inheritdoc cref="ContainerSet"/>
        public ContainerSet() { }

        /// <summary>
        /// Clear all tables and rows in the container.
        /// </summary>
        public void Clear()
        {
            foreach (var t in this.Tables)
                t.Clear();

            this.Tables.Clear();
        }
    }
}