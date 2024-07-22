using System;
using System.Linq;
using System.Runtime.Serialization;

namespace Dotmim.Sync
{

    /// <summary>
    /// Represents a Sync Set.
    /// </summary>
    [DataContract(Name = "s"), Serializable]
    public class SyncSet
    {
        /// <summary>
        /// Gets or Sets the sync set tables.
        /// </summary>
        [DataMember(Name = "t", IsRequired = false, EmitDefaultValue = false, Order = 1)]
        public SyncTables Tables { get; set; }

        /// <summary>
        /// Gets or Sets an array of every SchemaRelation belong to this Schema.
        /// </summary>
        [DataMember(Name = "r", IsRequired = false, EmitDefaultValue = false, Order = 2)]
        public SyncRelations Relations { get; set; }

        /// <summary>
        /// Gets or sets filters applied on tables.
        /// </summary>
        [DataMember(Name = "f", IsRequired = false, EmitDefaultValue = false, Order = 3)]
        public SyncFilters Filters { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncSet"/> class.
        /// Create a new SyncSet, empty.
        /// </summary>
        public SyncSet()
        {
            this.Tables = new SyncTables(this);
            this.Relations = new SyncRelations(this);
            this.Filters = new SyncFilters(this);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncSet"/> class.
        /// Creates a new SyncSet based on a Sync setup (containing tables).
        /// </summary>
        public SyncSet(SyncSetup setup)
            : this()
        {
            foreach (var filter in setup.Filters)
                this.Filters.Add(filter);

            foreach (var setupTable in setup.Tables)
                this.Tables.Add(new SyncTable(setupTable.TableName, setupTable.SchemaName));

            this.EnsureSchema();
        }

        /// <summary>
        /// Ensure all tables, filters and relations has the correct reference to this schema.
        /// </summary>
        public void EnsureSchema()
        {
            if (this.Tables != null)
                this.Tables.EnsureTables(this);

            if (this.Relations != null)
                this.Relations.EnsureRelations(this);

            if (this.Filters != null)
                this.Filters.EnsureFilters(this);
        }

        /// <summary>
        /// Clone the SyncSet schema (without data).
        /// </summary>
        public SyncSet Clone(bool includeTables = true)
        {
            var clone = new SyncSet();

            if (!includeTables)
                return clone;

            foreach (var f in this.Filters)
                clone.Filters.Add(f.Clone());

            foreach (var r in this.Relations)
                clone.Relations.Add(r.Clone());

            foreach (var t in this.Tables)
                clone.Tables.Add(t.Clone());

            // Ensure all elements has the correct ref to its parent
            clone.EnsureSchema();

            return clone;
        }

        /// <summary>
        /// Clear the SyncSet.
        /// </summary>
        public void Clear()
        {
            if (this.Tables != null)
            {
                this.Tables.Clear();
                this.Tables.Schema = null;
                this.Tables = null;
            }

            if (this.Relations != null)
            {
                this.Relations.Clear();
                this.Relations.Schema = null;
                this.Relations = null;
            }

            if (this.Filters != null)
            {
                this.Filters.Clear();
                this.Filters.Schema = null;
                this.Filters = null;
            }
        }

        /// <inheritdoc cref="SyncNamedItem{T}.EqualsByProperties(T)"/>
        public bool EqualsByProperties(SyncSet otherSet)
        {
            if (otherSet == null)
                return false;

            // Checking inner lists
            if (!this.Tables.CompareWith(otherSet.Tables))
                return false;

            if (!this.Filters.CompareWith(otherSet.Filters))
                return false;

            if (!this.Relations.CompareWith(otherSet.Relations))
                return false;

            return true;
        }

        /// <summary>
        /// Returns a string that represents the current SyncSet.
        /// </summary>
        public override string ToString() => $"{this.Tables.Count} tables";

        /// <summary>
        /// Gets a value indicating whether check if Schema has tables.
        /// </summary>
        public bool HasTables => this.Tables?.Count > 0;

        /// <summary>
        /// Gets a value indicating whether check if Schema has at least one table with columns.
        /// </summary>
        public bool HasColumns => this.Tables?.SelectMany(t => t.Columns).Count() > 0;  // using SelectMany to get columns and not Collection<Column>

        /// <summary>
        /// Gets a value indicating whether gets if at least one table as at least one row.
        /// </summary>
        public bool HasRows
        {
            get
            {
                if (!this.HasTables)
                    return false;

                // Check if any of the tables has rows inside
                return this.Tables.Any(t => t.Rows != null && t.Rows.Count > 0);
            }
        }

        /// <summary>
        /// Gets a true boolean if other instance is defined as same based on all properties.
        /// </summary>
        public bool Equals(SyncSet other) => this.EqualsByProperties(other);

        /// <summary>
        /// Gets a true boolean if other instance is defined as same based on all properties.
        /// </summary>
        public override bool Equals(object obj) => this.EqualsByProperties(obj as SyncSet);

        /// <summary>
        /// Returns the hash code for this instance.
        /// </summary>
        public override int GetHashCode() => base.GetHashCode();
    }
}