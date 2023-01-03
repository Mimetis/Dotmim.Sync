using Dotmim.Sync.Builders;


using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync
{

    [DataContract(Name = "s"), Serializable]
    public class SyncSet : IDisposable
    {
        /// <summary>
        /// Gets or Sets the sync set tables
        /// </summary>
        [DataMember(Name = "t", IsRequired = false, EmitDefaultValue = false, Order = 1)]
        public SyncTables Tables { get; set; }

        /// <summary>
        /// Gets or Sets an array of every SchemaRelation belong to this Schema
        /// </summary>
        [DataMember(Name = "r", IsRequired = false, EmitDefaultValue = false, Order = 2)]
        public SyncRelations Relations { get; set; }

        /// <summary>
        /// Filters applied on tables
        /// </summary>
        [DataMember(Name = "f", IsRequired = false, EmitDefaultValue = false, Order = 3)]
        public SyncFilters Filters { get; set; }

        /// <summary>
        /// Create a new SyncSet, empty
        /// </summary>
        public SyncSet()
        {
            this.Tables = new SyncTables(this);
            this.Relations = new SyncRelations(this);
            this.Filters = new SyncFilters(this);
        }


        /// <summary>
        /// Creates a new SyncSet based on a Sync setup (containing tables)
        /// </summary>
        /// <param name="setup"></param>
        public SyncSet(SyncSetup setup) : this()
        {
            foreach (var filter in setup.Filters)
                this.Filters.Add(filter);

            foreach (var setupTable in setup.Tables)
                this.Tables.Add(new SyncTable(setupTable.TableName, setupTable.SchemaName));

            this.EnsureSchema();
        }

        /// <summary>
        /// Ensure all tables, filters and relations has the correct reference to this schema
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
        /// Clone the SyncSet schema (without data)
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
        /// Clear the SyncSet
        /// </summary>
        public void Clear() => this.Dispose(true);


        /// <summary>
        /// Dispose the whole SyncSet
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
            if (this.Tables != null)
                this.Tables.Schema = null;
            if (this.Relations != null)
                this.Relations.Schema = null;
            if (this.Filters != null)
                this.Filters.Schema = null;

            //GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool cleanup)
        {
            // Dispose managed ressources
            if (cleanup)
            {
                if (this.Tables != null)
                {
                    this.Tables.Clear();
                    this.Tables = null;
                }

                if (this.Relations != null)
                {
                    this.Relations.Clear();
                    this.Relations = null;
                }

                if (this.Filters != null)
                {
                    this.Filters.Clear();
                    this.Filters = null;
                }
            }

            // Dispose unmanaged ressources
        }

      
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
        public override string ToString() => $"{this.Tables.Count} tables";

        /// <summary>
        /// Check if Schema has tables
        /// </summary>
        public bool HasTables => this.Tables?.Count > 0;

        /// <summary>
        /// Check if Schema has at least one table with columns
        /// </summary>
        public bool HasColumns => this.Tables?.SelectMany(t => t.Columns).Count() > 0;  // using SelectMany to get columns and not Collection<Column>


        /// <summary>
        /// Gets if at least one table as at least one row
        /// </summary>
        public bool HasRows
        {
            get
            {
                if (!HasTables)
                    return false;

                // Check if any of the tables has rows inside
                return this.Tables.Any(t => t.Rows != null && t.Rows.Count > 0);
            }
        }

        /// <summary>
        /// Gets a true boolean if other instance is defined as same based on all properties
        /// </summary>
        public bool Equals(SyncSet other) => this.EqualsByProperties(other);

        /// <summary>
        /// Gets a true boolean if other instance is defined as same based on all properties
        /// </summary>
        public override bool Equals(object obj) => this.EqualsByProperties(obj as SyncSet);

        public override int GetHashCode() => base.GetHashCode();

    }
}
