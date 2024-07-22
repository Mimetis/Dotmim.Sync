using Dotmim.Sync.Builders;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Runtime.Serialization;

namespace Dotmim.Sync
{
    /// <summary>
    /// Sync filter parameters collection.
    /// </summary>
    [CollectionDataContract(Name = "filtersparameters", ItemName = "param"), Serializable]
    public class SyncFilterParameters : ICollection<SyncFilterParameter>, IList<SyncFilterParameter>
    {
        /// <summary>
        /// Gets or sets exposing the InnerCollection for serialization purpose.
        /// </summary>
        [DataMember(Name = "c", IsRequired = true, Order = 1)]
        public Collection<SyncFilterParameter> InnerCollection { get; set; } = [];

        /// <summary>
        /// Gets filter's schema.
        /// </summary>
        [IgnoreDataMember]
        public SyncSet Schema { get; internal set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncFilterParameters"/> class.
        /// Create a default collection for SerializersFactory.
        /// </summary>
        public SyncFilterParameters()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncFilterParameters"/> class.
        /// Create a new collection of tables for a SyncSchema.
        /// </summary>
        public SyncFilterParameters(SyncSet schema) => this.Schema = schema;

        /// <summary>
        /// Since we don't serializer the reference to the schema, this method will reaffect the correct schema.
        /// </summary>
        public void EnsureFilters(SyncSet schema)
        {
            this.Schema = schema;

            foreach (var item in this)
                item.EnsureFilterParameter(schema);
        }

        /// <summary>
        /// Add a new filter.
        /// </summary>
        public void Add(SyncFilterParameter item)
        {
            item.Schema = this.Schema;
            this.InnerCollection.Add(item);
        }

        /// <summary>
        /// Get a table by its name.
        /// </summary>
        public SyncFilterParameter this[string name]
        {
            get
            {
                if (string.IsNullOrEmpty(name))
                    throw new ArgumentNullException(nameof(name));

                var parser = ParserName.Parse(name);
                var objectName = parser.ObjectName;

                // Create a tmp synctable to benefit the SyncTable.Equals() method
                return this.InnerCollection.FirstOrDefault(st => string.Equals(st.Name, name, SyncGlobalization.DataSourceStringComparison));
            }
        }

        /// <summary>
        /// Clear.
        /// </summary>
        public void Clear() => this.InnerCollection.Clear();

        /// <summary>
        /// Gets get the count of filters.
        /// </summary>
        public int Count => this.InnerCollection.Count;

        /// <summary>
        /// Gets a value indicating whether the collection is read-only.
        /// </summary>
        public bool IsReadOnly => false;

        /// <summary>
        /// Gets or sets the filter at the specified index.
        /// </summary>
        SyncFilterParameter IList<SyncFilterParameter>.this[int index] { get => this.InnerCollection[index]; set => this.InnerCollection[index] = value; }

        /// <summary>
        /// Gets or sets the filter at the specified index.
        /// </summary>
        public SyncFilterParameter this[int index] { get => this.InnerCollection[index]; set => this.InnerCollection[index] = value; }

        /// <summary>
        /// Insert a filter at the specified index.
        /// </summary>
        public void Insert(int index, SyncFilterParameter item) => this.InnerCollection.Insert(index, item);

        /// <summary>
        /// Remove a filter.
        /// </summary>
        public bool Remove(SyncFilterParameter item) => this.InnerCollection.Remove(item);

        /// <summary>
        /// Check if the collection contains a filter.
        /// </summary>
        public bool Contains(SyncFilterParameter item) => this.InnerCollection.Contains(item);

        /// <summary>
        /// Copy the collection to an array.
        /// </summary>
        public void CopyTo(SyncFilterParameter[] array, int arrayIndex) => this.InnerCollection.CopyTo(array, arrayIndex);

        /// <summary>
        /// Get the index of a filter.
        /// </summary>
        public int IndexOf(SyncFilterParameter item) => this.InnerCollection.IndexOf(item);

        /// <summary>
        /// Remove a filter at the specified index.
        /// </summary>
        public void RemoveAt(int index) => this.InnerCollection.RemoveAt(index);

        /// <summary>
        /// Get the enumerator.
        /// </summary>
        IEnumerator IEnumerable.GetEnumerator() => this.InnerCollection.GetEnumerator();

        /// <summary>
        /// Get the enumerator.
        /// </summary>
        public IEnumerator<SyncFilterParameter> GetEnumerator() => this.InnerCollection.GetEnumerator();

        /// <summary>
        /// Returns the name of the filter.
        /// </summary>
        public override string ToString() => this.InnerCollection.Count.ToString();
    }
}