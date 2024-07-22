using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Runtime.Serialization;

namespace Dotmim.Sync
{
    /// <summary>
    /// Represents a collection of filter joins.
    /// </summary>
    [CollectionDataContract(Name = "filtersjoins", ItemName = "join"), Serializable]
    public class SyncFilterJoins : ICollection<SyncFilterJoin>, IList<SyncFilterJoin>
    {
        /// <summary>
        /// Gets or sets exposing the InnerCollection for serialization purpose.
        /// </summary>
        [DataMember(Name = "c", IsRequired = true, Order = 1)]
        public Collection<SyncFilterJoin> InnerCollection { get; set; } = new Collection<SyncFilterJoin>();

        /// <summary>
        /// Gets filter's schema.
        /// </summary>
        [IgnoreDataMember]
        public SyncSet Schema { get; internal set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncFilterJoins"/> class.
        /// Create a default collection for SerializersFactory.
        /// </summary>
        public SyncFilterJoins()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncFilterJoins"/> class.
        /// Create a new collection of tables for a SyncSchema.
        /// </summary>
        public SyncFilterJoins(SyncSet schema) => this.Schema = schema;

        /// <summary>
        /// Since we don't serializer the reference to the schema, this method will reaffect the correct schema.
        /// </summary>
        public void EnsureFilters(SyncSet schema)
        {
            this.Schema = schema;

            foreach (var item in this)
                item.EnsureFilterJoin(schema);
        }

        /// <summary>
        /// Add a new filter.
        /// </summary>
        public void Add(SyncFilterJoin item)
        {
            item.Schema = this.Schema;
            this.InnerCollection.Add(item);
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
        SyncFilterJoin IList<SyncFilterJoin>.this[int index] { get => this.InnerCollection[index]; set => this.InnerCollection[index] = value; }

        /// <summary>
        /// Gets or sets the filter at the specified index.
        /// </summary>
        public SyncFilterJoin this[int index] { get => this.InnerCollection[index]; set => this.InnerCollection[index] = value; }

        /// <summary>
        /// Insert a filter at the specified index.
        /// </summary>
        public void Insert(int index, SyncFilterJoin item) => this.InnerCollection.Insert(index, item);

        /// <summary>
        /// Remove a filter.
        /// </summary>
        public bool Remove(SyncFilterJoin item) => this.InnerCollection.Remove(item);

        /// <summary>
        /// Check if the collection contains a filter.
        /// </summary>
        public bool Contains(SyncFilterJoin item) => this.InnerCollection.Contains(item);

        /// <summary>
        /// Copy the collection to an array.
        /// </summary>
        public void CopyTo(SyncFilterJoin[] array, int arrayIndex) => this.InnerCollection.CopyTo(array, arrayIndex);

        /// <summary>
        /// Get the index of a filter.
        /// </summary>
        public int IndexOf(SyncFilterJoin item) => this.InnerCollection.IndexOf(item);

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
        public IEnumerator<SyncFilterJoin> GetEnumerator() => this.InnerCollection.GetEnumerator();

        /// <summary>
        /// Get the enumerator.
        /// </summary>
        public override string ToString() => this.InnerCollection.Count.ToString();
    }
}