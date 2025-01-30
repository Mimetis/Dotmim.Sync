using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Runtime.Serialization;

namespace Dotmim.Sync
{
    /// <summary>
    /// Sync filters where side collection.
    /// </summary>
    [CollectionDataContract(Name = "filterswhereside", ItemName = "whereside"), Serializable]
    public class SyncFilterWhereSideItems : ICollection<SyncFilterWhereSideItem>, IList<SyncFilterWhereSideItem>
    {
        /// <summary>
        /// Gets or sets exposing the InnerCollection for serialization purpose.
        /// </summary>
        [DataMember(Name = "c", IsRequired = true, Order = 1)]
        public Collection<SyncFilterWhereSideItem> InnerCollection { get; set; } = [];

        /// <summary>
        /// Gets filter's schema.
        /// </summary>
        [IgnoreDataMember]
        public SyncSet Schema { get; internal set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncFilterWhereSideItems"/> class.
        /// Create a default collection for SerializersFactory.
        /// </summary>
        public SyncFilterWhereSideItems()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncFilterWhereSideItems"/> class.
        /// Create a new collection of tables for a SyncSchema.
        /// </summary>
        public SyncFilterWhereSideItems(SyncSet schema) => this.Schema = schema;

        /// <summary>
        /// Since we don't serializer the reference to the schema, this method will reaffect the correct schema.
        /// </summary>
        public void EnsureFilters(SyncSet schema)
        {
            this.Schema = schema;

            foreach (var item in this)
                item.EnsureFilterWhereSideItem(schema);
        }

        /// <summary>
        /// Add a new filter.
        /// </summary>
        public void Add(SyncFilterWhereSideItem item)
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
        /// Get a table by its index.
        /// </summary>
        public SyncFilterWhereSideItem this[int index] { get => this.InnerCollection[index]; set => this.InnerCollection[index] = value; }

        /// <summary>
        /// Insert a new filter at the specified index.
        /// </summary>
        public void Insert(int index, SyncFilterWhereSideItem item) => this.InnerCollection.Insert(index, item);

        /// <summary>
        /// Remove a filter from the collection.
        /// </summary>
        public bool Remove(SyncFilterWhereSideItem item) => this.InnerCollection.Remove(item);

        /// <summary>
        /// Check if the collection contains a filter.
        /// </summary>
        public bool Contains(SyncFilterWhereSideItem item) => this.InnerCollection.Contains(item);

        /// <summary>
        /// Copy the collection to an array.
        /// </summary>
        public void CopyTo(SyncFilterWhereSideItem[] array, int arrayIndex) => this.InnerCollection.CopyTo(array, arrayIndex);

        /// <summary>
        /// Get the index of a filter.
        /// </summary>
        public int IndexOf(SyncFilterWhereSideItem item) => this.InnerCollection.IndexOf(item);

        /// <summary>
        /// Remove a filter at the specified index.
        /// </summary>
        public void RemoveAt(int index) => this.InnerCollection.RemoveAt(index);

        /// <summary>
        /// Gets the enumerator.
        /// </summary>
        IEnumerator IEnumerable.GetEnumerator() => this.InnerCollection.GetEnumerator();

        /// <summary>
        /// Gets the enumerator.
        /// </summary>
        public IEnumerator<SyncFilterWhereSideItem> GetEnumerator() => this.InnerCollection.GetEnumerator();

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        public override string ToString() => this.InnerCollection.Count.ToString(CultureInfo.InvariantCulture);
    }
}