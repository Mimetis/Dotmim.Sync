using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Runtime.Serialization;

namespace Dotmim.Sync
{
    /// <summary>
    /// Sync relations collection.
    /// </summary>
    [CollectionDataContract(Name = "filters", ItemName = "filt"), Serializable]
    public class SyncRelations : ICollection<SyncRelation>, IList<SyncRelation>
    {
        /// <summary>
        /// Gets or sets exposing the InnerCollection for serialization purpose.
        /// </summary>
        [DataMember(Name = "c", IsRequired = true, Order = 1)]
        public Collection<SyncRelation> InnerCollection { get; set; } = [];

        /// <summary>
        /// Gets relation's schema.
        /// </summary>
        [IgnoreDataMember]
        public SyncSet Schema { get; internal set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncRelations"/> class.
        /// Create a default collection for SerializersFactory.
        /// </summary>
        public SyncRelations()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncRelations"/> class.
        /// Create a new collection of tables for a SyncSchema.
        /// </summary>
        public SyncRelations(SyncSet schema) => this.Schema = schema;

        /// <summary>
        /// Since we don't serializer the reference to the schema, this method will reaffect the correct schema.
        /// </summary>
        public void EnsureRelations(SyncSet schema)
        {
            this.Schema = schema;

            if (this.InnerCollection != null)
            {
                foreach (var item in this)
                    item.EnsureRelation(schema);
            }
        }

        /// <summary>
        /// Add a new table to the Schema table collection.
        /// </summary>
        public void Add(SyncRelation item)
        {
            item.Schema = this.Schema;
            this.InnerCollection.Add(item);
        }

        /// <summary>
        /// Clear all the relations.
        /// </summary>
        public void Clear()
        {
            foreach (var item in this.InnerCollection)
                item.Clear();

            this.InnerCollection.Clear();
        }

        /// <summary>
        /// Gets the count of relations.
        /// </summary>
        public int Count => this.InnerCollection.Count;

        /// <summary>
        /// Gets a value indicating whether the collection is read-only.
        /// </summary>
        public bool IsReadOnly => false;

        /// <summary>
        /// Remove a relation from the collection.
        /// </summary>
        public bool Remove(SyncRelation item) => this.InnerCollection.Remove(item);

        /// <summary>
        /// Check if the collection contains a relation.
        /// </summary>
        public bool Contains(SyncRelation item) => this.InnerCollection.Contains(item);

        /// <summary>
        /// Copy the collection to an array.
        /// </summary>
        public void CopyTo(SyncRelation[] array, int arrayIndex) => this.InnerCollection.CopyTo(array, arrayIndex);

        /// <summary>
        /// Get the index of a relation.
        /// </summary>
        public int IndexOf(SyncRelation item) => this.InnerCollection.IndexOf(item);

        /// <summary>
        /// Remove a relation at the specified index.
        /// </summary>
        public void RemoveAt(int index) => this.InnerCollection.RemoveAt(index);

        /// <summary>
        /// Get the enumerator.
        /// </summary>
        IEnumerator IEnumerable.GetEnumerator() => this.InnerCollection.GetEnumerator();

        /// <summary>
        /// Get the enumerator.
        /// </summary>
        public IEnumerator<SyncRelation> GetEnumerator() => this.InnerCollection.GetEnumerator();

        /// <summary>
        /// Gets or sets the relation at the specified index.
        /// </summary>
        public SyncRelation this[int index] { get => this.InnerCollection[index]; set => this.InnerCollection[index] = value; }

        /// <summary>
        /// Insert a relation at the specified index.
        /// </summary>
        public void Insert(int index, SyncRelation item) => this.InnerCollection.Insert(index, item);

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        public override string ToString() => this.InnerCollection.Count.ToString(CultureInfo.InvariantCulture);
    }
}