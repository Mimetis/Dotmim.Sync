using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Runtime.Serialization;

namespace Dotmim.Sync
{
    /// <summary>
    /// Represents a collection of columns.
    /// </summary>
    [CollectionDataContract(Name = "cols", ItemName = "col"), Serializable]
    public class SyncColumns : ICollection<SyncColumn>, IList<SyncColumn>
    {
        private Dictionary<string, int> indexes = new();
        private Collection<SyncColumn> innerCollection = new Collection<SyncColumn>();

        /// <summary>
        /// Gets or sets exposing the InnerCollection for serialization purpose.
        /// </summary>
        [DataMember(Name = "c", IsRequired = true, Order = 1)]
        public Collection<SyncColumn> InnerCollection
        {
            get => this.innerCollection;
            set
            {
                this.innerCollection = value;
                this.AffectOrder();
            }
        }

        /// <summary>
        /// Gets column's schema.
        /// </summary>
        [IgnoreDataMember]
        public SyncTable Table { get; internal set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncColumns"/> class.
        /// Create a default collection for SerializersFactory.
        /// </summary>
        public SyncColumns()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncColumns"/> class.
        /// Create a new collection of tables for a SyncSchema.
        /// </summary>
        public SyncColumns(SyncTable table) => this.Table = table;

        /// <summary>
        /// Since we don't serializer the reference to the schema, this method will reaffect the correct schema.
        /// </summary>
        public void EnsureColumns(SyncTable table) => this.Table = table;

        /// <summary>
        /// Get a Column by its name.
        /// </summary>
        public SyncColumn this[string columnName]
        {
            get
            {
                // InnerCollection.FirstOrDefault(c => string.Equals(c.ColumnName, columnName, SyncGlobalization.DataSourceStringComparison));
                if (this.indexes.TryGetValue(columnName.ToLowerInvariant(), out var index))
                    return this.InnerCollection[index];

                return null;
            }
        }

        /// <summary>
        /// Returns a bool indicating if the columns contains at least one column of type argument.
        /// </summary>
        /// <returns></returns>
        public bool HasSyncColumnOfType(Type type) => this.InnerCollection.Any(sc => sc.GetDataType() == type);

        /// <summary>
        /// Add a new Column to the Schema Column collection.
        /// </summary>
        public void Add(SyncColumn item)
        {
            this.InnerCollection.Add(item);
            this.AffectOrder();
        }

        /// <summary>
        /// Add a new Column to the Schema Column collection.
        /// </summary>
        public void Add(string columnName, Type type = null)
        {
            var item = new SyncColumn(columnName, type);
            this.InnerCollection.Add(item);
            this.AffectOrder();
        }

        /// <summary>
        /// Add a collection of columns.
        /// </summary>
        public void AddRange(SyncColumn[] addedColumns)
        {
            foreach (var item in addedColumns)
                this.InnerCollection.Add(item);

            this.AffectOrder();
        }

        /// <summary>
        /// Reorganize columns order.
        /// </summary>
        public void Reorder(SyncColumn column, int newPosition)
        {
            if (newPosition < 0 || newPosition > this.InnerCollection.Count - 1)
                throw new Exception($"InvalidOrdinal(ordinal, {newPosition}");

            // Remove column fro collection
            this.InnerCollection.Remove(column);

            // Add at the end or insert in new positions
            if (newPosition > this.InnerCollection.Count - 1)
                this.InnerCollection.Add(column);
            else
                this.InnerCollection.Insert(newPosition, column);

            this.AffectOrder();
        }

        /// <summary>
        /// Clear all the relations.
        /// </summary>
        public void Clear()
        {
            this.InnerCollection.Clear();
            this.indexes.Clear();
        }

        private void AffectOrder()
        {
            // now reordered correctly, affect new Ordinal property
            for (int i = 0; i < this.InnerCollection.Count; i++)
            {
                var column = this.InnerCollection[i];
                this.indexes[column.ColumnName.ToLowerInvariant()] = i;
                column.Ordinal = i;
            }
        }

        /// <summary>
        /// Gets the number of elements contained in the collection.
        /// </summary>
        public int Count => this.InnerCollection.Count;

        /// <summary>
        /// Gets a value indicating whether the collection is read-only.
        /// </summary>
        public bool IsReadOnly => false;

        /// <summary>
        /// Gets or sets the element at the specified index.
        /// </summary>
        SyncColumn IList<SyncColumn>.this[int index]
        {
            get => this.InnerCollection[index];
            set => this.InnerCollection[index] = value;
        }

        /// <summary>
        /// Gets or sets the element at the specified index.
        /// </summary>
        public SyncColumn this[int index]
        {
            get => this.InnerCollection[index];
            set => this.InnerCollection[index] = value;
        }

        /// <summary>
        /// Remove a column from the collection.
        /// </summary>
        public bool Remove(SyncColumn item) => this.InnerCollection.Remove(item);

        /// <summary>
        /// Check if the collection contains a column.
        /// </summary>
        public bool Contains(SyncColumn item) => this.InnerCollection.Contains(item);

        /// <summary>
        /// Copy the collection to an array.
        /// </summary>
        public void CopyTo(SyncColumn[] array, int arrayIndex) => this.InnerCollection.CopyTo(array, arrayIndex);

        /// <summary>
        /// Returns the index of a column in the collection.
        /// </summary>
        public int IndexOf(SyncColumn item) => this.indexes[item.ColumnName.ToLowerInvariant()];

        /// <summary>
        /// Returns the index of a column in the collection.
        /// </summary>
        public int IndexOf(string columnName) => this.indexes[columnName.ToLowerInvariant()];

        /// <summary>
        /// Remove a column at a specific index.
        /// </summary>
        public void RemoveAt(int index) => this.InnerCollection.RemoveAt(index);

        /// <summary>
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        IEnumerator IEnumerable.GetEnumerator() => this.InnerCollection.GetEnumerator();

        /// <summary>
        /// Returns an enumerator that iterates through the collection.
        /// </summary>
        public IEnumerator<SyncColumn> GetEnumerator() => this.InnerCollection.GetEnumerator();

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        public override string ToString() => this.InnerCollection.Count.ToString();

        /// <summary>
        /// Insert a column at a specific index.
        /// </summary>
        public void Insert(int index, SyncColumn item) => this.InnerCollection.Insert(index, item);
    }
}