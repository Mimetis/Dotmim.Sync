using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Runtime.Serialization;

namespace Dotmim.Sync
{

    /// <summary>
    /// Setup filters for a table.
    /// </summary>
    [CollectionDataContract(Name = "fils", ItemName = "fil"), Serializable]
    public class SetupFilters : ICollection<SetupFilter>, IList<SetupFilter>
    {
        /// <summary>
        /// Gets or sets exposing the InnerCollection for serialization purpose.
        /// </summary>
        [DataMember(Name = "c", IsRequired = true, Order = 1)]
        public Collection<SetupFilter> InnerCollection { get; set; } = new Collection<SetupFilter>();

        /// <summary>
        /// Initializes a new instance of the <see cref="SetupFilters"/> class.
        /// Create a default collection for SerializersFactory.
        /// </summary>
        public SetupFilters()
        {
        }

        /// <summary>
        /// Add a new filter.
        /// </summary>
        public void Add(SetupFilter item)
        {
            Guard.ThrowIfNullOrEmpty(item.TableName, "A SetupFilter needs a table name on which the filter is applied.");

            if (this.InnerCollection.Any(st => item.EqualsByName(st)))
                throw new FilterAlreadyExistsException(item.TableName);

            this.InnerCollection.Add(item);
        }

        /// <summary>
        /// Add a new filter.
        /// </summary>
        public SetupFilters Add(string tableName, string columnName, string schemaName = null, bool allowNull = false)
        {
            // Create a filter on the table
            var item = new SetupFilter(tableName, schemaName);

            // Add a column as parameter. This column will be automaticaly added in the tracking table
            item.AddParameter(columnName, tableName, schemaName, allowNull);

            // add the side where expression
            item.AddWhere(columnName, tableName, columnName, schemaName);

            this.Add(item);
            return this;
        }

        /// <summary>
        /// Clear all filters.
        /// </summary>
        public void Clear() => this.InnerCollection.Clear();

        /// <summary>
        /// Gets the number of elements contained in the collection.
        /// </summary>
        public int Count => this.InnerCollection.Count;

        /// <summary>
        /// Gets a value indicating whether the collection is read-only.
        /// </summary>
        public bool IsReadOnly => false;

        /// <summary>
        /// Gets a filter by its index.
        /// </summary>
        public SetupFilter this[int index] { get => this.InnerCollection[index]; set => this.InnerCollection[index] = value; }

        /// <summary>
        /// Insert a filter at a specific index.
        /// </summary>
        public void Insert(int index, SetupFilter item) => this.InnerCollection.Insert(index, item);

        /// <summary>
        /// Remove a filter from the collection.
        /// </summary>
        public bool Remove(SetupFilter item) => this.InnerCollection.Remove(item);

        /// <summary>
        /// Check if the collection contains a filter.
        /// </summary>
        public bool Contains(SetupFilter item) => this.InnerCollection.Any(f => f.EqualsByName(item));

        /// <summary>
        /// Copy the collection to an array.
        /// </summary>
        public void CopyTo(SetupFilter[] array, int arrayIndex) => this.InnerCollection.CopyTo(array, arrayIndex);

        /// <summary>
        /// Get the index of a filter in the collection.
        /// </summary>
        public int IndexOf(SetupFilter item) => this.InnerCollection.IndexOf(item);

        /// <summary>
        /// Remove a filter at a specific index.
        /// </summary>
        public void RemoveAt(int index) => this.InnerCollection.RemoveAt(index);

        /// <summary>
        /// Gets the enumerator for the collection.
        /// </summary>
        IEnumerator IEnumerable.GetEnumerator() => this.InnerCollection.GetEnumerator();

        /// <summary>
        /// Gets the enumerator for the collection.
        /// </summary>
        public IEnumerator<SetupFilter> GetEnumerator() => this.InnerCollection.GetEnumerator();

        /// <summary>
        /// Returns a string representation of the number of filters in the collection.
        /// </summary>
        public override string ToString() => this.InnerCollection.Count.ToString();
    }
}