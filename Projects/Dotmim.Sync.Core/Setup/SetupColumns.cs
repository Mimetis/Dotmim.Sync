using Dotmim.Sync.DatabaseStringParsers;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Runtime.Serialization;

namespace Dotmim.Sync
{
    /// <summary>
    /// List of columns within a table, to add to the sync process.
    /// </summary>
    [CollectionDataContract(Name = "cols", ItemName = "col"), Serializable]
    public class SetupColumns : ICollection<string>, IList<string>
    {
        /// <summary>
        /// Gets or sets exposing the InnerCollection for serialization purpose.
        /// </summary>
        [DataMember(Name = "c", IsRequired = true, Order = 1)]
        public Collection<string> InnerCollection { get; set; } = [];

        /// <inheritdoc cref="SetupColumns"/>
        public SetupColumns() { }

        /// <summary>
        /// Add a new column to the list of columns to be added to the sync.
        /// </summary>
        public void Add(string item)
        {
            var parserColumnName = new ObjectParser(item);
            var columnNameNormalized = parserColumnName.ObjectName;

            if (this.InnerCollection.Any(c => string.Equals(c, item, SyncGlobalization.DataSourceStringComparison)))
                throw new Exception($"Column name {columnNameNormalized} already exists in the table");

            this.InnerCollection.Add(columnNameNormalized);
        }

        /// <summary>
        /// Add a range of columns to the sync process setup.
        /// </summary>
        public void AddRange(IEnumerable<string> columnsName)
        {
            foreach (var columnName in columnsName)
                this.Add(columnName);
        }

        /// <summary>
        /// Add a range of columns to the sync process setup.
        /// </summary>
        public void AddRange(params string[] columnsName)
        {
            foreach (var columnName in columnsName)
                this.Add(columnName);
        }

        /// <summary>
        /// Clear all columns.
        /// </summary>
        public void Clear() => this.InnerCollection.Clear();

        /// <summary>
        /// Get a Column by its name.
        /// </summary>
        public string this[string columnName]
            => this.InnerCollection.FirstOrDefault(c => string.Equals(c, columnName, SyncGlobalization.DataSourceStringComparison));

        /// <summary>
        /// Gets get the count of columns.
        /// </summary>
        public int Count => this.InnerCollection.Count;

        /// <summary>
        /// Gets a value indicating whether get if the collection is readonly.
        /// </summary>
        public bool IsReadOnly => false;

        /// <summary>
        /// Get a Column by its index.
        /// </summary>
        string IList<string>.this[int index] { get => this.InnerCollection[index]; set => this.InnerCollection[index] = value; }

        /// <summary>
        /// Get a Column by its index.
        /// </summary>
        protected string this[int index] { get => this.InnerCollection[index]; set => this.InnerCollection[index] = value; }

        /// <summary>
        /// Remove a column from the list of columns to be added to the sync.
        /// </summary>
        public bool Remove(string item) => this.InnerCollection.Remove(item);

        /// <summary>
        /// Check if the column exists in the list of columns to be added to the sync.
        /// </summary>
        public bool Contains(string item) => this.InnerCollection.Any(c => string.Equals(c, item, SyncGlobalization.DataSourceStringComparison));

        /// <summary>
        /// Copy the list of columns to an array.
        /// </summary>
        public void CopyTo(string[] array, int arrayIndex) => this.InnerCollection.CopyTo(array, arrayIndex);

        /// <summary>
        /// Get the index of a column in the list of columns to be added to the sync.
        /// </summary>
        public int IndexOf(string item) => this.InnerCollection.IndexOf(item);

        /// <summary>
        /// Remove a column from the list of columns to be added to the sync.
        /// </summary>
        public void RemoveAt(int index) => this.InnerCollection.RemoveAt(index);

        /// <summary>
        /// Returns a string representation of the number of columns in the list.
        /// </summary>
        public override string ToString() => this.InnerCollection.Count.ToString();

        /// <summary>
        /// Insert a column at a specific index in the list of columns to be added to the sync.
        /// </summary>
        public void Insert(int index, string item) => this.InnerCollection.Insert(index, item);

        /// <summary>
        /// Returns an enumerator that iterates through the list of columns to be added to the sync.
        /// </summary>
        public IEnumerator<string> GetEnumerator() => this.InnerCollection.GetEnumerator();

        /// <summary>
        /// Returns an enumerator that iterates through the list of columns to be added to the sync.
        /// </summary>
        IEnumerator<string> IEnumerable<string>.GetEnumerator() => this.InnerCollection.GetEnumerator();

        /// <summary>
        /// Returns an enumerator that iterates through the list of columns to be added to the sync.
        /// </summary>
        IEnumerator IEnumerable.GetEnumerator() => this.InnerCollection.GetEnumerator();
    }
}