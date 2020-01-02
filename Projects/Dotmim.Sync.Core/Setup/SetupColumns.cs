using Dotmim.Sync.Builders;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync
{
    /// <summary>
    /// List of columns within a table, to add to the sync process 
    /// </summary>
    public class SetupColumns : ICollection<string>, IList<string>
    {
        // inner collection
        private List<string> innerCollection = new List<string>();
       
        public SetupColumns() { }

        /// <summary>
        /// Add a new column to the list of columns to be added to the sync
        /// </summary>
        public void Add(string columnName)
        {
            var parserColumnName = ParserName.Parse(columnName);
            var columnNameNormalized = parserColumnName.ObjectName;

            if (innerCollection.Any(c => string.Equals(c, columnName, SyncGlobalization.DataSourceStringComparison)))
                throw new Exception($"Column name {columnNameNormalized} already exists in the table");

            innerCollection.Add(columnNameNormalized);
        }

        /// <summary>
        /// Add a range of columns to the sync process setup
        /// </summary>
        public void AddRange(IEnumerable<string> columnsName)
        {
            foreach (var columnName in columnsName) 
                this.Add(columnName);
        }

        /// <summary>
        /// Clear all columns
        /// </summary>
        public void Clear() => this.innerCollection.Clear();


        /// <summary>
        /// Get a Column by its name
        /// </summary>
        public string this[string columnName] 
            => innerCollection.FirstOrDefault(c => string.Equals(c, columnName, SyncGlobalization.DataSourceStringComparison));


        public string this[int index] => innerCollection[index];
        public int Count => innerCollection.Count;
        public bool IsReadOnly => false;
        string IList<string>.this[int index] { get => this.innerCollection[index]; set => this.innerCollection[index] = value; }
        public bool Remove(string item) => innerCollection.Remove(item);
        public bool Contains(string item) => innerCollection.Any(c => string.Equals(c, item, SyncGlobalization.DataSourceStringComparison));
        public void CopyTo(string[] array, int arrayIndex) => innerCollection.CopyTo(array, arrayIndex);
        public int IndexOf(string item) => innerCollection.IndexOf(item);
        public void RemoveAt(int index) => innerCollection.RemoveAt(index);
        public override string ToString() => this.innerCollection.Count.ToString();
        public void Insert(int index, string item) => this.innerCollection.Insert(index, item);
        public IEnumerator<string> GetEnumerator() => innerCollection.GetEnumerator();
        IEnumerator<string> IEnumerable<string>.GetEnumerator() => this.innerCollection.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => this.innerCollection.GetEnumerator();

    }
}
