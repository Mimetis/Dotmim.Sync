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
    /// Represents a list of tables to be added to the sync process.
    /// </summary>
    [CollectionDataContract(Name = "tbls", ItemName = "tbl"), Serializable]
    public class SetupTables : ICollection<SetupTable>, IList<SetupTable>
    {

        /// <summary>
        /// Gets or sets exposing the InnerCollection for serialization purpose.
        /// </summary>
        [DataMember(Name = "c", IsRequired = true, Order = 1)]
        public Collection<SetupTable> InnerCollection { get; set; } = [];

        /// <summary>
        /// Initializes a new instance of the <see cref="SetupTables"/> class.
        /// ctor for serialization purpose.
        /// </summary>
        public SetupTables()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SetupTables"/> class.
        /// Create a list of tables to be added to the sync process.
        /// </summary>
        public SetupTables(IEnumerable<string> tables)
        {
            foreach (var table in tables)
                this.Add(table);
        }

        /// <summary>
        /// Add a new table to the collection of tables to be added to the sync process.
        /// </summary>
        public SetupTable Add(string tableName, string schemaName = null)
        {
            var st = new SetupTable(tableName, schemaName);
            this.Add(st);
            return st;
        }

        /// <summary>
        /// Add a new table to the collection of tables to be added to the sync process.
        /// </summary>
        public void Add(SetupTable item)
        {
            if (this[item.TableName, item.SchemaName] != null)
                throw new Exception($"Table {item.TableName} already exists in the collection");

            this.InnerCollection.Add(item);
        }

        /// <summary>
        /// Add a collection of tables to be added to the sync process.
        /// </summary>
        public void AddRange(IEnumerable<SetupTable> tables)
        {
            foreach (var table in tables)
                this.InnerCollection.Add(table);
        }

        /// <summary>
        /// Add a collection of tables to be added to the sync process.
        /// </summary>
        public void AddRange(IEnumerable<string> tables)
        {
            if (tables == null)
                return;

            foreach (var table in tables)
                this.InnerCollection.Add(new SetupTable(table));
        }

        /// <summary>
        /// Get a table by its name.
        /// </summary>
        public SetupTable this[string tableName]
        {
            get
            {
                if (string.IsNullOrEmpty(tableName))
                    throw new ArgumentNullException(nameof(tableName));

                var parser = new TableParser(tableName);
                var tblName = parser.TableName;
                var schemaName = parser.SchemaName;
                schemaName ??= string.Empty;

                var sc = SyncGlobalization.DataSourceStringComparison;

                var table = this.InnerCollection.FirstOrDefault(innerTable =>
                {
                    var innerTableSchemaName = string.IsNullOrEmpty(innerTable.SchemaName) ? string.Empty : innerTable.SchemaName;
                    return string.Equals(innerTable.TableName, tblName, sc) && string.Equals(innerTableSchemaName, schemaName, StringComparison.Ordinal);
                });

                return table;
            }
        }

        /// <summary>
        /// Get a table by its name.
        /// </summary>
        public SetupTable this[string tableName, string schemaName]
        {
            get
            {
                if (string.IsNullOrEmpty(tableName))
                    throw new ArgumentNullException(nameof(tableName));

                schemaName ??= string.Empty;

                var sc = SyncGlobalization.DataSourceStringComparison;

                var table = this.InnerCollection.FirstOrDefault(innerTable =>
                {
                    var innerTableSchemaName = string.IsNullOrEmpty(innerTable.SchemaName) ? string.Empty : innerTable.SchemaName;
                    return string.Equals(innerTable.TableName, tableName, sc) && string.Equals(innerTableSchemaName, schemaName, StringComparison.Ordinal);
                });

                return table;
            }
        }

        /// <summary>
        /// Gets a value indicating whether check if Setup has tables.
        /// </summary>
        public bool HasTables => this.InnerCollection?.Count > 0;

        /// <summary>
        /// Gets a value indicating whether check if Setup has at least one table with columns.
        /// </summary>
        public bool HasColumns => this.InnerCollection?.SelectMany(t => t.Columns).Count() > 0;  // using SelectMany to get columns and not Collection<Column>

        /// <summary>
        /// Clear all tables.
        /// </summary>
        public void Clear() => this.InnerCollection.Clear();

        /// <summary>
        /// Gets the count of tables.
        /// </summary>
        public int Count => this.InnerCollection.Count;

        /// <summary>
        /// Gets a value indicating whether the collection is readonly.
        /// </summary>
        public bool IsReadOnly => false;

        /// <summary>
        /// Get a table by its index.
        /// </summary>
        public SetupTable this[int index] { get => this.InnerCollection[index]; set => this.InnerCollection[index] = value; }

        /// <summary>
        /// Remove a table from the list of tables to be added to the sync.
        /// </summary>
        public bool Remove(SetupTable item) => this.InnerCollection.Remove(item);

        /// <summary>
        /// Check if the collection contains a table.
        /// </summary>
        public bool Contains(SetupTable item) => this[item.TableName, item.SchemaName] != null;

        /// <summary>
        /// Copy the list of tables to an array.
        /// </summary>
        public void CopyTo(SetupTable[] array, int arrayIndex) => this.InnerCollection.CopyTo(array, arrayIndex);

        /// <summary>
        /// Get the index of a table in the list of tables to be added to the sync.
        /// </summary>
        public int IndexOf(SetupTable item) => this.InnerCollection.IndexOf(item);

        /// <summary>
        /// Remove a table from the list of tables to be added to the sync.
        /// </summary>
        public void RemoveAt(int index) => this.InnerCollection.RemoveAt(index);

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        public override string ToString() => this.InnerCollection.Count.ToString();

        /// <summary>
        /// Insert a table at a specific index.
        /// </summary>
        public void Insert(int index, SetupTable item) => this.InnerCollection.Insert(index, item);

        /// <summary>
        /// Get the enumerator for the collection.
        /// </summary>
        public IEnumerator<SetupTable> GetEnumerator() => this.InnerCollection.GetEnumerator();

        /// <summary>
        /// Get the enumerator for the collection.
        /// </summary>
        IEnumerator<SetupTable> IEnumerable<SetupTable>.GetEnumerator() => this.InnerCollection.GetEnumerator();

        /// <summary>
        /// Get the enumerator for the collection.
        /// </summary>
        IEnumerator IEnumerable.GetEnumerator() => this.InnerCollection.GetEnumerator();
    }
}