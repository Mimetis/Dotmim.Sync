using Dotmim.Sync.Builders;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using System.Runtime.Serialization;

namespace Dotmim.Sync
{
    /// <summary>
    /// Represents a collection of tables used by the SyncSet.
    /// </summary>
    [CollectionDataContract(Name = "tbls", ItemName = "tbl"), Serializable]
    public class SyncTables : ICollection<SyncTable>, IList<SyncTable>
    {
        /// <summary>
        /// Gets or sets exposing the InnerCollection for serialization purpose.
        /// </summary>
        [DataMember(Name = "c", IsRequired = true, Order = 1)]
        public Collection<SyncTable> InnerCollection { get; set; } = [];

        /// <summary>
        /// Gets table's schema.
        /// </summary>
        [IgnoreDataMember]
        public SyncSet Schema { get; internal set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncTables"/> class.
        /// Create a default collection for SerializersFactory.
        /// </summary>
        public SyncTables()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncTables"/> class.
        /// Create a new collection of tables for a SyncSchema.
        /// </summary>
        public SyncTables(SyncSet schema) => this.Schema = schema;

        /// <summary>
        /// Since we don't serializer the reference to the schema, this method will reaffect the correct schema.
        /// </summary>
        public void EnsureTables(SyncSet schema)
        {
            this.Schema = schema;
            if (this.InnerCollection != null)
            {
                foreach (var table in this)
                    table.EnsureTable(schema);
            }
        }

        /// <summary>
        /// Get a table by its name.
        /// </summary>
        public SyncTable this[string tableName]
        {
            get
            {
                if (string.IsNullOrEmpty(tableName))
                    throw new ArgumentNullException(nameof(tableName));

                var parser = ParserName.Parse(tableName);
                var tblName = parser.ObjectName;
                var schemaName = parser.SchemaName;
                schemaName = schemaName == null ? string.Empty : schemaName;

                var sc = SyncGlobalization.DataSourceStringComparison;

                var table = this.InnerCollection.FirstOrDefault(innerTable =>
                {
                    var innerTableSchemaName = string.IsNullOrEmpty(innerTable.SchemaName) ? string.Empty : innerTable.SchemaName;
                    return string.Equals(innerTable.TableName, tblName, sc) && string.Equals(innerTableSchemaName, schemaName, StringComparison.Ordinal);
                });

                // trying a fallback on default schema name: dbo for sql server, public for postgresql
                table ??= this.InnerCollection.FirstOrDefault(innerTable =>
                    {
                        var innerTableSchemaName = string.IsNullOrEmpty(innerTable.SchemaName) ? string.Empty : innerTable.SchemaName;
                        return string.Equals(innerTable.TableName, tblName, sc) && string.Equals(innerTableSchemaName, "dbo", StringComparison.Ordinal);
                    });

                table ??= this.InnerCollection.FirstOrDefault(innerTable =>
                    {
                        var innerTableSchemaName = string.IsNullOrEmpty(innerTable.SchemaName) ? string.Empty : innerTable.SchemaName;
                        return string.Equals(innerTable.TableName, tblName, sc) && string.Equals(innerTableSchemaName, "public", StringComparison.Ordinal);
                    });

                return table;
            }
        }

        /// <summary>
        /// Get a table by its name.
        /// </summary>
        public SyncTable this[string tableName, string schemaName]
        {
            get
            {
                if (string.IsNullOrEmpty(tableName))
                    throw new ArgumentNullException(nameof(tableName));

                var parser = ParserName.Parse(tableName);
                var tblName = parser.ObjectName;

                schemaName = schemaName == null ? string.Empty : schemaName;

                var sc = SyncGlobalization.DataSourceStringComparison;

                var table = this.InnerCollection.FirstOrDefault(innerTable =>
                {
                    var innerTableSchemaName = string.IsNullOrEmpty(innerTable.SchemaName) ? string.Empty : innerTable.SchemaName;
                    return string.Equals(innerTable.TableName, tblName, sc) && string.Equals(innerTableSchemaName, schemaName, StringComparison.Ordinal);
                });

                // trying a fallback on default schema name: dbo for sql server, public for postgresql
                table ??= this.InnerCollection.FirstOrDefault(innerTable =>
                    {
                        var innerTableSchemaName = string.IsNullOrEmpty(innerTable.SchemaName) ? string.Empty : innerTable.SchemaName;
                        return string.Equals(innerTable.TableName, tblName, sc) && string.Equals(innerTableSchemaName, "dbo", StringComparison.Ordinal);
                    });

                table ??= this.InnerCollection.FirstOrDefault(innerTable =>
                    {
                        var innerTableSchemaName = string.IsNullOrEmpty(innerTable.SchemaName) ? string.Empty : innerTable.SchemaName;
                        return string.Equals(innerTable.TableName, tblName, sc) && string.Equals(innerTableSchemaName, "public", StringComparison.Ordinal);
                    });

                return table;
            }
        }

        /// <summary>
        /// Add a new table to the Schema table collection.
        /// </summary>
        public void Add(SyncTable item)
        {
            Guard.ThrowIfNull(item);

            item.Schema = this.Schema;
            this.InnerCollection.Add(item);
        }

        /// <summary>
        /// Add a table, by its name. Be careful, can contains schema name.
        /// </summary>
        public void Add(string table)
        {
            // Potentially user can pass something like [SalesLT].[Product]
            // or SalesLT.Product or Product. ParserName will handle it
            var parser = ParserName.Parse(table);

            var tableName = parser.ObjectName;
            var schemaName = parser.SchemaName;

            var sTable = new SyncTable(tableName, schemaName);

            this.Add(sTable);
        }

        /// <summary>
        /// Add some tables to ContainerSet Tables property.
        /// </summary>
        public void Add(IEnumerable<string> tables)
        {
            Guard.ThrowIfNull(tables);

            foreach (var t in tables)
                this.Add(t);
        }

        /// <summary>
        /// Clear all the Tables.
        /// </summary>
        public void Clear()
        {
            foreach (var table in this)
                table.Clear();

            this.InnerCollection.Clear();
        }

        /// <summary>
        /// Gets get the count of tables in the collection.
        /// </summary>
        public int Count => this.InnerCollection.Count;

        /// <summary>
        /// Gets a value indicating whether gets if the collection is readonly.
        /// </summary>
        public bool IsReadOnly => false;

        /// <summary>
        /// Get the index of a table in the collection.
        /// </summary>
        public SyncTable this[int index] { get => this.InnerCollection[index]; set => this.InnerCollection[index] = value; }

        /// <summary>
        /// Remove a table from the collection.
        /// </summary>
        public bool Remove(SyncTable item) => this.InnerCollection.Remove(item);

        /// <summary>
        /// Check if the collection contains a table.
        /// </summary>
        public bool Contains(SyncTable item) => this.InnerCollection.Contains(item);

        /// <summary>
        /// Copy the collection to an array.
        /// </summary>
        public void CopyTo(SyncTable[] array, int arrayIndex) => this.InnerCollection.CopyTo(array, arrayIndex);

        /// <summary>
        /// Get the index of a table in the collection.
        /// </summary>
        public int IndexOf(SyncTable item) => this.InnerCollection.IndexOf(item);

        /// <summary>
        /// Remove a table at a specific index.
        /// </summary>
        public void RemoveAt(int index) => this.InnerCollection.RemoveAt(index);

        /// <summary>
        /// Get the enumerator for the collection.
        /// </summary>
        IEnumerator IEnumerable.GetEnumerator() => this.InnerCollection.GetEnumerator();

        /// <summary>
        /// Get the enumerator for the collection.
        /// </summary>
        public IEnumerator<SyncTable> GetEnumerator() => this.InnerCollection.GetEnumerator();

        /// <summary>
        /// Insert a table at a specific index.
        /// </summary>
        public void Insert(int index, SyncTable item) => this.InnerCollection.Insert(index, item);

        /// <summary>
        /// Return the collection as a string representing the tables count.
        /// </summary>
        public override string ToString() => this.InnerCollection.Count.ToString(CultureInfo.InvariantCulture);
    }
}