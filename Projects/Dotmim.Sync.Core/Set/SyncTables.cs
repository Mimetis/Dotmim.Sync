using Dotmim.Sync.Builders;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync
{
    [CollectionDataContract(Name = "tbls", ItemName = "tbl"), Serializable]
    public class SyncTables : ICollection<SyncTable>, IList<SyncTable>
    {
        /// <summary>
        /// Exposing the InnerCollection for serialization purpose
        /// </summary>
        [DataMember(Name = "c", IsRequired = true, Order = 1)]
        public Collection<SyncTable> InnerCollection { get; set; } = new Collection<SyncTable>();

        /// <summary>
        /// Table's schema
        /// </summary>
        [IgnoreDataMember]
        public SyncSet Schema { get; internal set; }

        /// <summary>
        /// Create a default collection for Serializers
        /// </summary>
        public SyncTables()
        {
        }

        /// <summary>
        /// Create a new collection of tables for a SyncSchema
        /// </summary>
        public SyncTables(SyncSet schema) => this.Schema = schema;

        /// <summary>
        /// Since we don't serializer the reference to the schema, this method will reaffect the correct schema
        /// </summary>
        public void EnsureTables(SyncSet schema)
        {
            this.Schema = schema;
            if (InnerCollection != null)
                foreach (var table in this)
                    table.EnsureTable(schema);
        }

        /// <summary>
        /// Get a table by its name
        /// </summary>
        public SyncTable this[string tableName]
        {
            get
            {
                if (string.IsNullOrEmpty(tableName))
                    throw new ArgumentNullException("tableName");

                var parser = ParserName.Parse(tableName);
                var tblName = parser.ObjectName;
                var schemaName = parser.SchemaName;

                // Create a tmp synctable to benefit the SyncTable.Equals() method
                using (var tmpSt = new SyncTable(tblName, schemaName)) 
                    return InnerCollection.FirstOrDefault(st => st == tmpSt);
            }
        }

        /// <summary>
        /// Get a table by its name
        /// </summary>
        public SyncTable this[string tableName, string schemaName]
        {
            get
            {
                if (string.IsNullOrEmpty(tableName))
                    throw new ArgumentNullException("tableName");

                schemaName = schemaName ?? string.Empty;

                // Create a tmp synctable to benefit the SyncTable.Equals() method
                using (var tmpSt = new SyncTable(tableName, schemaName))
                    return InnerCollection.FirstOrDefault(c => c == tmpSt);
            }
        }

        /// <summary>
        /// Add a new table to the Schema table collection
        /// </summary>
        public void Add(SyncTable item)
        {
            item.Schema = Schema;
            InnerCollection.Add(item);
        }

        /// <summary>
        /// Add a table, by its name. Be careful, can contains schema name
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
        /// Add some tables to ContainerSet Tables property
        /// </summary>
        public void Add(IEnumerable<string> tables)
        {
            foreach (var t in tables)
                this.Add(t);
        }


        /// <summary>
        /// Clear all the Tables
        /// </summary>
        public void Clear()
        {
            foreach (var table in this)
                table.Clear();

            InnerCollection.Clear();
        }


        public SyncTable this[int index] => InnerCollection[index];
        public int Count => InnerCollection.Count;
        public bool IsReadOnly => false;
        public bool Remove(SyncTable item) => InnerCollection.Remove(item);
        public bool Contains(SyncTable item) => InnerCollection.Contains(item);
        public void CopyTo(SyncTable[] array, int arrayIndex) => InnerCollection.CopyTo(array, arrayIndex);
        public int IndexOf(SyncTable item) => InnerCollection.IndexOf(item);
        public void RemoveAt(int index) => InnerCollection.RemoveAt(index);
        IEnumerator IEnumerable.GetEnumerator() => InnerCollection.GetEnumerator();
        public IEnumerator<SyncTable> GetEnumerator() => InnerCollection.GetEnumerator();
        SyncTable IList<SyncTable>.this[int index] { get => InnerCollection[index]; set => InnerCollection[index] = value; }
        public void Insert(int index, SyncTable item) => InnerCollection.Insert(index, item);
        public override string ToString() => this.InnerCollection.Count.ToString();
    }

}
