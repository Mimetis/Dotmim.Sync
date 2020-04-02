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
    /// <summary>
    /// Represents a list of tables to be added to the sync process
    /// </summary>
    [CollectionDataContract(Name = "tbls", ItemName = "tbl"), Serializable]
    public class SetupTables : ICollection<SetupTable>, IList<SetupTable>
    {

        /// <summary>
        /// Exposing the InnerCollection for serialization purpose
        /// </summary>
        [DataMember(Name = "c", IsRequired = true, Order = 1)]
        public Collection<SetupTable> InnerCollection = new Collection<SetupTable>();

        public SetupTables()
        {

        }
        /// <summary>
        /// Create a list of tables to be added to the sync process
        /// </summary>
        /// <param name="caseSensitive">Specify if table names are case sensitive. Default is false</param>
        public SetupTables(IEnumerable<string> tables)
        {
            foreach (var table in tables)
                this.Add(table);
        }

        /// <summary>
        /// Add a new table to the collection of tables to be added to the sync process
        /// </summary>
        public SetupTable Add(string tableName, string schemaName = null)
        {
            var st = new SetupTable(tableName, schemaName);
            this.Add(st);
            return st;
        }

        /// <summary>
        /// Add a new table to the collection of tables to be added to the sync process
        /// </summary>
        public void Add(SetupTable table)
        {
            if (InnerCollection.Any(st => table == st))
                throw new Exception($"Table {table.TableName} already exists in the collection");

            InnerCollection.Add(table);
        }


        /// <summary>
        /// Add a collection of tables to be added to the sync process
        /// </summary>
        public void AddRange(IEnumerable<SetupTable> tables)
        {
            foreach (var table in tables)
                this.InnerCollection.Add(table);
        }


        /// <summary>
        /// Add a collection of tables to be added to the sync process
        /// </summary>
        public void AddRange(IEnumerable<string> tables)
        {
            if (tables == null)
                return;

            foreach (var table in tables)
                this.InnerCollection.Add(new SetupTable(table));
        }

        /// <summary>
        /// Get a table by its name
        /// </summary>
        public SetupTable this[string tableName]
        {
            get
            {
                if (string.IsNullOrEmpty(tableName))
                    throw new ArgumentNullException("tableName");

                var parser = ParserName.Parse(tableName);
                var tblName = parser.ObjectName;
                var schemaName = parser.SchemaName;

                // Create a tmp SetupTable to benefit the use of SetupTable.Equals()
                var tmpTable = new SetupTable(tblName, schemaName);

                var table = InnerCollection.FirstOrDefault(c => c == tmpTable);

                if (table == null)
                    throw new MissingTableException(tmpTable.ToString());

                return table;
            }
        }

        /// <summary>
        /// Get a table by its name
        /// </summary>
        public SetupTable this[string tableName, string schemaName]
        {
            get
            {
                if (string.IsNullOrEmpty(tableName))
                    throw new ArgumentNullException("tableName");

                // Create a tmp SetupTable to benefit the use of SetupTable.Equals()
                var tmpTable = new SetupTable(tableName, schemaName);


                var table = InnerCollection.FirstOrDefault(c => c == tmpTable);

                if (table == null)
                    throw new MissingTableException(tmpTable.ToString());

                return table;
            }
        }

        /// <summary>
        /// Check if Setup has tables
        /// </summary>
        public bool HasTables => this.InnerCollection?.Count > 0;

        /// <summary>
        /// Check if Setup has at least one table with columns
        /// </summary>
        public bool HasColumns => this.InnerCollection?.SelectMany(t => t.Columns).Count() > 0;  // using SelectMany to get columns and not Collection<Column>

        public void Clear() => this.InnerCollection.Clear();
        public SetupTable this[int index] => InnerCollection[index];
        public int Count => InnerCollection.Count;
        public bool IsReadOnly => false;
        SetupTable IList<SetupTable>.this[int index] { get => this.InnerCollection[index]; set => this.InnerCollection[index] = value; }
        public bool Remove(SetupTable item) => InnerCollection.Remove(item);
        public bool Contains(SetupTable item) => InnerCollection.Any(st => st == item);
        public void CopyTo(SetupTable[] array, int arrayIndex) => InnerCollection.CopyTo(array, arrayIndex);
        public int IndexOf(SetupTable item) => InnerCollection.IndexOf(item);
        public void RemoveAt(int index) => InnerCollection.RemoveAt(index);
        public override string ToString() => this.InnerCollection.Count.ToString();
        public void Insert(int index, SetupTable item) => this.InnerCollection.Insert(index, item);
        public IEnumerator<SetupTable> GetEnumerator() => InnerCollection.GetEnumerator();
        IEnumerator<SetupTable> IEnumerable<SetupTable>.GetEnumerator() => this.InnerCollection.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => this.InnerCollection.GetEnumerator();
    }
}
