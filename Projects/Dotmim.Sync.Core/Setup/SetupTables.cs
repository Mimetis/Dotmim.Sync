using Dotmim.Sync.Builders;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync
{
    /// <summary>
    /// Represents a list of tables to be added to the sync process
    /// </summary>
    public class SetupTables : ICollection<SetupTable>, IList<SetupTable>
    {

        private List<SetupTable> innerCollection = new List<SetupTable>();

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
            if (innerCollection.Any(st => table == st))
                throw new Exception($"Table {table.TableName} already exists in the collection");

            innerCollection.Add(table);
        }


        /// <summary>
        /// Add a collection of tables to be added to the sync process
        /// </summary>
        public void AddRange(IEnumerable<SetupTable> tables)
        {
            foreach (var table in tables)
                this.innerCollection.Add(table);
        }


        /// <summary>
        /// Add a collection of tables to be added to the sync process
        /// </summary>
        public void AddRange(IEnumerable<string> tables)
        {
            if (tables == null)
                return;

            foreach (var table in tables)
                this.innerCollection.Add(new SetupTable(table));
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

                var table = innerCollection.FirstOrDefault(c => c == tmpTable);

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


                var table = innerCollection.FirstOrDefault(c => c == tmpTable);

                if (table == null)
                    throw new MissingTableException(tmpTable.ToString());

                return table;
            }
        }

        /// <summary>
        /// Check if Setup has tables
        /// </summary>
        public bool HasTables => this.innerCollection?.Count > 0;

        /// <summary>
        /// Check if Setup has at least one table with columns
        /// </summary>
        public bool HasColumns => this.innerCollection?.SelectMany(t => t.Columns).Count() > 0;  // using SelectMany to get columns and not Collection<Column>

        public void Clear() => this.innerCollection.Clear();
        public SetupTable this[int index] => innerCollection[index];
        public int Count => innerCollection.Count;
        public bool IsReadOnly => false;
        SetupTable IList<SetupTable>.this[int index] { get => this.innerCollection[index]; set => this.innerCollection[index] = value; }
        public bool Remove(SetupTable item) => innerCollection.Remove(item);
        public bool Contains(SetupTable item) => innerCollection.Any(st => st == item);
        public void CopyTo(SetupTable[] array, int arrayIndex) => innerCollection.CopyTo(array, arrayIndex);
        public int IndexOf(SetupTable item) => innerCollection.IndexOf(item);
        public void RemoveAt(int index) => innerCollection.RemoveAt(index);
        public override string ToString() => this.innerCollection.Count.ToString();
        public void Insert(int index, SetupTable item) => this.innerCollection.Insert(index, item);
        public IEnumerator<SetupTable> GetEnumerator() => innerCollection.GetEnumerator();
        IEnumerator<SetupTable> IEnumerable<SetupTable>.GetEnumerator() => this.innerCollection.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => this.innerCollection.GetEnumerator();
    }
}
