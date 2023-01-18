using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Linq;
using System.Text;

namespace Dotmim.Sync
{

    public class SyncRows : ICollection<SyncRow>, IList<SyncRow>
    {
        public SyncTable Table { get; set; }
        private List<SyncRow> rows = new List<SyncRow>();

        public SyncRows(SyncTable table) => this.Table = table;

        /// <summary>
        /// Since we don't serializer the reference to the schema, this method will reaffect the correct schema
        /// </summary>
        public void EnsureRows(SyncTable table)
        {
            this.Table = table;

            if (rows != null)
                foreach (var row in this)
                    row.SchemaTable = table;
        }

        /// <summary>
        /// Add a new buffer row. Be careful, row should include state in first index
        /// </summary>
        public void Add(object[] row)
        {
            var schemaRow = new SyncRow(this.Table, row);
            rows.Add(schemaRow);
        }

        /// <summary>
        /// Add a rows. Be careful, row should include state in first index
        /// </summary>
        public void AddRange(IEnumerable<object[]> rows)
        {
            foreach (var row in rows)
            {
                var schemaRow = new SyncRow(this.Table, row);
                this.rows.Add(schemaRow);
            }
        }

        public void AddRange(List<SyncRow> rows)
        {
            foreach (var item in rows)
            {
                // TryEnsureData(item);
                item.SchemaTable = this.Table;
                this.rows.Add(item);

            }
        }
        /// <summary>
        /// Add a new row to the collection
        /// </summary>
        public void Add(SyncRow item)
        {
            // TryEnsureData(item);
            item.SchemaTable = this.Table;
            this.rows.Add(item);
        }
  
        /// <summary>
        /// Make a filter on primary keys
        /// </summary>
        public static SyncRow GetRowByPrimaryKeys(SyncRow criteria, IList<SyncRow> rows, SyncTable schemaTable )
        {
            // Get the primary keys to get the ordinal
            var primaryKeysColumn = schemaTable.GetPrimaryKeysColumns().ToList();
            var criteriaKeysColumn = criteria.SchemaTable.GetPrimaryKeysColumns().ToList();

            if (primaryKeysColumn.Count != criteriaKeysColumn.Count)
                throw new ArgumentOutOfRangeException($"Can't make a query on primary keys since number of primary keys columns in criteria is not matching the number of primary keys columns in this table");

            var filteredRow = rows.FirstOrDefault(itemRow =>
            {
                for (int i = 0; i < primaryKeysColumn.Count; i++)
                {
                    var syncColumn = primaryKeysColumn[i];

                    object critValue = SyncTypeConverter.TryConvertTo(criteria[syncColumn.ColumnName], syncColumn.GetDataType());
                    object itemValue = SyncTypeConverter.TryConvertTo(itemRow[syncColumn.ColumnName], syncColumn.GetDataType());

                    if (!critValue.Equals(itemValue))
                        return false;
                }
                return true;
            });

            return filteredRow;
        }


        /// <summary>
        /// Ensure schema and data are correctly related
        /// </summary>
        private void TryEnsureData(SyncRow row)
        {
            if (row.Length != this.Table.Columns.Count)
                throw new Exception("The row length does not fit with the DataTable columns count");

            for (int i = 0; i < row.Length; i++)
            {
                var cell = row[i];

                // we can't check the value with the column type, so that's life, go next
                if (cell == null)
                    continue;

                var column = this.Table.Columns[i];
                var columnType = column.GetDataType();
                var cellType = cell.GetType();

                if (columnType.IsEquivalentTo(cellType))
                    continue;

                // if object, no need to verify anything on this column
                if (columnType == typeof(object))
                    continue;

                // everything can be convert to string, I guess :D
                if (columnType == typeof(string))
                    continue;

                var converter = GetConverter(columnType);

                if (converter != null && converter.CanConvertFrom(cellType))
                    continue;

                throw new Exception($"The type of column {columnType.Name} is not compatible with type of row index cell {cellType.Name}");
            }

        }



        /// <summary>
        /// Get type converter
        /// </summary>
        public static TypeConverter GetConverter(Type type)
        {
            var converter = TypeDescriptor.GetConverter(type);

            // Every object could use a TypeConverter, so we exclude it
            if (converter != null && converter.GetType() != typeof(TypeConverter) && converter.CanConvertTo(typeof(string)))
                return converter;

            return null;
        }

        /// <summary>
        /// Clear all rows
        /// </summary>
        public void Clear()
        {
            foreach (var row in rows)
                row.Clear();

            rows.Clear();
        }


        public SyncRow this[int index] => rows[index];
        public int Count => rows.Count;
        public bool IsReadOnly => false;
        SyncRow IList<SyncRow>.this[int index]
        {
            get => this.rows[index];
            set => this.rows[index] = value;
        }
        public bool Remove(SyncRow item) => rows.Remove(item);
        public bool Contains(SyncRow item) => rows.Contains(item);
        public void CopyTo(SyncRow[] array, int arrayIndex) => rows.CopyTo(array, arrayIndex);
        public int IndexOf(SyncRow item) => rows.IndexOf(item);
        public void RemoveAt(int index) => rows.RemoveAt(index);
        public override string ToString() => this.rows.Count.ToString();
        public void Insert(int index, SyncRow item)
        {
            item.SchemaTable = this.Table;
            this.rows.Insert(index, item);
        }
        public IEnumerator<SyncRow> GetEnumerator() => rows.GetEnumerator();
        IEnumerator<SyncRow> IEnumerable<SyncRow>.GetEnumerator() => this.rows.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => this.rows.GetEnumerator();


    }
}
