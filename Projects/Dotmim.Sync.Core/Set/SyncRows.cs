using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;

namespace Dotmim.Sync
{

    /// <summary>
    /// Represents a collection of rows.
    /// </summary>
    public class SyncRows : ICollection<SyncRow>, IList<SyncRow>
    {
        private List<SyncRow> rows = [];

        /// <inheritdoc cref="SyncRows"/>
        public SyncRows(SyncTable table) => this.Table = table;

        /// <summary>
        /// Gets or sets the table associated with the rows.
        /// </summary>
        public SyncTable Table { get; set; }

        /// <summary>
        /// Make a filter on primary keys.
        /// </summary>
        public static SyncRow GetRowByPrimaryKeys(SyncRow criteria, IList<SyncRow> rows, SyncTable schemaTable)
        {
            // Get the primary keys to get the ordinal
            var primaryKeysColumn = schemaTable.GetPrimaryKeysColumns().ToList();
            var criteriaKeysColumn = criteria.SchemaTable.GetPrimaryKeysColumns().ToList();

            if (primaryKeysColumn.Count != criteriaKeysColumn.Count)
                throw new ArgumentException($"Can't make a query on primary keys since number of primary keys columns in criteria is not matching the number of primary keys columns in this table");

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
        /// Get type converter.
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
        /// Since we don't serializer the reference to the schema, this method will reaffect the correct schema.
        /// </summary>
        public void EnsureRows(SyncTable table)
        {
            this.Table = table;

            if (this.rows != null)
            {
                foreach (var row in this)
                    row.SchemaTable = table;
            }
        }

        /// <summary>
        /// Add a new buffer row. Be careful, row should include state in first index.
        /// </summary>
        public void Add(object[] row)
        {
            Guard.ThrowIfNull(row);

            var schemaRow = new SyncRow(this.Table, row);
            this.rows.Add(schemaRow);
        }

        /// <summary>
        /// Add rows. Be careful, row should include state in first index.
        /// </summary>
        public void AddRange(IEnumerable<object[]> rows)
        {
            Guard.ThrowIfNull(rows);

            foreach (var row in rows)
            {
                var schemaRow = new SyncRow(this.Table, row);
                this.rows.Add(schemaRow);
            }
        }

        /// <summary>
        /// Add syncrows. Be careful, row should include state in first index.
        /// </summary>
        public void AddRange(IEnumerable<SyncRow> rows)
        {
            Guard.ThrowIfNull(rows);

            foreach (var item in rows)
            {
                // TryEnsureData(item);
                item.SchemaTable = this.Table;
                this.rows.Add(item);
            }
        }

        /// <summary>
        /// Add a new row to the collection.
        /// </summary>
        public void Add(SyncRow item)
        {
            Guard.ThrowIfNull(item);

            item.SchemaTable = this.Table;
            this.rows.Add(item);
        }

        /// <summary>
        /// Clear all rows.
        /// </summary>
        public void Clear()
        {
            foreach (var row in this.rows)
                row.Clear();

            this.rows.Clear();
        }

        ///// <summary>
        ///// Ensure schema and data are correctly related.
        ///// </summary>
        // [Obsolete("It seems we dont want to check the schema and data, so we can remove this method")]
        // private void TryEnsureData(SyncRow row)
        // {
        //    if (row.Length != this.Table.Columns.Count)
        //        throw new Exception("The row length does not fit with the DataTable columns count");

        // for (int i = 0; i < row.Length; i++)
        //    {
        //        var cell = row[i];

        // // we can't check the value with the column type, so that's life, go next
        //        if (cell == null)
        //            continue;

        // var column = this.Table.Columns[i];
        //        var columnType = column.GetDataType();
        //        var cellType = cell.GetType();

        // if (columnType.IsEquivalentTo(cellType))
        //            continue;

        // // if object, no need to verify anything on this column
        //        if (columnType == typeof(object))
        //            continue;

        // // everything can be convert to string, I guess :D
        //        if (columnType == typeof(string))
        //            continue;

        // var converter = GetConverter(columnType);

        // if (converter != null && converter.CanConvertFrom(cellType))
        //            continue;

        // throw new Exception($"The type of column {columnType.Name} is not compatible with type of row index cell {cellType.Name}");
        //    }
        // }

        /// <summary>
        /// Gets the count of rows.
        /// </summary>
        public int Count => this.rows.Count;

        /// <summary>
        /// Gets a value indicating whether the collection is read-only.
        /// </summary>
        public bool IsReadOnly => false;

        /// <summary>
        /// Get a row by its index.
        /// </summary>
        public SyncRow this[int index]
        {
            get => this.rows[index];
            set => this.rows[index] = value;
        }

        /// <summary>
        /// Remove a row from the collection.
        /// </summary>
        public bool Remove(SyncRow item) => this.rows.Remove(item);

        /// <summary>
        /// Check if the collection contains a row.
        /// </summary>
        public bool Contains(SyncRow item) => this.rows.Contains(item);

        /// <summary>
        /// Copy the collection to an array.
        /// </summary>
        public void CopyTo(SyncRow[] array, int arrayIndex) => this.rows.CopyTo(array, arrayIndex);

        /// <summary>
        /// Gets the index of a row.
        /// </summary>
        public int IndexOf(SyncRow item) => this.rows.IndexOf(item);

        /// <summary>
        /// Remove a row at the specified index.
        /// </summary>
        public void RemoveAt(int index) => this.rows.RemoveAt(index);

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        public override string ToString() => this.rows.Count.ToString(CultureInfo.InvariantCulture);

        /// <summary>
        /// Insert a row at the specified index.
        /// </summary>
        public void Insert(int index, SyncRow item)
        {
            item.SchemaTable = this.Table;
            this.rows.Insert(index, item);
        }

        /// <summary>
        /// Gets the enumerator.
        /// </summary>
        public IEnumerator<SyncRow> GetEnumerator() => this.rows.GetEnumerator();

        /// <summary>
        /// Gets the enumerator.
        /// </summary>
        IEnumerator<SyncRow> IEnumerable<SyncRow>.GetEnumerator() => this.rows.GetEnumerator();

        /// <summary>
        /// Gets the enumerator.
        /// </summary>
        IEnumerator IEnumerable.GetEnumerator() => this.rows.GetEnumerator();
    }
}