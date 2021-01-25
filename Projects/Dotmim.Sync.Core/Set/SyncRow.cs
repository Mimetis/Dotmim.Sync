using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace Dotmim.Sync
{
    public class SyncRow
    {

        // all the values for this line
        private object[] buffer;

        /// <summary>
        /// Gets or Sets the row's table
        /// </summary>
        public SyncTable Table { get; set; }

        /// <summary>
        /// Creates an instance, in which data can be written to,
        /// with the default initial capacity.
        /// </summary>
        public SyncRow(int bufferSize) 
        {
            this.buffer = new object[bufferSize];
            this.Length = bufferSize;
        }

        /// <summary>
        /// Add a new buffer row
        /// </summary>
        public SyncRow(SyncTable table, DataRowState state = DataRowState.Unchanged)
        {
            this.buffer = new object[table.Columns.Count];

            // set correct length
            this.Length = table.Columns.Count;

            // Get a ref
            this.Table = table;

            // Affect new state
            this.RowState = state;
        }


        /// <summary>
        /// Add a new buffer row. This ctor does not make a copy
        /// </summary>
        public SyncRow(SyncTable table, object[] row, DataRowState state = DataRowState.Unchanged) 
        {
            // Direct set of the buffer
            this.buffer = row;

            // set correct length
            this.Length = row.Length;

            // Get a ref
            this.Table = table;

            // Affect new state
            this.RowState = state;

        }

        /// <summary>
        /// Gets or Sets the state of the row
        /// </summary>
        public DataRowState RowState { get; set; }

        /// <summary>
        /// Gets the row Length
        /// </summary>
        public int Length { get; }

        /// <summary>
        /// Get the value in the array that correspond to the column index given
        /// </summary>
        public object this[int index]
        {
            get => buffer[index];
            set => this.buffer[index] = value;
        }

        /// <summary>
        /// Get the value in the array that correspond to the SchemaColumn instance given
        /// </summary>
        public object this[SyncColumn column] => this[column.ColumnName];

        /// <summary>
        /// Get the value in the array that correspond to the column name given
        /// </summary>
        public object this[string columnName]
        {
            get
            {
                var column = this.Table.Columns[columnName];

                if (column == null)
                    throw new ArgumentException("Column is null");

                var index = this.Table.Columns.IndexOf(column);

                return this[index];
            }
            set
            {
                var column = this.Table.Columns[columnName];

                if (column == null)
                    throw new ArgumentException("Column is null");

                var index = this.Table.Columns.IndexOf(column);

                this[index] = value;
            }
        }

        /// <summary>
        /// Get the inner array with state on Index 0. Need to replace with ReadOnlySpan<object> !!!!
        /// </summary>
        /// <returns></returns>
        public object[] ToArray()
        {
            var array = new object[this.Length + 1];
            Array.Copy(this.buffer, 0, array, 1, this.Length);
            
            // set row state on index 0 of my buffer
            array[0] = (int)this.RowState;

            return array;
        }

        /// <summary>
        /// Import a raw array, containing state on Index 0
        /// </summary>
        public void FromArray(object[] row)
        {
            var length = Table.Columns.Count;

            if (row.Length != length + 1)
                throw new Exception("row must contains State on position 0 and UpdateScopeId on position 1");

            Array.Copy(row, 1, this.buffer, 0, length);
            this.RowState = (DataRowState)Convert.ToInt32(row[0]);
        }

        /// <summary>
        /// Clear the data in the buffer
        /// </summary>
        public void Clear()
        {
            Array.Clear(this.buffer, 0, this.buffer.Length);
            this.Table = null;
        }


        /// <summary>
        /// ToString()
        /// </summary>
        public override string ToString()
        {
            if (this.buffer == null || this.buffer.Length == 0)
                return "empty row";

            if (this.Table == null)
                return this.buffer.ToString();

            var sb = new StringBuilder();

            sb.Append($"[Sync state]:{this.RowState}");

            var columns = this.RowState == DataRowState.Deleted ? this.Table.GetPrimaryKeysColumns() : this.Table.Columns;

            foreach(var c in columns)
            {
                var o = this[c.ColumnName];
                var os = o == null ? "<NULL />" : o.ToString();

                sb.Append($", [{c.ColumnName}]:{os}");
            }

            return sb.ToString();
        }
    }
}
