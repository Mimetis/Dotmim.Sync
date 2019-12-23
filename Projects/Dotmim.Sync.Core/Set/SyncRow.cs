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
        private const int MinimumBufferSize = 32;
        private const int BufferGrowthSize = 16;


        // all the values for this line
        private object[] buffer;
        private readonly int length;

        /// <summary>
        /// Gets or Sets the row's table
        /// </summary>
        public SyncTable Table { get; set; }

        /// <summary>
        /// Creates an instance, in which data can be written to,
        /// with the default initial capacity.
        /// </summary>
        public SyncRow() => this.buffer = new object[MinimumBufferSize];

        /// <summary>
        /// Add a new buffer row
        /// </summary>
        public SyncRow(SyncTable table, DataRowState state = DataRowState.Unchanged)
        {
            this.buffer = new object[table.Columns.Count];

            // set correct length
            this.length = table.Columns.Count;

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
            this.length = row.Length;

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
        public int Length => this.length;

        /// <summary>
        /// Get the value in the array that correspond to the column index given
        /// </summary>
        public object this[int index]
        {
            get
            {
                CheckLength(index + 1);
                return buffer[index];
            }
            set
            {
                CheckLength(index + 1);
                this.buffer[index] = value;
            }
        }

        /// <summary>
        /// Check if the buffer size is enough.
        /// If not, resizing the buffer
        /// It implies a copy of the whole buffer, so the less we do, the better it is
        /// </summary>
        private void CheckLength(int maxLength)
        {
            while (maxLength > this.buffer.Length)
            {
                Debug.WriteLine($"Resizing buffer to {this.Length} to {this.Length + BufferGrowthSize }");
                Array.Resize(ref buffer, this.Length + BufferGrowthSize);
            }
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
        /// Get the inner array. Need to replace with ReadOnlySpan<object> !!!!
        /// </summary>
        /// <returns></returns>
        public object[] ToArray()
        {
            var array = new object[this.length + 1];
            Array.Copy(this.buffer, 0, array, 1, this.length);
            
            // set row state on index 0 of my buffer
            array[0] = (int)this.RowState;

            return array;
        }

        /// <summary>
        /// Clear the data in the buffer
        /// </summary>
        public void Clear() => Array.Clear(this.buffer, 0, this.buffer.Length);


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

            sb.Append($"{this.RowState}, ");

            foreach(var c in this.Table.Columns)
            {
                var o = this[c.ColumnName];
                var os = o == null ? "<NULL />" : o.ToString();

                sb.Append($"{c.ColumnName}:{os}, ");
            }

            return sb.ToString();
        }
    }
}
