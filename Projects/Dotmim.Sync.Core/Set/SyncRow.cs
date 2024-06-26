using Dotmim.Sync.Enumerations;
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
        
        public SyncTable SchemaTable { get; set; }

        public SyncRow()
        {
            
        }

        /// <summary>
        /// Add a new buffer row
        /// </summary>
        public SyncRow(SyncTable schemaTable, SyncRowState state = SyncRowState.None)
        {
            // Buffer is +1 to store state
            this.buffer = new object[schemaTable.Columns.Count + 1];

            // set correct length
            this.Length = schemaTable.Columns.Count;

            // Get a ref
            this.SchemaTable = schemaTable;

            // Affect new state
            this.buffer[0] = (int)state;

        }


        /// <summary>
        /// Add a new buffer row. This ctor does not make a copy
        /// </summary>
        public SyncRow(SyncTable schemaTable, object[] row)
        {
            if (row.Length <= schemaTable.Columns.Count)
                throw new ArgumentException("row array must have one more item to store state");

            if (row.Length > schemaTable.Columns.Count + 1)
                throw new ArgumentException("row array has too many items");

            // Direct set of the buffer
            this.buffer = row;

            // set columns count as length
            this.Length = schemaTable.Columns.Count;

            // Get a ref
            this.SchemaTable = schemaTable;

        }

        /// <summary>
        /// Gets the state of the row
        /// </summary>
        public SyncRowState RowState
        {
            get => (SyncRowState)Convert.ToInt32(this.buffer[0]);
            set => this.buffer[0] = (int)value;
        }

        /// <summary>
        /// Gets the row Length
        /// </summary>
        public int Length { get; }

        /// <summary>
        /// Get the value in the array that correspond to the column index given
        /// </summary>
        public object this[int index]
        {
            get => buffer[index + 1];
            set => this.buffer[index + 1] = value;
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
                var column = this.SchemaTable.Columns[columnName];

                if (column == null)
                    throw new ArgumentException("Column is null");

                var index = this.SchemaTable.Columns.IndexOf(column);

                return this[index];
            }
            set
            {
                var column = this.SchemaTable.Columns[columnName];

                if (column == null)
                    throw new ArgumentException("Column is null");

                var index = this.SchemaTable.Columns.IndexOf(column);

                this[index] = value;
            }
        }

        /// <summary>
        /// Get the inner copy array
        /// </summary>
        /// <returns></returns>
        public object[] ToArray() => this.buffer;


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

            if (this.SchemaTable == null)
                return this.buffer.ToString();

            var sb = new StringBuilder();

            sb.Append($"[Sync state]:{this.RowState}");

            var columns = this.RowState == SyncRowState.Deleted ? this.SchemaTable.GetPrimaryKeysColumns() : this.SchemaTable.Columns;

            foreach (var c in columns)
            {
                var o = this[c.ColumnName];
                var os = o == null ? "<NULL />" : o.ToString();

                sb.Append($", [{c.ColumnName}]:{os}");
            }

            return sb.ToString();
        }
    }
}
