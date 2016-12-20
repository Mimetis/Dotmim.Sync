using Dotmim.Sync.Data;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;

namespace Dotmim.Sync.Data.Surrogate
{
    /// <summary>
    /// Represents a surrogate of a DmTable object, which DotMim Sync uses during custom binary serialization.
    /// </summary>
    [Serializable]
    public class DmTableSurrogate : IDisposable
    {

        /// <summary>
        /// Gets or sets the locale information used to compare strings within the table.
        /// </summary>
        public String CultureInfoName { get; set; }

        /// <summary>Gets or sets the Case sensitive rul of the DmTable that the DmTableSurrogate object represents.</summary>
        public Boolean CaseSensitive { get; set; }

        /// <summary>
        /// Get or Set the prefix used for the DmTableSurrogate
        /// </summary>
        public String Prefix { get; set; }

        /// <summary>
        /// Gets or sets an array that represents the state of each row in the table.
        /// </summary>
        public DmRowState[] RowStates { get; set; }

        /// <summary>
        /// Gets or sets the name of the table that the DmTableSurrogate object represents.
        /// </summary>
        public string TableName { get; set; }

        /// <summary>
        /// Gets an array of DmColumnSurrogate objects that comprise the table that is represented by the DmTableSurrogate object.
        /// </summary>
        public DmColumnSurrogate[] DmColumnSurrogates { get; set; }

        /// <summary>
        /// Gets an array of DmColumnSurrogate objects that represent the PrimaryKeys.
        /// </summary>
        public DmColumnSurrogate[] DmPrimaryKeySurrogates { get; set; }

        /// <summary>Gets an array of objects that represent the columns and rows of dm in the <see cref="T:Microsoft.Synchronization.Dm.DmTableSurrogate" /> object.</summary>
        public Dictionary<int, List<object>> Records { get; set; }

        /// <summary>
        /// Only used for Serialization
        /// </summary>
        public DmTableSurrogate()
        {

        }

        /// <summary>
        /// Initializes a new instance of the DmTableSurrogate class.
        /// </summary>
        public DmTableSurrogate(DmTable dt)
        {
            if (dt == null)
                throw new ArgumentNullException("dt", "DmTable");

            this.TableName = dt.TableName;
            this.CultureInfoName = dt.Culture.Name;
            this.CaseSensitive = dt.CaseSensitive;
            this.Prefix = dt.Prefix;

            // Create the columns
            this.DmColumnSurrogates = new DmColumnSurrogate[dt.Columns.Count];

            for (int i = 0; i < dt.Columns.Count; i++)
                this.DmColumnSurrogates[i] = new DmColumnSurrogate(dt.Columns[i]);

            // Primary Keys
            if (dt.PrimaryKey != null)
            {
                this.DmPrimaryKeySurrogates = new DmColumnSurrogate[dt.PrimaryKey.Columns.Length];
                for (int i = 0; i < dt.PrimaryKey.Columns.Length; i++)
                    this.DmPrimaryKeySurrogates[i] = new DmColumnSurrogate(dt.PrimaryKey.Columns[i]);
            }

            // Fill the rows
            if (dt.Rows.Count <= 0)
                return;

            // the BitArray contains bit values initialized to false. We will use it to store row state
            this.RowStates = new DmRowState[dt.Rows.Count];

            // Records in a straightforward object array
            this.Records = new Dictionary<int, List<object>>(dt.Columns.Count);

            for (int j = 0; j < dt.Columns.Count; j++)
                this.Records[j] = new List<object>(dt.Rows.Count);

            for (int k = 0; k < dt.Rows.Count; k++)
            {
                this.RowStates[k] = dt.Rows[k].RowState;
                this.ConvertToSurrogateRecords(dt.Rows[k]);
            }

        }

        /// <summary>
        /// Copies the table schema from a DmTableSurrogate object into a DmTable object.
        /// </summary>
        public void ReadSchemaIntoDmTable(DmTable dt)
        {
            if (dt == null)
                throw new ArgumentNullException("dt", "DmTable");

            dt.TableName = this.TableName;
            dt.Culture = new CultureInfo(this.CultureInfoName);
            dt.Prefix = this.Prefix;
            dt.CaseSensitive = this.CaseSensitive;

            for (int i = 0; i < this.DmColumnSurrogates.Length; i++)
            {
                DmColumn dmColumn = this.DmColumnSurrogates[i].ConvertToDmColumn();
                dt.Columns.Add(dmColumn);
            }

            if (this.DmPrimaryKeySurrogates != null && this.DmPrimaryKeySurrogates.Length > 0)
            {
                DmColumn[] keyColumns = new DmColumn[this.DmPrimaryKeySurrogates.Length];

                for (int i = 0; i < this.DmPrimaryKeySurrogates.Length; i++)
                    keyColumns[i] = this.DmPrimaryKeySurrogates[i].ConvertToDmColumn();

                DmKey key = new DmKey(keyColumns);

                dt.PrimaryKey = key;
            }
        }


        /// <summary>
        /// Copies the table schema from a DmTableSurrogate object into a DmTable object.
        /// </summary>
        public void ReadDatasIntoDmTable(DmTable dt)
        {
            if (this.Records != null && dt != null && dt.Columns.Count > 0)
            {
                int length = Records[0].Count;
                for (int i = 0; i < length; i++)
                    this.ConvertToDmRow(dt, i);
            }
        }

        private DmRow ConvertToDmRow(DmTable dt, int bitIndex)
        {
            DmRowState rowState = this.RowStates[bitIndex];
            return this.ConstructRow(dt, rowState, bitIndex);
        }

        /// <summary>
        /// Construct a row from a dmTable, a rowState and the bitIndex
        /// </summary>
        private DmRow ConstructRow(DmTable dt, DmRowState rowState, int bitIndex)
        {
            DmRow dmRow = dt.NewRow();
            int count = dt.Columns.Count;

            dmRow.BeginEdit();
            for (int i = 0; i < count; i++)
                dmRow[i] = this.Records[i][bitIndex];

            dt.Rows.Add(dmRow);

            switch (rowState)
            {
                case DmRowState.Unchanged:
                    {
                        dmRow.AcceptChanges();
                        dmRow.EndEdit();
                        return dmRow;
                    }
                case DmRowState.Added:
                    {
                        dmRow.EndEdit();
                        return dmRow;
                    }
                case DmRowState.Deleted:
                    {
                        dmRow.AcceptChanges();
                        dmRow.Delete();
                        dmRow.EndEdit();
                        return dmRow;

                    }
                case DmRowState.Modified:
                    {
                        dmRow.AcceptChanges();
                        dmRow.SetModified();
                        dmRow.EndEdit();
                        return dmRow;
                    }
                default:
                    throw new ArgumentException("InvalidRowState");
            }
        }

        private void ConvertToSurrogateRecords(DmRow row)
        {
            int count = row.Table.Columns.Count;
            DmRowState rowState = row.RowState;
            DmRowVersion rowVersion = rowState == DmRowState.Deleted ? DmRowVersion.Original : DmRowVersion.Current;

            for (int i = 0; i < count; i++)
                this.Records[i].Add(row[i, rowVersion]);
        }



        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool cleanup)
        {
            this.Records = null;
            this.DmColumnSurrogates = null;
        }





    }
}