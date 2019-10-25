using System;
using System.Linq;
using System.Text;

using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.Serialization;
using Dotmim.Sync.Data.Surrogate;

namespace Dotmim.Sync.Data
{

    [Serializable]
    public class DmSet : ISerializable
    {
        string dmSetName = "NewDataSet";

        // globalization stuff
        bool caseSensitive;
        // Case insensitive compare options
        CompareOptions compareFlags;
        CultureInfo culture;

        /// <summary>
        /// Get or Set the DmSet name
        /// </summary>
        public string DmSetName
        {
            get
            {
                return dmSetName;
            }
            set
            {
                if (value != dmSetName)
                {
                    if (value == null || value.Length == 0)
                        throw new Exception("DmSet name ca'nt be null");

                    bool conflicting = Tables.Any(t => t.TableName == value);

                    if (conflicting)
                        throw new Exception($"DmSet conflicting name with an existable DmTable {value}");

                    this.dmSetName = value;
                }
            }
        }

        /// <summary>
        /// Set the case sensitive property for the DmSet and all DmTables affiliated. Default is false
        /// </summary>
        public bool CaseSensitive
        {
            get
            {
                return caseSensitive;
            }
            set
            {
                if (caseSensitive != value)
                {
                    caseSensitive = value;

                    if (caseSensitive)
                        compareFlags = CompareOptions.None;
                    else
                        compareFlags = CompareOptions.IgnoreCase | CompareOptions.IgnoreKanaType | CompareOptions.IgnoreWidth;

                    foreach (DmTable table in Tables)
                        table.CaseSensitive = value;
                }
            }
        }

        /// <summary>
        /// Compare string with the table CultureInfo and CaseSensitive flags
        /// </summary>
        public Boolean IsEqual(string s1, string s2)
        {
            return this.culture.CompareInfo.Compare(s1, s2, this.compareFlags) == 0;
        }

        public CultureInfo Culture
        {
            get
            {
                return culture;
            }
            set
            {
                culture = value;
                foreach (DmTable table in Tables)
                    table.Culture = value;
            }
        }

        /// <summary>
        /// Gets the collection of tables contained in the DmSet
        /// </summary>
        public DmTableCollection Tables { get; }

        /// <summary>
        /// Gets all the relations that link tables and allow navigations
        /// </summary>
        public List<DmRelation> Relations { get; }




        public DmSet()
        {
            this.Tables = new DmTableCollection(this);
            this.Culture = CultureInfo.CurrentCulture; // Set default locale
            this.CaseSensitive = false;
            this.compareFlags = CompareOptions.IgnoreCase | CompareOptions.IgnoreKanaType | CompareOptions.IgnoreWidth;
            this.Relations = new List<DmRelation>();
            this.DmSetName = "NewDmSet";

        }

        public DmSet(string dataSetName)
            : this()
        {
            this.DmSetName = dataSetName;
        }

        public DmSet(SerializationInfo info, StreamingContext context) : this()
        {
            var dmSetSurrogate = info.GetValue("surrogate", typeof(DmSetSurrogate)) as DmSetSurrogate;

            if (dmSetSurrogate != null)
            {
                this.Culture = new CultureInfo(dmSetSurrogate.CultureInfoName);
                this.CaseSensitive = dmSetSurrogate.CaseSensitive;
                this.DmSetName = dmSetSurrogate.DmSetName;
                dmSetSurrogate.ReadSchemaIntoDmSet(this);
                dmSetSurrogate.ReadDataIntoDmSet(this);
            }
        }


        /// <summary>
        /// How to serialize
        /// </summary>
        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            var surrogate = new DmSetSurrogate(this);

            info.AddValue("surrogate", surrogate, typeof(DmSetSurrogate));
        }

        

        /// <summary>
        /// Commits all the changes made to this DmSet
        /// </summary>
        public void AcceptChanges()
        {
            for (int i = 0; i < Tables.Count; i++)
                Tables[i].AcceptChanges();
        }


        /// <summary>
        /// Clears the DmSet of any data by removing all rows in all tables.
        /// </summary>
        public void Clear()
        {
            if (this.Tables != null)
                for (int i = 0; i < Tables.Count; i++)
                    Tables[i].Clear();
        }

        /// <summary>
        /// Clone the current DmSet schema. No rows are imported
        /// </summary>
        public virtual DmSet Clone()
        {
            DmSet ds = new DmSet
            {
                DmSetName = this.DmSetName,
                CaseSensitive = this.CaseSensitive,
                Culture = this.Culture
            };

            for (int i = 0; i < Tables.Count; i++)
            {
                DmTable dt = Tables[i].Clone();
                ds.Tables.Add(dt);
            }

            List<DmRelation> rels = Relations;

            for (int i = 0; i < rels.Count; i++)
            {
                DmRelation rel = rels[i].Clone(ds);
                ds.Relations.Add(rel);
            }

            return ds;
        }

        /// <summary>
        /// Copies both the structure and data for this DmSet
        /// </summary>
        /// <returns></returns>
        public DmSet Copy()
        {
            DmSet dsNew = Clone();
            foreach (DmTable table in this.Tables)
            {
                DmTable destTable = dsNew.Tables.First(t => t.TableName == table.TableName);

                foreach (DmRow row in table.Rows)
                    table.ImportRow(row);
            }

            return dsNew;
        }


        public DmSet GetChanges() => GetChanges(DmRowState.Added | DmRowState.Deleted | DmRowState.Modified);

        struct TableChanges
        {
            BitArray _rowChanges;
            int _hasChanges;

            internal TableChanges(int rowCount)
            {
                _rowChanges = new BitArray(rowCount);
                _hasChanges = 0;
            }
            internal int HasChanges
            {
                get { return _hasChanges; }
                set { _hasChanges = value; }
            }
            internal bool this[int index]
            {
                get { return _rowChanges[index]; }
                set
                {
                    Debug.Assert(value && !_rowChanges[index], "setting twice or to false");
                    _rowChanges[index] = value;
                    _hasChanges++;
                }
            }
        }

        public DmSet GetChanges(DmRowState rowStates)
        {
            DmSet dsNew = null;

            if (0 != (rowStates & ~(DmRowState.Added | DmRowState.Deleted | DmRowState.Modified | DmRowState.Unchanged)))
                throw new Exception($"InvalidRowState {rowStates}");

            // Initialize all the individual table bitmaps.
            TableChanges[] bitMatrix = new TableChanges[Tables.Count];

            for (int i = 0; i < bitMatrix.Length; ++i)
                bitMatrix[i] = new TableChanges(Tables[i].Rows.Count);

            // find all the modified rows and their parents
            MarkModifiedRows(bitMatrix, rowStates);

            // copy the changes to a cloned table
            for (int i = 0; i < bitMatrix.Length; ++i)
            {
                Debug.Assert(0 <= bitMatrix[i].HasChanges, "negative change count");
                if (0 < bitMatrix[i].HasChanges)
                {
                    if (dsNew == null)
                        dsNew = this.Clone();

                    DmTable table = this.Tables[i];
                    DmTable destTable = dsNew.Tables.First(t => t.TableName == table.TableName);

                    for (int j = 0; 0 < bitMatrix[i].HasChanges; ++j)
                    { // Loop through the rows.
                        if (bitMatrix[i][j])
                        {
                            destTable.ImportRow(table.Rows[j]);
                            bitMatrix[i].HasChanges--;
                        }
                    }
                }
            }
            return dsNew;
        }

        private void MarkModifiedRows(TableChanges[] bitMatrix, DmRowState rowStates)
        {
            // for every table, every row & every relation find the modified rows and for non-deleted rows, their parents
            for (int tableIndex = 0; tableIndex < bitMatrix.Length; ++tableIndex)
            {
                var rows = Tables[tableIndex].Rows;
                int rowCount = rows.Count;

                for (int rowIndex = 0; rowIndex < rowCount; ++rowIndex)
                {
                    DmRow row = rows[rowIndex];
                    DmRowState rowState = row.RowState;

                    // if bit not already set and row is modified
                    if (((rowStates & rowState) != 0) && !bitMatrix[tableIndex][rowIndex])
                        bitMatrix[tableIndex][rowIndex] = true;
                }
            }
        }


        /// <summary>
        /// Gets if the current DmSet has at least one table
        /// </summary>
        public bool HasTables => this.Tables?.Count > 0;

        /// <summary>
        /// Gets if the DmSet has at least one column in at least one table
        /// </summary>
        public bool HasColumns =>
                // using SelectMany to get DmColumns and not DmColumnCollection
                this.Tables?.SelectMany(t => t.Columns).Count() > 0;

        /// <summary>
        /// Gets a value indicating whether the DmSet has changes, including new, deleted, or modified rows.
        /// </summary>
        public bool HasChanges() => HasChanges(DmRowState.Added | DmRowState.Deleted | DmRowState.Modified);

        /// <summary>
        /// Gets a value indicating whether the DmState has changes
        /// </summary>
        public bool HasChanges(DmRowState rowStates)
        {
            try
            {
                const DmRowState allRowStates = DmRowState.Detached | DmRowState.Unchanged | DmRowState.Added | DmRowState.Deleted | DmRowState.Modified;

                if ((rowStates & (~allRowStates)) != 0)
                    throw new ArgumentOutOfRangeException(nameof(rowStates));

                for (int i = 0; i < Tables.Count; i++)
                {
                    DmTable table = Tables[i];

                    for (int j = 0; j < table.Rows.Count; j++)
                    {
                        DmRow row = table.Rows[j];
                        if ((row.RowState & rowStates) != 0)
                        {
                            return true;
                        }
                    }
                }
                return false;
            }
            finally
            {
            }
        }

        bool IsEmpty()
        {

            foreach (DmTable table in this.Tables)
                if (table.Rows.Count > 0)
                    return false;

            return true;
        }


        /// <devdoc>
        /// This method rolls back all the changes to have been made to this DataSet since
        /// it was loaded or the last time AcceptChanges was called.
        /// Any rows still in edit-mode cancel their edits.  New rows get removed.  Modified and
        /// Deleted rows return back to their original state.
        /// </devdoc>
        public virtual void RejectChanges()
        {
            try
            {
                for (int i = 0; i < Tables.Count; i++)
                    Tables[i].RejectChanges();
            }
            finally
            {
            }
        }


        public virtual void Reset()
        {
            try
            {
                this.Clear();
                Relations.Clear();
                Tables.Clear();
            }
            finally
            {
            }
        }

       
    }
}