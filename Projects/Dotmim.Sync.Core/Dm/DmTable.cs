using System;
using System.Linq;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;
using System.Data;

namespace Dotmim.Sync.Data
{

    public class DmTable
    {

        // rows
        /// <summary>
        /// Monotonically increasing number representing the order <see cref="DmRow"/> have been added to <see cref="DmRowCollection"/>.
        /// </summary>
        internal long nextRowID;

        // columns
        internal readonly DmColumnCollection columns;

        // props
        string tableName = "";
        string tablePrefix = "";

        // globalization stuff
        CultureInfo culture;
        bool caseSensitive;

        // Case insensitive compare options
        internal CompareOptions compareFlags;

        // primary key info
        readonly static Int32[] zeroIntegers = new Int32[0];
        internal readonly static DmColumn[] zeroColumns = new DmColumn[0];
        internal readonly static DmRow[] zeroRows = new DmRow[0];

        // rows collection
        DmRowCollection rows;

        // primary key
        DmKey primaryKey;

        public DmTable()
        {
            this.nextRowID = 1;
            this.rows = new DmRowCollection(this);
            this.columns = new DmColumnCollection(this);

            Culture = CultureInfo.CurrentCulture;
            CaseSensitive = true;

        }
        public DmTable(string tableName) : this()
        {
            this.tableName = tableName ?? "";
        }


        public DmColumnCollection Columns
        {
            get
            {
                return columns;
            }
        }


        public DmSet DmSet { get; internal set; }

        /// <summary>
        /// Set if the Table is case sensitive (in column name and datas)
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

                    this.CheckNameCompliance(this.tableName);
                }
            }
        }

        /// <summary>
        /// Set the culture used to make comparison
        /// </summary>
        public CultureInfo Culture 
        {
            get
            {

                return culture;
            }
            set
            {
                if (null == value)
                    value = (DmSet != null) ? DmSet.Culture : CultureInfo.InvariantCulture;

                if (culture != value)
                {
                    culture = value;
                    this.CheckNameCompliance(this.tableName);
                }

            }
        }

        /// <summary>
        /// Get a DmRow by its primary key 
        /// </summary>
        public DmRow FindByKey(object key)
        {
            return this.FindByKey(new object[] { key });
        }

        /// <summary>
        /// Get a DmRow by its primary key (composed by a multi columns key)
        /// </summary>
        public DmRow FindByKey(object[] key)
        {

            if (!primaryKey.HasValue)
                return null;

            return this.Rows.FirstOrDefault(r => primaryKey.ValuesAreEqual(r, key));
        }


        /// <summary>
        /// Primary key used to identify uniquely a dmRow
        /// </summary>
        public DmKey PrimaryKey
        {
            get
            {
                if (!primaryKey.HasValue)
                    return new DmKey(zeroColumns);

                return primaryKey;
            }
            set
            {
                if (value == primaryKey || value.Equals(primaryKey))
                    return;

                primaryKey = value;

                for (int i = 0; i < primaryKey.Columns.Length; i++)
                    primaryKey.Columns[i].AllowDBNull = false;

                // if we have only One Column, so must be unique
                if (primaryKey.Columns.Length == 1)
                    primaryKey.Columns[0].Unique = true;
            }
        }

        /// <summary>
        /// Get the rows
        /// </summary>
        public DmRowCollection Rows => rows;

        /// <summary>
        /// Get or Set the table name
        /// </summary>
        public string TableName
        {
            get
            {
                return tableName;
            }
            set
            {
                try
                {
                    if (value == null)
                        value = "";

                    if (DmSet != null)
                    {
                        if (value.Length == 0)
                            throw new ArgumentException("NoTableName");

                        CheckNameCompliance(value);
                    }
                    tableName = value;
                }
                finally
                {
                }
            }
        }

        /// <summary>
        /// Ckeck if the table name is valid
        /// </summary>
        internal void CheckNameCompliance(string tableName)
        {
            if (DmSet != null && this.IsEqual(tableName, DmSet.DmSetName))
                throw new ArgumentException("Conflicting name. DmTable Name must be different than DmSet Name");
        }

        /// <summary>
        /// Table Prefix. [NOT USED]
        /// </summary>
        public string Prefix
        {
            get { return tablePrefix; }
            set
            {
                tablePrefix = value == null ? string.Empty : value;
            }
        }

        /// <summary>
        /// Accept all changes in every DmRow in this DmTable
        /// </summary>
        public void AcceptChanges()
        {
            foreach (var r in this.Rows)
                if (r.RowId != -1)
                    r.AcceptChanges();
        }

        /// <summary>
        /// Create a clone of the current DmTable
        /// </summary>
        public DmTable Clone()
        {
            DmTable clone = new DmTable();

            // Set All properties
            clone.TableName = tableName;
            clone.Prefix = tablePrefix;
            clone.Culture = culture;
            clone.CaseSensitive = caseSensitive;

            // add all columns
            var clmns = this.Columns;
            for (int i = 0; i < clmns.Count; i++)
                clone.Columns.Add(clmns[i].Clone());

            // Create PrimaryKey
            DmColumn[] pkey = PrimaryKey.Columns;
            if (pkey.Length > 0)
            {
                DmColumn[] key = new DmColumn[pkey.Length];

                for (int i = 0; i < pkey.Length; i++)
                    key[i] = clone.Columns[pkey[i].Ordinal];

                clone.PrimaryKey = new DmKey(key);
            }
            return clone;
        }

        /// <summary>
        /// Clone then import rows
        /// </summary>
        public DmTable Copy()
        {
            try
            {
                DmTable destTable = this.Clone();

                foreach (DmRow row in Rows)
                    destTable.ImportRow(row);

                return destTable;
            }
            finally
            {
            }
        }

        /// <summary>
        /// Clear the DmTable rows
        /// </summary>
        public void Clear()
        {
            foreach (var c in this.columns)
                c.Clear();
            Rows.Clear();
        }

        /// <summary>
        /// Compare string with the table CultureInfo and CaseSensitive flags
        /// </summary>
        internal Boolean IsEqual(string s1, string s2)
        {
            return this.culture.CompareInfo.Compare(s1, s2, this.compareFlags) == 0;
        }

        /// <summary>
        /// Get Changes from the DmTable
        /// </summary>
       public DmTable GetChanges()
        {
            DmTable dtChanges = this.Clone();
            DmRow row = null;

            for (int i = 0; i < Rows.Count; i++)
            {
                row = Rows[i];
                if (row.oldRecord != row.newRecord)
                    dtChanges.ImportRow(row);
            }

            if (dtChanges.Rows.Count == 0)
                return null;

            return dtChanges;
        }

        /// <summary>
        /// Get Changes from the DmTable, with a DmRowState value
        /// </summary>
        public DmTable GetChanges(DmRowState rowStates)
        {
            DmTable dtChanges = this.Clone();
            DmRow row = null;

            for (int i = 0; i < Rows.Count; i++)
            {
                row = Rows[i];
                if ((row.RowState & rowStates) == row.RowState)
                    dtChanges.ImportRow(row);
            }

            if (dtChanges.Rows.Count == 0)
                return null;

            return dtChanges;
        }

        /// <summary>
        /// Copy a record, whatever the state (Added, Modified, Deleted etc...)
        /// </summary>
        internal void CopyRecords(DmTable src, int srcRecord, int newRecord)
        {
            // Parcours de toutes les colonnes de destination
            for (int i = 0; i < this.Columns.Count; ++i)
            {
                // colonne de destination
                DmColumn dstColumn = this.Columns[i];
                // colonne à copier
                DmColumn srcColumn = src.Columns.FirstOrDefault(c => this.IsEqual(c.ColumnName, dstColumn.ColumnName));

                if (srcColumn == null)
                {
                    dstColumn.Init(newRecord);
                    continue;
                }

                if (dstColumn.IsValueType)
                {
                    dstColumn[newRecord] = srcColumn[srcRecord];
                    continue;
                }

                if (dstColumn.dataType == typeof(byte[]))
                {
                    byte[] srcArray = (byte[])srcColumn[srcRecord];
                    byte[] destArray = (byte[])dstColumn[newRecord];

                    Buffer.BlockCopy(srcArray, 0, destArray, 0, srcArray.Length);
                    continue;
                }
                if (dstColumn.dataType == typeof(char[]))
                {
                    char[] srcArray = (char[])srcColumn[srcRecord];
                    char[] destArray = (char[])dstColumn[newRecord];

                    Buffer.BlockCopy(srcArray, 0, destArray, 0, srcArray.Length);
                    continue;
                }

                dstColumn[newRecord] = srcColumn[srcRecord];

            }
        }

        /// <summary>
        /// Import a row from a DataTable
        /// </summary>
        public DmRow ImportRow(DmRow rowToImport)
        {
            int oldRecord = -1, newRecord = -1;

            if (rowToImport == null)
                return null;

            // Copy d'une version ancienne (Deleted ou Modified)
            // Création de cette version ancienne et récupération de cet id
            if (rowToImport.oldRecord != -1)
            {
                // Get a new id
                oldRecord = this.Rows.GetNewVersionId();

                // ancienne version de la ligne à enregistrer dans la nouvelle ligne
                CopyRecords(rowToImport.Table, rowToImport.oldRecord, oldRecord);
            }


            // Si c'est une ligne Added ou Modified
            if (rowToImport.newRecord != -1)
            {
                // row not deleted
                if (rowToImport.RowState != DmRowState.Unchanged)
                {
                    // Get a new Id
                    newRecord = this.Rows.GetNewVersionId();

                    // not unchanged, it means Added or modified
                    CopyRecords(rowToImport.Table, rowToImport.newRecord, newRecord);
                }
                else
                    newRecord = oldRecord;
            }

            // Don't init columns since we already have them
            var r = new DmRow(this, false);
            r.oldRecord = oldRecord;
            r.newRecord = newRecord;

            // Si la ligne n'est pas detached
            if (oldRecord != -1 || newRecord != -1)
                this.Rows.Add(r);

            return r;

        }

        /// <summary>
        /// Load an array
        /// </summary>
        public DmRow LoadDataRow(object[] values, bool acceptChanges)
        {
            var dr = new DmRow(this);

            // Check if less values than columns
            bool hasLessValues = false;
            if (values.Length == columns.Count - 1)
                hasLessValues = true;

            int j = 0;
            for (int i = 0; i < columns.Count; i++)
            {
                var column = columns[i];

                if (column.AutoIncrement && hasLessValues)
                    continue;

                dr[i] = values[j];
                j++;
            }

            this.Rows.Add(dr);

            if (acceptChanges)
                dr.AcceptChanges();

            return dr;
        }

        /// <summary>
        /// Create a new empty row
        /// </summary>
        public DmRow NewRow() => new DmRow(this);

        protected virtual Type GetRowType()
        {
            return typeof(DmRow);
        }


        /// <summary>
        /// Reject every changes made since last AcceptChanges()
        /// </summary>
        public void RejectChanges()
        {
            DmRow[] oldRows = new DmRow[Rows.Count];
            Rows.CopyTo(oldRows, 0);

            for (int i = 0; i < oldRows.Length; i++)
                oldRows[i].Rollback();


        }

       
        public override string ToString() => this.TableName;

        /// <summary>
        /// Merge a dmTable in this dmTable without perserving changes
        /// </summary>
        public void Merge(DmTable table)
        {
            Merge(table, false);
        }

        /// <summary>
        /// Merge a dmTable in this dmTable perserving changes
        /// </summary>
        public void Merge(DmTable table, bool preserveChanges)
        {
            if (table == null)
                throw new ArgumentNullException(nameof(table));

            DmMerger merger = new DmMerger(this, preserveChanges);
            merger.MergeTable(table);
        }

        internal void MergeRow(DmRow row, DmRow targetRow, bool preserveChanges)
        {
            // Si le merge ne concerne pas une ligne déjà existante
            if (targetRow == null)
            {
                ImportRow(row);
                return;
            }

            int proposedRecord = targetRow.tempRecord; // by saving off the tempRecord, EndEdit won't free newRecord
            targetRow.tempRecord = -1;
            try
            {
                DmRowState saveRowState = targetRow.RowState;
                int saveIdxRecord = (saveRowState == DmRowState.Added) ? targetRow.newRecord : saveIdxRecord = targetRow.oldRecord;
                int newRecord;
                int oldRecord;
                if (targetRow.RowState == DmRowState.Unchanged && row.RowState == DmRowState.Unchanged)
                {
                    // unchanged row merging with unchanged row
                    oldRecord = targetRow.oldRecord;

                    if (preserveChanges)
                    {
                        newRecord = this.Rows.GetNewVersionId();
                        CopyRecords(this, oldRecord, newRecord);
                    }
                    else
                    {
                        newRecord = targetRow.newRecord;
                    }


                    CopyRecords(row.Table, row.oldRecord, targetRow.oldRecord);
                }
                else if (row.newRecord == -1)
                {
                    // Incoming row is deleted
                    oldRecord = targetRow.oldRecord;
                    if (preserveChanges)
                    {
                        if (targetRow.RowState == DmRowState.Unchanged)
                        {
                            newRecord = this.Rows.GetNewVersionId();
                            CopyRecords(this, oldRecord, newRecord);
                        }
                        else
                        {
                            newRecord = targetRow.newRecord;
                        }
                    }
                    else
                        newRecord = -1;

                    CopyRecords(row.Table, row.oldRecord, oldRecord);

                }
                else
                {
                    // incoming row is added, modified or unchanged (targetRow is not unchanged)
                    oldRecord = targetRow.oldRecord;
                    newRecord = targetRow.newRecord;
                    if (targetRow.RowState == DmRowState.Unchanged)
                    {
                        newRecord = this.Rows.GetNewVersionId();
                        CopyRecords(this, oldRecord, newRecord);
                    }
                    CopyRecords(row.Table, row.oldRecord, oldRecord);

                    if (!preserveChanges)
                    {
                        CopyRecords(row.Table, row.newRecord, newRecord);
                    }
                }

            }
            finally
            {
                targetRow.tempRecord = proposedRecord;
            }

        }

        /// <summary>
        /// Fill the DmTable from a IDataReader connected to any kind of database
        /// </summary>
        public void Fill(IDataReader reader)
        {
            var readerFieldCount = reader.FieldCount;

            if (readerFieldCount == 0)
                return;

            if (this.Columns.Count == 0)
            {

                List<DmColumn> keyColumns = new List<DmColumn>();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    var columnName = reader.GetName(i);
                    var columnType = reader.GetFieldType(i);
                    DmColumn column = DmColumn.CreateColumn(columnName, columnType);
                    this.Columns.Add(column);
                }
            }

            // Count - 2 becoz we can have a autoinc columns
            if (readerFieldCount < this.columns.Count - 2)
                return;

            while (reader.Read())
            {
                object[] readerDataValues = new object[reader.FieldCount];

                reader.GetValues(readerDataValues);
                var dataRow = this.LoadDataRow(readerDataValues, true);

            }
        }

    }
}
