using Dotmim.Sync.Enumerations;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;

namespace Dotmim.Sync.Data
{

    public class DmTable
    {

        /// <summary>
        /// Monotonically increasing number representing the order <see cref="DmRow"/> have been added to <see cref="DmRowCollection"/>.
        /// </summary>
        internal long nextRowID;

        // columns
        internal readonly DmColumnCollection columns;

        // props
        private string tableName = string.Empty;
        private string schema = string.Empty;

        // globalization stuff
        private CultureInfo culture;
        private bool caseSensitive;

        // Case insensitive compare options
        internal CompareOptions compareFlags;

        // primary key info
        private static readonly int[] zeroIntegers = new int[0];
        internal static readonly DmRow[] zeroRows = new DmRow[0];

        // primary key
        private DmKey primaryKey;

        public DmTable()
        {
            this.nextRowID = 1;
            this.Rows = new DmRowCollection(this);
            this.columns = new DmColumnCollection(this);
            this.Culture = CultureInfo.CurrentCulture;
            this.CaseSensitive = false;
            this.compareFlags = CompareOptions.IgnoreCase | CompareOptions.IgnoreKanaType | CompareOptions.IgnoreWidth;
        }

        public DmTable(string tableName) : this() => this.tableName = tableName ?? "";

        /// <summary>
        /// Columns collection
        /// </summary>
        public DmColumnCollection Columns => this.columns;

        public DmSet DmSet { get; internal set; }

        /// <summary>
        /// Set if the Table is case sensitive (in column name and datas)
        /// </summary>
        public bool CaseSensitive
        {
            get => this.caseSensitive;
            set
            {
                if (this.caseSensitive != value)
                {
                    this.caseSensitive = value;

                    if (this.caseSensitive)
                        this.compareFlags = CompareOptions.None;
                    else
                        this.compareFlags = CompareOptions.IgnoreCase | CompareOptions.IgnoreKanaType | CompareOptions.IgnoreWidth;

                    this.CheckNameCompliance(this.tableName);
                }
            }
        }

        /// <summary>
        /// Gets or Sets the original provider (SqlServer, MySql, Sqlite, Oracle, PostgreSQL)
        /// </summary>
        public string OriginalProvider { get; set; }

        /// <summary>
        /// Gets or Sets the Sync direction (may be Bidirectional, DownloadOnly, UploadOnly) 
        /// Default is Bidirectional
        /// </summary>
        public SyncDirection SyncDirection { get; set; } = SyncDirection.Bidirectional;

        /// <summary>
        /// Specify a prefix for naming stored procedure. Default is empty string
        /// </summary>
        public string StoredProceduresPrefix { get; set; }

        /// <summary>
        /// Specify a suffix for naming triggers. Default is empty string
        /// </summary>
        public string TriggersSuffix { get; set; }

        /// <summary>
        /// Specify a prefix for triggers. Default is empty string
        /// </summary>
        public string TriggersPrefix { get; set; }

        /// <summary>
        /// Specify a suffix for naming stored procedures. Default is empty string
        /// </summary>
        public string StoredProceduresSuffix { get; set; }

        /// <summary>
        /// Specify a prefix for naming tracking tables. Default is empty string
        /// </summary>
        public string TrackingTablesPrefix { get; set; }

        /// <summary>
        /// Specify a suffix for naming tracking tables. Default is empty string
        /// </summary>
        public string TrackingTablesSuffix { get; set; }


        /// <summary>
        /// Set the culture used to make comparison
        /// </summary>
        public CultureInfo Culture
        {
            get => this.culture;
            set
            {
                if (null == value)
                    value = (this.DmSet != null) ? this.DmSet.Culture : CultureInfo.InvariantCulture;

                if (this.culture != value)
                {
                    this.culture = value;
                    this.CheckNameCompliance(this.tableName);
                }

            }
        }

        /// <summary>
        /// Gets the collection of child relations for this DmTable.
        /// </summary>
        public List<DmRelation> ChildRelations
        {
            get
            {
                if (this.DmSet == null)
                    return null;

                var lst = this.DmSet.Relations.Where(r => r.ParentTable == this).ToList();

                return lst;
            }
        }

        /// <summary>
        /// Gets the collection of parent relations for this DmTable.
        /// </summary>
        public List<DmRelation> ParentRelations
        {
            get
            {
                if (this.DmSet == null)
                    return null;

                var lst = this.DmSet.Relations.Where(r => r.ChildTable == this).ToList();

                return lst;

            }
        }

        /// <summary>
        /// Get all foreign keys from current table
        /// </summary>
        public IEnumerable<DmRelation> ForeignKeys
        {
            get
            {
                if (this.DmSet == null || this.DmSet.Relations == null || this.DmSet.Relations.Count == 0)
                    return null;

                var relations = this.DmSet.Relations.Where(r => r.ChildTable == this);

                return relations;
            }
        }

        public DmRelation AddForeignKey(string relationName, DmColumn childColumn, DmColumn parentColumn)
        {
            // check if child column is from the current table, because foreign key are allways referenced in child table
            if (!this.Columns.Contains(childColumn))
                throw new Exception("Child column should belong to the dmTable");


            var dr = new DmRelation(relationName, parentColumn, childColumn);
            this.DmSet.Relations.Add(dr);

            return dr;
        }

        public DmRelation AddForeignKey(DmRelation relation)
        {
            // check if child column is from the current table
            foreach (var c in relation.ChildColumns)
                if (!this.Columns.Contains(c))
                    throw new Exception("Child column should belong to the dmTable");

            this.DmSet.Relations.Add(relation);

            return relation;
        }
        /// <summary>
        /// Get a DmRow by its primary key 
        /// </summary>
        public DmRow FindByKey(object key) => this.FindByKey(new object[] { key });

        /// <summary>
        /// Get a DmRow by its primary key (composed by a multi columns key)
        /// </summary>
        public DmRow FindByKey(object[] key)
        {

            if (!this.primaryKey.HasValue)
                return null;

            return this.Rows.FirstOrDefault(r => this.primaryKey.ValuesAreEqual(r, key));
        }

        /// <summary>
        /// Primary key used to identify uniquely a dmRow
        /// </summary>
        public DmKey PrimaryKey
        {
            get => this.primaryKey;
            set
            {
                if (value == this.primaryKey || value.Equals(this.primaryKey))
                    return;

                this.primaryKey = value;

                if (!this.primaryKey.HasValue)
                    return;

                for (var i = 0; i < this.primaryKey.Columns.Length; i++)
                    this.primaryKey.Columns[i].AllowDBNull = false;

                // if we have only One Column, so must be unique
                if (this.primaryKey.Columns.Length == 1)
                    this.primaryKey.Columns[0].IsUnique = true;
            }
        }

        /// <summary>
        /// Get the rows
        /// </summary>
        public DmRowCollection Rows { get; }

        /// <summary>
        /// Get or Set the table name
        /// </summary>
        public string TableName
        {
            get => this.tableName;
            set
            {
                try
                {
                    if (value == null)
                        value = "";

                    if (this.DmSet != null)
                    {
                        if (value.Length == 0)
                            throw new ArgumentException("NoTableName");

                        this.CheckNameCompliance(value);
                    }
                    this.tableName = value;
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
            if (this.DmSet != null && this.IsEqual(tableName, this.DmSet.DmSetName))
                throw new ArgumentException("Conflicting name. DmTable Name must be different than DmSet Name");
        }

        /// <summary>
        /// Table schema. Useful in Sql Server Provider
        /// </summary>
        public string Schema
        {
            get => this.schema;
            set => this.schema = value ?? string.Empty;
        }

        /// <summary>
        /// Gets all columns that are not Primary columns
        /// </summary>
        public IEnumerable<DmColumn> NonPkColumns
        {
            get
            {
                foreach (var column in this.Columns)
                {
                    if (this.primaryKey == null || !this.primaryKey.Columns.Contains(column))
                        yield return column;
                }
            }
        }


        /// <summary>
        /// Gets all columns that can be updated and are not Primary columns
        /// </summary>
        public IEnumerable<DmColumn> MutableColumns
        {
            get
            {
                foreach (var column in this.Columns)
                {
                    if (
                        (this.primaryKey == null || !this.primaryKey.Columns.Contains(column))
                        &&
                        !column.IsCompute
                        &&
                        !column.IsReadOnly
                        )
                        yield return column;
                }
            }
        }


        /// <summary>
        /// Gets all columns that can be updated and are not Primary columns and not auto increment
        /// </summary>
        public IEnumerable<DmColumn> MutableColumnsAndNotAutoInc
        {
            get
            {
                foreach (var column in this.Columns)
                {
                    if (
                        (this.primaryKey == null || !this.primaryKey.Columns.Contains(column))
                        &&
                        !column.IsCompute
                        &&
                        !column.IsReadOnly
                        &&
                        !column.IsAutoIncrement
                        )
                        yield return column;
                }
            }
        }

        /// <summary>
        /// Gets a value returning if the dmTable contains an auto increment column
        /// </summary>
        public bool HasAutoIncrementColumns => this.Columns.Any(c => c.IsAutoIncrement);


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
            var clone = new DmTable
            {

                // Set All properties
                TableName = tableName,
                Schema = schema,
                Culture = culture,
                CaseSensitive = caseSensitive,
                OriginalProvider = OriginalProvider,
                SyncDirection = SyncDirection,
                TrackingTablesPrefix = TrackingTablesPrefix,
                TrackingTablesSuffix = TrackingTablesSuffix,
                StoredProceduresPrefix = StoredProceduresPrefix,
                StoredProceduresSuffix = StoredProceduresSuffix,
                TriggersPrefix = TriggersPrefix,
                TriggersSuffix = TriggersSuffix
            };

            // add all columns
            var clmns = this.Columns;
            for (var i = 0; i < clmns.Count; i++)
                clone.Columns.Add(clmns[i].Clone());

            // Create PrimaryKey
            var pkey = this.PrimaryKey.Columns;
            if (pkey != null && pkey.Length > 0)
            {
                var key = new DmColumn[pkey.Length];

                for (var i = 0; i < pkey.Length; i++)
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
                var destTable = this.Clone();

                foreach (var row in this.Rows)
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

            this.Rows.Clear();
        }

        /// <summary>
        /// Compare string with the table CultureInfo and CaseSensitive flags
        /// </summary>
        public bool IsEqual(string s1, string s2) => this.culture.CompareInfo.Compare(s1, s2, this.compareFlags) == 0;

        /// <summary>
        /// Get Changes from the DmTable
        /// </summary>
        public DmTable GetChanges()
        {
            var dtChanges = this.Clone();
            DmRow row = null;

            for (var i = 0; i < this.Rows.Count; i++)
            {
                row = this.Rows[i];
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
            var dtChanges = this.Clone();
            DmRow row = null;

            for (var i = 0; i < this.Rows.Count; i++)
            {
                row = this.Rows[i];
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
            for (var i = 0; i < this.Columns.Count; ++i)
            {
                // colonne de destination
                var dstColumn = this.Columns[i];
                // colonne à copier
                var srcColumn = src.Columns.FirstOrDefault(c => this.IsEqual(c.ColumnName, dstColumn.ColumnName));


                if (srcColumn == null)
                {
                    dstColumn.Init(newRecord);
                    continue;
                }

                if (srcColumn[srcRecord] is ICloneable cloneable)
                {
                    dstColumn[newRecord] = ((ICloneable)srcColumn[srcRecord]).Clone();
                    continue;
                }
                if (dstColumn.IsValueType)
                {
                    dstColumn[newRecord] = srcColumn[srcRecord];
                    continue;
                }
                if (dstColumn.DataType == typeof(byte[]))
                {
                    if (srcColumn[srcRecord] is byte[] srcArray && srcArray.Length > 0)
                    {
                        var destArray = (byte[])dstColumn[newRecord];
                        destArray = new byte[srcArray.Length];
                        Buffer.BlockCopy(srcArray, 0, destArray, 0, srcArray.Length);
                    }
                    continue;
                }
                if (dstColumn.DataType == typeof(char[]))
                {
                    if (srcColumn[srcRecord] is char[] srcArray && srcArray.Length > 0)
                    {
                        var destArray = (char[])dstColumn[newRecord];
                        destArray = new char[srcArray.Length];
                        Buffer.BlockCopy(srcArray, 0, destArray, 0, srcArray.Length);
                    }

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
                this.CopyRecords(rowToImport.Table, rowToImport.oldRecord, oldRecord);
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
                    this.CopyRecords(rowToImport.Table, rowToImport.newRecord, newRecord);
                }
                else
                    newRecord = oldRecord;
            }

            // Don't init columns since we already have them
            var r = new DmRow(this, false)
            {
                oldRecord = oldRecord,
                newRecord = newRecord
            };

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
            var hasLessValues = false;
            if (values.Length == this.columns.Count - 1)
                hasLessValues = true;

            var j = 0;
            for (var i = 0; i < this.columns.Count; i++)
            {
                var column = this.columns[i];

                if (column.IsAutoIncrement && hasLessValues)
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

        /// <summary>
        /// Reject every changes made since last AcceptChanges()
        /// </summary>
        public void RejectChanges()
        {
            var oldRows = new DmRow[this.Rows.Count];
            this.Rows.CopyTo(oldRows, 0);

            for (var i = 0; i < oldRows.Length; i++)
                oldRows[i].Rollback();

        }

        public override string ToString() => $"{this.TableName} ({this.Rows.ToString()})";

        /// <summary>
        /// Merge a dmTable in this dmTable without perserving changes
        /// </summary>
        public void Merge(DmTable table) => this.Merge(table, false);

        /// <summary>
        /// Merge a dmTable in this dmTable perserving changes
        /// </summary>
        public void Merge(DmTable table, bool preserveChanges)
        {
            if (table == null)
                throw new ArgumentNullException(nameof(table));

            var merger = new DmMerger(this, preserveChanges);
            merger.MergeTable(table);
        }

        internal void MergeRow(DmRow row, DmRow targetRow, bool preserveChanges)
        {
            // Si le merge ne concerne pas une ligne déjà existante
            if (targetRow == null)
            {
                this.ImportRow(row);
                return;
            }

            var proposedRecord = targetRow.tempRecord; // by saving off the tempRecord, EndEdit won't free newRecord
            targetRow.tempRecord = -1;
            try
            {
                var saveRowState = targetRow.RowState;
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
                        this.CopyRecords(this, oldRecord, newRecord);
                    }
                    else
                    {
                        newRecord = targetRow.newRecord;
                    }


                    this.CopyRecords(row.Table, row.oldRecord, targetRow.oldRecord);
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
                            this.CopyRecords(this, oldRecord, newRecord);
                        }
                        else
                        {
                            newRecord = targetRow.newRecord;
                        }
                    }
                    else
                        newRecord = -1;

                    this.CopyRecords(row.Table, row.oldRecord, oldRecord);

                }
                else
                {
                    // incoming row is added, modified or unchanged (targetRow is not unchanged)
                    oldRecord = targetRow.oldRecord;
                    newRecord = targetRow.newRecord;
                    if (targetRow.RowState == DmRowState.Unchanged)
                    {
                        newRecord = this.Rows.GetNewVersionId();
                        this.CopyRecords(this, oldRecord, newRecord);
                    }
                    this.CopyRecords(row.Table, row.oldRecord, oldRecord);

                    if (!preserveChanges)
                    {
                        this.CopyRecords(row.Table, row.newRecord, newRecord);
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

                for (var i = 0; i < reader.FieldCount; i++)
                {
                    var columnName = reader.GetName(i);
                    var columnType = reader.GetFieldType(i);
                    var column = DmColumn.CreateColumn(columnName, columnType);
                    this.Columns.Add(column);
                }
            }

            // Count - 2 becoz we can have a autoinc columns
            if (readerFieldCount < this.columns.Count - 2)
                return;

            while (reader.Read())
            {
                var readerDataValues = new object[reader.FieldCount];

                reader.GetValues(readerDataValues);
                var dataRow = this.LoadDataRow(readerDataValues, true);

            }
        }



        /// <summary>
        /// Get a List of DmRelation from the current table to the childTable
        /// </summary>
        public List<DmRelation> GetChildsTo(DmTable childTable)
        {
            // bool validating we found the correct path from this to childTable
            var checkFound = false;

            // final list containing all relation from this to childTable
            var childsRelations = new List<DmRelation>();

            void checkBranch(DmTable dmTable, ref List<DmRelation> lst, ref bool found)
            {
                // if we don't have any foreign key in the current table (so no parent tables) we can return false
                if (dmTable.ChildRelations == null || dmTable.ChildRelations.Count == 0)
                    return;

                // check all child relations
                foreach (var childRelation in dmTable.ChildRelations)
                {
                    // new list cloned
                    var nList = new List<DmRelation>(lst)
                    {
                        // add the parent table name to the new cloned list
                        childRelation
                    };

                    // Check if finally we reach the correct table we are looking for
                    if (childRelation.ChildTable.TableName.ToLowerInvariant() == childTable.TableName.ToLowerInvariant())
                    {
                        // set the correct flag
                        found = true;
                        // replace the referenced list with the good one
                        childsRelations = nList;
                        return;
                    }

                    checkBranch(childRelation.ChildTable, ref nList, ref found);
                }
            }

            checkBranch(this, ref childsRelations, ref checkFound);

            if (!checkFound)
                childsRelations.Clear();

            return childsRelations;

        }

        /// <summary>
        /// Get a List of DmRelation from the current table to the parentTable
        /// </summary>
        public List<DmRelation> GetParentsTo(DmTable rootTable)
        {

            // bool validating we found the correct path from this to parentTable
            var checkFound = false;

            // final list containing all relation from this to parentTable
            var parentRelations = new List<DmRelation>();

            // check a whole branch
            void checkBranch(DmTable dmTable, ref List<DmRelation> lst, ref bool found)
            {
                // if we don't have any foreign key in the current table (so no parent tables) we can return false
                if (dmTable.ParentRelations == null || dmTable.ParentRelations.Count == 0)
                    return;

                // check all parent relations
                foreach (var parentRelation in dmTable.ParentRelations)
                {
                    // new list cloned
                    var nList = new List<DmRelation>(lst)
                    {

                        // add the parent table name to the new cloned list
                        parentRelation
                    };

                    // Check if the parent relation is the parentTable name we are searching
                    if (parentRelation.ParentTable.TableName.ToLowerInvariant() == rootTable.TableName.ToLowerInvariant())
                    {
                        // set the correct flag
                        found = true;
                        // replace the referenced list with the good one
                        parentRelations = nList;
                        return;
                    }

                    checkBranch(parentRelation.ParentTable, ref nList, ref found);
                }
            }

            checkBranch(this, ref parentRelations, ref checkFound);

            if (!checkFound)
                parentRelations.Clear();

            return parentRelations;
        }

    }
}
