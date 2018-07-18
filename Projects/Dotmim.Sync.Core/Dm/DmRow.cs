using System;
using System.Linq;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;
using System.Xml;
using System.Collections.ObjectModel;
using System.Text;

namespace Dotmim.Sync.Data
{

    /// <summary>
    /// Represents a row of data in a DmTable
    /// </summary>
    public class DmRow
    {
        readonly DmTable table;
        readonly DmColumnCollection columns;

        // représente l'id de la ligne 
        int rowId = -1;

        // représente l'index de la version avant modification de la ligne
        internal int oldRecord = -1;

        // représente l'index de la nouvelle version de la ligne
        internal int newRecord = -1;

        // représente un index temporaire, lors de la création d'une nouvelle ligne pas encore ajoutée
        internal int tempRecord = -1;

        // si tempRecord != -1 alors l'enregistrement est proposé. On vient de le créer
        // si oldRecord == -1 && newRecord != -1 alors l'enregistrement est Added
        // si oldRecord != -1 && newRecord == -1 alors l'enregistrement est Deleted
        // si oldRecord != -1 && newRecord != -1 alors l'enregistrement est Modified
        // si oldRecord == -1 && newRecord == -1 alors l'enregisrement est Detached


        internal int RowId
        {
            get
            {
                return rowId;
            }
            set
            {
                rowId = value;
            }
        }

        /// <devdoc>
        ///    <para>Gets the current state of the row in regards to its relationship to the table.</para>
        /// </devdoc>
        public DmRowState RowState
        {
            get
            {

                if (oldRecord == -1 && newRecord == -1)
                    return DmRowState.Detached; // 2
                else if (oldRecord == newRecord)
                    return DmRowState.Unchanged; // 3
                else if (oldRecord == -1)
                    return DmRowState.Added; // 2
                else if (newRecord == -1)
                    return DmRowState.Deleted; // 3
                else
                    return DmRowState.Modified; // 3

            }
        }
        public DmTable Table
        {
            get
            {
                return table;
            }

        }

        //protected internal DmRow(DmTable table, DmRowState state)
        //{
        //    this.table = table;
        //    this.columns = table.Columns;
        //    var record = this.Table.Rows.GetNewVersionId();

        //    this.tempRecord = record;

        //    // si oldRecord == -1 && newRecord != -1 alors l'enregistrement est Added
        //    // si oldRecord != -1 && newRecord == -1 alors l'enregistrement est Deleted
        //    // si oldRecord != -1 && newRecord != -1 alors l'enregistrement est Modified
        //    // si oldRecord == -1 && newRecord == -1 alors l'enregisrement est Detached
        //    switch (state)
        //    {
        //        case DmRowState.Added:
        //            this.oldRecord = -1;
        //            this.newRecord = this.Table.Rows.GetNewVersionId();
        //            break;
        //        case DmRowState.Deleted:
        //            this.oldRecord = this.Table.Rows.GetNewVersionId();
        //            this.newRecord = -1;
        //            break;
        //        case DmRowState.Modified:
        //            this.newRecord = this.Table.Rows.GetNewVersionId();
        //            this.oldRecord = this.Table.Rows.GetNewVersionId();
        //            break;
        //        case DmRowState.Detached:
        //            this.oldRecord = -1;
        //            this.newRecord = -1;
        //            break;
        //    }

        //}

        protected internal DmRow(DmTable table, bool initColumns = true)
        {
            this.table = table;
            this.columns = table.Columns;

            // Init each columns and create a tempRecord
            if (initColumns)
            {
                this.tempRecord = this.Table.Rows.GetNewVersionId();

                foreach (var c in columns)
                    c.Init(this.tempRecord);
            }

        }

        /// <summary>
        /// Gets or sets the data stored in the column specified by index.
        /// </summary>
        public object this[int columnIndex]
        {
            get
            {
                DmColumn column = columns[columnIndex];
                int record = GetRecordId();
                return column[record];
            }
            set
            {
                DmColumn c = columns[columnIndex];
                this[c] = value;
            }
        }

        /// <summary>
        /// Gets or sets the data stored in the column specified by name.
        /// </summary>
        public object this[string columnName]
        {
            get
            {
                DmColumn column = GetDataColumn(columnName);
                int record = GetRecordId();
                return column[record];
            }
            set
            {
                DmColumn column = GetDataColumn(columnName);
                this[column] = value;
            }
        }

        /// <summary>
        /// Gets or sets the data stored in the column
        /// </summary>
        public object this[DmColumn column]
        {
            get
            {
                CheckColumn(column);
                int record = GetRecordId();
                return column[record];
            }
            set
            {
                CheckColumn(column);

                if (RowId != -1 && column.IsReadOnly)
                    throw new Exception($"ReadOnly {column.ColumnName}");

                if (value == null && !column.AllowDBNull)
                    throw new Exception($"Cannot set null to this {column}");

                // we are going to change a value, so try to enter begin edit
                bool immediate = BeginEditInternal();
                try
                {
                    int record = GetProposedRecordId();
                    column[record] = value;
                }
                catch (Exception)
                {
                    throw;
                }

                if (immediate)
                    EndEdit();
            }
        }


        /// <summary>
        /// Gets the data stored in the row, specified by index and version of the data to retrieve.
        /// </summary>
        public object this[int rowIndex, DmRowVersion version]
        {
            get
            {
                DmColumn row = columns[rowIndex];
                int record = GetRecordFromVersion(version);
                return row[record];
            }
        }

        /// <summary>
        /// Gets the specified version of data stored in the named row.
        /// </summary>
        public object this[string rowName, DmRowVersion version]
        {
            get
            {
                DmColumn row = GetDataColumn(rowName);
                int record = GetRecordFromVersion(version);
                return row[record];
            }
        }

        /// <summary>
        /// Gets the specified version of data stored in the specified DmColumn />.
        /// </summary>
        public object this[DmColumn row, DmRowVersion version]
        {
            get
            {
                CheckColumn(row);
                int record = GetRecordFromVersion(version);
                return row[record];
            }
        }


        public string ToString(DmRowVersion version)
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < columns.Count; i++)
            {
                var c = columns[i];
                var o = this[c, version];
                var os = o == null ? "<NULL />" : o.ToString();

                sb.Append($"{c.ColumnName}: {os}, ");
            }

            return sb.ToString();
        }
        public override string ToString()
        {
            if (ItemArray == null || ItemArray.Length == 0)
                return "empty row";

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < columns.Count; i++)
            {
                var c = columns[i];
                var o = ItemArray[i];
                var os = o == null ? "<NULL />" : o.ToString();

                sb.Append($"{c.ColumnName}: {os}, ");
            }

            return sb.ToString();
        }
        /// <summary>
        /// Gets or sets all of the values for this row through an array.
        /// </summary>
        public object[] ItemArray
        {
            get
            {
                int record = GetRecordId();

                object[] values = new object[columns.Count];

                for (int i = 0; i < values.Length; i++)
                {
                    DmColumn row = columns[i];
                    values[i] = row[record];
                }
                return values;
            }
            set
            {
                if (null == value)
                    throw new ArgumentNullException(nameof(value), "ItemArray");

                if (columns.Count < value.Length)
                    throw new Exception("ValueArrayLength");

                bool immediate = BeginEditInternal();

                for (int i = 0; i < value.Length; ++i)
                {
                    // Empty means don't change the row.
                    if (value[i] != null)
                    {
                        // may throw exception if user removes row from table during event
                        DmColumn column = columns[i];

                        if (RowId != -1 && column.IsReadOnly)
                            throw new Exception($"ReadOnly {column.ColumnName}");


                        if (column.Table != table)
                            // user removed row from table during OnColumnChanging event
                            throw new Exception($"ColumnNotInTheTable {column.ColumnName} , {table.TableName}");

                        if (tempRecord == -1)
                            // user affected CancelEdit or EndEdit during OnColumnChanging event of the last value
                            BeginEditInternal();

                        object proposed = value[i];
                        if (proposed == null && column.IsValueType)
                            throw new ArgumentNullException(column.ColumnName);

                        try
                        {
                            // must get proposed record after each event because user may have
                            // called EndEdit(), AcceptChanges(), BeginEdit() during the event
                            int record = GetProposedRecordId();
                            column[record] = proposed;
                        }
                        catch (Exception)
                        {
                            throw;
                        }
                    }
                }
                EndEdit();
            }
        }


        /// <summary>
        /// Commits all the changes made to this row since the last time AcceptChanges was called.
        /// </summary>
        public void AcceptChanges()
        {
            // Pass tempRecord if exist as actual row
            EndEdit();

            // proposing the actual row as a new Old one !
            this.ProposeAsOldRecordId(this.newRecord);
        }


        /// <devdoc>
        /// <para>Begins an edit operation on a DmRow object.</para>
        /// </devdoc>
        public void BeginEdit()
        {
            BeginEditInternal();
        }

        /// <summary>
        /// Edit the current row
        /// this method is always called even if you dont call it explicitly when you create a new row
        /// </summary>
        bool BeginEditInternal()
        {
            // Line is deleted can't edit
            if (oldRecord != -1 && newRecord == -1)
                throw new Exception("Deleted Row Inaccessible");

            var notAlreadyInEditMode = this.tempRecord == -1;

            // Create a temp row and add it as the tempRecordId
            if (this.tempRecord == -1)
                this.tempRecord = this.Table.Rows.GetNewVersionId();

            // ancienne version de la ligne à enregistrer dans la nouvelle ligne
            if (this.newRecord != -1)
                this.Table.CopyRecords(Table, this.newRecord, this.tempRecord);

            return notAlreadyInEditMode;
        }

        public void EndEdit()
        {
            if (newRecord == -1)
                return; // this is meaningless, detatched row case

            // suppressing the ensure property changed because it's possible that no values have been modified
            if (tempRecord != -1)
                this.ProposeAsNewRecordId(tempRecord);
        }

        /// <summary>
        /// Cancel the current edition, but it doesn't rollback the line
        /// </summary>
        public void CancelEdit()
        {
            // Remove record from all column
            this.RemoveColumnsRecord(tempRecord);

            // set the tempRecord to init value
            this.tempRecord = -1;
        }

        public void RejectChanges()
        {
            this.Rollback();
        }

        /// <summary>
        /// Remove record from the version collection
        /// </summary>
        internal void RemoveColumnsRecord(int recordId)
        {
            // Remove the value from the internal storage
            foreach (var c in this.table.Columns)
                c.RemoveRecord(recordId);
        }

        internal void Rollback()
        {
            if (this.oldRecord == -1)
                throw new Exception("You have to AcceptChanges, then make a change based on this accepted version, then RejectChanges.");

            // cancel the edition
            this.CancelEdit();

            // get back the oldrecord as the current version
            this.ProposeAsNewRecordId(this.oldRecord);
        }

        void CheckColumn(DmColumn column)
        {
            if (column == null)
                throw new ArgumentNullException(nameof(column));

            if (column.Table != table)
                throw new Exception($"ColumnNotInTheTable {column.ColumnName} : {table.TableName}");

        }

        public void SetAdded()
        {
            if (this.RowState != DmRowState.Unchanged)
                throw new Exception("Set AddedAndModified called on non unchanged");

            // set the tempRecord to init value
            this.oldRecord = -1;
        }

        public void SetModified()
        {
            if (this.RowState != DmRowState.Unchanged)
                throw new Exception("Set AddedAndModified called on non unchanged");

            // Create a new oldRecord 
            this.oldRecord = this.Table.Rows.GetNewVersionId();

            // ancienne version de la ligne à enregistrer dans la nouvelle ligne
            this.Table.CopyRecords(Table, this.newRecord, this.oldRecord);
        }

        /// <summary>
        /// Delete the row
        /// </summary>
        public void Delete()
        {
            if (newRecord == -1)
                return;

            // set the record to be the old one
            this.ProposeAsOldRecordId(newRecord);

            // Set this row to be in deleted state
            this.ProposeAsNewRecordId(-1);
        }

        /// <summary>
        /// Gets the child rows of this DmRow using the
        /// specified DmRelation.
        /// </summary>
        public DmRow[] GetChildRows(string relationName) =>
            GetChildRows(this.table.ChildRelations.FirstOrDefault(r => r.RelationName == relationName), DmRowVersion.Default);

        /// <summary>
        /// Gets the child rows of this DmRow using the
        /// specified DmRelation.
        /// </summary>
        public DmRow[] GetChildRows(string relationName, DmRowVersion version) =>
            GetChildRows(table.ChildRelations.FirstOrDefault(r => r.RelationName == relationName), version);

        /// <summary>
        /// Gets the child rows of this DmRow using the
        /// specified DmRelation.
        /// </summary>
        public DmRow[] GetChildRows(DmRelation relation) =>
            GetChildRows(relation, DmRowVersion.Default);

        /// <summary>
        /// Gets the child rows of this DmRow using the specified DmRelation and the specified DmRowVersion
        /// </summary>
        public DmRow[] GetChildRows(DmRelation relation, DmRowVersion version)
        {
            if (relation == null)
                return new DmRow[] { table.NewRow() };

            if (relation.DmSet != table.DmSet)
                throw new Exception("RowNotInTheDataSet");

            if (relation.ParentKey.Table != table)
                throw new Exception("RelationForeignTable");

            return DmRelation.GetChildRows(relation.ParentKey, relation.ChildKey, this, version);
        }

        public DmRow GetParentRow(string relationName) =>
            GetParentRow(table.ParentRelations.FirstOrDefault(r => r.RelationName == relationName), DmRowVersion.Default);

        public DmRow GetParentRow(string relationName, DmRowVersion version) =>
            GetParentRow(table.ParentRelations.FirstOrDefault(r => r.RelationName == relationName), version);

        /// <summary>
        /// Gets the parent row of this DmRow using the specified DmRelation .
        /// </summary>
        public DmRow GetParentRow(DmRelation relation) =>
            GetParentRow(relation, DmRowVersion.Default);

        /// <summary>
        /// Gets the parent rows of this DmRow using the specified DmRelation .
        /// </summary>
        public DmRow[] GetParentRows(string relationName) =>
                    GetParentRows(table.ParentRelations.FirstOrDefault(r => r.RelationName == relationName), DmRowVersion.Default);

        /// <summary>
        /// Gets the parent rows of this DmRow using the specified DmRelation .
        /// </summary>
        public DmRow[] GetParentRows(string relationName, DmRowVersion version) =>
            GetParentRows(table.ParentRelations.FirstOrDefault(r => r.RelationName == relationName), version);

        /// <summary>
        /// Gets the parent rows of this DmRow using the specified DmRelation .
        /// </summary>
        public DmRow[] GetParentRows(DmRelation relation) =>
            GetParentRows(relation, DmRowVersion.Default);

        /// <summary>
        /// Gets the parent rows of this DmRow using the specified DmRelation .
        /// </summary>
        public DmRow[] GetParentRows(DmRelation relation, DmRowVersion version)
        {
            if (relation == null)
                return new DmRow[] { table.NewRow() };

            if (relation.DmSet != table.DmSet)
                throw new Exception("RowNotInTheDataSet");

            if (relation.ChildKey.Table != table)
                throw new Exception("RelationForeignTable");

            return DmRelation.GetParentRows(relation.ParentKey, relation.ChildKey, this, version);

        }

        /// <summary>
        /// Gets the parent row of this <see cref='System.Data.DataRow'/>
        /// using the specified <see cref='System.Data.DataRelation'/> and <see cref='System.Data.DataRowVersion'/>.
        /// </summary>
        public DmRow GetParentRow(DmRelation relation, DmRowVersion version)
        {
            if (relation == null)
                return null;

            if (relation.DmSet != table.DmSet)
                throw new Exception("RowNotInTheDataSet");

            if (relation.ChildKey.Table != table)
                throw new Exception("RelationForeignTable");


            return DmRelation.GetParentRow(relation.ParentKey, relation.ChildKey, this, version);
        }

        internal DmColumn GetDataColumn(string rowName)
        {
            DmColumn column = columns.FirstOrDefault(c => this.Table.IsEqual(c.ColumnName, rowName));
            if (null != column)
                return column;

            throw new Exception("ColumnNotInTheTable");
        }
        internal object[] GetColumnValues(DmColumn[] rows)
        {
            return GetColumnValues(rows, DmRowVersion.Default);
        }
        internal object[] GetColumnValues(DmColumn[] rows, DmRowVersion version)
        {
            DmKey key = new DmKey(rows); // temporary key, don't copy rows
            return GetKeyValues(key, version);
        }

        public object[] GetKeyValues()
        {
            return GetKeyValues(this.table.PrimaryKey);
        }

        internal object[] GetKeyValues(DmRowVersion version)
        {
            int record = GetRecordFromVersion(version);
            return this.table.PrimaryKey.GetKeyValues(record);
        }
        internal object[] GetKeyValues(DmKey key)
        {
            int record = GetRecordId();
            return key.GetKeyValues(record);
        }
        internal object[] GetKeyValues(DmKey key, DmRowVersion version)
        {
            int record = GetRecordFromVersion(version);
            return key.GetKeyValues(record);
        }

        /// <summary>
        /// Get the default record id. could be a proposed or a added or a modified
        /// </summary>
        internal int GetRecordId()
        {
            if (tempRecord != -1)
                return tempRecord;

            if (newRecord != -1)
                return newRecord;

            // If row has oldRecord - this is deleted row.
            if (oldRecord == -1)
                throw new Exception("RowRemovedFromTheTable");
            else
                throw new Exception("DeletedRowInaccessible");
        }

        /// <summary>
        /// Get current record id
        /// </summary>
        internal int GetCurrentRecordId()
        {
            if (newRecord == -1)
                throw new Exception("NoCurrentData");

            return newRecord;
        }

        /// <summary>
        /// Get the original record id
        /// </summary>
        internal int GetOriginalRecordId()
        {
            if (oldRecord == -1)
                throw new Exception("NoOriginalData");

            return oldRecord;
        }

        /// <summary>
        /// Get the proposed record id
        /// </summary>
        int GetProposedRecordId()
        {
            if (tempRecord == -1)
                throw new Exception("NoProposedData");

            return tempRecord;
        }

        /// <summary>
        /// Get the record id from a version, if exists
        /// </summary>
        internal int GetRecordFromVersion(DmRowVersion version)
        {
            switch (version)
            {
                case DmRowVersion.Original:
                    return GetOriginalRecordId();
                case DmRowVersion.Current:
                    return GetCurrentRecordId();
                case DmRowVersion.Proposed:
                    return GetProposedRecordId();
                case DmRowVersion.Default:
                    return GetRecordId();
                default:
                    throw new Exception("InvalidRowVersion");
            }
        }

        /// <summary>
        /// Prend une ligne avec un état et l'enregistre en tant que ligne actuelle
        /// Si la ligne 
        /// This is the event workhorse... it will throw the changing/changed events
        /// and update the indexes. Used by change, add, delete, revert.
        /// </summary>
        internal void ProposeAsNewRecordId(int proposedRecordId)
        {
            // SI ce n'est pas une row Proposed (temporaire)
            // Si c'est une ligne déja avec une version new, fin
            if (this.tempRecord != proposedRecordId && this.newRecord == proposedRecordId)
                return;

            // Si on propose comme nouveau record, ce n'est plus un temporaire
            this.tempRecord = -1;

            // Sauvegarde la version actuelle de la row
            int currentNewRecord = this.newRecord;

            // Affectation de la valeure actuelle
            this.newRecord = proposedRecordId;

            // Si l'ex version couranteexiste et qu'elle n'est pas l'actuelle ou temporaire ou old
            // alors on supprime complètement les records
            if (currentNewRecord != -1 && currentNewRecord != this.tempRecord && currentNewRecord != this.oldRecord && currentNewRecord != this.newRecord)
                RemoveColumnsRecord(currentNewRecord);
        }

        /// <summary>
        /// Propose a record as an old one
        /// </summary>
        /// <param name="proposedRecord"></param>
        internal void ProposeAsOldRecordId(int proposedRecord)
        {
            if (RowId == -1)
                throw new Exception("RowNotInTheTable");

            if (this.tempRecord != -1)
            {
                this.EndEdit();
                throw new Exception("ModifyingRow");
            }

            if (proposedRecord == this.oldRecord)
                return;

            // Cache l'ancienne version pour post vérification
            int originalRecord = this.oldRecord;

            // changement du old record
            this.oldRecord = proposedRecord;

            // Si l'ex ancienne version existe et qu'elle n'est pas l'actuelle ou temporaire ou old
            // alors on supprime complètement les records
            if (originalRecord != -1 && originalRecord != this.tempRecord && originalRecord != this.oldRecord && originalRecord != this.newRecord)
                RemoveColumnsRecord(originalRecord);
        }



        internal bool HasKeyChanged(DmKey key)
        {
            return HasKeyChanged(key, DmRowVersion.Current, DmRowVersion.Proposed);
        }
        internal bool HasKeyChanged(DmKey key, DmRowVersion version1, DmRowVersion version2)
        {
            if (!HasVersion(version1) || !HasVersion(version2))
                return true;
            return !key.RecordsEqual(GetRecordFromVersion(version1), GetRecordFromVersion(version2));
        }

        public bool HasVersion(DmRowVersion version)
        {
            switch (version)
            {
                // si oldRecord == -1 && newRecord != -1 alors l'enregistrement est Added
                // si oldRecord != -1 && newRecord == -1 alors l'enregistrement est Deleted
                // si oldRecord != -1 && newRecord != -1 alors l'enregistrement est Modified
                // si oldRecord == -1 && newRecord == -1 alors l'enregisrement est Detached

                case DmRowVersion.Original:
                    return (oldRecord != -1);
                case DmRowVersion.Current:
                    return (newRecord != -1);
                case DmRowVersion.Proposed:
                    return (tempRecord != -1);
                case DmRowVersion.Default:
                    return (tempRecord != -1 || newRecord != -1);
                default:
                    throw new Exception("InvalidRowVersion");
            }
        }
        internal bool HasChanges()
        {
            if (!HasVersion(DmRowVersion.Original) || !HasVersion(DmRowVersion.Current))
                return true; // if does not have original, its added row, if does not have current, its deleted row so it has changes

            foreach (DmColumn dc in Table.Columns)
            {
                if (dc.Compare(oldRecord, newRecord) != 0)
                    return true;
            }
            return false;
        }
        internal bool HaveValuesChanged(DmColumn[] rows)
        {
            return HaveValuesChanged(rows, DmRowVersion.Current, DmRowVersion.Proposed);
        }
        internal bool HaveValuesChanged(DmColumn[] rows, DmRowVersion version1, DmRowVersion version2)
        {
            for (int i = 0; i < rows.Length; i++)
                CheckColumn(rows[i]);

            DmKey key = new DmKey(rows); // temporary key, don't copy rows
            return HasKeyChanged(key, version1, version2);
        }



        public bool IsNull(int columnIndex)
        {
            DmColumn column = columns[columnIndex];
            int record = this.GetRecordId();
            return column.IsNull(record);
        }
        public bool IsNull(string columnName)
        {
            DmColumn column = GetDataColumn(columnName);
            int record = GetRecordId();
            return column.IsNull(record);
        }
        public bool IsNull(DmColumn column)
        {
            CheckColumn(column);
            int record = GetRecordId();
            return column.IsNull(record);
        }
        public bool IsNull(DmColumn column, DmRowVersion version)
        {
            CheckColumn(column);
            int record = GetRecordFromVersion(version);
            return column.IsNull(record);
        }
        internal void SetKeyValues(DmKey key, object[] keyValues)
        {
            bool fFirstCall = true;
            bool immediate = (tempRecord == -1);

            for (int i = 0; i < keyValues.Length; i++)
            {
                object value = this[key.Columns[i]];
                if (!value.Equals(keyValues[i]))
                {
                    if (immediate && fFirstCall)
                    {
                        fFirstCall = false;
                        BeginEditInternal();
                    }
                    this[key.Columns[i]] = keyValues[i];
                }
            }
            if (!fFirstCall)
                EndEdit();
        }

    }
}