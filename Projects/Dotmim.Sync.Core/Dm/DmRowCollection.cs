using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Data
{
    /// <summary>
    /// Collection of DataRow
    /// </summary>
    public class DmRowCollection : ICollection<DmRow>
    {
        DmTable table;

        // Représente le prochain Id pour les collection dans chaque colonne
        int nextVersionColumnsId = 1;


        /// <summary>
        /// Rows Collection
        /// </summary>
        internal Collection<DmRow> internalRows { get; set; } = new Collection<DmRow>();

        internal DmRowCollection(DmTable dt)
        {
            this.table = dt;
        }


        /// <summary>
        /// Get a Row
        /// </summary>
        public DmRow this[int index]
        {
            get
            {
                return internalRows[index];
            }
        }
        public int Count
        {
            get
            {
                return internalRows.Count;
            }
        }
        public bool IsReadOnly
        {
            get
            {
                return this.table == null;
            }
        }

        internal int GetNewVersionId()
        {
            // Todo : maybe a lock object here ?
            var newVersionId = nextVersionColumnsId;
            nextVersionColumnsId += 1;
            Debug.WriteLine($"Getting a new version id {newVersionId}");
            return newVersionId;
        }

        /// <summary>
        /// Add a Row to the collection of rows
        /// </summary>
        public void Add(DmRow row)
        {
            if (row == null)
                throw new ArgumentNullException(nameof(row));

            if (row.Table != this.table)
                throw new Exception("RowAlreadyInOtherCollection");

            if (row.RowId != -1)
                throw new Exception("RowAlreadyInTheCollection");

            this.internalRows.Add(row);
            row.RowId = this.internalRows.IndexOf(row);

            // Since we adding this row to the collection, we set the temporary record as the current record
            if (row.tempRecord != -1)
                row.ProposeAsNewRecordId(row.tempRecord);
        }

        /// <summary>
        /// Completely clear all the rows. 
        /// If you want to just mark as Deleted, call Delete() on each row
        /// </summary>
        public void Clear()
        {
            internalRows.Clear();
        }


        /// <summary>
        /// Completely remove a row by its index
        /// If you want to just mark as Deleted, call Delete() on the row
        /// </summary>
        public void RemoveAt(int index)
        {
            Remove(this[index]);
        }

        /// <summary>
        /// Completely remove a row
        /// If you want to just mark as Deleted, call Delete() on the row
        /// </summary>
        public bool Remove(DmRow row)
        {
            if ((row == null) || (row.Table != table) || (-1 == row.RowId))
                throw new IndexOutOfRangeException();

            // Remove all
            RemoveRow(row);

            return true;
        }

        /// <summary>
        /// Remove a row from the Collection Rows AND the Versioned Rows
        /// When a row is removed, all data in that row is lost
        /// </summary>
        internal void RemoveRow(DmRow row)
        {
            if (row == null || row.Table != table)
                throw new Exception("this row doesn't exist or not in the current table");

            if (row.RowId == -1)
                throw new Exception("this row has been already removed");

            int oldRecord = row.oldRecord;
            int newRecord = row.newRecord;
            int tempRecord = row.tempRecord;

            // Suppression des pointeurs de versions
            row.oldRecord = -1;
            row.newRecord = -1;
            row.tempRecord = -1;

            if (oldRecord == newRecord)
                oldRecord = -1;

            if (tempRecord == newRecord)
                tempRecord = -1;

            // Suppression des données old et new
            row.RemoveColumnsRecord(oldRecord);
            row.RemoveColumnsRecord(newRecord);
            row.RemoveColumnsRecord(tempRecord);

            // Suppresion de la lignes dans la collection
            if (internalRows.Contains(row))
                internalRows.Remove(row);

            // Suppression de l'id
            row.RowId = -1;

        }

        public bool Contains(DmRow item)
        {
            return this.internalRows.Contains(item);
        }
 
        public void CopyTo(DmRow[] array, int arrayIndex)
        {
            for (int i = 0; i < internalRows.Count; ++i)
                array[arrayIndex + i] = internalRows[i];
        }
        public int IndexOf(DmRow row)
        {
            return internalRows.IndexOf(row);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return internalRows.GetEnumerator();
        }
        public IEnumerator<DmRow> GetEnumerator()
        {
            return internalRows.GetEnumerator();
        }

    }

}
