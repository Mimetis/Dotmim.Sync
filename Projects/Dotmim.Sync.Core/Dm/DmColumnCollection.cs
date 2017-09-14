using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Data
{
    /// <summary>
    /// Collection of DataColumn
    /// </summary>
    public class DmColumnCollection : ICollection<DmColumn>
    {
        Collection<DmColumn> collection = new Collection<DmColumn>();
        DmTable table;
        public DmColumnCollection(DmTable dt)
        {
            this.table = dt;
        }
        public DmColumn this[int index]
        {
            get
            {
                return collection[index];
            }
        }
        public DmColumn this[string name]
        {
            get
            {
                if (string.IsNullOrEmpty(name))
                    throw new ArgumentNullException("name");

                return collection.FirstOrDefault(c => this.table.IsEqual(c.ColumnName,name));
            }
        }
        public int Count
        {
            get
            {
                return collection.Count;
            }
        }
        public bool IsReadOnly
        {
            get
            {
                return false;
            }
        }
        public DmColumn Add<T>(string columnName)
        {
            DmColumn column = new DmColumn<T>(columnName);
            AddAt(-1, column);
            return column;
        }

        public DmColumn Add(string columnName, Type dataType)
        {
            DmColumn column = DmColumn.CreateColumn(columnName, dataType);
            AddAt(-1, column);
            return column;
        }

        public void Add<T>(DmColumn<T> item)
        {
            AddAt(-1, item);
        }
        public void Add(DmColumn item)
        {
            AddAt(-1, item);
        }
        public void AddRange(DmColumn[] columns)
        {
            if (columns != null)
                foreach (DmColumn column in columns)
                    if (column != null)
                        Add(column);
        }
        void AddAt(int index, DmColumn column)
        {
            column.SetTable(table);

            if (index != -1)
            {
                collection.Insert(index, column);

                for (int i = 0; i < Count; i++)
                    collection[i].SetOrdinalInternal(i);
            }
            else
            {
                collection.Add(column);
                column.SetOrdinalInternal(collection.Count - 1);
            }
        }
        public void Clear()
        {
            collection.Clear();
        }
        public bool Contains(DmColumn item)
        {
            return Contains(item.ColumnName, false);
        }
        public bool Contains(string name)
        {
            return Contains(name, false);
        }
        public bool Contains(string name, bool caseSensitive)
        {
            var sc = caseSensitive ? StringComparison.CurrentCulture : StringComparison.CurrentCultureIgnoreCase;
            return collection.Any(c => c.ColumnName.Equals(name, sc));
        }
        public void CopyTo(DmColumn[] array, int arrayIndex)
        {
            for (int i = 0; i < collection.Count; ++i)
                array[arrayIndex + i] = collection[i];
        }
        public int IndexOf(DmColumn column)
        {
            return collection.IndexOf(column);
        }
        public int IndexOf(string columnName)
        {
            if (string.IsNullOrEmpty(columnName))
                throw new ArgumentNullException(columnName);

            int count = Count;
            DmColumn column = collection.FirstOrDefault(c => c.ColumnName == columnName);

            if (column == null)
                return -1;

            for (int j = 0; j < count; j++)
                if (column == collection[j])
                    return j;

            return -1;
        }

        /// <summary>
        /// Move a column to a new position
        /// </summary>
        public void MoveTo(DmColumn column, int newPosition)
        {
            if (newPosition < 0 || newPosition > this.Count - 1)
                throw new Exception($"InvalidOrdinal(ordinal, {newPosition}");

            collection.Remove(column);

            if (newPosition > this.Count - 1)
                collection.Add(column);
            else
                collection.Insert(newPosition, column);

            for (int i = 0; i < Count; i++)
                collection[i].SetOrdinalInternal(i);

        }

        /// <summary>
        /// Remove a column
        /// </summary>
        public bool Remove(DmColumn column)
        {
            column.SetOrdinalInternal(-1);
            column.SetTable(null);

            var r = collection.Remove(column);

            for (int i = 0; i < collection.Count; i++)
                collection[i].SetOrdinalInternal(i);

            return r;
        }
        public void RemoveAt(int index)
        {
            DmColumn dc = this[index];
            if (dc == null)
                throw new ArgumentOutOfRangeException($"index {index}");

            Remove(dc);
        }
        public void Remove(string name)
        {
            DmColumn dc = this[name];
            if (dc == null)
                throw new Exception($"ColumnNotInTheTable({name}, {table.TableName}");
            Remove(dc);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return collection.GetEnumerator();
        }
        public IEnumerator<DmColumn> GetEnumerator()
        {
            return collection.GetEnumerator();
        }

    }
}
