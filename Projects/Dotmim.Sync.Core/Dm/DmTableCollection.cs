using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Globalization;
using System.Linq;
using System.Text;

namespace Dotmim.Sync.Data
{
    public class DmTableCollection : ICollection<DmTable>
    {
        Collection<DmTable> collection = new Collection<DmTable>();
        DmSet innerSet;

        internal DmTableCollection(DmSet set)
        {
            this.innerSet = set;
        }
        public DmTable this[int index]
        {
            get
            {
                return collection[index];
            }
        }
        public DmTable this[string name]
        {
            get
            {
                if (string.IsNullOrEmpty(name))
                    throw new ArgumentNullException("name");

                return collection.FirstOrDefault(c => c.TableName == name);
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
      

        public DmTable Add(string name)
        {
            DmTable table = new DmTable(name);
            Add(table);
            return table;
        }
        public void Add(DmTable table)
        {
            // Affect correct Culture / CaseSensitive
            table.DmSet = innerSet;
            table.Culture = innerSet.Culture;
            table.CaseSensitive = innerSet.CaseSensitive;
            table.CheckNameCompliance(table.TableName);
            collection.Add(table);
        }

        /// <summary>
        /// Remove a table
        /// </summary>
        public bool Remove(DmTable table)
        {
            var isRemoved = collection.Remove(table);

            table.DmSet = null;
            table.Culture = CultureInfo.InvariantCulture;
            table.CaseSensitive = true;

            return isRemoved;
        }
        public void AddRange(DmTable[] tables)
        {
            if (tables != null)
                foreach (DmTable table in tables)
                    if (table != null)
                        Add(table);
        }
 
        public void Clear()
        {
            collection.Clear();
        }
        public bool Contains(DmTable item)
        {
            return Contains(item.TableName, false);
        }
        public bool Contains(string name)
        {
            return Contains(name, false);
        }
        public bool Contains(string name, bool caseSensitive)
        {
            var sc = caseSensitive ? StringComparison.CurrentCulture : StringComparison.CurrentCultureIgnoreCase;
            return collection.Any(c => c.TableName.Equals(name, sc));
        }
        public void CopyTo(DmTable[] array, int arrayIndex)
        {
            for (int i = 0; i < collection.Count; ++i)
                array[arrayIndex + i] = collection[i];
        }
        public int IndexOf(DmTable table)
        {
            return collection.IndexOf(table);
        }
        public int IndexOf(string name)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentNullException(name);

            int count = Count;
            DmTable table = collection.FirstOrDefault(c => c.TableName == name);

            if (table == null)
                return -1;

            for (int j = 0; j < count; j++)
                if (table == collection[j])
                    return j;

            return -1;
        }

       


        public void RemoveAt(int index)
        {
            DmTable dc = this[index];
            if (dc == null)
                throw new ArgumentOutOfRangeException($"index {index}");

            Remove(dc);
        }
        public void Remove(string name)
        {
            DmTable dc = this[name];
            if (dc == null)
                throw new Exception($"ColumnNotInTheTable({name}, {dc.TableName}");
            Remove(dc);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return collection.GetEnumerator();
        }
        public IEnumerator<DmTable> GetEnumerator()
        {
            return collection.GetEnumerator();
        }

    }
}
