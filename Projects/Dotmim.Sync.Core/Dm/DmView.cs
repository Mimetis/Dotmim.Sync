using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Data
{

    public class DmView : ICollection<DmRow>, IDisposable
    {
        List<DmRow> internalRows { get; } = new List<DmRow>();
    
        public DmTable Table { get; }

        public DmView(DmTable table, DmRowState state)
        {
            if (table == null)
                return;

            this.Table = table;
            Predicate<DmRow> filter = new Predicate<DmRow>(r => r.RowState == state);
            this.internalFilter(filter, this.Table.Rows);
        }

        public DmView(DmTable table, Predicate<DmRow> filter = null)
        {
            if (table == null)
                return;

            this.Table = table;
            this.internalFilter(filter, this.Table.Rows);
        }

        public DmView(DmView view, Predicate<DmRow> filter = null)
        {
            if (view == null)
                return;

            this.Table = view.Table;
            this.internalFilter(filter, view);
        }


        public DmView Order(Comparison<DmRow> comparer)
        {
            if (this.Count == 0)
                return this;

            this.internalRows.Sort(comparer);

            return this;
        }

        void internalFilter(Predicate<DmRow> filter, ICollection<DmRow> rows)
        {
            this.Clear();

            if (rows.Count == 0)
                return;

            if (filter == null)
            {
                internalRows.AddRange(rows);
                return;
            }

            foreach (var row in rows)
            {
                if (filter(row))
                    internalRows.Add(row);
            }
        }
        public DmView Filter(Predicate<DmRow> filter) => new DmView(this, filter);

        public int Count => this.internalRows.Count;

        public bool IsReadOnly => true;

        public DmRow this[int index] => internalRows[index];

        IEnumerator<DmRow> IEnumerable<DmRow>.GetEnumerator() => internalRows.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => internalRows.GetEnumerator();

        public void Add(DmRow item)
        {
            this.internalRows.Add(item);
        }

        public void Clear()
        {
            internalRows.Clear();
        }

        public bool Contains(DmRow item) => this.internalRows.Contains(item);

        public void CopyTo(DmRow[] array, int arrayIndex)
        {
            for (int i = 0; i < internalRows.Count; ++i)
                array[arrayIndex + i] = internalRows[i];
        }

        public bool Remove(DmRow row)
        {
            // Suppresion de la lignes dans la collection
            if (internalRows.Contains(row))
                internalRows.Remove(row);

            return true;
        }

        public void Dispose()
        {
            internalRows.Clear();

        }
    }
}
