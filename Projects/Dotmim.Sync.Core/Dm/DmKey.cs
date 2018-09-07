using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Data
{
    public struct DmKey
    {
        const int maxColumns = 32;


        public DmKey(DmColumn[] columns)
        {
            this.Columns = columns;
        }

        public DmKey(DmColumn column)
        {
            this.Columns = new[] { column };
        }

        public DmColumn[] Columns { get; set; }

        internal bool HasValue
        {
            get
            {
                return (null != Columns && Columns.Length > 0);
            }
        }

        internal DmTable Table
        {
            get
            {
                return Columns[0].Table;
            }
        }

        //internal void CheckState()
        //{
        //    DmTable table = columns[0].Table;

        //    if (table == null)
        //        throw new Exception("ColumnNotInAnyTable");

        //    for (int i = 1; i < columns.Length; i++)
        //    {
        //        if (columns[i].Table == null)
        //            throw new Exception("ColumnNotInAnyTable");

        //        if (columns[i].Table != table)
        //            throw new Exception("KeyTableMismatch");
        //    }
        //}

        //check to see if this.columns && key2's columns are equal regardless of order
        internal bool ColumnsEqual(DmKey key)
        {
            return ColumnsEqual(this.Columns, ((DmKey)key).Columns);
        }

        //check to see if Columns && columns2 are equal regardless of order
        internal static bool ColumnsEqual(DmColumn[] column1, DmColumn[] column2)
        {

            if (column1 == column2)
            {
                return true;
            }
            else if (column1 == null || column2 == null)
            {
                return false;
            }
            else if (column1.Length != column2.Length)
            {
                return false;
            }
            else
            {
                int i, j;
                for (i = 0; i < column1.Length; i++)
                {
                    bool check = false;
                    for (j = 0; j < column2.Length; j++)
                    {
                        if (column1[i].Equals(column2[j]))
                        {
                            check = true;
                            break;
                        }
                    }
                    if (!check)
                    {
                        return false;
                    }
                }
            }
            return true;
        }

        internal bool ContainsColumn(DmColumn column)
        {
            for (int i = 0; i < Columns.Length; i++)
            {
                if (column == Columns[i])
                {
                    return true;
                }
            }
            return false;
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        public static bool operator ==(DmKey x, DmKey y)
        {
            return x.Equals((object)y);
        }

        public static bool operator !=(DmKey x, DmKey y)
        {
            return !x.Equals((object)y);
        }

        public override bool Equals(object value)
        {
            return Equals((DmKey)value);
        }

        internal bool Equals(DmKey value)
        {
            //check to see if this.columns && key2's columns are equal...
            DmColumn[] column1 = this.Columns;
            DmColumn[] column2 = value.Columns;

            if (column1 == column2)
            {
                return true;
            }
            else if (column1 == null || column2 == null)
            {
                return false;
            }
            else if (column1.Length != column2.Length)
            {
                return false;
            }
            else
            {
                for (int i = 0; i < column1.Length; i++)
                {
                    if (!column1[i].Equals(column2[i]))
                    {
                        return false;
                    }
                }
                return true;
            }
        }

        internal string[] GetColumnNames()
        {
            string[] values = new string[Columns.Length];
            for (int i = 0; i < Columns.Length; ++i)
            {
                values[i] = Columns[i].ColumnName;
            }
            return values;
        }

        internal object[] GetKeyValues(int record)
        {
            object[] values = new object[Columns.Length];
            for (int i = 0; i < Columns.Length; i++)
            {
                values[i] = Columns[i][record];
            }
            return values;
        }

        internal bool ValuesAreEqual(DmRow row, object[] comparables)
        {
            var record = row.GetCurrentRecordId();
            return ValuesAreEqual(record, comparables);
        }
        internal bool ValuesAreEqual(int record, object[] comparables)
        {
            object[] values = new object[Columns.Length];

            if (values.Length != comparables.Length)
                return false;

            for (int i = 0; i < Columns.Length; i ++)
            {
                if (Columns[i].CompareValueTo(record, comparables[i]) != 0)
                    return false;
            }

            return true;
        }

        internal bool RecordsEqual(int record1, int record2)
        {
            for (int i = 0; i < Columns.Length; i++)
            {
                if (Columns[i].Compare(record1, record2) != 0)
                {
                    return false;
                }
            }
            return true;
        }

        internal DmColumn[] ToArray()
        {
            DmColumn[] values = new DmColumn[Columns.Length];
            for (int i = 0; i < Columns.Length; ++i)
            {
                values[i] = Columns[i];
            }
            return values;
        }


    }
}
