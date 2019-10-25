using System;
using System.Collections.Generic;
using System.Text;

namespace Dotmim.Sync.Data.Surrogate
{
    [Serializable]
    public class DmRecordSurrogate : IComparable<DmRecordSurrogate>
    {
        public DmRecordSurrogate(int index, int maxRecords)
        {
            this.Index = index;
            this.Records = new List<object>(maxRecords);
        }

        public List<object> Records { get; set; }
        public int Index { get; }

        //public int Compare(DmRecordSurrogate x, DmRecordSurrogate y)
        //{
        //    return x.Index.CompareTo(y.Index);
        //}

        public int CompareTo(DmRecordSurrogate other)
        {
            return this.Index.CompareTo(other.Index);
        }
    }
}
