using Dotmim.Sync.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Dotmim.Sync.SampleConsole
{
    public class DateConverter : IConverter
    {
        public string Key => "cuscom";

        public void BeforeSerialize(SyncRow row)
        {
            // Convert all DateTime columns to ticks
            foreach (var col in row.Table.Columns.Where(c => c.GetDataType() == typeof(DateTime)))
            {
                if (row[col.ColumnName] != null)
                    row[col.ColumnName] = ((DateTime)row[col.ColumnName]).Ticks;
            }
        }

        public void AfterDeserialized(SyncRow row)
        {
            // Convert all DateTime back from ticks
            foreach (var col in row.Table.Columns.Where(c => c.GetDataType() == typeof(DateTime)))
            {
                if (row[col.ColumnName] != null)
                    row[col.ColumnName] = new DateTime(Convert.ToInt64(row[col.ColumnName]));
            }
        }
    }
}
