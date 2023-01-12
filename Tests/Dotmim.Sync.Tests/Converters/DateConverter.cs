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

        public void BeforeSerialize(SyncRow row, SyncTable schemaTable)
        {
            // Convert all DateTime columns to ticks
            foreach (var col in schemaTable.Columns.Where(c => c.GetDataType() == typeof(DateTime) || c.GetDataType() == typeof(DateTimeOffset)))
            {
                var index = schemaTable.Columns.IndexOf(col);

                if (row[index] == null)
                    continue;

                long ticks = 0;
                
                if (col.GetDataType() == typeof(DateTime))
                    ticks = SyncTypeConverter.TryConvertTo<DateTime>(row[index]).Ticks;
                else
                    ticks = SyncTypeConverter.TryConvertTo<DateTimeOffset>(row[index]).Ticks;

                row[index] = ticks;
            }
        }

        public void AfterDeserialized(SyncRow row, SyncTable schemaTable)
        {
            // Convert all DateTime back from ticks
            foreach (var col in schemaTable.Columns.Where(c => c.GetDataType() == typeof(DateTime) || c.GetDataType() == typeof(DateTimeOffset)))
            {
                var index = schemaTable.Columns.IndexOf(col);

                if (row[index] == null)
                    continue;

                if (col.GetDataType() == typeof(DateTime))
                    row[index] = new DateTime(Convert.ToInt64(row[index]));
                else
                    row[index] = new DateTimeOffset(Convert.ToInt64(row[index]), TimeSpan.Zero);
            }
        }
    }
}
