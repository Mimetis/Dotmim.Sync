using Dotmim.Sync.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Dotmim.Sync.SampleConsole
{
    public class CustomConverter : IConverter
    {
        public string Key => "cuscom";

        public void BeforeSerialize(SyncRow row)
        {
            // Each row belongs to a Table with its own Schema
            // Easy to filter if needed
            if (row.Table.TableName != "Product")
                return;

            // Encode a specific column, named "ThumbNailPhoto"
            if (row["ThumbNailPhoto"] != null)
                row["ThumbNailPhoto"] = Convert.ToBase64String((byte[])row["ThumbNailPhoto"]);

            // Convert all DateTime columns to ticks
            foreach (var col in row.Table.Columns.Where(c => c.GetDataType() == typeof(DateTime)))
            {
                if (row[col.ColumnName] != null)
                    row[col.ColumnName] = ((DateTime)row[col.ColumnName]).Ticks;
            }
        }

        public void AfterDeserialized(SyncRow row)
        {
            // Only convert for table Product
            if (row.Table.TableName != "Product")
                return;

            // Decode photo
            row["ThumbNailPhoto"] = Convert.FromBase64String((string)row["ThumbNailPhoto"]);

            // Convert all DateTime back from ticks
            foreach (var col in row.Table.Columns.Where(c => c.GetDataType() == typeof(DateTime)))
            {
                if (row[col.ColumnName] != null)
                    row[col.ColumnName] = new DateTime(Convert.ToInt64(row[col.ColumnName]));
            }
        }
    }
}
