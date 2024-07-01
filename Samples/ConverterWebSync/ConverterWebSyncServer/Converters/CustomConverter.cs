using Dotmim.Sync;
using Dotmim.Sync.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ConverterWebSyncServer.Converters
{
    public class CustomConverter : IConverter
    {
        public string Key => "cuscom";

        public void BeforeSerialize(SyncRow row, SyncTable schemaTable)
        {
            // Each row belongs to a Table with its own Schema
            // Easy to filter if needed
            if (schemaTable.TableName == "Product")
            {
                var photoColumn = schemaTable.Columns["ThumbNailPhoto"];
                var index = schemaTable.Columns.IndexOf(photoColumn);
                // Encode a specific column, named "ThumbNailPhoto"
                if (row[index] != null)
                    row[index] = Convert.ToBase64String((byte[])row[index]);
            }

            // Convert all DateTime columns to ticks
            foreach (var col in schemaTable.Columns.Where(c => c.GetDataType() == typeof(DateTime)))
            {
                var colIndex = schemaTable.Columns.IndexOf(col);
                if (row[colIndex] != null)
                    row[colIndex] = ((DateTime)row[colIndex]).Ticks;
            }
            // Convert all DateTime columns to ticks
            foreach (var col in schemaTable.Columns.Where(c => c.GetDataType() == typeof(DateTimeOffset)))
            {
                var colIndex = schemaTable.Columns.IndexOf(col);
                if (row[colIndex] != null)
                    row[colIndex] = ((DateTimeOffset)row[colIndex]).Ticks;
            }

        }

        public void AfterDeserialized(SyncRow row, SyncTable schemaTable)
        {
            // Only convert for table Product
            if (schemaTable.TableName == "Product")
            {
                var photoColumn = schemaTable.Columns["ThumbNailPhoto"];
                var index = schemaTable.Columns.IndexOf(photoColumn);
                // Decode photo
                if (row[index] != null)
                    row[index] = Convert.FromBase64String((string)row[index]);
            }

            // Convert all DateTime back from ticks
            foreach (var col in schemaTable.Columns.Where(c => c.GetDataType() == typeof(DateTime)))
            {
                var colIndex = schemaTable.Columns.IndexOf(col);
                if (row[colIndex] != null)
                    row[colIndex] = new DateTime(Convert.ToInt64(row[colIndex]));
            }
            foreach (var col in schemaTable.Columns.Where(c => c.GetDataType() == typeof(DateTimeOffset)))
            {
                var colIndex = schemaTable.Columns.IndexOf(col);
                if (row[colIndex] != null)
                    row[colIndex] = new DateTimeOffset(Convert.ToInt64(row[colIndex]), TimeSpan.Zero);
            }
        }
    }
}
