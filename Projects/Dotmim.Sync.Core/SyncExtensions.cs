
using System.Collections.Generic;
using System.Data;
using System.Linq;


namespace Dotmim.Sync
{
    /// <summary>
    /// Contains only extensions methods
    /// </summary>
    public static class SyncExtensions
    {
        /// <summary>
        /// Validates, that all column filters do refer to a an existing column of the target table
        /// </summary>
        /// <param name="filters"></param>
        /// <param name="tableDescription"></param>
        public static void ValidateColumnFilters(this SyncFilter filter, SyncTable tableDescription)
        {
            if (filter == null)
                return;

            // TODO : Validate column filters
            //foreach (var c in filters)
            //{
            //    if (c.IsVirtual)
            //        continue;

            //    var columnFilter = tableDescription.Columns[c.ColumnName];

            //    if (columnFilter == null)
            //        throw new InvalidExpressionException($"Column {c.ColumnName} does not exist in Table {tableDescription.TableName}");
            //}
        }
    }
}
