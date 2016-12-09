using Dotmim.Sync.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Data
{
    internal sealed class DmMerger
    {
        DmSet dataSet = null;
        DmTable dataTable = null;
        bool preserveChanges;

        internal DmMerger(DmTable dataTable, bool preserveChanges)
        {
            this.dataTable = dataTable;
            this.preserveChanges = preserveChanges;
        }

        internal void MergeDmSet(DmSet source)
        {
            if (source == dataSet) return;  //somebody is doing an 'automerge'

            List<DmColumn> existingColumns = null;// need to cache existing columns

            existingColumns = new List<DmColumn>(); // need to cache existing columns
            foreach (DmTable dt in dataSet.Tables)
            {
                foreach (DmColumn dc in dt.Columns)
                {
                    existingColumns.Add(dc);
                }
            }


            for (int i = 0; i < source.Tables.Count; i++)
            {
                MergeTableData(source.Tables[i]); // since column expression might have dependency on relation, we do not set
                //column expression at this point. We need to set it after adding relations
            }

            foreach (DmTable sourceTable in source.Tables)
            {
                DmTable targetTable;
                targetTable = dataSet.Tables.First(s => s.TableName == sourceTable.TableName);

            }

        }

        internal void MergeTable(DmTable src)
        {
            if (src == dataTable) return; //somebody is doing an 'automerge'

            MergeTableData(src);

            DmTable dt = dataTable;
            if (dt == null && dataSet != null)
                dt = dataSet.Tables.First(t => t.TableName == src.TableName);
        }

        void MergeTable(DmTable src, DmTable dst)
        {

            if (src.Rows.Count == 0)
                return;

            DmKey key = default(DmKey);
            try
            {
                foreach (DmRow sourceRow in src.Rows)
                {
                    DmRow targetRow = null;
                    if (dst.Rows.Count > 0 && dst.PrimaryKey != null)
                    {
                        key = GetSrcKey(src, dst);
                        var keyValue = sourceRow.GetKeyValues(key);

                        targetRow = dst.FindByKey(keyValue);

                    }
                    dst.MergeRow(sourceRow, targetRow, preserveChanges);
                }
            }
            finally
            {
            }
        }

        internal void MergeRows(DmRow[] rows)
        {
            DmTable src = null;
            DmTable dst = null;
            DmKey key = default(DmKey);

            for (int i = 0; i < rows.Length; i++)
            {
                DmRow row = rows[i];

                if (row == null)
                    throw new ArgumentNullException("rows[" + i + "]");
                if (row.Table == null)
                    throw new ArgumentNullException("rows[" + i + "].Table");

                //somebody is doing an 'automerge'
                if (row.Table.DmSet == dataSet)
                    continue;

                if (src != row.Table)
                {                     // row.Table changed from prev. row.
                    src = row.Table;
                    dst = MergeSchema(row.Table);
                    if (dst.PrimaryKey != null)
                    {
                        key = GetSrcKey(src, dst);
                    }
                }

                if (row.newRecord == -1 && row.oldRecord == -1)
                    continue;

                DmRow targetRow = null;

                dst.MergeRow(row, targetRow, preserveChanges);

            }

        }

        DmTable MergeSchema(DmTable table)
        {

            if (dataTable == null)
            {
                dataTable = table.Clone();

                if (dataTable.DmSet != null)
                    dataTable.DmSet.Tables.Add(dataTable);
            }
            else
            {
                // Do the columns
                int oldCount = dataTable.Columns.Count;
                for (int i = 0; i < table.Columns.Count; i++)
                {
                    DmColumn src = table.Columns[i];
                    DmColumn dest = (dataTable.Columns.Contains(src.ColumnName, true)) ? dataTable.Columns[src.ColumnName] : null;

                    // If columns doesn't exist, create it
                    if (dest == null)
                    {
                        dest = src.Clone();
                        dataTable.Columns.Add(dest);
                    }
                }

                // check the PrimaryKey
                DmColumn[] targetPKey = dataTable.PrimaryKey.Columns;
                DmColumn[] tablePKey = table.PrimaryKey.Columns;
                if (targetPKey.Length != tablePKey.Length)
                {
                    if (tablePKey.Length != 0)
                        throw new Exception("Merge Failed, keys are differents");

                    // special case when the target table does not have the PrimaryKey
                    if (targetPKey.Length == 0)
                    {
                        DmColumn[] key = new DmColumn[tablePKey.Length];
                        for (int i = 0; i < tablePKey.Length; i++)
                            key[i] = dataTable.Columns[tablePKey[i].ColumnName];

                        dataTable.PrimaryKey = new DmKey(key);
                    }

                }
                else
                {
                    // Check the keys are same
                    for (int i = 0; i < targetPKey.Length; i++)
                    {
                        if (!table.IsEqual(targetPKey[i].ColumnName, tablePKey[i].ColumnName))
                            throw new Exception("Merge Failed, keys are differents");
                    }
                }
            }

            return dataTable;
        }

        void MergeTableData(DmTable src)
        {
            DmTable dest = MergeSchema(src);
            if (dest == null) return;

            MergeTable(src, dest);
        }


        DmKey GetSrcKey(DmTable src, DmTable dst)
        {
            if (src.PrimaryKey != null)
                return src.PrimaryKey;

            DmKey key = default(DmKey);
            if (dst.PrimaryKey != null)
            {
                DmColumn[] dstColumns = dst.PrimaryKey.Columns;
                DmColumn[] srcColumns = new DmColumn[dstColumns.Length];
                for (int j = 0; j < dstColumns.Length; j++)
                {
                    srcColumns[j] = src.Columns[dstColumns[j].ColumnName];
                }

                key = new DmKey(srcColumns); // DmKey will take ownership of srcColumns
            }

            return key;
        }
    }
}
