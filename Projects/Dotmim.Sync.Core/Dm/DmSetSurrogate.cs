using Dotmim.Sync.Data;

using System;
using System.Collections.Generic;
using System.Globalization;

namespace Dotmim.Sync.Data.Surrogate
{
    /// <summary>
    /// Represents a surrogate of a DmSet object, which DotMim Sync uses during custom binary serialization.
    /// </summary>
    [Serializable]
    public class DmSetSurrogate : IDisposable
    {

        /// <summary>Gets or sets the name of the DmSet that the DmSetSurrogate object represents.</summary>
        public string DmSetName { get; set; }

        /// <summary>Gets or sets the locale information used to compare strings within the table.</summary>
        public string CultureInfoName { get; set; }

        /// <summary>Gets or sets the Case sensitive rul of the DmSet that the DmSetSurrogate object represents.</summary>
        public bool CaseSensitive { get; set; }

        /// <summary>
        /// Gets or Sets an array of DmTableSurrogate objects that comprise 
        /// the dm set that is represented by the DmSetSurrogate object.
        /// </summary>
        public List<DmTableSurrogate> Tables { get; set; }

        /// <summary>
        /// Gets or Sets an array of every DmRelationSurrogate belong to this DmSet
        /// </summary>
        public List<DmRelationSurrogate> Relations { get; set; }

        /// <summary>
        /// Initializes a new instance of the DmSetSurrogate class from an existing DmSet
        /// </summary>
        public DmSetSurrogate(DmSet ds)
        {
            if (ds == null)
                throw new ArgumentNullException("ds");


            this.DmSetName = ds.DmSetName;
            this.CultureInfoName = ds.Culture.Name;
            this.CaseSensitive = ds.CaseSensitive;

            this.Tables = new List<DmTableSurrogate>(ds.Tables.Count);

            for (int i = 0; i < ds.Tables.Count; i++)
                this.Tables.Add(new DmTableSurrogate(ds.Tables[i]));

            if (ds.Relations != null && ds.Relations.Count > 0)
            {
                this.Relations = new List<DmRelationSurrogate>(ds.Relations.Count);

                for (int i = 0; i < ds.Relations.Count; i++)
                {
                    DmRelation dr = ds.Relations[i];
                    DmRelationSurrogate drs = new DmRelationSurrogate();

                    drs.ChildKeySurrogates = new DmColumnSurrogate[dr.ChildKey.Columns.Length];
                    for (int keyIndex = 0; keyIndex < dr.ChildKey.Columns.Length; keyIndex++)
                        drs.ChildKeySurrogates[keyIndex] = new DmColumnSurrogate(dr.ChildKey.Columns[keyIndex]);

                    drs.ParentKeySurrogates = new DmColumnSurrogate[dr.ParentKey.Columns.Length];
                    for (int keyIndex = 0; keyIndex < dr.ParentKey.Columns.Length; keyIndex++)
                        drs.ParentKeySurrogates[keyIndex] = new DmColumnSurrogate(dr.ParentKey.Columns[keyIndex]);

                    drs.RelationName = dr.RelationName;

                    this.Relations.Add(drs);
                }
            }
        }

        /// <summary>
        /// Only used for Serialization
        /// </summary>
        public DmSetSurrogate()
        {
            //this.DmTableSurrogates = new List<DmTableSurrogate>();
            //this.DmRelationSurrogates = new List<DmRelationSurrogate>();
        }

        /// <summary>
        /// Constructs a DmSet object based on a DmSetSurrogate object.
        /// </summary>
        public DmSet ConvertToDmSet()
        {
            DmSet dmSet = new DmSet()
            {
                Culture = new CultureInfo(this.CultureInfoName),
                CaseSensitive = this.CaseSensitive,
                DmSetName = this.DmSetName
            };
            this.ReadSchemaIntoDmSet(dmSet);
            this.ReadDataIntoDmSet(dmSet);
            return dmSet;
        }

        /// <summary>
        /// Clone the originla DmSet and copy datas from the DmSetSurrogate
        /// </summary>
        public DmSet ConvertToDmSet(DmSet originalDmSet)
        {
            DmSet dmSet = originalDmSet.Clone();
            this.ReadDataIntoDmSet(dmSet);
            return dmSet;
        }

        internal void ReadDataIntoDmSet(DmSet ds)
        {
            for (int i = 0; i < ds.Tables.Count; i++)
            {
                DmTableSurrogate dmTableSurrogate = this.Tables[i];
                dmTableSurrogate.ReadDatasIntoDmTable(ds.Tables[i]);
            }
        }

        internal void ReadSchemaIntoDmSet(DmSet ds)
        {
            var dmTableSurrogateArray = this.Tables;
            for (int i = 0; i < dmTableSurrogateArray.Count; i++)
            {
                var dmTableSurrogate = dmTableSurrogateArray[i];
                var dmTable = new DmTable();
                dmTableSurrogate.ReadSchemaIntoDmTable(dmTable);

                dmTable.Culture = new CultureInfo(dmTableSurrogate.CultureInfoName);
                dmTable.CaseSensitive = dmTableSurrogate.CaseSensitive;
                dmTable.TableName = dmTableSurrogate.TableName;

                ds.Tables.Add(dmTable);
            }

            if (this.Relations != null && this.Relations.Count > 0)
            {
                foreach(var dmRelationSurrogate in this.Relations)
                {
                    DmColumn[] parentColumns = new DmColumn[dmRelationSurrogate.ParentKeySurrogates.Length];
                    DmColumn[] childColumns = new DmColumn[dmRelationSurrogate.ChildKeySurrogates.Length];

                    for (int i = 0; i < parentColumns.Length; i++)
                    {
                        var columnName = dmRelationSurrogate.ParentKeySurrogates[i].ColumnName;
                        var tableName = dmRelationSurrogate.ParentKeySurrogates[i].TableName;

                        parentColumns[i]  = ds.Tables[tableName].Columns[columnName];

                        columnName = dmRelationSurrogate.ChildKeySurrogates[i].ColumnName;
                        tableName = dmRelationSurrogate.ChildKeySurrogates[i].TableName;

                        childColumns[i] = ds.Tables[tableName].Columns[columnName];

                    }

                    DmRelation relation = new DmRelation(dmRelationSurrogate.RelationName, parentColumns, childColumns);
                    ds.Relations.Add(relation);
                }
            }
        }

        public void Clear()
        {
            if (this.Relations != null)
            {
                this.Relations.Clear();
                this.Relations = null;
            }

            if (this.Tables != null)
            {
                foreach(var tbl in this.Tables)
                    tbl.Clear();

                this.Tables.Clear();
                this.Tables = null;
            }
        }
        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool cleanup)
        {
            this.Clear();
        }
    }
}