using Dotmim.Sync.Serialization;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync.Data
{
    [DataContract(Name = "ds")]
    public class DmSetLight
    {
        /// <summary>Gets or sets the name of the DmSet that the DmSetSurrogate object represents.</summary>
        [DataMember(Name = "n")]
        public string DmSetName { get; set; }

        /// <summary>Gets or sets the Case sensitive rul of the DmSet that the DmSetSurrogate object represents.</summary>
        [DataMember(Name = "c")]
        public bool CaseSensitive { get; set; }

        /// <summary>
        /// List of tables
        /// </summary>
        [DataMember(Name = "t")]
        public IList<DmTableLight> Tables { get; set; } = new List<DmTableLight>();

        /// <summary>
        /// Get Table by name
        /// </summary>
        public DmTableLight GetTable(string name, string schema)
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentNullException("name");

            if (string.IsNullOrEmpty(schema))
                schema = string.Empty;

            var sc = this.CaseSensitive? StringComparison.InvariantCulture: StringComparison.InvariantCultureIgnoreCase;

            return Tables.FirstOrDefault(c => string.Equals(c.TableName, name, sc) && string.Equals(c.Schema, schema, sc));

        }

        public bool HasTables => this.Tables?.Count > 0;

        public DmSetLight()
        {

        }

        public DmSetLight(string name, bool caseSensitive = false)
        {
            this.DmSetName = name;
            this.CaseSensitive = caseSensitive;
        }


        public DmSetLight(DmSet ds)
        {
            this.DmSetName = ds.DmSetName;
            this.CaseSensitive = ds.CaseSensitive;

            foreach (var dt in ds.Tables)
            {
                if (dt.Rows != null && dt.Rows.Count > 0)
                {
                    var tbl = new DmTableLight(dt)
                    {
                        TableName = dt.TableName,
                        Schema = dt.Schema
                    };

                    this.Tables.Add(tbl);
                }
            }
        }

        public void WriteToDmSet(DmSet set)
        {
            for (int i = 0; i < this.Tables.Count; i++)
            {
                var dmTableLight = this.Tables[i];

                var dmTable = set.Tables[dmTableLight.TableName, dmTableLight.Schema];

                if (dmTable == null)
                    continue;

                dmTableLight.WriteToDmTable(dmTable);
            }
        }

        public void Clear()
        {
            foreach (var t in Tables)
                t.Clear();
        }

    }



    [DataContract(Name = "dt")]
    public class DmTableLight
    {
        public DmTableLight()
        {

        }

        public void ImportRow(DmRow row)
        {
            // array with one more space for state
            var itemArray = new object[row.Table.Columns.Count + 1];

            // save row state on position 0
            itemArray[0] = (int)row.RowState;

            var rowVersion = row.RowState == DmRowState.Deleted ? DmRowVersion.Original : DmRowVersion.Current;

            for (int i = 0; i < row.Table.Columns.Count; i++)
            {
                var val = row[i, rowVersion];
                itemArray[i + 1] = val;
            }

            this.Rows.Add(itemArray);

        }

        public DmTableLight(DmTable dt)
        {
            this.TableName = dt.TableName;
            this.Schema = dt.Schema;

            foreach (var row in dt.Rows)
                this.ImportRow(row);
        }

        /// <summary>
        /// Gets or sets the name of the table that the DmTableSurrogate object represents.
        /// </summary>
        [DataMember(Name = "n")]
        public string TableName { get; set; }

        /// <summary>
        /// Get or Set the schema used for the DmTableSurrogate
        /// </summary>
        [DataMember(Name = "s")]
        public string Schema { get; set; }

        /// <summary>
        /// List of rows
        /// </summary>
        [DataMember(Name = "r")]
        public List<object[]> Rows { get; set; } = new List<object[]>();


        /// <summary>
        /// Import all rows in a DataTable
        /// </summary>
        public void WriteToDmTable(DmTable dt)
        {
            foreach (var row in this.Rows)
                this.ConstructRow(dt, row);

        }


        /// <summary>
        /// Create a DmRow from an array based on the schema from the Dmtable argument
        /// </summary>
        private DmRow ConstructRow(DmTable dt, object[] row)
        {
            var dmRow = dt.NewRow();
            int count = dt.Columns.Count;

            var rowState = (DmRowState)Convert.ToInt32(row[0]);

            dmRow.BeginEdit();
            for (int i = 0; i < count; i++)
            {
                object dmRowObject = row[i + 1];

                // Sometimes, a serializer could potentially serialize type into string
                // For example JSON.Net will serialize GUID into STRING
                // So we try to deserialize in correct type
                if (dmRowObject != null)
                {
                    var columnType = dt.Columns[i].DataType;
                    var dmRowObjectType = dmRowObject.GetType();

                    if (dmRowObjectType != columnType && columnType != typeof(object))
                    {
                        if (columnType == typeof(Guid) && (dmRowObject as string) != null)
                            dmRowObject = new Guid(dmRowObject.ToString());
                        if (columnType == typeof(Guid) && (dmRowObject.GetType() == typeof(byte[])))
                            dmRowObject = new Guid((byte[])dmRowObject);
                        else if (columnType == typeof(Int32) && dmRowObjectType != typeof(Int32))
                            dmRowObject = Convert.ToInt32(dmRowObject);
                        else if (columnType == typeof(UInt32) && dmRowObjectType != typeof(UInt32))
                            dmRowObject = Convert.ToUInt32(dmRowObject);
                        else if (columnType == typeof(Int16) && dmRowObjectType != typeof(Int16))
                            dmRowObject = Convert.ToInt16(dmRowObject);
                        else if (columnType == typeof(UInt16) && dmRowObjectType != typeof(UInt16))
                            dmRowObject = Convert.ToUInt16(dmRowObject);
                        else if (columnType == typeof(Int64) && dmRowObjectType != typeof(Int64))
                            dmRowObject = Convert.ToInt64(dmRowObject);
                        else if (columnType == typeof(UInt64) && dmRowObjectType != typeof(UInt64))
                            dmRowObject = Convert.ToUInt64(dmRowObject);
                        else if (columnType == typeof(Byte) && dmRowObjectType != typeof(Byte))
                            dmRowObject = Convert.ToByte(dmRowObject);
                        else if (columnType == typeof(Char) && dmRowObjectType != typeof(Char))
                            dmRowObject = Convert.ToChar(dmRowObject);
                        else if (columnType == typeof(DateTime) && dmRowObjectType != typeof(DateTime))
                            dmRowObject = Convert.ToDateTime(dmRowObject);
                        else if (columnType == typeof(Decimal) && dmRowObjectType != typeof(Decimal))
                            dmRowObject = Convert.ToDecimal(dmRowObject);
                        else if (columnType == typeof(Double) && dmRowObjectType != typeof(Double))
                            dmRowObject = Convert.ToDouble(dmRowObject);
                        else if (columnType == typeof(SByte) && dmRowObjectType != typeof(SByte))
                            dmRowObject = Convert.ToSByte(dmRowObject);
                        else if (columnType == typeof(Single) && dmRowObjectType != typeof(Single))
                            dmRowObject = Convert.ToSingle(dmRowObject);
                        else if (columnType == typeof(String) && dmRowObjectType != typeof(String))
                            dmRowObject = Convert.ToString(dmRowObject);
                        else if (columnType == typeof(Boolean) && dmRowObjectType != typeof(Boolean))
                            dmRowObject = Convert.ToBoolean(dmRowObject);
                        else if (columnType == typeof(Byte[]) && dmRowObjectType != typeof(Byte[]) && dmRowObjectType == typeof(String))
                            dmRowObject = Convert.FromBase64String(dmRowObject.ToString());
                        else if (dmRowObjectType != columnType)
                        {
                            var t = dmRowObject.GetType();
                            var converter = columnType.GetConverter();
                            if (converter != null && converter.CanConvertFrom(t))
                                dmRowObject = converter.ConvertFrom(dmRowObject);
                        }
                    }
                }

                if (rowState == DmRowState.Deleted)
                {
                    // Since some columns might be not null (and we have null because the row is deleted)
                    if (row[i] != null)
                        dmRow[i] = dmRowObject;
                }
                else
                {
                    dmRow[i] = dmRowObject;
                }

            }

            dt.Rows.Add(dmRow);

            switch (rowState)
            {
                case DmRowState.Unchanged:
                    {
                        dmRow.AcceptChanges();
                        dmRow.EndEdit();
                        return dmRow;
                    }
                case DmRowState.Added:
                    {
                        dmRow.EndEdit();
                        return dmRow;
                    }
                case DmRowState.Deleted:
                    {
                        dmRow.AcceptChanges();
                        dmRow.Delete();
                        dmRow.EndEdit();
                        return dmRow;

                    }
                case DmRowState.Modified:
                    {
                        dmRow.AcceptChanges();
                        dmRow.SetModified();
                        dmRow.EndEdit();
                        return dmRow;
                    }
                default:
                    throw new ArgumentException("InvalidRowState");
            }
        }

        public void Clear() => Rows.Clear();

    }


}
