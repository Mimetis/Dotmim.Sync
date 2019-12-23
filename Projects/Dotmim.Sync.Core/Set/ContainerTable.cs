using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using Dotmim.Sync.Serialization;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync
{
    [DataContract(Name = "ct"), Serializable]
    public class ContainerTable
    {
        /// <summary>
        /// Gets or sets the name of the table that the DmTableSurrogate object represents.
        /// </summary>
        [DataMember(Name = "n", IsRequired = true, Order = 1)]
        public string TableName { get; set; }

        /// <summary>
        /// Get or Set the schema used for the DmTableSurrogate
        /// </summary>
        [DataMember(Name = "s", IsRequired = false, EmitDefaultValue = false, Order = 2)]
        public string SchemaName { get; set; }

        /// <summary>
        /// List of rows
        /// </summary>
        [DataMember(Name = "r", IsRequired = false, Order = 3)]
        public List<object[]> Rows { get; set; }

        public ContainerTable()
        {

        }

        public ContainerTable(SyncTable table)
        {
            this.TableName = table.TableName;
            this.SchemaName = table.SchemaName;
        }

        public ContainerTable(string tableName, string schemaName = null, List<object[]> rows = null)
        {
            this.TableName = tableName;
            this.SchemaName = schemaName;

            if (rows != null)
                this.Rows = rows;
        }


        //public ContainerTable(DmTable dt)
        //{
        //    this.TableName = dt.TableName;
        //    this.SchemaName = dt.Schema;

        //    foreach (DmRow row in dt.Rows)
        //        this.ImportRow(row);
        //}

        public ContainerTable(DataTable dt)
        {
            this.TableName = dt.TableName;
            this.SchemaName = dt.Namespace;

            foreach (DataRow row in dt.Rows)
                this.ImportRow(row);
        }

        /// <summary>
        /// Check if we have rows in this container table
        /// </summary>
        public bool HasRows => this.Rows.Count > 0;

        public void ImportRow(DataRow row)
        {
            // array with one more space for state
            var itemArray = new object[row.Table.Columns.Count + 1];

            // save row state on position 0
            itemArray[0] = (int)row.RowState;

            var rowVersion = row.RowState == DataRowState.Deleted ? DataRowVersion.Original : DataRowVersion.Current;

            for (int i = 0; i < row.Table.Columns.Count; i++)
            {
                var val = row[i, rowVersion];
                itemArray[i + 1] = val;
            }

            this.Rows.Add(itemArray);

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

        /// <summary>
        /// Import all rows in a DataTable
        /// </summary>
        public void WriteToDmTable(DmTable dt)
        {
            foreach (var row in this.Rows)
                this.ConstructRow(dt, row);

        }

        public void WriteToDataTable(DataTable dt)
        {
            foreach (var row in this.Rows)
            {
                var dmRow = dt.NewRow();
                int count = dt.Columns.Count;
                var rowState = (DataRowState)Convert.ToInt32(row[0]);
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

                    if (rowState == DataRowState.Deleted)
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
                    case DataRowState.Unchanged:
                        {
                            dmRow.AcceptChanges();
                            dmRow.EndEdit();
                            break;
                        }
                    case DataRowState.Added:
                        {
                            dmRow.EndEdit();
                            break;
                        }
                    case DataRowState.Deleted:
                        {
                            dmRow.AcceptChanges();
                            dmRow.Delete();
                            dmRow.EndEdit();
                            break;

                        }
                    case DataRowState.Modified:
                        {
                            dmRow.AcceptChanges();
                            dmRow.SetModified();
                            dmRow.EndEdit();
                            break;
                        }
                    default:
                        throw new ArgumentException("InvalidRowState");
                }
            }
        }

        /// <summary>
        /// Create a DmRow from an array based on the schema from the Dmtable argument
        /// </summary>
        private DmRow ConstructRow(DmTable dt, object[] row)
        {
            var dmRow = dt.NewRow();
            int count = dt.Columns.Count;
            var orderedColumns = dt.Columns.OrderBy(c => c.Ordinal).ToArray();

            var dataRowState = (DataRowState)Convert.ToInt32(row[0], dt.Culture);

            var rowState = DmRowState.Unchanged;
            if (dataRowState == DataRowState.Deleted)
                rowState = DmRowState.Deleted;
            else if (dataRowState == DataRowState.Added)
                rowState = DmRowState.Added;
            else if (dataRowState == DataRowState.Modified)
                rowState = DmRowState.Modified;
            else if (dataRowState == DataRowState.Unchanged)
                rowState = DmRowState.Unchanged;


            dmRow.BeginEdit();
            for (int i = 0; i < count; i++)
            {
                object dmRowObject = row[i + 1];

                // Sometimes, a serializer could potentially serialize type into string
                // For example JSON.Net will serialize GUID into STRING
                // So we try to deserialize in correct type
                if (dmRowObject != null)
                {
                    var columnType = orderedColumns[i].DataType;
                    var dmRowObjectType = dmRowObject.GetType();

                    if (dmRowObjectType != columnType && columnType != typeof(object))
                    {
                        if (columnType == typeof(Guid) && (dmRowObject as string) != null)
                            dmRowObject = new Guid(dmRowObject.ToString());
                        if (columnType == typeof(Guid) && (dmRowObject.GetType() == typeof(byte[])))
                            dmRowObject = new Guid((byte[])dmRowObject);
                        else if (columnType == typeof(Int32) && dmRowObjectType != typeof(Int32))
                            dmRowObject = Convert.ToInt32(dmRowObject, dt.Culture);
                        else if (columnType == typeof(UInt32) && dmRowObjectType != typeof(UInt32))
                            dmRowObject = Convert.ToUInt32(dmRowObject, dt.Culture);
                        else if (columnType == typeof(Int16) && dmRowObjectType != typeof(Int16))
                            dmRowObject = Convert.ToInt16(dmRowObject, dt.Culture);
                        else if (columnType == typeof(UInt16) && dmRowObjectType != typeof(UInt16))
                            dmRowObject = Convert.ToUInt16(dmRowObject, dt.Culture);
                        else if (columnType == typeof(Int64) && dmRowObjectType != typeof(Int64))
                            dmRowObject = Convert.ToInt64(dmRowObject, dt.Culture);
                        else if (columnType == typeof(UInt64) && dmRowObjectType != typeof(UInt64))
                            dmRowObject = Convert.ToUInt64(dmRowObject, dt.Culture);
                        else if (columnType == typeof(Byte) && dmRowObjectType != typeof(Byte))
                            dmRowObject = Convert.ToByte(dmRowObject, dt.Culture);
                        else if (columnType == typeof(Char) && dmRowObjectType != typeof(Char))
                            dmRowObject = Convert.ToChar(dmRowObject, dt.Culture);
                        else if (columnType == typeof(DateTime) && dmRowObjectType != typeof(DateTime))
                            dmRowObject = Convert.ToDateTime(dmRowObject, dt.Culture);
                        else if (columnType == typeof(Decimal) && dmRowObjectType != typeof(Decimal))
                            dmRowObject = Convert.ToDecimal(dmRowObject, dt.Culture);
                        else if (columnType == typeof(Double) && dmRowObjectType != typeof(Double))
                            dmRowObject = Convert.ToDouble(dmRowObject, dt.Culture);
                        else if (columnType == typeof(SByte) && dmRowObjectType != typeof(SByte))
                            dmRowObject = Convert.ToSByte(dmRowObject, dt.Culture);
                        else if (columnType == typeof(Single) && dmRowObjectType != typeof(Single))
                            dmRowObject = Convert.ToSingle(dmRowObject, dt.Culture);
                        else if (columnType == typeof(String) && dmRowObjectType != typeof(String))
                            dmRowObject = Convert.ToString(dmRowObject, dt.Culture);
                        else if (columnType == typeof(Boolean) && dmRowObjectType != typeof(Boolean))
                            dmRowObject = Convert.ToBoolean(dmRowObject, dt.Culture);
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

 
        /// <summary>
        /// Calculate an estimation of the dictionary values size
        /// </summary>
        public static long GetRowSizeFromDataRow(object[] itemArray)
        {
            long byteCount = 0;

            foreach (var obj in itemArray)
            {
                var objType = obj?.GetType();

                if (obj == null)
                    byteCount += 5;
                else if (obj is DBNull)
                    byteCount += 5;
                else if (objType == stringType)
                    byteCount += Encoding.UTF8.GetByteCount((string)obj);
                else if (objType == byteArrayType)
                    byteCount += ((byte[])obj).Length;
                else
                    byteCount += GetSizeForType(obj.GetType());

                // Size for the type
                if (objType != null)
                    byteCount += Encoding.UTF8.GetBytes(DmUtils.GetAssemblyQualifiedName(objType)).Length;

                // State
                byteCount += 4L;

                // Index
                byteCount += 4L;

            }
            return byteCount;
        }

        private static readonly Type stringType = typeof(string);
        private static readonly Type objectType = typeof(object);
        private static readonly Type byteType = typeof(Byte);
        private static readonly Type byteArrayType = typeof(Byte[]);
        private static readonly Type longType = typeof(long);
        private static readonly Type ulongType = typeof(ulong);
        private static readonly Type doubleType = typeof(double);
        private static readonly Type datetimeType = typeof(DateTime);
        private static readonly Type dbnullType = typeof(DBNull);
        private static readonly Type boolType = typeof(Boolean);
        private static readonly Type sbyteType = typeof(sbyte);
        private static readonly Type charType = typeof(char);
        private static readonly Type shortType = typeof(short);
        private static readonly Type ushortType = typeof(ushort);
        private static readonly Type intType = typeof(int);
        private static readonly Type uintType = typeof(uint);
        private static readonly Type floatType = typeof(float);
        private static readonly Type decimalType = typeof(decimal);
        private static readonly Type guidType = typeof(Guid);

        /// <summary>
        /// Gets a size for a given type
        /// </summary>
        public static long GetSizeForType(Type type)
        {

            if (type == objectType || type == longType || type == ulongType ||
                type == doubleType || type == datetimeType)
                return 8L;

            if (type == dbnullType)
                return 0L;

            if (type == boolType || type == sbyteType || type == byteType)
                return 1L;

            if (type == charType || type == shortType || type == ushortType)
                return 2L;

            if (type == intType || type == uintType || type == floatType)
                return 4L;

            if (type == decimalType || type == guidType)
                return 16L;

            return 0L;

        }



    }


    /// <summary>
    /// Extensions methods for ContainerTable
    /// </summary>
    public static class ContainerTableExtensions
    {
        public static ContainerTable FirstOrDefault(this Collection<ContainerTable> containerTables, string tableName, string schemaName, SyncSet schema)
        {
            return containerTables.FirstOrDefault(ct =>
            {
                return schema.StringEquals(tableName, schemaName, ct.TableName, ct.SchemaName);
            });
        }

        public static ContainerTable FirstOrDefault(this Collection<ContainerTable> containerTables, ContainerTable containerTable, SyncSet schema)
        {
            return containerTables.FirstOrDefault(ct =>
            {
                return schema.StringEquals(containerTable.TableName, containerTable.SchemaName, ct.TableName, ct.SchemaName);
            });
        }

        public static ContainerTable FirstOrDefault(this Collection<ContainerTable> containerTables, SyncTable schemaTable)
        {
            return containerTables.FirstOrDefault(ct =>
            {
                return schemaTable.Schema.StringEquals(schemaTable.TableName, schemaTable.SchemaName, ct.TableName, ct.SchemaName);
            });
        }

        public static ContainerTable FirstOrDefault(this Collection<ContainerTable> containerTables, string tableName, SyncSet schema)
        {
            return containerTables.FirstOrDefault(ct =>
            {
                return schema.StringEquals(ParserName.Parse(tableName).ObjectName, ParserName.Parse(tableName).SchemaName, ct.TableName, ct.SchemaName);
            });
        }
    }

}
