using Dotmim.Sync.Serialization;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using Dotmim.Sync.Enumerations;

namespace Dotmim.Sync.Data.Surrogate
{
    /// <summary>
    /// Represents a surrogate of a DmTable object, which DotMim Sync uses during custom binary serialization.
    /// </summary>
    [Serializable]
    public class DmTableSurrogate : IDisposable
    {
        /// <summary>
        /// Gets or sets the locale information used to compare strings within the table.
        /// </summary>
        public String CultureInfoName { get; set; }

        /// <summary>Gets or sets the Case sensitive rul of the DmTable that the DmTableSurrogate object represents.</summary>
        public Boolean CaseSensitive { get; set; }

        /// <summary>
        /// Get or Set the schema used for the DmTableSurrogate
        /// </summary>
        public String Schema { get; set; }

        /// <summary>
        /// Gets or sets an array that represents the state of each row in the table.
        /// </summary>
        public int[] RowStates { get; set; }

        /// <summary>
        /// Gets or sets the name of the table that the DmTableSurrogate object represents.
        /// </summary>
        public string TableName { get; set; }

        /// <summary>
        /// Gets an array of DmColumnSurrogate objects that comprise the table that is represented by the DmTableSurrogate object.
        /// </summary>
        public List<DmColumnSurrogate> Columns { get; set; } = new List<DmColumnSurrogate>();

        /// <summary>
        /// Gets an array of DmColumnSurrogate objects that represent the PrimaryKeys.
        /// </summary>
        public List<string> PrimaryKeys { get; set; } = new List<string>();

        /// <summary>
        /// Gets an array of objects that represent the columns and rows of dm in the dmTable.
        /// </summary>
        public Dictionary<int, List<object>> Records { get; set; }


        /// <summary>
        /// Gets or Sets the original provider (SqlServer, MySql, Sqlite, Oracle, PostgreSQL)
        /// </summary>
        public string OriginalProvider { get; set; }
       
        /// <summary>
        /// Gets or Sets the Sync direction (may be Bidirectional, DownloadOnly, UploadOnly) 
        /// Default is Bidirectional
        /// </summary>
        public SyncDirection SyncDirection { get;  set; }

        /// <summary>
        /// Specify a prefix for naming stored procedure. Default is empty string
        /// </summary>
        public String StoredProceduresPrefix { get; set; }

        /// <summary>
        /// Specify a suffix for naming stored procedures. Default is empty string
        /// </summary>
        public String StoredProceduresSuffix { get; set; }
        /// <summary>
        /// Specify a prefix for naming stored procedure. Default is empty string
        /// </summary>
        public String TriggersPrefix { get; set; }

        /// <summary>
        /// Specify a suffix for naming stored procedures. Default is empty string
        /// </summary>
        public String TriggersSuffix { get; set; }

        /// <summary>
        /// Specify a prefix for naming tracking tables. Default is empty string
        /// </summary>
        public String TrackingTablesPrefix { get; set; }

        /// <summary>
        /// Specify a suffix for naming tracking tables. Default is "_tracking"
        /// </summary>
        public String TrackingTablesSuffix { get; set; }



        public long GetEmptyBytesLength()
        {
            long bytesLength = String.IsNullOrEmpty(CultureInfoName) ? 1L : Encoding.UTF8.GetBytes(CultureInfoName).Length;
            bytesLength += 1L; // CasSensitive
            bytesLength += String.IsNullOrEmpty(Schema) ? 1L : Encoding.UTF8.GetBytes(Schema).Length;
            bytesLength += String.IsNullOrEmpty(TableName) ? 1L : Encoding.UTF8.GetBytes(TableName).Length;

            // TODO : Potentially error in bytes length calcul
            bytesLength += Encoding.UTF8.GetBytes(this.GetType().GetAssemblyQualifiedName()).Length; // Type

            return bytesLength;

        }

        /// <summary>
        /// Only used for Serialization
        /// </summary>
        public DmTableSurrogate()
        {

        }

        /// <summary>
        /// Initializes a new instance of the DmTableSurrogate class.
        /// </summary>
        public DmTableSurrogate(DmTable dt)
        {
            if (dt == null)
                throw new ArgumentNullException("dt", "DmTable");

            this.TableName = dt.TableName;
            this.CultureInfoName = dt.Culture.Name;
            this.CaseSensitive = dt.CaseSensitive;
            this.Schema = dt.Schema;
            this.OriginalProvider = dt.OriginalProvider;
            this.SyncDirection = dt.SyncDirection;
            this.TrackingTablesPrefix = dt.TrackingTablesPrefix;
            this.TrackingTablesSuffix = dt.TrackingTablesSuffix;
            this.StoredProceduresPrefix = dt.StoredProceduresPrefix;
            this.StoredProceduresSuffix = dt.StoredProceduresSuffix;
            this.TriggersPrefix = dt.TriggersPrefix;
            this.TriggersSuffix = dt.TriggersSuffix;

            for (int i = 0; i < dt.Columns.Count; i++)
                this.Columns.Add(new DmColumnSurrogate(dt.Columns[i]));

            // Primary Keys
            if (dt.PrimaryKey != null && dt.PrimaryKey.Columns != null && dt.PrimaryKey.Columns.Length > 0)
            {
                for (int i = 0; i < dt.PrimaryKey.Columns.Length; i++)
                    this.PrimaryKeys.Add(dt.PrimaryKey.Columns[i].ColumnName);
            }

            // Fill the rows
            if (dt.Rows.Count <= 0)
                return;

            // the BitArray contains bit values initialized to false. We will use it to store row state
            this.RowStates = new int[dt.Rows.Count];

            // Records in a straightforward object array
            this.Records = new Dictionary<int, List<object>>(dt.Columns.Count);

            for (int j = 0; j < dt.Columns.Count; j++)
                this.Records[j] = new List<object>(dt.Rows.Count);

            for (int k = 0; k < dt.Rows.Count; k++)
            {
                this.RowStates[k] = (int)dt.Rows[k].RowState;
                this.ConvertToSurrogateRecords(dt.Rows[k]);
            }

        }

        /// <summary>
        /// Copies the table schema from a DmTableSurrogate object into a DmTable object.
        /// </summary>
        public void ReadSchemaIntoDmTable(DmTable dt)
        {
            if (dt == null)
                throw new ArgumentNullException("dt", "DmTable");

            dt.TableName = this.TableName;
            dt.Culture = new CultureInfo(this.CultureInfoName);
            dt.Schema = this.Schema;
            dt.CaseSensitive = this.CaseSensitive;
            dt.OriginalProvider = this.OriginalProvider;
            dt.SyncDirection = this.SyncDirection;
            dt.TrackingTablesPrefix = this.TrackingTablesPrefix;
            dt.TrackingTablesSuffix = this.TrackingTablesSuffix;
            dt.StoredProceduresPrefix = this.StoredProceduresPrefix;
            dt.StoredProceduresSuffix = this.StoredProceduresSuffix;

            for (int i = 0; i < this.Columns.Count; i++)
            {
                DmColumn dmColumn = this.Columns[i].ConvertToDmColumn();
                dt.Columns.Add(dmColumn);
            }

            if (this.PrimaryKeys != null && this.PrimaryKeys.Count > 0)
            {
                DmColumn[] keyColumns = new DmColumn[this.PrimaryKeys.Count];

                for (int i = 0; i < this.PrimaryKeys.Count; i++)
                {
                    string columnName = this.PrimaryKeys[i];
                    keyColumns[i] = dt.Columns.First(c => dt.IsEqual(c.ColumnName, columnName));
                }

                DmKey key = new DmKey(keyColumns);

                dt.PrimaryKey = key;
            }
        }


        /// <summary>
        /// Copies the table schema from a DmTableSurrogate object into a DmTable object.
        /// </summary>
        public void ReadDatasIntoDmTable(DmTable dt)
        {
            if (this.Records != null && dt != null && dt.Columns.Count > 0)
            {
                int length = Records[0].Count;
                for (int i = 0; i < length; i++)
                    this.ConvertToDmRow(dt, i);
            }
        }

        private DmRow ConvertToDmRow(DmTable dt, int bitIndex)
        {
            DmRowState rowState = (DmRowState)this.RowStates[bitIndex];
            return this.ConstructRow(dt, rowState, bitIndex);
        }

        /// <summary>
        /// Construct a row from a dmTable, a rowState and the bitIndex
        /// </summary>
        private DmRow ConstructRow(DmTable dt, DmRowState rowState, int bitIndex)
        {
            DmRow dmRow = dt.NewRow();
            int count = dt.Columns.Count;

            dmRow.BeginEdit();

            for (int i = 0; i < count; i++)
            {
                object dmRowObject = this.Records[i][bitIndex];

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
                    if (this.Records[i][bitIndex] != null)
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

        private void ConvertToSurrogateRecords(DmRow row)
        {
            int count = row.Table.Columns.Count;
            DmRowState rowState = row.RowState;
            DmRowVersion rowVersion = rowState == DmRowState.Deleted ? DmRowVersion.Original : DmRowVersion.Current;

            for (int i = 0; i < count; i++)
                this.Records[i].Add(row[i, rowVersion]);
        }


        /// <summary>
        /// Get a row size
        /// </summary>
        public static long GetRowSizeFromDataRow(DmRow row)
        {
            bool isRowDeleted = false;

            if (row.RowState == DmRowState.Deleted)
            {
                row.RejectChanges();
                isRowDeleted = true;
            }

            long byteCount = 0;
            object[] itemArray = row.ItemArray;

            for (int i = 0; i < itemArray.Length; i++)
            {
                // Size for the value
                object obj = itemArray[i];
                Type objType = obj != null ? obj.GetType() : null;


                if (obj == null)
                    byteCount = byteCount + 5;
                else if (obj is DBNull)
                    byteCount = byteCount + 5;
                else if (objType == stringType)
                    byteCount = byteCount + Encoding.UTF8.GetByteCount((string)obj);
                else if (objType == byteArrayType)
                    byteCount = byteCount + ((byte[])obj).Length;
                else
                    byteCount = byteCount + GetSizeForType(obj.GetType());

                // Size for the type
                var typeofobject = row.Table.Columns[i].DataType;
                var byteslengthtype = Encoding.UTF8.GetBytes(DmUtils.GetAssemblyQualifiedName(typeofobject)).Length;
                byteCount += byteslengthtype;

                // State
                byteCount += 4L;

                // Index
                byteCount += 4L;



            }
            if (isRowDeleted)
            {
                row.Delete();
            }
            return byteCount;
        }

  
        private static Type stringType = typeof(string);
        private static Type objectType = typeof(object);
        private static Type byteType = typeof(Byte);
        private static Type byteArrayType = typeof(Byte[]);
        private static Type longType = typeof(long);
        private static Type ulongType = typeof(ulong);
        private static Type doubleType = typeof(double);
        private static Type datetimeType = typeof(DateTime);
        private static Type dbnullType = typeof(DBNull);
        private static Type boolType = typeof(Boolean);
        private static Type sbyteType = typeof(sbyte);
        private static Type charType = typeof(char);
        private static Type shortType = typeof(short);
        private static Type ushortType = typeof(ushort);
        private static Type intType = typeof(int);
        private static Type uintType = typeof(uint);
        private static Type floatType = typeof(float);
        private static Type decimalType = typeof(decimal);
        private static Type guidType = typeof(Guid);

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
        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool cleanup)
        {
            this.Clear();
        }

        public void Clear()
        {
            if (this.Records != null)
            {
                foreach (var d in this.Records)
                    d.Value.Clear();

                this.Records.Clear();
                this.Records = null;
            }
            if (this.PrimaryKeys != null)
            {
                this.PrimaryKeys.Clear();
                this.PrimaryKeys = null;
            }

            if (this.Columns != null)
            {
                this.Columns.Clear();
                this.Columns = null;
            }
        }




    }
}