using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Reflection;

namespace Dotmim.Sync.Data
{

    /// <summary>
    /// Since the DataTable have differents columns of any type, we should have a non typed base class
    /// </summary>
    public abstract class DmColumn
    {
        internal int maxLength = -1;
        internal DbType dbType;
        internal bool dbTypeAllowed;
        byte precision;
        byte scale;

        /// <summary>
        ///  Collection of autorized types
        ///  each type is marked as Value type or not
        /// </summary>
        internal static readonly Dictionary<Type, bool> StorageClassType = new Dictionary<Type, bool>();

        static DmColumn()
        {
            StorageClassType.Add(typeof(bool), true);
            StorageClassType.Add(typeof(char), true);
            StorageClassType.Add(typeof(sbyte), true);
            StorageClassType.Add(typeof(byte), true);
            StorageClassType.Add(typeof(short), true);
            StorageClassType.Add(typeof(ushort), true);
            StorageClassType.Add(typeof(int), true);
            StorageClassType.Add(typeof(uint), true);
            StorageClassType.Add(typeof(long), true);
            StorageClassType.Add(typeof(ulong), true);
            StorageClassType.Add(typeof(float), true);
            StorageClassType.Add(typeof(double), true);
            StorageClassType.Add(typeof(decimal), true);
            StorageClassType.Add(typeof(DateTime), true);
            StorageClassType.Add(typeof(TimeSpan), true);
            StorageClassType.Add(typeof(DateTimeOffset), true);
            StorageClassType.Add(typeof(Guid), true);

            StorageClassType.Add(typeof(string), true);

            // Not a value type but authorized
            StorageClassType.Add(typeof(byte[]), false);
            StorageClassType.Add(typeof(char[]), false);

            // test to add object type
            StorageClassType.Add(typeof(object), false);

        }

        /// <summary>
        /// Create a DmColumn with specified type
        /// </summary>
        public static DmColumn CreateColumn(string columName, Type dataType)
        {
            if (dataType == null)
                throw new ArgumentNullException(nameof(dataType), "type is not defined");

            if (!StorageClassType.ContainsKey(dataType))
                throw new ArgumentException($"This type is not authorized {dataType.FullName}");

            if (dataType == typeof(bool))
                return new DmColumn<bool>(columName);
            if (dataType == typeof(byte))
                return new DmColumn<byte>(columName);
            if (dataType == typeof(char))
                return new DmColumn<char>(columName);
            if (dataType == typeof(sbyte))
                return new DmColumn<sbyte>(columName);
            if (dataType == typeof(short))
                return new DmColumn<short>(columName);
            if (dataType == typeof(ushort))
                return new DmColumn<ushort>(columName);
            if (dataType == typeof(int))
                return new DmColumn<int>(columName);
            if (dataType == typeof(uint))
                return new DmColumn<uint>(columName);
            if (dataType == typeof(long))
                return new DmColumn<long>(columName);
            if (dataType == typeof(ulong))
                return new DmColumn<ulong>(columName);
            if (dataType == typeof(float))
                return new DmColumn<float>(columName);
            if (dataType == typeof(double))
                return new DmColumn<double>(columName);
            if (dataType == typeof(decimal))
                return new DmColumn<decimal>(columName);
            if (dataType == typeof(DateTime))
                return new DmColumn<DateTime>(columName);
            if (dataType == typeof(TimeSpan))
                return new DmColumn<TimeSpan>(columName);
            if (dataType == typeof(DateTimeOffset))
                return new DmColumn<DateTimeOffset>(columName);
            if (dataType == typeof(string))
                return new DmColumn<string>(columName);
            if (dataType == typeof(Guid))
                return new DmColumn<Guid>(columName);
            if (dataType == typeof(byte[]))
                return new DmColumn<byte[]>(columName);
            if (dataType == typeof(char[]))
                return new DmColumn<char[]>(columName);
            if (dataType == typeof(Object))
                return new DmColumn<Object>(columName);

            throw new Exception("this datatype is not supported for DmColumn");
        }

        /// <summary>
        /// Gets or Sets the default value
        /// </summary>
        public dynamic DefaultValue
        {
            get
            {
                var type = this.DataType;

                if (type == typeof(string))
                    return null;

                return this.IsValueType ? Activator.CreateInstance(type) : null;
            }
        }

        /// <summary>
        /// Gets or Sets if the column allow null values
        /// </summary>
        public bool AllowDBNull { get; set; } = true;

        /// <summary>
        /// Optional string indicating the orginal ADO.NET Db type from the database involved
        /// </summary>
        public string OriginalDbType { get; set; }

        /// <summary>
        /// Gets or sets the column datastore type name
        /// </summary>
        public string OriginalTypeName { get; set; }

        /// <summary>
        /// Gets or sets if the column is unsigned
        /// </summary>
        public bool IsUnsigned { get; set; }

        /// <summary>
        /// Gets or sets if the column is unicode 
        /// </summary>
        public bool IsUnicode { get; set; }

        /// <summary>
        /// Gets or sets if the column is a computed column
        /// </summary>
        public bool IsCompute { get; set; }

        /// <summary>
        /// Returns the Column Type
        /// </summary>
        public Type DataType { get; internal set; }

        /// <summary>
        /// Gets or sets the DbType associated
        /// </summary>
        public DbType DbType
        {
            get
            {
                // Should be fix by the user so just return it
                if (this.dbTypeAllowed)
                    return this.dbType;

                if (DataType == typeof(bool))
                    return DbType.Boolean;

                if (DataType == typeof(char))
                    return DbType.StringFixedLength;

                if (DataType == typeof(sbyte))
                    return DbType.Byte;

                if (DataType == typeof(short))
                    return DbType.Int16;

                if (DataType == typeof(ushort))
                    return DbType.UInt16;

                if (DataType == typeof(int))
                    return DbType.Int32;

                if (DataType == typeof(uint))
                    return DbType.UInt32;

                if (DataType == typeof(long))
                    return DbType.Int64;

                if (DataType == typeof(ulong))
                    return DbType.UInt64;

                if (DataType == typeof(float))
                    return DbType.Double;

                if (DataType == typeof(double))
                    return DbType.Double;

                if (DataType == typeof(decimal))
                    return DbType.Decimal;

                if (DataType == typeof(DateTime))
                    return DbType.DateTime;

                if (DataType == typeof(TimeSpan))
                    return DbType.Time;

                if (DataType == typeof(DateTimeOffset))
                    return DbType.DateTimeOffset;

                if (DataType == typeof(string) && this.MaxLength > 0)
                    return DbType.StringFixedLength;

                if (DataType == typeof(string) && this.MaxLength <= 0)
                    return DbType.String;

                if (DataType == typeof(Guid))
                    return DbType.Guid;

                if (DataType == typeof(byte[]))
                    return DbType.Binary;

                if (DataType == typeof(char[]))
                    return DbType.Binary;

                return DbType.Object;
            }
            set
            {
                this.dbType = value;
                this.dbTypeAllowed = value != DbType.Object;
            }
        }

        /// <summary>
        /// Gets or sets if the values for this column are unique
        /// </summary>
        public bool IsUnique { get; set; } = false;

        /// <summary>
        /// Gets or Sets the column name
        /// </summary>
        public string ColumnName { get; set; }

        /// <summary>
        /// Gets or Sets if the column is readonly
        /// </summary>
        public bool IsReadOnly { get; set; } = false;

        /// <summary>
        /// Gets the table associated with this column
        /// </summary>
        public DmTable Table { get; internal set; }

        /// <summary>
        /// Gets or Sets the columnd max length
        /// </summary>
        public Int32 MaxLength
        {
            get
            {
                return maxLength;
            }
            set
            {
                if (maxLength != value)
                    maxLength = Math.Max(value, -1);
            }
        }

        /// <summary>
        /// Gets the column position (use SetOrdinal method to set the position)
        /// </summary>
        public int Ordinal { get; internal set; } = -1;

        /// <summary>
        /// Gets if the column type is a value type
        /// </summary>
        public bool IsValueType
        {
            get
            {
                if (StorageClassType.ContainsKey(DataType))
                    return StorageClassType[DataType];

                return DataType.GetTypeInfo().IsValueType;
            }
        }

        /// <summary>
        /// Gets if the column could be auto increment
        /// </summary>
        internal static bool IsAutoIncrementType(Type dataType) =>
            (dataType == typeof(byte) || dataType == typeof(int) || dataType == typeof(long) || dataType == typeof(short) || dataType == typeof(decimal));

        /// <summary>
        /// Change the ordinal and reorder
        /// </summary>
        public void SetOrdinal(int ordinal)
        {
            if (ordinal == -1)
                throw new Exception("Ordinal must be 0 or more");

            // check if we have to move
            if (this.Ordinal != ordinal)
            {
                if (Table != null)
                    Table.Columns.MoveTo(this, ordinal);
                else
                    this.Ordinal = ordinal;
            }
        }

        /// <summary>
        /// Setting the ordinal without reordoring the collection
        /// </summary>
        internal void SetOrdinalInternal(int o)
        {
            if (this.Ordinal == o)
                return;

            this.Ordinal = o;
        }

        /// <summary>
        /// Gets or sets if column is auto increment
        /// </summary>
        public abstract bool IsAutoIncrement { get; set; }

        /// <summary>
        /// Gets or Sets if precision is specified
        /// </summary>
        public bool PrecisionSpecified { get; set; }

        /// <summary>
        /// Gets or Sets if scale is specified
        /// </summary>
        public bool ScaleSpecified { get; set; }


        /// <summary>
        /// For numeric value, we can specify the precision value
        /// </summary>
        public Byte Precision
        {
            get
            {
                return precision;
            }
            set
            {
                PrecisionSpecified = value > 0;
                precision = value;
            }
        }

        /// <summary>
        /// For numeric value, we can specify the scale value
        /// </summary>
        public Byte Scale
        {
            get
            {
                return scale;
            }
            set
            {
                ScaleSpecified = value > 0;
                scale = value;
            }
        }

        internal abstract void Init(int record);
        internal abstract object this[int record] { get; set; }
        internal abstract void SetTable(DmTable table);
        internal abstract DmColumn Clone();
        internal abstract int Compare(int record1, int record2);
        internal abstract int CompareValueTo(int record1, object value);
        internal abstract void Copy(int srcRecordNo, int dstRecordNo);
        internal abstract void RemoveRecord(int record);
        internal abstract void Clear();
        internal abstract bool IsNull(int recordKey);
        public abstract (int Step, int Seed) GetAutoIncrementSeedAndStep();
        public abstract void SetAutoIncrementSeedAndStep(int step, int seed);
    }

    /// <summary>
    /// Typed DataColumn
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class DmColumn<T> : DmColumn
    {
        internal T defaultValue;


        AutoIncrement<T> autoInc;
        Dictionary<long, object> storage = new Dictionary<long, object>();

        public DmColumn(string columnName)
        {
            ColumnName = columnName ?? string.Empty;
            DataType = typeof(T);

            if (DataType == null)
                throw new ArgumentNullException(nameof(columnName), "type is not defined");

            if (!StorageClassType.ContainsKey(DataType))
                throw new ArgumentException($"This type is not authorized {DataType.FullName}");
        }

        public override bool IsAutoIncrement
        {
            get
            {
                return (null != autoInc) && autoInc.Auto;
            }
            set
            {
                if (this.IsAutoIncrement == value)
                    return;

                var canBeAutoIncrement = IsAutoIncrementType(DataType);

                if (!canBeAutoIncrement)
                    throw new ArgumentException($"This column can't be an Auto Increment column, due to its type not supported");

                this.AutoInc.Auto = canBeAutoIncrement;
            }
        }
        internal T AutoIncrementCurrent
        {
            get { return ((null != this.autoInc) ? this.autoInc.Current : this.AutoIncrementSeed); }
            set
            {
                this.AutoInc.SetCurrent(value);
            }
        }
        internal AutoIncrement<T> AutoInc =>
            (this.autoInc ?? (this.autoInc = new AutoIncrement<T>()));

        public T AutoIncrementSeed
        {
            get
            {
                return ((this.autoInc != null) ? this.autoInc.Seed : default(T));
            }
            set
            {
                if (!EqualityComparer<T>.Default.Equals(this.autoInc.Seed, value))
                    this.AutoInc.Seed = value;
            }
        }
        public T AutoIncrementStep
        {
            get
            {
                if (this.autoInc != null)
                    return this.autoInc.Step;

                dynamic s = default(T);
                return s + 1;
            }
            set
            {
                if (!EqualityComparer<T>.Default.Equals(this.autoInc.Step, value))
                    this.AutoInc.Step = value;
            }
        }

        public override void SetAutoIncrementSeedAndStep(int step, int seed)
        {
            dynamic seed1 = default(T);
            seed1 += seed;
            dynamic step1 = default(T);
            step1 += step;

            this.AutoIncrementSeed = seed1;
            this.AutoIncrementStep = step1;
        }
        public override (int Step, int Seed) GetAutoIncrementSeedAndStep()
        {
            int step = Convert.ToInt32(AutoIncrementStep);
            var seed = Convert.ToInt32(AutoIncrementSeed);

            return (step, seed);

        }
        public new T DefaultValue
        {
            get
            {
                return defaultValue;
            }
            set
            {
                defaultValue = value;
            }
        }
        internal override void SetTable(DmTable table)
        {
            if (this.Table == table)
                return;

            this.Table = table;
            this.storage.Clear();
            this.storage = null;

            if (table != null)
                this.storage = new Dictionary<long, object>();

        }
        internal override bool IsNull(int recordKey)
        {
            var v = GetValue(recordKey);

            return v == null;
        }
        internal override void Clear()
        {
            this.storage.Clear();
        }

        /// <summary>
        /// Remove a value from the internal dictionary with the recordKey
        /// </summary>
        internal override void RemoveRecord(int record)
        {
            if (storage.ContainsKey(record))
                storage.Remove(record);
        }

        /// <summary>
        /// Create a new entry in the dictionary with a new key 
        /// </summary>
        internal override void Init(int record)
        {
            if (IsAutoIncrement)
            {
                T value = this.autoInc.Current;
                this.autoInc.MoveAfter();
                Set(record, value);
            }
            else
                Set(record, null);

        }

        /// <summary>
        /// Get Value. Could be null
        /// </summary>
        public object GetValue(int recordKey)
        {
            if (storage.ContainsKey(recordKey))
                return storage[recordKey];

            if (this.AllowDBNull)
                return null;

            return default(T);
        }

        void Set(int recordKey, object value)
        {

            //if (value != null)
            //{
            //    var columnType = this.DataType;
            //    var valueType = value.GetType();

            //    if (valueType != columnType)
            //    {
            //        if (columnType == typeof(Guid) && (value as string) != null)
            //            value = new Guid(value.ToString());
            //        else if (columnType == typeof(Int32) && valueType != typeof(Int32))
            //            value = Convert.ToInt32(value);
            //        else if (columnType == typeof(UInt32) && valueType != typeof(UInt32))
            //            value = Convert.ToUInt32(value);
            //        else if (columnType == typeof(Int16) && valueType != typeof(Int16))
            //            value = Convert.ToInt16(value);
            //        else if (columnType == typeof(UInt16) && valueType != typeof(UInt16))
            //            value = Convert.ToUInt16(value);
            //        else if (columnType == typeof(Int64) && valueType != typeof(Int64))
            //            value = Convert.ToInt64(value);
            //        else if (columnType == typeof(UInt64) && valueType != typeof(UInt64))
            //            value = Convert.ToUInt64(value);
            //        else if (columnType == typeof(Byte) && valueType != typeof(Byte))
            //            value = Convert.ToByte(value);
            //        else if (columnType == typeof(Char) && valueType != typeof(Char))
            //            value = Convert.ToChar(value);
            //        else if (columnType == typeof(DateTime) && valueType != typeof(DateTime))
            //            value = Convert.ToDateTime(value);
            //        else if (columnType == typeof(Decimal) && valueType != typeof(Decimal))
            //            value = Convert.ToDecimal(value);
            //        else if (columnType == typeof(Double) && valueType != typeof(Double))
            //            value = Convert.ToDouble(value);
            //        else if (columnType == typeof(SByte) && valueType != typeof(SByte))
            //            value = Convert.ToSByte(value);
            //        else if (columnType == typeof(Single) && valueType != typeof(Single))
            //            value = Convert.ToSingle(value);
            //        else if (columnType == typeof(String) && valueType != typeof(String))
            //            value = Convert.ToString(value);
            //        else if (columnType == typeof(Boolean) && valueType != typeof(Boolean))
            //            value = Convert.ToBoolean(value);
            //        else if (valueType != columnType)
            //        {
            //            var t = value.GetType();
            //            var converter = columnType.GetConverter();
            //            if (converter.CanConvertFrom(t))
            //                value = converter.ConvertFrom(value);
            //        }
            //    }
            //}


            if (storage.ContainsKey(recordKey))
                storage[recordKey] = value;
            else
                storage.Add(recordKey, value);
        }

        /// <devdoc>
        /// This is how data is pushed in and out of the column.
        /// </devdoc>
        internal override object this[int record]
        {
            get
            {
                return GetValue(record);
            }
            set
            {
                try
                {
                    Set(record, value);

                    if (!IsAutoIncrement)
                        return;

                    var recordValue = GetValue(record);

                    this.AutoInc.SetCurrentAndIncrement(recordValue);

                    //if (!EqualityComparer<T>.Default.Equals((T)recordValue, default(T)))
                    //    this.AutoInc.SetCurrentAndIncrement((T)recordValue);
                }
                catch (Exception e)
                {
                    throw new Exception($"Error trying to add a value {value} in internal DataColumn storage :  {e.Message}");
                }


            }
        }
        internal override DmColumn Clone()
        {
            return CloneTyped();
        }
        internal DmColumn<T> CloneTyped()
        {
            DmColumn<T> clone = new DmColumn<T>("");
            clone.AllowDBNull = AllowDBNull;
            clone.ColumnName = ColumnName;
            clone.DefaultValue = DefaultValue;
            clone.IsReadOnly = IsReadOnly;
            clone.MaxLength = MaxLength;

            clone.dbTypeAllowed = dbTypeAllowed;
            if (clone.dbTypeAllowed)
                clone.DbType = DbType;

            clone.OriginalDbType = OriginalDbType;
            clone.Precision = Precision;
            clone.PrecisionSpecified = PrecisionSpecified;
            clone.Scale = Scale;
            clone.ScaleSpecified = ScaleSpecified;
            clone.IsUnique = IsUnique;
            clone.IsCompute = IsCompute;
            clone.IsUnicode = IsUnicode;
            clone.IsUnsigned = IsUnsigned;
            clone.OriginalTypeName = OriginalTypeName;

            if (this.IsAutoIncrement && this.autoInc != null)
                clone.autoInc = this.autoInc.Clone();

            return clone;
        }

        internal override int Compare(int record1, int record2)
        {
            var o1 = GetValue(record1);
            var o2 = GetValue(record2);

            if (o1 == null && o2 == null)
                return 0;

            if (o1 == null)
                return -1;

            if (o2 == null)
                return 1;

            T v1 = (T)o1;
            T v2 = (T)o2;

            if (typeof(T) == typeof(string))
                return this.Table.Culture.CompareInfo.Compare((string)o1, (string)o2, this.Table.compareFlags);


            return Comparer<T>.Default.Compare(v1, v2);
        }
        internal override int CompareValueTo(int record1, object value)
        {
            var o1 = GetValue(record1);
            if (o1 == null)
                return -1;

            T v1 = (T)o1;
            T v2 = (T)value;

            if (typeof(T) == typeof(string))
            {
                return this.Table.Culture.CompareInfo.Compare((string)o1, (string)value,
                             this.Table.compareFlags);
            }

            return Comparer<T>.Default.Compare(v1, v2);
        }
        internal override void Copy(int srcRecordNo, int dstRecordNo)
        {
            var v1 = GetValue(srcRecordNo);
            Set(dstRecordNo, v1);
        }
        public override string ToString() => this.ColumnName;


    }

    class AutoIncrement
    {
        bool auto;
        internal Type dataType;
        internal virtual Type DataType => dataType;
        internal bool CanBeIncremented => ((DataType == typeof(Int32)) || (DataType == typeof(Int64)) || (DataType == typeof(Int16)) || (DataType == typeof(Decimal)));

        internal bool Auto
        {
            get { return this.auto; }
            set { this.auto = value; }
        }
    }

    /// <summary>
    /// the auto stepped value with Int64 representation
    /// </summary>
    sealed class AutoIncrement<T> : AutoIncrement
    {

        T current;
        T seed;
        T step;

        /// <summary>Gets and sets the current auto incremented value to use</summary>
        internal T Current
        {
            get { return this.current; }
            set { this.current = value; }
        }

        public AutoIncrement()
        {
            this.dataType = typeof(T);
            dynamic c = this.Step;
            if (c == 0)
                this.Step = (T)Increment(this.Step);

            dynamic s = this.Seed;
            if (s == 0)
                this.Seed = (T)Increment(this.Seed);

        }
        private Object Increment(object val)
        {
            if (val.GetType() == typeof(Byte))
                return (byte)(Convert.ToByte(val) + 1);
            if (val.GetType() == typeof(Int32))
                return (Int32)(Convert.ToInt32(val) + 1);
            if (val.GetType() == typeof(Int16))
                return (Int16)(Convert.ToInt16(val) + 1);
            if (val.GetType() == typeof(Int64))
                return (Int64)(Convert.ToInt64(val) + 1);
            if (val.GetType() == typeof(Decimal))
                return (Decimal)(Convert.ToDecimal(val) + 1);

            throw new Exception($"Type {val.GetType()} is not supported");
        }

        internal override Type DataType
        {
            get
            {
                return typeof(T);
            }
        }


        internal AutoIncrement<T> Clone()
        {
            AutoIncrement<T> clone = new AutoIncrement<T>();
            clone.Auto = this.Auto;
            clone.Seed = this.Seed;
            clone.Step = this.Step;
            clone.Current = this.Current;
            return clone;
        }

        /// <summary>Get and sets the initial seed value.</summary>
        internal T Seed
        {
            get { return this.seed; }
            set
            {
                if (EqualityComparer<T>.Default.Equals(this.current, this.seed))
                    this.current = value;

                this.seed = value;
            }
        }

        /// <summary>Get and sets the stepping value.</summary>
        /// <exception cref="ArugmentException">if value is 0</exception>
        internal T Step
        {
            get { return this.step; }
            set
            {
                if (EqualityComparer<T>.Default.Equals(default(T), value))
                    throw new ArgumentNullException("cant be 0");

                if (!EqualityComparer<T>.Default.Equals(this.step, value))
                {
                    if (!EqualityComparer<T>.Default.Equals(this.current, this.Seed))
                    {
                        dynamic aCurrent = this.current;
                        dynamic aStep = this.step;
                        dynamic aValue = value;
                        this.current = unchecked(aCurrent - aStep + aValue);
                    }

                    this.step = value;
                }
            }
        }

        internal void MoveAfter()
        {
            dynamic aCurrent = this.current;
            dynamic aStep = this.step;

            this.current = unchecked(aCurrent + aStep);
        }

        internal void SetCurrent(object value)
        {
            this.current = (T)Parse(value);
        }

        internal object Parse(object val)
        {
            var columnType = typeof(T);
            var valType = val.GetType();

            if (valType == columnType)
                return val;

            if (columnType == typeof(Int32) && valType != typeof(Int32))
                return Convert.ToInt32(val);
            else if (columnType == typeof(UInt32) && valType != typeof(UInt32))
                return Convert.ToUInt32(val);
            else if (columnType == typeof(Int16) && valType != typeof(Int16))
                return Convert.ToInt16(val);
            else if (columnType == typeof(UInt16) && valType != typeof(UInt16))
                return Convert.ToUInt16(val);
            else if (columnType == typeof(Int64) && valType != typeof(Int64))
                return Convert.ToInt64(val);
            else if (columnType == typeof(UInt64) && valType != typeof(UInt64))
                return Convert.ToUInt64(val);
            else if (columnType == typeof(Byte) && valType != typeof(Byte))
                return Convert.ToByte(val);

            return val;
        }

        internal void SetCurrentAndIncrement(object value)
        {
            dynamic aValue = Parse(value);
            dynamic aStep = this.step;

            this.current = unchecked(aValue + aStep);
        }

    }

}