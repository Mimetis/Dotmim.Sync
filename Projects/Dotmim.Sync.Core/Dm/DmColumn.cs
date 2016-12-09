using System;
using System.Linq;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;
using System.Reflection;

namespace Dotmim.Sync.Data
{

    /// <summary>
    /// Since the DataTable have differents columns of any type, we should have a non typed base class
    /// </summary>
    public abstract class DmColumn
    {
        internal int maxLength = -1;
        internal int ordinal = -1;
        internal Type dataType = null;

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
        }
        public static DmColumn CreateColumn(string columName, Type dataType)
        {
            if (dataType == null)
                throw new ArgumentNullException(nameof(dataType), "type is not defined");

            if (!DmColumn.StorageClassType.ContainsKey(dataType))
                throw new ArgumentException($"This type is not authorized {dataType.FullName}");

            if (dataType == typeof(bool))
                return new DmColumn<bool>(columName);
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



            throw new Exception("this datatype is not supported for DmColumn");
        }
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
        public bool AllowDBNull { get; set; } = true;
        public bool Unique { get; set; } = false;
        public string ColumnName { get; set; }
        public string Prefix { get; set; }
        public bool ReadOnly { get; set; } = false;
        public DmTable Table { get; internal set; }
        public int MaxLength
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
        public int Ordinal => ordinal;
        public bool IsValueType
        {
            get
            {
                if (StorageClassType.ContainsKey(dataType))
                    return StorageClassType[dataType];

                return dataType.GetTypeInfo().IsValueType;
            }
        }
        public Type DataType => dataType;
        internal static bool IsAutoIncrementType(Type dataType) => (dataType == typeof(int) || dataType == typeof(long) || dataType == typeof(short) || dataType == typeof(decimal));

        /// <summary>
        /// Change the ordinal and reorder
        /// </summary>
        public void SetOrdinal(int ordinal)
        {
            if (this.ordinal == -1)
                throw new Exception("Ordinal must be 0 or more");

            // check if we have to move
            if (this.ordinal != ordinal)
                Table.Columns.MoveTo(this, ordinal);
        }

        /// <summary>
        /// Setting the ordinal without reordoring the collection
        /// </summary>
        internal void SetOrdinalInternal(int o)
        {
            if (this.ordinal == o)
                return;

            this.ordinal = o;
        }
        public abstract bool AutoIncrement { get; set; }
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
            dataType = typeof(T);

            if (dataType == null)
                throw new ArgumentNullException(nameof(columnName), "type is not defined");

            if (!StorageClassType.ContainsKey(dataType))
                throw new ArgumentException($"This type is not authorized {dataType.FullName}");
        }

        public override bool AutoIncrement
        {
            get
            {
                return ((null != autoInc) && (autoInc.Auto));
            }
            set
            {
                if (this.AutoIncrement == value)
                    return;

                var canBeAutoIncrement = IsAutoIncrementType(dataType);

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
        internal AutoIncrement<T> AutoInc => (this.autoInc ?? (this.autoInc = new AutoIncrement<T>()));
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
            if (AutoIncrement)
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
            Debug.WriteLine($"Adding a record to key {recordKey} with value {value}");
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

                    if (!AutoIncrement)
                        return;

                    var recordValue = GetValue(record);

                    if (!EqualityComparer<T>.Default.Equals((T)recordValue, default(T)))
                        this.AutoInc.SetCurrentAndIncrement((T)recordValue);
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
            clone.ReadOnly = ReadOnly;
            clone.MaxLength = MaxLength;

            if (this.autoInc != null)
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
                this.Step = (dynamic)1;

            dynamic s = this.Seed;
            if (s == 0)
                this.Seed = (dynamic)1;

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

        internal void SetCurrent(T value)
        {
            this.current = value;
        }

        internal void SetCurrentAndIncrement(T value)
        {
            dynamic aValue = value;
            dynamic aStep = this.step;

            this.current = unchecked(aValue + aStep);
        }

    }

}