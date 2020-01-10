using Dotmim.Sync.Data;
using Dotmim.Sync.Serialization;
using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync
{


    [DataContract(Name = "sc"), Serializable]
    public class SyncColumn : IEquatable<SyncColumn>
    {
        /// <summary>Gets or sets the name of the column</summary>
        [DataMember(Name = "n", IsRequired = true, Order = 1)]
        public string ColumnName { get; set; }

        [DataMember(Name = "dt", IsRequired = false, EmitDefaultValue = false, Order = 2)]
        public string DataType { get; set; }

        [DataMember(Name = "an", IsRequired = false, EmitDefaultValue = false, Order = 3)]
        public bool AllowDBNull { get; set; }

        [DataMember(Name = "iu", IsRequired = false, EmitDefaultValue = false, Order = 4)]
        public bool IsUnique { get; set; }

        [DataMember(Name = "ir", IsRequired = false, EmitDefaultValue = false, Order = 5)]
        public bool IsReadOnly { get; set; }

        [DataMember(Name = "ia", IsRequired = false, EmitDefaultValue = false, Order = 6)]
        public bool IsAutoIncrement { get; set; }

        [DataMember(Name = "seed", IsRequired = false, EmitDefaultValue = false, Order = 7)]
        public int AutoIncrementSeed { get; set; }

        [DataMember(Name = "step", IsRequired = false, EmitDefaultValue = false, Order = 8)]
        public int AutoIncrementStep { get; set; }

        [DataMember(Name = "ius", IsRequired = false, EmitDefaultValue = false, Order = 9)]
        public bool IsUnsigned { get; set; }

        [DataMember(Name = "iuc", IsRequired = false, EmitDefaultValue = false, Order = 10)]
        public bool IsUnicode { get; set; }

        [DataMember(Name = "ico", IsRequired = false, EmitDefaultValue = false, Order = 11)]
        public bool IsCompute { get; set; }

        [DataMember(Name = "ml", IsRequired = false, EmitDefaultValue = false, Order = 12)]
        public Int32 MaxLength { get; set; }

        [DataMember(Name = "o", IsRequired = false, EmitDefaultValue = false, Order = 13)]
        public int Ordinal { get; set; }

        [DataMember(Name = "ps", IsRequired = false, EmitDefaultValue = false, Order = 14)]
        public bool PrecisionSpecified { get; set; }

        [DataMember(Name = "p1", Order = 15)]
        public byte Precision { get; set; }

        [DataMember(Name = "ss", IsRequired = false, EmitDefaultValue = false, Order = 16)]
        public bool ScaleSpecified { get; set; }

        [DataMember(Name = "sc", EmitDefaultValue = false, Order = 17)]
        public byte Scale { get; set; }

        [DataMember(Name = "odb", IsRequired = false, EmitDefaultValue = false, Order = 18)]
        public string OriginalDbType { get; set; }

        [DataMember(Name = "oty", IsRequired = false, EmitDefaultValue = false, Order = 19)]
        public string OriginalTypeName { get; set; }

        [DataMember(Name = "db", IsRequired = false, EmitDefaultValue = false, Order = 20)]
        public int DbType { get; set; }

        [DataMember(Name = "dv", IsRequired = false, EmitDefaultValue = false, Order = 21)]
        public string DefaultValue { get; set; }

        /// <summary>
        /// Ctor for serialization purpose
        /// </summary>
        public SyncColumn() { }

        /// <summary>
        /// Create a new column with the given name
        /// </summary>
        public SyncColumn(string columnName, Type type) : this()
        {
            this.ColumnName = columnName ?? throw new ArgumentNullException(nameof(columnName));
            this.DataType = GetAssemblyQualifiedName(type);
            this.DbType = (int)CoerceDbType();
        }

        /// <summary>
        /// Create a new SchemaColumn of type T
        /// </summary>
        public static SyncColumn Create<T>(string columnName) => new SyncColumn(columnName, typeof(T));


        /// <summary>
        /// Clone a SyncColumn
        /// </summary>
        /// <returns></returns>
        public SyncColumn Clone()
        {
            var clone = new SyncColumn();
            clone.AllowDBNull = this.AllowDBNull;
            clone.AutoIncrementSeed = this.AutoIncrementSeed;
            clone.AutoIncrementStep = this.AutoIncrementStep;
            clone.ColumnName = this.ColumnName;
            clone.DataType = this.DataType;
            clone.DbType = this.DbType;
            clone.IsAutoIncrement = this.IsAutoIncrement;
            clone.IsCompute = this.IsCompute;
            clone.IsReadOnly = this.IsReadOnly;
            clone.IsUnicode = this.IsUnicode;
            clone.IsUnique = this.IsUnique;
            clone.IsUnsigned = this.IsUnsigned;
            clone.MaxLength = this.MaxLength;
            clone.Ordinal = this.Ordinal;
            clone.OriginalDbType = this.OriginalDbType;
            clone.OriginalTypeName = this.OriginalTypeName;
            clone.Precision = this.Precision;
            clone.PrecisionSpecified = this.PrecisionSpecified;
            clone.Scale = this.Scale;
            clone.ScaleSpecified = this.ScaleSpecified;
            clone.DefaultValue = this.DefaultValue;
            return clone;
        }

 
        /// <summary>
        /// Get the DbType in a normal DbType type
        /// </summary>
        public DbType GetDbType() => (DbType)this.DbType;

        /// <summary>
        /// Evaluate DbType, if needed
        /// </summary>
        /// <returns></returns>
        public DbType CoerceDbType()
        {
            // Otherwise fallback on a deduction from DataType
            if (DataType == "1")
                return System.Data.DbType.Boolean;

            if (DataType == "2")
                return System.Data.DbType.Byte;

            if (DataType == "3")
                return System.Data.DbType.StringFixedLength;

            if (DataType == "4")
                return System.Data.DbType.Double;

            if (DataType == "5")
                return System.Data.DbType.Double;

            if (DataType == "6")
                return System.Data.DbType.Int32;

            if (DataType == "7")
                return System.Data.DbType.Int64;

            if (DataType == "8")
                return System.Data.DbType.Int16;

            if (DataType == "9")
                return System.Data.DbType.UInt32;

            if (DataType == "10")
                return System.Data.DbType.UInt64;

            if (DataType == "11")
                return System.Data.DbType.UInt16;

            if (DataType == "12")
                return System.Data.DbType.Binary;

            if (DataType == "13")
                return System.Data.DbType.DateTime;

            if (DataType == "14")
                return System.Data.DbType.DateTimeOffset;

            if (DataType == "15")
                return System.Data.DbType.Decimal;

            if (DataType == "16")
                return System.Data.DbType.Guid;

            if (DataType == "17" && this.MaxLength <= 0)
                return System.Data.DbType.String;

            if (DataType == "17" && this.MaxLength > 0)
                return System.Data.DbType.StringFixedLength;

            if (DataType == "18")
                return System.Data.DbType.SByte;

            if (DataType == "19")
                return System.Data.DbType.Time;

            if (DataType == "20")
                return System.Data.DbType.Binary;

            return System.Data.DbType.Object;
        }

        /// <summary>
        /// Get DataType from compressed string type
        /// </summary>
        /// <returns></returns>
        public Type GetDataType() => GetTypeFromAssemblyQualifiedName(this.DataType);

        /// <summary>
        /// Get DataType from a string value
        /// </summary>
        private static Type GetTypeFromAssemblyQualifiedName(string valueType)
        {
            if (valueType == "1")
                return typeof(bool);
            else if (valueType == "2")
                return typeof(byte);
            else if (valueType == "3")
                return typeof(char);
            else if (valueType == "4")
                return typeof(double);
            else if (valueType == "5")
                return typeof(float);
            else if (valueType == "6")
                return typeof(int);
            else if (valueType == "7")
                return typeof(long);
            else if (valueType == "8")
                return typeof(short);
            else if (valueType == "9")
                return typeof(uint);
            else if (valueType == "10")
                return typeof(ulong);
            else if (valueType == "11")
                return typeof(ushort);
            else if (valueType == "12")
                return typeof(byte[]);
            else if (valueType == "13")
                return typeof(DateTime);
            else if (valueType == "14")
                return typeof(DateTimeOffset);
            else if (valueType == "15")
                return typeof(decimal);
            else if (valueType == "16")
                return typeof(Guid);
            else if (valueType == "17")
                return typeof(string);
            else if (valueType == "18")
                return typeof(sbyte);
            else if (valueType == "19")
                return typeof(TimeSpan);
            else if (valueType == "20")
                return typeof(char[]);

            return Type.GetType(valueType, true, true);

        }

        /// <summary>
        /// Compress string representation of the DataType to be more concise in the serialized stream
        /// </summary>
        private static string GetAssemblyQualifiedName(Type valueType)
        {
            if (valueType == typeof(bool))
                return "1";
            else if (valueType == typeof(byte))
                return "2";
            else if (valueType == typeof(char))
                return "3";
            else if (valueType == typeof(double))
                return "4";
            else if (valueType == typeof(float))
                return "5";
            else if (valueType == typeof(int))
                return "6";
            else if (valueType == typeof(long))
                return "7";
            else if (valueType == typeof(short))
                return "8";
            else if (valueType == typeof(uint))
                return "9";
            else if (valueType == typeof(ulong))
                return "10";
            else if (valueType == typeof(ushort))
                return "11";
            else if (valueType == typeof(byte[]))
                return "12";
            else if (valueType == typeof(DateTime))
                return "13";
            else if (valueType == typeof(DateTimeOffset))
                return "14";
            else if (valueType == typeof(decimal))
                return "15";
            else if (valueType == typeof(Guid))
                return "16";
            else if (valueType == typeof(string))
                return "17";
            else if (valueType == typeof(sbyte))
                return "18";
            else if (valueType == typeof(TimeSpan))
                return "19";
            else if (valueType == typeof(char[]))
                return "20";

            return valueType.AssemblyQualifiedName;

        }

        /// <summary>
        /// Get auto inc values, coercing Step
        /// </summary>
        public (int Seed, int Step) GetAutoIncrementSeedAndStep()
        {
            var seed = this.AutoIncrementSeed;
            var step = this.AutoIncrementStep <= 0 ? 1 : this.AutoIncrementStep;

            return (seed, step);
        }

        /// <summary>
        /// Gets or Sets the default value
        /// </summary>
        public dynamic GetDefaultValue()
        {
            var type = this.GetDataType();

            if (type == typeof(string))
                return null;

            return this.IsValueType() ? Activator.CreateInstance(type) : null;

        }

        /// <summary>
        /// Gets if the column type is a value type
        /// </summary>
        public bool IsValueType()
        {
            var type = this.GetDataType();

            if (StorageClassType.ContainsKey(type))
                return StorageClassType[type];

            return type.GetTypeInfo().IsValueType;
        }


        public override string ToString()
        {
            return string.Format($"{this.ColumnName} - {this.GetDataType().Name}");
        }

        public override bool Equals(object obj)
        {
            return this.Equals(obj as SyncColumn);
        }

        public bool Equals(SyncColumn other)
        {
            if (other == null)
                return false;

            var sc = SyncGlobalization.DataSourceStringComparison;

            return this.ColumnName.Equals(other.ColumnName, sc);
        }

        public override int GetHashCode()
        {
            return -1862699260 + EqualityComparer<string>.Default.GetHashCode(this.ColumnName);
        }


        public static bool operator ==(SyncColumn left, SyncColumn right)
        {
            return EqualityComparer<SyncColumn>.Default.Equals(left, right);
        }

        public static bool operator !=(SyncColumn left, SyncColumn right)
        {
            return !(left == right);
        }

        /// <summary>
        ///  Collection of autorized types
        ///  each type is marked as Value type or not
        /// </summary>
        internal static readonly Dictionary<Type, bool> StorageClassType = new Dictionary<Type, bool>();

        static SyncColumn()
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

    }


}
