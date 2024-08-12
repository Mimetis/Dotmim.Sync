using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Reflection;
using System.Runtime.Serialization;

namespace Dotmim.Sync
{

    /// <summary>
    /// Represents a column in a table.
    /// </summary>
    [DataContract(Name = "sc"), Serializable]
    public class SyncColumn : SyncNamedItem<SyncColumn>
    {
        /// <summary>
        /// Gets or Sets the column name.
        /// </summary>
        [DataMember(Name = "n", IsRequired = true, Order = 1)]
        public string ColumnName { get; set; }

        /// <summary>
        /// Gets or Sets the column data type.
        /// </summary>
        [DataMember(Name = "dt", IsRequired = false, EmitDefaultValue = false, Order = 2)]
        public string DataType { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether gets or Sets if the column allows null values.
        /// </summary>
        [DataMember(Name = "an", IsRequired = false, Order = 3)]
        public bool AllowDBNull { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether gets or Sets if the column is unique.
        /// </summary>
        [DataMember(Name = "iu", IsRequired = false, Order = 4)]
        public bool IsUnique { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether gets or Sets if the column is read only.
        /// </summary>
        [DataMember(Name = "ir", IsRequired = false, Order = 5)]
        public bool IsReadOnly { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether gets or Sets if the column is auto increment.
        /// </summary>
        [DataMember(Name = "ia", IsRequired = false, Order = 6)]
        public bool IsAutoIncrement { get; set; }

        /// <summary>
        /// Gets or Sets the seed for the auto increment.
        /// </summary>
        [DataMember(Name = "seed", IsRequired = false, Order = 7)]
        public long AutoIncrementSeed { get; set; }

        /// <summary>
        /// Gets or Sets the step for the auto increment.
        /// </summary>
        [DataMember(Name = "step", IsRequired = false, Order = 8)]
        public long AutoIncrementStep { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether gets or Sets if the column is unsigned.
        /// </summary>
        [DataMember(Name = "ius", IsRequired = false, Order = 9)]
        public bool IsUnsigned { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether gets or Sets if the column is unicode.
        /// </summary>
        [DataMember(Name = "iuc", IsRequired = false, Order = 10)]
        public bool IsUnicode { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether gets or sets indicating if the column is a computed.
        /// </summary>
        [DataMember(Name = "ico", IsRequired = false, Order = 11)]
        public bool IsCompute { get; set; }

        /// <summary>
        /// Gets or Sets the max length for the column.
        /// </summary>
        [DataMember(Name = "ml", IsRequired = false, Order = 12)]
        public int MaxLength { get; set; }

        /// <summary>
        /// Gets or Sets the column ordinal.
        /// </summary>
        [DataMember(Name = "o", IsRequired = false, Order = 13)]
        public int Ordinal { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether gets or Sets if the precision for the column is specified.
        /// </summary>
        [DataMember(Name = "ps", IsRequired = false, Order = 14)]
        public bool PrecisionIsSpecified { get; set; }

        /// <summary>
        /// Gets or Sets the precision for the column.
        /// </summary>
        [DataMember(Name = "p1", IsRequired = false, Order = 15)]
        public byte Precision { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether gets or Sets if the scale for the column is specified.
        /// </summary>
        [DataMember(Name = "ss", IsRequired = false, Order = 16)]
        public bool ScaleIsSpecified { get; set; }

        /// <summary>
        /// Gets or Sets the scale for the column.
        /// </summary>
        [DataMember(Name = "sc", IsRequired = false, Order = 17)]
        public byte Scale { get; set; }

        /// <summary>
        /// Gets or Sets the original db type.
        /// </summary>
        [DataMember(Name = "odb", IsRequired = false, EmitDefaultValue = false, Order = 18)]
        public string OriginalDbType { get; set; }

        /// <summary>
        /// Gets or Sets the original type name.
        /// </summary>
        [DataMember(Name = "oty", IsRequired = false, EmitDefaultValue = false, Order = 19)]
        public string OriginalTypeName { get; set; }

        /// <summary>
        /// Gets or Sets the db type.
        /// </summary>
        [DataMember(Name = "db", IsRequired = false, Order = 20)]
        public int DbType { get; set; }

        /// <summary>
        /// Gets or Sets the default value.
        /// </summary>
        [DataMember(Name = "dv", IsRequired = false, EmitDefaultValue = false, Order = 21)]
        public string DefaultValue { get; set; }

        /// <summary>
        /// Gets or Sets the extra property 1.
        /// </summary>
        [DataMember(Name = "ext1", IsRequired = false, EmitDefaultValue = false, Order = 22)]
        public string ExtraProperty1 { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncColumn"/> class.
        /// Ctor for serialization purpose.
        /// </summary>
        public SyncColumn() => this.DataType = "-1";

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncColumn"/> class.
        /// Create a new column with the given name.
        /// </summary>
        public SyncColumn(string columnName)
            : this() => this.ColumnName = columnName ?? throw new ArgumentNullException(nameof(columnName));

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncColumn"/> class.
        /// Create a new column with the given name and given type.
        /// </summary>
        public SyncColumn(string columnName, Type type)
            : this(columnName) => this.SetType(type);

        /// <summary>
        /// Create a new SchemaColumn of type T.
        /// </summary>
        public static SyncColumn Create<T>(string columnName) => new(columnName, typeof(T));

        /// <summary>
        /// Compress string representation of the DataType to be more concise in the serialized stream.
        /// </summary>
        public static string GetAssemblyQualifiedName(Type valueType)
        {
            Guard.ThrowIfNull(valueType);

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
            else if (valueType == typeof(object))
                return "-1";

            return valueType.AssemblyQualifiedName;
        }

        /// <summary>
        /// Set the SyncColumn Type (if the type was not set with the correct ctor).
        /// </summary>
        public void SetType(Type type)
        {
            this.DataType = type == null ? "-1" : GetAssemblyQualifiedName(type);
            this.DbType = (int)this.CoerceDbType();
        }

        /// <summary>
        /// Clone a SyncColumn.
        /// </summary>
        public SyncColumn Clone()
        {
            var clone = new SyncColumn
            {
                AllowDBNull = this.AllowDBNull,
                AutoIncrementSeed = this.AutoIncrementSeed,
                AutoIncrementStep = this.AutoIncrementStep,
                ColumnName = this.ColumnName,
                DataType = this.DataType,
                DbType = this.DbType,
                IsAutoIncrement = this.IsAutoIncrement,
                IsCompute = this.IsCompute,
                IsReadOnly = this.IsReadOnly,
                IsUnicode = this.IsUnicode,
                IsUnique = this.IsUnique,
                IsUnsigned = this.IsUnsigned,
                MaxLength = this.MaxLength,
                Ordinal = this.Ordinal,
                OriginalDbType = this.OriginalDbType,
                OriginalTypeName = this.OriginalTypeName,
                Precision = this.Precision,
                PrecisionIsSpecified = this.PrecisionIsSpecified,
                Scale = this.Scale,
                ScaleIsSpecified = this.ScaleIsSpecified,
                DefaultValue = this.DefaultValue,
                ExtraProperty1 = this.ExtraProperty1,
            };
            return clone;
        }

        /// <summary>
        /// Get the DbType in a normal DbType type.
        /// </summary>
        public DbType GetDbType() => (DbType)this.DbType;

        /// <summary>
        /// Evaluate DbType, if needed.
        /// </summary>
        public DbType CoerceDbType()
        {
            // Otherwise fallback on a deduction from DataType
            if (this.DataType == "1")
                return System.Data.DbType.Boolean;

            if (this.DataType == "2")
                return System.Data.DbType.Byte;

            if (this.DataType == "3")
                return System.Data.DbType.StringFixedLength;

            if (this.DataType == "4")
                return System.Data.DbType.Double;

            if (this.DataType == "5")
                return System.Data.DbType.Single;

            if (this.DataType == "6")
                return System.Data.DbType.Int32;

            if (this.DataType == "7")
                return System.Data.DbType.Int64;

            if (this.DataType == "8")
                return System.Data.DbType.Int16;

            if (this.DataType == "9")
                return System.Data.DbType.UInt32;

            if (this.DataType == "10")
                return System.Data.DbType.UInt64;

            if (this.DataType == "11")
                return System.Data.DbType.UInt16;

            if (this.DataType == "12")
                return System.Data.DbType.Binary;

            if (this.DataType == "13" && this.DbType == 26)
                return System.Data.DbType.DateTime2;

            if (this.DataType == "13")
                return System.Data.DbType.DateTime;

            if (this.DataType == "14")
                return System.Data.DbType.DateTimeOffset;

            if (this.DataType == "15")
                return System.Data.DbType.Decimal;

            if (this.DataType == "16")
                return System.Data.DbType.Guid;

            if (this.DataType == "17")
                return System.Data.DbType.String;

            // if (DataType == "17" && this.MaxLength > 0)
            //    return System.Data.DbType.StringFixedLength;
            if (this.DataType == "18")
                return System.Data.DbType.SByte;

            if (this.DataType == "19")
                return System.Data.DbType.Time;

            if (this.DataType == "20")
                return System.Data.DbType.Binary;

            return System.Data.DbType.Object;
        }

        /// <summary>
        /// Get DataType from compressed string type.
        /// </summary>
        public Type GetDataType() => GetTypeFromAssemblyQualifiedName(this.DataType);

        /// <summary>
        /// Get auto inc values, coercing Step.
        /// </summary>
        public (long Seed, long Step) GetAutoIncrementSeedAndStep()
        {
            var seed = this.AutoIncrementSeed;
            var step = this.AutoIncrementStep <= 0 ? 1 : this.AutoIncrementStep;

            return (seed, step);
        }

        /// <summary>
        /// Gets or Sets the default value.
        /// </summary>
        public dynamic GetDefaultValue()
        {
            var type = this.GetDataType();

            if (type == typeof(string))
                return null;

            return this.IsValueType() ? Activator.CreateInstance(type) : null;
        }

        /// <summary>
        /// Gets if the column type is a value type.
        /// </summary>
        public bool IsValueType()
        {
            var type = this.GetDataType();

            if (StorageClassType.TryGetValue(type, out var storageClassType))
                return storageClassType;

            return type.GetTypeInfo().IsValueType;
        }

        /// <summary>
        /// Returns a string that represents the current object.
        /// </summary>
        public override string ToString() => string.Format(CultureInfo.InvariantCulture, $"{this.ColumnName} - {this.GetDataType().Name}");

        /// <inheritdoc cref="SyncNamedItem{T}.GetAllNamesProperties"/>
        public override IEnumerable<string> GetAllNamesProperties()
        {
            yield return this.ColumnName;
        }

        /// <inheritdoc cref="SyncNamedItem{T}.EqualsByProperties(T)"/>
        public override bool EqualsByProperties(SyncColumn otherInstance)
        {
            if (otherInstance == null)
                return false;

            if (!this.EqualsByName(otherInstance))
                return false;

            var sc = SyncGlobalization.DataSourceStringComparison;

            return
               string.Equals(this.DataType, otherInstance.DataType, sc) &&
               this.AllowDBNull == otherInstance.AllowDBNull &&
               this.IsUnique == otherInstance.IsUnique &&
               this.IsReadOnly == otherInstance.IsReadOnly &&
               this.IsAutoIncrement == otherInstance.IsAutoIncrement &&
               this.AutoIncrementSeed == otherInstance.AutoIncrementSeed &&
               this.AutoIncrementStep == otherInstance.AutoIncrementStep &&
               this.IsUnsigned == otherInstance.IsUnsigned &&
               this.IsUnicode == otherInstance.IsUnicode &&
               this.IsCompute == otherInstance.IsCompute &&
               this.MaxLength == otherInstance.MaxLength &&
               this.Ordinal == otherInstance.Ordinal &&
               this.PrecisionIsSpecified == otherInstance.PrecisionIsSpecified &&
               this.Precision == otherInstance.Precision &&
               this.ScaleIsSpecified == otherInstance.ScaleIsSpecified &&
               this.Scale == otherInstance.Scale &&
               string.Equals(this.OriginalDbType, otherInstance.OriginalDbType, sc) &&
               string.Equals(this.OriginalTypeName, otherInstance.OriginalTypeName, sc) &&
               this.DbType == otherInstance.DbType &&
               string.Equals(this.DefaultValue, otherInstance.DefaultValue, sc) &&
               string.Equals(this.ExtraProperty1, otherInstance.ExtraProperty1, sc);
        }

        /// <summary>
        ///  Collection of autorized types
        ///  each type is marked as Value type or not.
        /// </summary>
        internal static readonly Dictionary<Type, bool> StorageClassType = [];

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

        /// <summary>
        /// Get DataType from a string value.
        /// </summary>
        internal static Type GetTypeFromAssemblyQualifiedName(string valueType)
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
            else if (valueType == "-1")
                return typeof(object);

            return Type.GetType(valueType, true, true);
        }
    }
}