using Dotmim.Sync.Builders;

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
    public class ContainerTable : IEquatable<ContainerTable>
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

        /// <summary>
        /// Check if we have rows in this container table
        /// </summary>
        public bool HasRows => this.Rows.Count > 0;

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

        public override bool Equals(object obj)
        {
            return this.Equals(obj as ContainerTable);
        }

        public bool Equals(ContainerTable other)
        {
            if (other == null)
                return false;

            var sc = SyncGlobalization.DataSourceStringComparison;

            var sn = this.SchemaName == null ? string.Empty : this.SchemaName;
            var otherSn = other.SchemaName == null ? string.Empty : other.SchemaName;

            return other != null &&
                   this.TableName.Equals(other.TableName, sc) &&
                   sn.Equals(otherSn, sc);
        }

        public override int GetHashCode()
        {
            var hashCode = 1627045777;
            hashCode = hashCode * -1521134295 + EqualityComparer<string>.Default.GetHashCode(this.TableName);
            hashCode = hashCode * -1521134295 + EqualityComparer<string>.Default.GetHashCode(this.SchemaName);
            return hashCode;
        }

        public static bool operator ==(ContainerTable left, ContainerTable right)
        {
            return EqualityComparer<ContainerTable>.Default.Equals(left, right);
        }

        public static bool operator !=(ContainerTable left, ContainerTable right)
        {
            return !(left == right);
        }
    }

}
