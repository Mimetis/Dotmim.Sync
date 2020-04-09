using System;
using System.Collections.Generic;
using System.Data;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync.Setup
{
    /// <summary>
    /// Represents a filter parameters
    /// For example : @CustomerID int NULL = 12
    /// </summary>
    [DataContract(Name = "sfp"), Serializable]
    public class SetupFilterParameter : IEquatable<SetupFilterParameter>
    {
        /// <summary>
        /// Gets or sets the name of the parameter.
        /// for SQL, will be named @{ParamterName}
        /// for MySql, will be named in_{ParameterName}
        /// </summary>
        [DataMember(Name = "n", IsRequired = true, Order = 1)]
        public string Name { get; set; }

        /// <summary>
        /// Gets of Sets the table name if parameter is a column 
        /// </summary>
        [DataMember(Name = "tn", IsRequired = true, Order = 2)]
        public string TableName { get; set; }


        /// <summary>
        /// Gets of Sets the table schema name if parameter is a column 
        /// </summary>
        [DataMember(Name = "sn", IsRequired = true, Order = 3)]
        public string SchemaName { get; set; }


        /// <summary>
        /// Gets or Sets the parameter db type
        /// </summary>
        [DataMember(Name = "dt", IsRequired = false, EmitDefaultValue = false, Order = 4)]
        public DbType? DbType { get; set; }

        /// <summary>
        /// Gets or Sets the parameter default value expression.
        /// Be careful, must be expresse in data source language
        /// </summary>
        [DataMember(Name = "dv", IsRequired = false, EmitDefaultValue = false, Order = 5)]
        public string DefaultValue { get; set; }

        /// <summary>
        /// Gets or Sets if the parameter is default null
        /// </summary>
        [DataMember(Name = "an", IsRequired = false, EmitDefaultValue = false, Order = 6)]
        public bool AllowNull { get; set; } = false;

        /// <summary>
        /// Gets or Sets the parameter max length (if needed)
        /// </summary>
        [DataMember(Name = "ml", IsRequired = false, EmitDefaultValue = false, Order = 7)]
        public int MaxLength { get; set; }


        public bool Equals(SetupFilterParameter other)
        {
            if (other == null)
                return false;

            var sc = SyncGlobalization.DataSourceStringComparison;

            var sn = this.SchemaName == null ? string.Empty : this.SchemaName;
            var otherSn = other.SchemaName == null ? string.Empty : other.SchemaName;

            return string.Equals(this.Name, other.Name, sc)
                && string.Equals(this.TableName, other.TableName, sc)
                && string.Equals(sn, otherSn, sc)
                && string.Equals(this.DefaultValue, other.DefaultValue, sc)
                && this.DbType.Equals(other.DbType)
                && this.AllowNull.Equals(other.AllowNull)
                && this.MaxLength.Equals(other.MaxLength);
        }

        public override bool Equals(object obj) => this.Equals(obj as SetupFilterParameter);

        public override int GetHashCode() => base.GetHashCode();

        public static bool operator ==(SetupFilterParameter left, SetupFilterParameter right)
            => EqualityComparer<SetupFilterParameter>.Default.Equals(left, right);

        public static bool operator !=(SetupFilterParameter left, SetupFilterParameter right)
            => !(left == right);
    }
}
