using System;
using System.Collections.Generic;
using System.Data;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync
{
    /// <summary>
    /// Represents a filter parameters
    /// For example : @CustomerID int NULL = 12
    /// </summary>
    [DataContract(Name = "sfp"), Serializable]
    public class SyncFilterParameter : SyncNamedItem<SyncFilterParameter>
    {


        /// <summary>
        /// Gets the ShemaTable's SyncSchema
        /// </summary>
        [IgnoreDataMember]
        public SyncSet Schema { get; set; }

        public SyncFilterParameter()
        {

        }

        /// <summary>
        /// Create a new filter parameter with the given name
        /// </summary>
        public SyncFilterParameter(string name, string tableName) : this(name, tableName, string.Empty) { }

        /// <summary>
        /// Create a new filter parameter with the given name
        /// </summary>
        public SyncFilterParameter(string name, string tableName, string schemaName) : this()
        {
            this.Name = name;
            this.TableName = tableName;
            this.SchemaName = schemaName;
        }

        /// <summary>
        /// Ensure filter parameter as the correct schema (since the property is not serialized)
        /// </summary>
        public void EnsureFilterParameter(SyncSet schema) => this.Schema = schema;

        /// <summary>
        /// Gets or sets the name of the parameter.
        /// for SQL, will be named @{ParamterName}
        /// for MySql, will be named in_{ParameterName}
        /// </summary>
        [DataMember(Name = "n", IsRequired = true, Order = 1)]
        public string Name { get; set; }

        /// <summary>
        /// Gets or Sets table name, if parameter is linked to a table
        /// </summary>
        [DataMember(Name = "t", IsRequired = true, Order = 2)]
        public string TableName { get; set; }

        /// <summary>
        /// Gets or sets schema name, if parameter is linked to a table
        /// </summary>
        [DataMember(Name = "s", IsRequired = false, EmitDefaultValue = false, Order = 3)]
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

        public override string ToString() => this.Name;


        /// <summary>
        /// Get all comparable fields to determine if two instances are identifed as same by name
        /// </summary>
        public override IEnumerable<string> GetAllNamesProperties()
        {
            yield return this.TableName;
            yield return this.SchemaName;
            yield return this.Name;
        }

        public override bool EqualsByProperties(SyncFilterParameter other)
        {
            if (other == null)
                return false;

            if (!this.EqualsByName(other))
                return false;

            var sc = SyncGlobalization.DataSourceStringComparison;

            // Can be null since it'as a nullable value
            var sameDbType = (this.DbType.HasValue && other.DbType.HasValue && this.DbType.Equals(other.DbType))
                            || (!this.DbType.HasValue && !other.DbType.HasValue);

            return sameDbType
                && this.AllowNull.Equals(other.AllowNull)
                && this.MaxLength.Equals(other.MaxLength)
                && string.Equals(this.DefaultValue, other.DefaultValue, sc);
        }
    }
}
