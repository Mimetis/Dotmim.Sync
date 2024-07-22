using System;
using System.Collections.Generic;
using System.Runtime.Serialization;

namespace Dotmim.Sync
{
    /// <summary>
    /// SyncFilterWhereSideItem represents a filter where side item.
    /// </summary>
    [DataContract(Name = "sfwsi"), Serializable]
    public class SyncFilterWhereSideItem : SyncNamedItem<SyncFilterWhereSideItem>
    {

        /// <summary>
        /// Gets or sets the column name.
        /// </summary>
        [DataMember(Name = "c", IsRequired = true, Order = 1)]
        public string ColumnName { get; set; }

        /// <summary>
        /// Gets or sets the table name.
        /// </summary>
        [DataMember(Name = "t", IsRequired = true, Order = 2)]
        public string TableName { get; set; }

        /// <summary>
        /// Gets or Sets the schema name.
        /// </summary>
        [DataMember(Name = "s", IsRequired = false, EmitDefaultValue = false, Order = 3)]
        public string SchemaName { get; set; }

        /// <summary>
        /// Gets or sets the parameter name.
        /// </summary>
        [DataMember(Name = "p", IsRequired = true, Order = 4)]
        public string ParameterName { get; set; }

        /// <summary>
        /// Gets or sets the ShemaTable's SyncSchema.
        /// </summary>
        [IgnoreDataMember]
        public SyncSet Schema { get; set; }

        /// <summary>
        /// Ensure filter parameter as the correct schema (since the property is not serialized).
        /// </summary>
        public void EnsureFilterWhereSideItem(SyncSet schema) => this.Schema = schema;

        /// <inheritdoc cref="SyncNamedItem{T}.GetAllNamesProperties"/>
        public override IEnumerable<string> GetAllNamesProperties()
        {
            yield return this.TableName;
            yield return this.SchemaName;
            yield return this.ColumnName;
            yield return this.ParameterName;
        }
    }
}