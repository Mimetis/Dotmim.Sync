using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Runtime.Serialization;

namespace Dotmim.Sync
{
    /// <summary>
    /// Design a filter clause on Dmtable.
    /// </summary>
    [DataContract(Name = "sf"), Serializable]
    public class SyncFilter : SyncNamedItem<SyncFilter>
    {

        /// <summary>
        /// Gets or sets the name of the table where the filter will be applied (and so the _Changes stored proc).
        /// </summary>
        [DataMember(Name = "t", IsRequired = true, Order = 1)]
        public string TableName { get; set; }

        /// <summary>
        /// Gets or Sets the schema name of the table where the filter will be applied (and so the _Changes stored proc).
        /// </summary>
        [DataMember(Name = "s", IsRequired = false, EmitDefaultValue = false, Order = 2)]
        public string SchemaName { get; set; }

        /// <summary>
        /// Gets or Sets the parameters list, used as input in the stored procedure.
        /// </summary>
        [DataMember(Name = "p", IsRequired = false, EmitDefaultValue = false, Order = 3)]
        public SyncFilterParameters Parameters { get; set; } = [];

        /// <summary>
        /// Gets or Sets side where filters list.
        /// </summary>
        [DataMember(Name = "w", IsRequired = false, EmitDefaultValue = false, Order = 4)]
        public SyncFilterWhereSideItems Wheres { get; set; } = [];

        /// <summary>
        /// Gets or Sets side where filters list.
        /// </summary>
        [DataMember(Name = "j", IsRequired = false, EmitDefaultValue = false, Order = 5)]
        public SyncFilterJoins Joins { get; set; } = [];

        /// <summary>
        /// Gets or Sets customs where.
        /// </summary>
        [DataMember(Name = "cw", IsRequired = false, EmitDefaultValue = false, Order = 6)]
        public List<string> CustomWheres { get; set; } = [];

        /// <summary>
        /// Gets or sets the ShemaFilter's SyncSchema.
        /// </summary>
        [IgnoreDataMember]
        public SyncSet Schema { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncFilter"/> class.
        /// Creates a filterclause allowing to specify a different DbType.
        /// If you specify the columnType, Dotmim.Sync will expect that the column does not exist on the table, and the filter is only
        /// used as a parameter for the selectchanges stored procedure. Thus, IsVirtual would be true.
        /// </summary>
        public SyncFilter(string tableName, string schemaName = null)
        {
            this.TableName = tableName;
            this.SchemaName = schemaName;
        }

        /// <summary>
        /// Clone the SyncFilter.
        /// </summary>
        public SyncFilter Clone()
        {
            var clone = new SyncFilter
            {
                SchemaName = this.SchemaName,
                TableName = this.TableName,
            };

            return clone;
        }

        /// <inheritdoc cref="SyncNamedItem{T}.GetAllNamesProperties"/>
        public override IEnumerable<string> GetAllNamesProperties()
        {
            yield return this.TableName;
            yield return this.SchemaName;
        }

        /// <summary>
        /// Ensure filter has the correct schema (since the property is not serialized.
        /// </summary>
        public void EnsureFilter(SyncSet schema)
        {
            this.Schema = schema;

            this.Parameters.EnsureFilters(this.Schema);
            this.Wheres.EnsureFilters(this.Schema);
            this.Joins.EnsureFilters(this.Schema);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SyncFilter"/> class.
        /// For Serializer.
        /// </summary>
        public SyncFilter()
        {
        }

        /// <summary>
        /// Get unique filter name, composed by all parameters name.
        /// </summary>
        public string GetFilterName()
        {
            string name = string.Empty;
            string sep = string.Empty;
            foreach (var parameterName in this.Parameters.Select(f => f.Name))
            {
                var columnName = ParserName.Parse(parameterName).Unquoted().Normalized().ToString();
                name += $"{columnName}{sep}";
                sep = "_";
            }

            return name;
        }

        /// <inheritdoc cref="SyncNamedItem{T}.EqualsByProperties(T)"/>
        public override bool EqualsByProperties(SyncFilter otherInstance)
        {
            if (otherInstance == null)
                return false;

            var sc = SyncGlobalization.DataSourceStringComparison;

            // Check name properties
            if (!this.EqualsByName(otherInstance))
                return false;

            // Compare all list properties
            // For each, check if they are both null or not null
            // If not null, compare each item
            if (!this.CustomWheres.CompareWith(otherInstance.CustomWheres, (cw, ocw) => string.Equals(ocw, cw, sc)))
                return false;

            if (!this.Joins.CompareWith(otherInstance.Joins))
                return false;

            if (!this.Parameters.CompareWith(otherInstance.Parameters))
                return false;

            if (!this.Wheres.CompareWith(otherInstance.Wheres))
                return false;

            return true;
        }

        /// <summary>
        /// Clear.
        /// </summary>
        public void Clear() => this.Schema = null;
    }
}