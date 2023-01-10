using Dotmim.Sync.Setup;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace Dotmim.Sync
{
    /// <summary>
    /// Design a filter clause on Dmtable
    /// </summary>
    [DataContract(Name = "sf"), Serializable]
    public class SetupFilter : SyncNamedItem<SetupFilter>
    {

        /// <summary>
        /// Gets or Sets the name of the table where the filter will be applied (and so the _Changes stored proc)
        /// </summary>
        [DataMember(Name = "tn", IsRequired = true, Order = 1)]
        public string TableName { get; set; }

        /// <summary>
        /// Gets or Sets the schema name of the table where the filter will be applied (and so the _Changes stored proc)
        /// </summary>
        [DataMember(Name = "sn", IsRequired = false, EmitDefaultValue = false, Order = 2)]
        public string SchemaName { get; set; }

        /// <summary>
        /// Gets the custom joins list, used with custom wheres
        /// </summary>
        [DataMember(Name = "j", IsRequired = false, EmitDefaultValue = false, Order = 3)]
        public List<SetupFilterJoin> Joins { get; set; } = new List<SetupFilterJoin>();

        /// <summary>
        /// Gets the custom joins list, used with custom wheres
        /// </summary>
        [DataMember(Name = "cw", IsRequired = false, EmitDefaultValue = false, Order = 4)]
        public List<string> CustomWheres { get; set; } = new List<string>();

        /// <summary>
        /// Gets the parameters list, used as input in the stored procedure
        /// </summary>
        [DataMember(Name = "p", IsRequired = false, EmitDefaultValue = false, Order = 5)]
        public List<SetupFilterParameter> Parameters { get; set; } = new List<SetupFilterParameter>();

        /// <summary>
        /// Side where filters list
        /// </summary>
        [DataMember(Name = "w", IsRequired = false, EmitDefaultValue = false, Order = 6)]
        public List<SetupFilterWhere> Wheres { get; set; } = new List<SetupFilterWhere>();

        /// <summary>
        /// Creates a filterclause allowing to specify a different DbType.
        /// If you specify the columnType, Dotmim.Sync will expect that the column does not exist on the table, and the filter is only
        /// used as a parameter for the selectchanges stored procedure. Thus, IsVirtual would be true
        /// </summary>
        public SetupFilter(string tableName, string schemaName = null)
        {
            this.TableName = tableName;
            this.SchemaName = schemaName;
        }

        /// <summary>
        /// Add a parameter as input to stored procedure
        /// For SQL Server, parameter will be added as @{parameterName}
        /// For MySql, parameter will be added as in_{parameterName}
        /// </summary>
        public void AddParameter(string parameterName, DbType type, bool allowNull = false, string defaultValue = null, int maxLength = 0)
        {

            if (this.Parameters.Any(p => string.Equals(p.Name, parameterName, SyncGlobalization.DataSourceStringComparison)))
                throw new FilterParameterAlreadyExistsException(parameterName, this.TableName);

            var parameter = new SetupFilterParameter { Name = parameterName, DbType = type, DefaultValue = defaultValue, AllowNull = allowNull, MaxLength = maxLength };

            this.Parameters.Add(parameter);
        }

        /// <summary>
        /// Add a parameter based on a column. 
        /// For SQL Server, parameter will be added as @{parameterName}
        /// For MySql, parameter will be added as in_{parameterName}
        /// </summary>
        public void AddParameter(string parameterName, string tableName, string schemaName, bool allowNull = false, string defaultValue = null)
        {

            if (this.Parameters.Any(p => string.Equals(p.Name, parameterName, SyncGlobalization.DataSourceStringComparison)))
                throw new FilterParameterAlreadyExistsException(parameterName, this.TableName);

            this.Parameters.Add(new SetupFilterParameter { Name = parameterName, TableName = tableName, SchemaName = schemaName, DefaultValue = defaultValue, AllowNull = allowNull });
        }


        /// <summary>
        /// Add a parameter based on a column. 
        /// For SQL Server, parameter will be added as @{parameterName}
        /// For MySql, parameter will be added as in_{parameterName}
        /// </summary>
        public void AddParameter(string parameterName, string tableName, bool allowNull = false, string defaultValue = null)
        {
            if (this.Parameters.Any(p => string.Equals(p.Name, parameterName, SyncGlobalization.DataSourceStringComparison)))
                throw new FilterParameterAlreadyExistsException(parameterName, this.TableName);

            this.Parameters.Add(new SetupFilterParameter { Name = parameterName, TableName = tableName, DefaultValue = defaultValue, AllowNull = allowNull });
        }


        /// <summary>
        /// Add a custom filter clause
        /// </summary>
        public SetupFilterOn AddJoin(Join join, string tableName) => new SetupFilterOn(this, join, tableName);

        /// <summary>
        /// Internal add custom join
        /// </summary>
        internal void AddJoin(SetupFilterJoin setupFilterJoin)
        {
            this.Joins.Add(setupFilterJoin);
        }


        /// <summary>
        /// Add a Where clause. 
        /// </summary>
        public SetupFilter AddWhere(string columnName, string tableName, string parameterName, string schemaName = null)
        {
            if (!this.Parameters.Any(p => string.Equals(p.Name, parameterName, SyncGlobalization.DataSourceStringComparison)))
                throw new FilterTrackingWhereException(parameterName);

            this.Wheres.Add(new SetupFilterWhere { ColumnName = columnName, TableName = tableName, ParameterName = parameterName, SchemaName = schemaName });
            return this;
        }

        /// <summary>
        /// Add a custom Where clause. 
        /// </summary>
        public SetupFilter AddCustomWhere(string where)
        {
            // check we don't add a null value
            where = where ?? string.Empty;

            this.CustomWheres.Add(where);
            return this;
        }

        /// <summary>
        /// For Serializer
        /// </summary>
        public SetupFilter()
        {
        }

        /// <summary>
        /// Get all comparable fields to determine if two instances are identifed as same by name
        /// </summary>
        public override IEnumerable<string> GetAllNamesProperties()
        {
            yield return this.TableName;
            yield return this.SchemaName;
        }


        /// <summary>
        /// Compare all properties to see if object are Equals by all properties
        /// </summary>
        public override bool EqualsByProperties(SetupFilter other)
        {
            if (other == null)
                return false;

            // Check name properties
            if (!this.EqualsByName(other))
                return false;

            // Compare all list properties
            // For each, check if they are both null or not null
            // If not null, compare each item

            if (!this.Joins.CompareWith(other.Joins))
                return false;

            if (!this.Parameters.CompareWith(other.Parameters))
                return false;

            if (!this.Wheres.CompareWith(other.Wheres))
                return false;

            // since it's string comparison, don't rely on internal comparison and provide our own comparison func, using StringComparison
            var sc = SyncGlobalization.DataSourceStringComparison;
            if (!this.CustomWheres.CompareWith(other.CustomWheres, (c, oc) => string.Equals(c, oc, sc)))
                return false;

            return true;
        }

        public override string ToString()
        {
            if (string.IsNullOrEmpty(this.TableName))
                return base.ToString();

            if (!string.IsNullOrEmpty(this.SchemaName))
                return $"{this.SchemaName}.{this.TableName}";
            else
                return this.TableName;
        }

    }
}
