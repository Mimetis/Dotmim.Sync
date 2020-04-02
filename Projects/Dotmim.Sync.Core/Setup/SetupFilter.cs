using Dotmim.Sync.Setup;
using System;
using System.Collections.Generic;
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
    public class SetupFilter : IEquatable<SetupFilter>
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
        public List<SetupFilterJoin> Joins { get; } = new List<SetupFilterJoin>();

        /// <summary>
        /// Gets the custom joins list, used with custom wheres
        /// </summary>
        [DataMember(Name = "cw", IsRequired = false, EmitDefaultValue = false, Order = 4)]
        public List<string> CustomWheres { get; } = new List<string>();

        /// <summary>
        /// Gets the parameters list, used as input in the stored procedure
        /// </summary>
        [DataMember(Name = "p", IsRequired = false, EmitDefaultValue = false, Order = 5)]
        public List<SetupFilterParameter> Parameters { get; } = new List<SetupFilterParameter>();

        /// <summary>
        /// Side where filters list
        /// </summary>
        [DataMember(Name = "w", IsRequired = false, EmitDefaultValue = false, Order = 5)]
        public List<SetupFilterWhere> Where { get; } = new List<SetupFilterWhere>();

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
        public void AddParameter(string columnName, string tableName, string schemaName, bool allowNull = false, string defaultValue = null)
        {

            if (this.Parameters.Any(p => string.Equals(p.Name, columnName, SyncGlobalization.DataSourceStringComparison)))
                throw new FilterParameterAlreadyExistsException(columnName, this.TableName);

            this.Parameters.Add(new SetupFilterParameter { Name = columnName, TableName = tableName, SchemaName = schemaName, DefaultValue = defaultValue, AllowNull = allowNull });
        }


        /// <summary>
        /// Add a parameter based on a column. 
        /// For SQL Server, parameter will be added as @{parameterName}
        /// For MySql, parameter will be added as in_{parameterName}
        /// </summary>
        public void AddParameter(string columnName, string tableName, bool allowNull = false, string defaultValue = null)
        {
            if (this.Parameters.Any(p => string.Equals(p.Name, columnName, SyncGlobalization.DataSourceStringComparison)))
                throw new FilterParameterAlreadyExistsException(columnName, this.TableName);

            this.Parameters.Add(new SetupFilterParameter { Name = columnName, TableName = tableName, DefaultValue = defaultValue, AllowNull = allowNull });
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

            this.Where.Add(new SetupFilterWhere { ColumnName = columnName, TableName = tableName, ParameterName = parameterName, SchemaName = schemaName });
            return this;
        }

        /// <summary>
        /// Add a custom Where clause. 
        /// </summary>
        public SetupFilter AddCustomerWhere(string where)
        {
            this.CustomWheres.Add(where);
            return this;
        }

        /// <summary>
        /// For Serializer
        /// </summary>
        public SetupFilter()
        {
        }

        public override bool Equals(object obj) => this.Equals(obj as SetupFilter);


        public bool Equals(SetupFilter other)
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
            var hashCode = -1896683325;
            hashCode = hashCode * -1521134295 + this.TableName.GetHashCode();
            hashCode = hashCode * -1521134295 + this.SchemaName.GetHashCode();
            return hashCode;
        }

        public static bool operator ==(SetupFilter left, SetupFilter right)
            => EqualityComparer<SetupFilter>.Default.Equals(left, right);

        public static bool operator !=(SetupFilter left, SetupFilter right)
            => !(left == right);
    }
}
