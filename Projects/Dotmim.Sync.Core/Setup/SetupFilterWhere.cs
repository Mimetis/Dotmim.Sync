using System;
using System.Collections.Generic;
using System.Runtime.Serialization;

namespace Dotmim.Sync.Setup
{
    /// <summary>
    /// Setup filter where clause.
    /// </summary>
    [DataContract(Name = "sfw"), Serializable]
    public class SetupFilterWhere : SyncNamedItem<SetupFilterWhere>
    {
        /// <summary>
        /// Gets or sets the table name.
        /// </summary>
        [DataMember(Name = "tn", IsRequired = true, Order = 1)]
        public string TableName { get; set; }

        /// <summary>
        /// Gets or sets the schema name.
        /// </summary>
        [DataMember(Name = "sn", IsRequired = false, EmitDefaultValue = false, Order = 2)]
        public string SchemaName { get; set; }

        /// <summary>
        /// Gets or sets the column name.
        /// </summary>
        [DataMember(Name = "cn", IsRequired = true, Order = 3)]
        public string ColumnName { get; set; }

        /// <summary>
        /// Gets or sets the parameter name.
        /// </summary>
        [DataMember(Name = "pn", IsRequired = true, Order = 4)]
        public string ParameterName { get; set; }

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