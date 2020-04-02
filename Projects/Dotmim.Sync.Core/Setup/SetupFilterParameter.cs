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
    public class SetupFilterParameter
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
    }
}
