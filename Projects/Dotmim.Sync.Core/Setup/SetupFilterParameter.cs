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
    public class SetupFilterParameter
    {
        /// <summary>
        /// Gets or sets the name of the parameter.
        /// for SQL, will be named @{ParamterName}
        /// for MySql, will be named in_{ParameterName}
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Gets of Sets the table name if parameter is a column 
        /// </summary>
        public string TableName { get; set; }


        /// <summary>
        /// Gets of Sets the table schema name if parameter is a column 
        /// </summary>
        public string SchemaName { get; set; }


        /// <summary>
        /// Gets or Sets the parameter db type
        /// </summary>
        public DbType? DbType { get; set; }

        /// <summary>
        /// Gets or Sets the parameter default value expression.
        /// Be careful, must be expresse in data source language
        /// </summary>
        public string DefaultValue { get; set; }

        /// <summary>
        /// Gets or Sets if the parameter is default null
        /// </summary>
        public bool AllowNull { get; set; } = false;

        /// <summary>
        /// Gets or Sets the parameter max length (if needed)
        /// </summary>
        public int MaxLength { get; set; }
    }
}
