using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using Dotmim.Sync.Data.Surrogate;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Dotmim.Sync.Filter
{
    /// <summary>
    /// Design a filter clause on Dmtable
    /// </summary>
    [Serializable]
    public class FilterClause2
    {
        /// <summary>
        /// Gets Or Sets Parameter to be set in the stored procedure declaration
        /// Use a DmColumnSurrogate since it's easy to serialize and we don't need to attach it to any DmTable
        /// </summary>
        public DmColumnSurrogate InParameter { get; set; }

        /// <summary>
        /// Gets Or Sets joined table
        /// </summary>
        public FilterJoinReference2 FilterTable { get; set; } = null;


        /// <summary>
        /// Parameterless filter for Serialization
        /// </summary>
        public FilterClause2()
        {

        }

        /// <summary>
        /// Creates a filter on the provided table name
        /// Will add the parameter used in the filter.
        /// A DmColumn like 
        /// new DmColumn("empId")
        /// {
        ///        AllowDBNull = true,
        ///        DbType = DbType.Int32
        ///};
        /// will generate something like "@empId int = null" in the stored procedure parameters
        /// </summary>
        /// <param name="parameter">parameter type, name, length, precision, scale</param>
        public FilterClause2(DmColumn inParamater)
        {
            if (inParamater == null)
                throw new ArgumentNullException(nameof(inParamater));

            if (String.IsNullOrEmpty(inParamater.ColumnName))
                throw new ArgumentNullException("ColumnName");

            this.InParameter = new DmColumnSurrogate(inParamater);
        }

        /// <summary>
        /// Filter on the first level of the filter clause.
        /// </summary>
        /// <param name="tableName">table to filter</param>
        public FilterJoinReference2 Filter(string tableName)
        {
            if (string.IsNullOrEmpty(tableName))
                throw new ArgumentNullException(nameof(tableName));

            FilterTable = new FilterJoinReference2(tableName);
            return FilterTable;
        }

        /// <summary>
        /// Add a custom sql code to join what you want with the where clause you want, too
        /// </summary>
        public void AddCustomClause(string joinClause, string whereClause)
        {
            throw new NotImplementedException("Will be implemented .... in the near future");
        }



    }
}
