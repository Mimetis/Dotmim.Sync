//using Dotmim.Sync.Data;
//using System;
//using System.Data;

//namespace Dotmim.Sync.Filter
//{
//    /// <summary>
//    /// Design a filter clause on Dmtable
//    /// </summary>
//    [Serializable]
//    public class FilterClause
//    {
//        public String TableName { get; set; }

//        public String ColumnName { get; set; }

//        public DbType? ColumnType { get; set; }

//        /// <summary>
//        /// Gets whether the filter is targeting an existing column of the target table (not virtual) or it is only used as a parameter in the selectchanges stored procedure (virtual)
//        /// </summary>
//        public bool IsVirtual => ColumnType.HasValue;

//        /// <summary>
//        /// Creates a filterclause allowing to specify a DbType.
//        /// If you specify the columnType, Dotmim.Sync will expect that the column does not exist on the table, and the filter is only
//        /// used as a parameter for the selectchanges stored procedure. Thus, IsVirtual would be true
//        /// </summary>
//        /// <param name="tableName">The table to be filtered</param>
//        /// <param name="columnName">The name of the column - or the filterparameter</param>
//        /// <param name="columnType">Pass null to filter on a real table column, pass a DbType in case the filter should only be a parameter in the selectchanges stored procedure</param>
//        public FilterClause(string tableName, string columnName, DbType? columnType)
//            : this(tableName, columnName)
//        {
//            ColumnType = columnType;
//        }

//        public FilterClause(string tableName, string columnName)
//        {
//            this.TableName = tableName;
//            this.ColumnName = columnName;
//        }

//        public FilterClause()
//        {
//        }


//        /// <summary>
//        /// Column on which you want to filter
//        /// </summary>
//        /// <param name="columnName"></param>
//        public void AddColumnName(string columnName)
//        {

//        }


//        /// <summary>
//        /// Will add the parameter used in the filter.
//        /// A DmColumn like 
//        /// new DmColumn("empId")
//        /// {
//        ///        AllowDBNull = true,
//        ///        DbType = DbType.Int32
//        ///};
//        /// will generate something like "@empId int = null" in the stored procedure parameters
//        /// </summary>
//        /// <param name="parameter">parameter type, name, length, precision, scale</param>
//        public void AddParameter(DmColumn parameter)
//        {

//        }

//        /// <summary>
//        /// Add a join instruction.
//        /// Will generate something like
//        /// LEFT JOIN [Customer] [fj1] ON  [base].[CustomerID] = [fj1].[CustomerID]
//        /// AND 
//        /// WHERE  [fj1].[EmployeeID] = @empId OR [fj1].[EmployeeID] IS NULL Or @empId is null
//        /// </summary>
//        /// <param name="joinTable">the table we want to join from the current table</param>
//        /// <param name="columName">the current table </param>
//        public void AddJoin((string TableName, string ColumnName) joinTable, string columName)
//        {

//        }


//        /// <summary>
//        /// Add a custom sql code to join what you want with the where clause you want, too
//        /// </summary>
//        public void AddCustomClause(string joinClause, string whereClause)
//        {

//        }



//    }
//}
