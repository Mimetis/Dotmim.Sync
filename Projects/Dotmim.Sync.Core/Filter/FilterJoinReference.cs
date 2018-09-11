using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Runtime.Serialization;
using System.Security.Permissions;
using System.Text;

namespace Dotmim.Sync.Filter
{
    [Serializable]
    public class FilterJoinReference2 : ISerializable
    {
        private string input;

        // Constructor should be protected for unsealed classes, private for sealed classes.
        [SecurityPermission(SecurityAction.Demand, SerializationFormatter = true)]
        protected FilterJoinReference2(SerializationInfo info, StreamingContext context)
        {
            this.input = info.GetString("TableName");
            this.TableName = new ObjectNameParser(this.input);
            this.ColumnName = info.GetString("ColumnName");

            this.FilterTable = info.GetValue("FilterTable", typeof(FilterJoinReference2)) as FilterJoinReference2;
        }


        [SecurityPermission(SecurityAction.Demand, SerializationFormatter = true)]
        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            if (info == null)
                throw new ArgumentNullException("info");

            info.AddValue("TableName", this.input);
            info.AddValue("ColumnName", this.ColumnName);

            info.AddValue("FilterTable", this.FilterTable, typeof(FilterJoinReference2));
        }


        /// <summary>
        /// Gets the name of the table to Join
        /// </summary>
        public ObjectNameParser TableName { get; }

        /// <summary>
        /// Gets the name of the column to compare with FilterParameter
        /// If NULL the join is generated with the foreign key relation
        /// </summary>
        public string ColumnName { get; private set; }

        /// <summary>
        /// Get the next FilterTable
        /// </summary>
        public FilterJoinReference2 FilterTable { get; private set; }

        public FilterJoinReference2(string tableName)
        {
            input = tableName;
            TableName = new ObjectNameParser(tableName);
        }

        /// <summary>
        /// Add a sub reference
        /// </summary>
        public FilterJoinReference2 Join(string joinTableName)
        {
            this.FilterTable = new FilterJoinReference2(joinTableName);
            return FilterTable;
        }


        /// <summary>
        /// Set the column name value which will be used in the WHERE clause, 
        /// and compared with the InParameter value.
        /// </summary>
        public void On(string joinColumnName)
        {
            this.ColumnName = joinColumnName;
        }

        public override string ToString()
        {
            if (this.TableName != null)
                return this.TableName.FullUnquotedString; 

            return base.ToString();
        }
    }
}
