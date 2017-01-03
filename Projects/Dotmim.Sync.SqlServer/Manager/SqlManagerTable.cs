using Dotmim.Sync.Core.Manager;
using System;
using System.Collections.Generic;
using System.Text;
using Dotmim.Sync.Data;
using System.Data.Common;
using System.Linq;
using Dotmim.Sync.Core.Scope;
using Dotmim.Sync.SqlServer.Builders;
using System.Data.SqlClient;

namespace Dotmim.Sync.SqlServer.Manager
{
    public class SqlManagerTable : IDbManagerTable
    {
        private string tableName;
        private SqlTransaction sqlTransaction;
        private SqlConnection sqlConnection;

        public string TableName { set => tableName = value; }

        public SqlManagerTable(DbConnection connection, DbTransaction transaction = null)
        {
            this.sqlConnection = connection as SqlConnection;
            this.sqlTransaction = transaction as SqlTransaction;
        }
        public DmTable GetTableDefinition()
        {
    
            DmTable table = new DmTable(this.tableName);

            // Get the columns definition
            var dmColumnsList = SqlManagementUtils.ColumnsForTable(sqlConnection, sqlTransaction, this.tableName);

            foreach (var c in dmColumnsList.Rows.OrderBy(r => (int)r["column_id"]))
            {
                var name = c["name"].ToString();
                var ordinal = (int)c["column_id"];
                var typeString = c["type"].ToString();
                var maxLength = (Int16)c["max_length"];
                var precision = (byte)c["precision"];
                var scale = (byte)c["scale"];
                var isNullable = (bool)c["is_nullable"];
                var isIdentity = (bool)c["is_identity"];

                Type objectType = SyncDataTypeMapping.GetType(typeString);

                var newColumn = DmColumn.CreateColumn(name, objectType);
                newColumn.AllowDBNull = isNullable;
                newColumn.AutoIncrement = isIdentity;
                newColumn.MaxLength = maxLength > -1 ? maxLength : 0;
                newColumn.SetOrdinal(ordinal);

                if ((objectType == typeof(double) || objectType == typeof(decimal)) && precision > 0)
                {
                    newColumn.Precision = precision;
                    newColumn.Scale = scale;
                }

                table.Columns.Add(newColumn);
            }

            // Get PrimaryKey
            var dmTableKeys = SqlManagementUtils.PrimaryKeysForTable(sqlConnection, sqlTransaction, tableName);

            if (dmTableKeys == null || dmTableKeys.Rows.Count == 0)
                throw new Exception("No Primary Keys in this table, it' can't happen :) ");

            DmColumn[] columnsForKey = new DmColumn[dmTableKeys.Rows.Count];

            for (int i = 0; i < dmTableKeys.Rows.Count; i++)
            {
                var rowColumn = dmTableKeys.Rows[i];

                var columnKey = table.Columns.FirstOrDefault(c => c.ColumnName == rowColumn["columnName"].ToString());

                columnsForKey[i] = columnKey ?? throw new Exception("Primary key found is not present in the columns list");
            }

            // Set the primary Key
            table.PrimaryKey = new DmKey(columnsForKey);


            // TODO
            // Get the datarelation

            return table;
        }
    }
}
