using Dotmim.Sync.Data;
using Dotmim.Sync.Manager;
using System;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Collections.Generic;

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
  
        public List<DbRelationDefinition> GetTableRelations()
        {
            var dmRelations = SqlManagementUtils.RelationsForTable(sqlConnection, sqlTransaction, tableName);

            if (dmRelations == null || dmRelations.Rows.Count == 0)
                return null;

            List<DbRelationDefinition> relations = new List<DbRelationDefinition>();

            foreach (var dmRow in dmRelations.Rows)
            {
                DbRelationDefinition relationDefinition = new DbRelationDefinition();
                relationDefinition.ForeignKey = (string)dmRow["ForeignKey"];
                relationDefinition.ColumnName = (string)dmRow["ColumnName"];
                relationDefinition.ReferenceColumnName = (string)dmRow["ReferenceColumnName"];
                relationDefinition.ReferenceTableName = (string)dmRow["ReferenceTableName"];
                relationDefinition.TableName = (string)dmRow["TableName"];

                relations.Add(relationDefinition);
            }

            return relations;
        }

        public List<DbColumnDefinition> GetTableDefinition()
        {
            List<DbColumnDefinition> columns = new List<DbColumnDefinition>();
            // Get the columns definition
            var dmColumnsList = SqlManagementUtils.ColumnsForTable(sqlConnection, sqlTransaction, this.tableName);

            foreach (var c in dmColumnsList.Rows.OrderBy(r => (int)r["column_id"]))
            {
                DbColumnDefinition dbColumn = new DbColumnDefinition();

                dbColumn.Name = c["name"].ToString();
                dbColumn.Ordinal = (int)c["column_id"];
                dbColumn.TypeName = c["type"].ToString();
                dbColumn.MaxLength = Convert.ToInt64(c["max_length"]);
                dbColumn.Precision = (byte)c["precision"];
                dbColumn.Scale = (byte)c["scale"];
                dbColumn.IsNullable = (bool)c["is_nullable"];
                dbColumn.IsIdentity = (bool)c["is_identity"];
                dbColumn.IsCompute = (bool)c["is_computed"];

                switch (dbColumn.TypeName.ToLowerInvariant())
                {
                    case "nchar":
                    case "nvarchar":
                        dbColumn.IsUnicode = true;
                        break;
                    default:
                        dbColumn.IsUnicode = false;
                        break;
                }

                // No unsigned type in SQL Server
                dbColumn.IsUnsigned = false;

                columns.Add(dbColumn);

            }
            return columns;
        }
        
        public List<string> GetTablePrimaryKeys()
        {
            var dmTableKeys = SqlManagementUtils.PrimaryKeysForTable(sqlConnection, sqlTransaction, tableName);
            var lstKeys = new List<String>();

            foreach(var dmKey in dmTableKeys.Rows)
                lstKeys.Add((string)dmKey["columnName"]);

            return lstKeys;
        }
    }
}
