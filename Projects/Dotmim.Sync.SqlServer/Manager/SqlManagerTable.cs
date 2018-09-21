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

        public IEnumerable<DbRelationDefinition> GetTableRelations()
        {
            List<DbRelationDefinition> relations = new List<DbRelationDefinition>();
            var dmRelations = SqlManagementUtils.RelationsForTable(sqlConnection, sqlTransaction, tableName);



            if (dmRelations != null && dmRelations.Rows.Count > 0)
                foreach (var fk in dmRelations.Rows.GroupBy(row => new { Name = (string)row["ForeignKey"], TableName = (string)row["TableName"], ReferenceTableName = (string)row["ReferenceTableName"] }))
                {
                    var relationDefinition = new DbRelationDefinition()
                    {
                        ForeignKey = fk.Key.Name,
                        TableName = fk.Key.TableName,
                        ReferenceTableName = fk.Key.ReferenceTableName,
                    };

                    relationDefinition.KeyColumnsName = fk.Select(dmRow => (string)dmRow["ColumnName"]).ToArray();
                    relationDefinition.ReferenceColumnsName = fk.Select(dmRow => (string)dmRow["ReferenceColumnName"]).ToArray();

                    relations.Add(relationDefinition);
                }

            return relations.ToArray();
        }

        public IEnumerable<DmColumn> GetTableDefinition()
        {
            List<DmColumn> columns = new List<DmColumn>();
            // Get the columns definition
            var dmColumnsList = SqlManagementUtils.ColumnsForTable(sqlConnection, sqlTransaction, this.tableName);
            var sqlDbMetadata = new SqlDbMetadata();

            foreach (var c in dmColumnsList.Rows.OrderBy(r => (int)r["column_id"]))
            {
                var typeName = c["type"].ToString();
                var name = c["name"].ToString();
                var maxLengthLong = Convert.ToInt64(c["max_length"]);

                // Gets the datastore owner dbType 
                SqlDbType datastoreDbType = (SqlDbType)sqlDbMetadata.ValidateOwnerDbType(typeName, false, false, maxLengthLong);
                // once we have the datastore type, we can have the managed type
                Type columnType = sqlDbMetadata.ValidateType(datastoreDbType);

                var dbColumn = DmColumn.CreateColumn(name, columnType);

                dbColumn.SetOrdinal((int)c["column_id"]);
                dbColumn.OriginalTypeName = c["type"].ToString();

                dbColumn.MaxLength = maxLengthLong > Int32.MaxValue ? Int32.MaxValue : (Int32)maxLengthLong;
                dbColumn.Precision = (byte)c["precision"];
                dbColumn.Scale = (byte)c["scale"];
                dbColumn.AllowDBNull = (bool)c["is_nullable"];
                dbColumn.IsAutoIncrement = (bool)c["is_identity"];
                dbColumn.IsUnique = c["is_unique"] != DBNull.Value ? (bool)c["is_unique"] : false;

                dbColumn.IsCompute = (bool)c["is_computed"];

                switch (dbColumn.OriginalTypeName.ToLowerInvariant())
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
            return columns.ToArray();
        }
        
        public IEnumerable<string> GetTablePrimaryKeys()
        {
            var dmTableKeys = SqlManagementUtils.PrimaryKeysForTable(sqlConnection, sqlTransaction, tableName);
            var lstKeys = new List<String>();

            foreach (var dmKey in dmTableKeys.Rows)
                lstKeys.Add((string)dmKey["columnName"]);

            return lstKeys.ToArray();
        }
    }
}
