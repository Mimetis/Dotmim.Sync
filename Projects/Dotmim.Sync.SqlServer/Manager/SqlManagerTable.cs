using Dotmim.Sync.Data;
using Dotmim.Sync.Manager;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;

namespace Dotmim.Sync.SqlServer.Manager
{
    public class SqlManagerTable : IDbManagerTable
    {
        private string tableName;
        private string schemaName;
        private readonly SqlTransaction sqlTransaction;
        private readonly SqlConnection sqlConnection;

        public string TableName { set => this.tableName = value; }
        public string SchemaName { set => this.schemaName = value; }

        public SqlManagerTable(DbConnection connection, DbTransaction transaction = null)
        {
            this.sqlConnection = connection as SqlConnection;
            this.sqlTransaction = transaction as SqlTransaction;
        }

        public IEnumerable<DbRelationDefinition> GetTableRelations()
        {
            var relations = new List<DbRelationDefinition>();
            var dmRelations = SqlManagementUtils.RelationsForTable(this.sqlConnection, this.sqlTransaction, this.tableName, this.schemaName);

            if (dmRelations != null && dmRelations.Rows.Count > 0)
                foreach (var fk in dmRelations.Rows.GroupBy(row => 
                new { Name = (string)row["ForeignKey"],
                    TableName = (string)row["TableName"],
                    SchemaName = (string)row["SchemaName"] == "dbo" ? "" : (string)row["SchemaName"],
                    ReferenceTableName = (string)row["ReferenceTableName"],
                    ReferenceSchemaName = (string)row["ReferenceSchemaName"] == "dbo" ? "" : (string)row["ReferenceSchemaName"],
                }))
                {
                    var relationDefinition = new DbRelationDefinition()
                    {
                        ForeignKey = fk.Key.Name,
                        TableName = fk.Key.TableName,
                        SchemaName = fk.Key.SchemaName,
                        ReferenceTableName = fk.Key.ReferenceTableName,
                        ReferenceSchemaName = fk.Key.ReferenceSchemaName,
                    };

                    relationDefinition.Columns.AddRange(fk.Select(dmRow =>
                       new DbRelationColumnDefinition
                       {
                           KeyColumnName = (string)dmRow["ColumnName"],
                           ReferenceColumnName = (string)dmRow["ReferenceColumnName"],
                           Order = (int)dmRow["ForeignKeyOrder"]
                       }));

                    relations.Add(relationDefinition);
                }

            return relations.ToArray();
        }

        public IEnumerable<DmColumn> GetTableDefinition()
        {
            List<DmColumn> columns = new List<DmColumn>();
            // Get the columns definition
            var dmColumnsList = SqlManagementUtils.ColumnsForTable(this.sqlConnection, this.sqlTransaction, this.tableName);
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

                dbColumn.MaxLength = maxLengthLong > int.MaxValue ? int.MaxValue : (int)maxLengthLong;
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

        public IEnumerable<DmColumn> GetTablePrimaryKeys()
        {
            var dmTableKeys = SqlManagementUtils.PrimaryKeysForTable(this.sqlConnection, this.sqlTransaction, this.tableName);

            var lstKeys = new List<DmColumn>();

            foreach (var dmKey in dmTableKeys.Rows)
            {
                var keyColumn = new DmColumn<string>((string)dmKey["columnName"]);
                keyColumn.SetOrdinal(Convert.ToInt32(dmKey["key_ordinal"]));
                lstKeys.Add(keyColumn);
            }

            return lstKeys;
        }
    }
}
