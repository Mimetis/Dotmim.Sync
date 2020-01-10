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
    public class SqlManagerTable : IDbTableManager
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

        public SyncTable GetTable()
        {
            var dmTable = SqlManagementUtils.Table(this.sqlConnection, this.sqlTransaction, this.tableName, this.schemaName);

            if (dmTable == null || dmTable.Rows == null || dmTable.Rows.Count <= 0)
                return null;

            // Get Table
            var dmRow = dmTable.Rows[0];
            var tblName = dmRow["TableName"].ToString();
            var schName = dmRow["SchemaName"].ToString();

            if (schName == "dbo")
                schName = null;

            return new SyncTable(tblName, schName);
        }

        public IEnumerable<DbRelationDefinition> GetRelations()
        {
            var relations = new List<DbRelationDefinition>();
            var dmRelations = SqlManagementUtils.RelationsForTable(this.sqlConnection, this.sqlTransaction, this.tableName, this.schemaName);

            if (dmRelations != null && dmRelations.Rows.Count > 0)
            {

                foreach (var fk in dmRelations.Rows.GroupBy(row =>
                new
                {
                    Name = (string)row["ForeignKey"],
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

            }
            return relations.ToArray();
        }

        public IEnumerable<SyncColumn> GetColumns()
        {
            var columns = new List<SyncColumn>();
            // Get the columns definition
            var dmColumnsList = SqlManagementUtils.ColumnsForTable(this.sqlConnection, this.sqlTransaction, this.tableName, this.schemaName);
            var sqlDbMetadata = new SqlDbMetadata();

            foreach (var c in dmColumnsList.Rows.OrderBy(r => (int)r["column_id"]))
            {
                var typeName = c["type"].ToString();
                var name = c["name"].ToString();
                var maxLengthLong = Convert.ToInt64(c["max_length"]);

                // Gets the datastore owner dbType 
                var datastoreDbType = (SqlDbType)sqlDbMetadata.ValidateOwnerDbType(typeName, false, false, maxLengthLong);
                // once we have the datastore type, we can have the managed type
                var columnType = sqlDbMetadata.ValidateType(datastoreDbType);

                var sColumn = new SyncColumn(name, columnType);
                sColumn.OriginalDbType = datastoreDbType.ToString();
                sColumn.Ordinal = (int)c["column_id"];
                sColumn.OriginalTypeName = c["type"].ToString();
                sColumn.MaxLength = maxLengthLong > int.MaxValue ? int.MaxValue : (int)maxLengthLong;
                sColumn.Precision = (byte)c["precision"];
                sColumn.Scale = (byte)c["scale"];
                sColumn.AllowDBNull = (bool)c["is_nullable"];
                sColumn.IsAutoIncrement = (bool)c["is_identity"];
                sColumn.IsUnique = c["is_unique"] != DBNull.Value ? (bool)c["is_unique"] : false;
                sColumn.IsCompute = (bool)c["is_computed"];
                sColumn.DefaultValue = c["defaultvalue"].ToString();

                if (sColumn.IsAutoIncrement)
                {
                    sColumn.AutoIncrementSeed = Convert.ToInt32(c["seed"]);
                    sColumn.AutoIncrementStep = Convert.ToInt32(c["step"]);
                }

                switch (sColumn.OriginalTypeName.ToLowerInvariant())
                {
                    case "nchar":
                    case "nvarchar":
                        sColumn.IsUnicode = true;
                        break;
                    default:
                        sColumn.IsUnicode = false;
                        break;
                }

                // No unsigned type in SQL Server
                sColumn.IsUnsigned = false;

                columns.Add(sColumn);
            }

            return columns;
        }

        public IEnumerable<SyncColumn> GetPrimaryKeys()
        {
            var dmTableKeys = SqlManagementUtils.PrimaryKeysForTable(this.sqlConnection, this.sqlTransaction, this.tableName);

            var lstKeys = new List<SyncColumn>();

            foreach (var dmKey in dmTableKeys.Rows)
            {
                var keyColumn = SyncColumn.Create<string>((string)dmKey["columnName"]);
                keyColumn.Ordinal = Convert.ToInt32(dmKey["key_ordinal"]);
                lstKeys.Add(keyColumn);
            }

            return lstKeys;
        }
    }
}
