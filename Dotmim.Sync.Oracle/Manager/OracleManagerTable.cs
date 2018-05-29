using Dotmim.Sync.Data;
using Dotmim.Sync.Manager;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Data.OracleClient;

namespace Dotmim.Sync.Oracle.Manager
{
    public class OracleManagerTable : IDbManagerTable
    {
        private string tableName;
        private OracleTransaction oracleTransaction;
        private OracleConnection oracleConnection;

        public OracleManagerTable(DbConnection connection, DbTransaction transaction = null)
        {
            this.oracleConnection = connection as OracleConnection;
            this.oracleTransaction = transaction as OracleTransaction;
        }

        public string TableName { set => tableName = value; }

        public List<DmColumn> GetTableDefinition()
        {
            List<DmColumn> columns = new List<DmColumn>();
            // Get the columns definition
            var dmColumnsList = OracleManagementUtils.ColumnsForTable(oracleConnection, oracleTransaction, this.tableName);
            var oracleDbMetadata = new OracleDbMetadata();

            foreach (var c in dmColumnsList.Rows.OrderBy(r => (int)r["column_id"]))
            {
                var typeName = c["data_type"].ToString();
                var name = c["name"].ToString();

                // Gets the datastore owner dbType 
                var datastoreDbType = (OracleType)oracleDbMetadata.ValidateOwnerDbType(typeName, false, false);
                // once we have the datastore type, we can have the managed type
                Type columnType = oracleDbMetadata.ValidateType(datastoreDbType);

                var dbColumn = DmColumn.CreateColumn(name, columnType);

                dbColumn.SetOrdinal((int)c["column_id"]);
                dbColumn.OriginalTypeName = c["data_type"].ToString();
                var maxLengthLong = Convert.ToInt64(c["data_length"]);

                dbColumn.MaxLength = maxLengthLong > Int32.MaxValue ? Int32.MaxValue : (Int32)maxLengthLong;
                dbColumn.Precision = (byte)c["data_precision"];
                dbColumn.Scale = (byte)c["data_scale"];
                dbColumn.AllowDBNull = (int)c["is_nullable"] == 1 ? true : false;
                dbColumn.AutoIncrement = false;

                switch (dbColumn.OriginalTypeName.ToLowerInvariant())
                {
                    case "nchar":
                    case "nvarchar":
                    case "nvarchar2":
                        dbColumn.IsUnicode = true;
                        break;
                    default:
                        dbColumn.IsUnicode = false;
                        break;
                }
                columns.Add(dbColumn);
            }
            return columns;
        }

        public List<string> GetTablePrimaryKeys()
        {
            var dmTableKeys = OracleManagementUtils.PrimaryKeysForTable(oracleConnection, oracleTransaction, tableName);
            var lstKeys = new List<String>();

            foreach (var dmKey in dmTableKeys.Rows)
                lstKeys.Add((string)dmKey["columnName"]);

            return lstKeys;
        }

        public List<DbRelationDefinition> GetTableRelations()
        {
            var dmRelations = OracleManagementUtils.RelationsForTable(oracleConnection, oracleTransaction, tableName);

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
    }
}
