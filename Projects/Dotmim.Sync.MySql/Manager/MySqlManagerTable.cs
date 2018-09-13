using Dotmim.Sync.Data;
using Dotmim.Sync.Manager;
using Dotmim.Sync.MySql;
using MySql.Data.MySqlClient;
using System;
using System.Data;
using System.Data.Common;

using System.Linq;
using System.Collections.Generic;
using Dotmim.Sync.MySql.Builders;

namespace Dotmim.Sync.MySql
{
    public class MySqlManagerTable : IDbManagerTable
    {
        private string tableName;
        private MySqlTransaction sqlTransaction;
        private MySqlConnection sqlConnection;
        private MySqlDbMetadata mySqlDbMetadata;

        public string TableName { set => tableName = value; }

        public MySqlManagerTable(DbConnection connection, DbTransaction transaction = null)
        {
            this.sqlConnection = connection as MySqlConnection;
            this.sqlTransaction = transaction as MySqlTransaction;
            this.mySqlDbMetadata = new MySqlDbMetadata();
        }


        public DmTable GetTableRelations()
        {
            return MySqlManagementUtils.RelationsForTable(sqlConnection, sqlTransaction, tableName);
        }

        List<DmColumn> IDbManagerTable.GetTableDefinition()
        {
            List<DmColumn> columns = new List<DmColumn>();

            // Get the columns definition
            Console.WriteLine("Getting table from " + sqlConnection.ConnectionString);

            var dmColumnsList = MySqlManagementUtils.ColumnsForTable(sqlConnection, sqlTransaction, this.tableName);
            var mySqlDbMetadata = new MySqlDbMetadata();

            foreach (var row in dmColumnsList.Rows)
            {
                Console.WriteLine(row);
            }

            foreach (var c in dmColumnsList.Rows.OrderBy(r => Convert.ToUInt64(r["ordinal_position"])))
            {
                var typeName = c["data_type"].ToString();
                var name = c["column_name"].ToString();
                var isUnsigned = c["column_type"] != DBNull.Value ? ((string)c["column_type"]).Contains("unsigned") : false;


                Console.WriteLine("Name : " + name + " " + typeName);


                var maxLengthLong = c["character_maximum_length"] != DBNull.Value ? Convert.ToInt64(c["character_maximum_length"]) : 0;

                // Gets the datastore owner dbType 
                MySqlDbType datastoreDbType = (MySqlDbType)mySqlDbMetadata.ValidateOwnerDbType(typeName, isUnsigned, false, maxLengthLong);
      
                // once we have the datastore type, we can have the managed type
                Type columnType = mySqlDbMetadata.ValidateType(datastoreDbType);

                var dbColumn = DmColumn.CreateColumn(name, columnType);
                dbColumn.OriginalTypeName = typeName;
                dbColumn.SetOrdinal(Convert.ToInt32(c["ordinal_position"]));

                dbColumn.MaxLength = maxLengthLong > Int32.MaxValue ? Int32.MaxValue : (Int32)maxLengthLong;
                dbColumn.Precision = c["numeric_precision"] != DBNull.Value ? Convert.ToByte(c["numeric_precision"]) : (byte)0;
                dbColumn.Scale = c["numeric_scale"] != DBNull.Value ? Convert.ToByte(c["numeric_scale"]) : (byte)0;
                dbColumn.AllowDBNull = (String)c["is_nullable"] == "NO" ? false : true;

                String extra = c["extra"] != DBNull.Value ? ((string)c["extra"]).ToLowerInvariant() : null;

                if (!string.IsNullOrEmpty(extra) && (extra.Contains("auto increment") || extra.Contains("auto_increment")))
                    dbColumn.IsAutoIncrement = true;

                dbColumn.IsUnsigned = isUnsigned;
                dbColumn.IsUnique = c["column_key"] != DBNull.Value ? ((string)c["column_key"]).ToLowerInvariant().Contains("uni") : false;



                columns.Add(dbColumn);

            }

            return columns;
        }

        List<DbRelationDefinition> IDbManagerTable.GetTableRelations()
        {

            var dmRelations = MySqlManagementUtils.RelationsForTable(sqlConnection, sqlTransaction, tableName);

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

        public List<string> GetTablePrimaryKeys()
        {
            // Get PrimaryKey
            var dmTableKeys = MySqlManagementUtils.PrimaryKeysForTable(sqlConnection, sqlTransaction, tableName);

            if (dmTableKeys == null || dmTableKeys.Rows.Count == 0)
                throw new Exception("No Primary Keys in this table, it' can't happen :) ");

            var lstKeys = new List<String>();

            foreach (var dmKey in dmTableKeys.Rows)
                lstKeys.Add((string)dmKey["COLUMN_NAME"]);

            return lstKeys;

        }
    }
}
