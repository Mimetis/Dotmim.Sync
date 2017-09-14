using Dotmim.Sync.Data;
using Dotmim.Sync.Manager;
using Dotmim.Sync.MySql;
using MySql.Data.MySqlClient;
using System;
using System.Data;
using System.Data.Common;

using System.Linq;
using System.Collections.Generic;

namespace Dotmim.Sync.MySql
{
    public class MySqlManagerTable : IDbManagerTable
    {
        private string tableName;
        private MySqlTransaction sqlTransaction;
        private MySqlConnection sqlConnection;

        public string TableName { set => tableName = value; }

        public MySqlManagerTable(DbConnection connection, DbTransaction transaction = null)
        {
            this.sqlConnection = connection as MySqlConnection;
            this.sqlTransaction = transaction as MySqlTransaction;
        }

        /// <summary>
        /// Get DmTable
        /// </summary>
        public DmTable GetTableDefinition()
        {
            DmTable table = new DmTable(this.tableName);

            // Get the columns definition
            var dmColumnsList = MySqlManagementUtils.ColumnsForTable(sqlConnection, sqlTransaction, this.tableName);

            foreach (var c in dmColumnsList.Rows.OrderBy(r => (UInt64)r["ordinal_position"]))
            {
                var name = c["column_name"].ToString();
                var ordinal = Convert.ToInt32(c["ordinal_position"]);
                var typeString = c["data_type"].ToString();
                long maxLength = c["character_octet_length"] != DBNull.Value ? Convert.ToInt64(c["character_octet_length"]) : 0;
                byte precision = c["numeric_precision"] != DBNull.Value ? Convert.ToByte(c["numeric_precision"]) : (byte)0;
                byte scale = c["numeric_scale"] != DBNull.Value ? Convert.ToByte(c["numeric_scale"]) : (byte)0;
                var isNullable = (String)c["is_nullable"] == "NO" ? false : true;
                var isIdentity = c["extra"] != DBNull.Value ? ((string)c["extra"]).Contains("auto increment") : false;
                var unsigned = c["column_type"] != DBNull.Value ? ((string)c["column_type"]).Contains("unsigned") : false;

                // Get mysql dbtype
                MySqlDbType sqlDbType = MySqlMetadata.NameToMySqlDbType(typeString, unsigned);

                // That's why we go trough double parsing String => SqlDbType => Type
                Type objectType = MySqlMetadata.MySqlDbTypeToType(sqlDbType);

                var newColumn = DmColumn.CreateColumn(name, objectType);
                newColumn.AllowDBNull = isNullable;
                newColumn.AutoIncrement = isIdentity;
                if (sqlDbType != MySqlDbType.LongText && sqlDbType != MySqlDbType.MediumText)
                    newColumn.MaxLength = maxLength > -1 ? maxLength : 0;
                newColumn.SetOrdinal(ordinal);
                newColumn.OrginalDbType = typeString;

                if (sqlDbType == MySqlDbType.Timestamp)
                    newColumn.ReadOnly = true;

                if (precision > 0)
                {
                    newColumn.Precision = precision;
                    newColumn.Scale = scale;
                }

                table.Columns.Add(newColumn);
            }

            // Get PrimaryKey
            var dmTableKeys = MySqlManagementUtils.PrimaryKeysForTable(sqlConnection, sqlTransaction, tableName);

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

            return table;


        }

        public DmTable GetTableRelations()
        {
            return MySqlManagementUtils.RelationsForTable(sqlConnection, sqlTransaction, tableName);
        }

        List<DbColumnDefinition> IDbManagerTable.GetTableDefinition()
        {
            throw new NotImplementedException();
        }

        List<DbRelationDefinition> IDbManagerTable.GetTableRelations()
        {
            throw new NotImplementedException();
        }

        public List<string> GetTablePrimaryKeys()
        {
            throw new NotImplementedException();
        }
    }
}
