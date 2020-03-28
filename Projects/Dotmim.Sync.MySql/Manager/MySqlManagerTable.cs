
using Dotmim.Sync.Manager;
using Dotmim.Sync.MySql.Builders;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Dotmim.Sync.MySql
{
    public class MySqlManagerTable : IDbTableManager
    {
        private string tableName;
        private readonly MySqlTransaction sqlTransaction;
        private readonly MySqlConnection sqlConnection;
        private readonly MySqlDbMetadata mySqlDbMetadata;

        public string TableName { set => this.tableName = value; }
        public string SchemaName { set { } }

        public MySqlManagerTable(DbConnection connection, DbTransaction transaction = null)
        {
            this.sqlConnection = connection as MySqlConnection;
            this.sqlTransaction = transaction as MySqlTransaction;
            this.mySqlDbMetadata = new MySqlDbMetadata();
        }

        public async Task<SyncTable> GetTableAsync()
        {
            var syncTable = await MySqlManagementUtils.GetTableAsync(this.sqlConnection, this.sqlTransaction, this.tableName).ConfigureAwait(false);

            if (syncTable == null || syncTable.Rows == null || syncTable.Rows.Count <= 0)
                return null;

            // Get Table
            var row = syncTable.Rows[0];
            var tblName = row["TABLE_NAME"].ToString();

            return new SyncTable(tblName);
        }



        public async Task<IEnumerable<SyncColumn>> GetColumnsAsync()
        {
            var columns = new List<SyncColumn>();

            // Get the columns definition

            var columnsList = await MySqlManagementUtils.GetColumnsForTableAsync(this.sqlConnection, this.sqlTransaction, this.tableName).ConfigureAwait(false);
            var mySqlDbMetadata = new MySqlDbMetadata();

            foreach (var c in columnsList.Rows.OrderBy(r => Convert.ToUInt64(r["ordinal_position"])))
            {
                var typeName = c["data_type"].ToString();
                var name = c["column_name"].ToString();
                var isUnsigned = c["column_type"] != DBNull.Value ? ((string)c["column_type"]).Contains("unsigned") : false;

                var maxLengthLong = c["character_maximum_length"] != DBNull.Value ? Convert.ToInt64(c["character_maximum_length"]) : 0;

                // Gets the datastore owner dbType 
                var datastoreDbType = (MySqlDbType)mySqlDbMetadata.ValidateOwnerDbType(typeName, isUnsigned, false, maxLengthLong);

                // once we have the datastore type, we can have the managed type
                var columnType = mySqlDbMetadata.ValidateType(datastoreDbType);

                var sColumn = new SyncColumn(name, columnType);
                sColumn.OriginalTypeName = typeName;
                sColumn.Ordinal = Convert.ToInt32(c["ordinal_position"]);
                sColumn.MaxLength = maxLengthLong > int.MaxValue ? int.MaxValue : (int)maxLengthLong;
                sColumn.Precision = c["numeric_precision"] != DBNull.Value ? Convert.ToByte(c["numeric_precision"]) : (byte)0;
                sColumn.Scale = c["numeric_scale"] != DBNull.Value ? Convert.ToByte(c["numeric_scale"]) : (byte)0;
                sColumn.AllowDBNull = (string)c["is_nullable"] == "NO" ? false : true;
                sColumn.DefaultValue = c["COLUMN_DEFAULT"].ToString();

                var extra = c["extra"] != DBNull.Value ? ((string)c["extra"]).ToLowerInvariant() : null;

                if (!string.IsNullOrEmpty(extra) && (extra.Contains("auto increment") || extra.Contains("auto_increment")))
                {
                    sColumn.IsAutoIncrement = true;
                    sColumn.AutoIncrementSeed = 1;
                    sColumn.AutoIncrementStep = 1;
                }

                sColumn.IsUnsigned = isUnsigned;
                sColumn.IsUnique = c["column_key"] != DBNull.Value ? ((string)c["column_key"]).ToLowerInvariant().Contains("uni") : false;

                columns.Add(sColumn);

            }
            return columns.ToArray();
        }

        public async Task<IEnumerable<DbRelationDefinition>> GetRelationsAsync()
        {
            var relations = new List<DbRelationDefinition>();

            var relationsList = await MySqlManagementUtils.GetRelationsForTableAsync(this.sqlConnection, this.sqlTransaction, this.tableName).ConfigureAwait(false);

            if (relationsList != null && relationsList.Rows.Count > 0)
            {
                foreach (var fk in relationsList.Rows.GroupBy(row => 
                    new { Name = (string)row["ForeignKey"], TableName = (string)row["TableName"], ReferenceTableName = (string)row["ReferenceTableName"] }))
                {
                    var relationDefinition = new DbRelationDefinition()
                    {
                        ForeignKey = fk.Key.Name,
                        TableName = fk.Key.TableName,
                        ReferenceTableName = fk.Key.ReferenceTableName,
                    };

                    relationDefinition.Columns.AddRange(fk.Select(dmRow =>
                       new DbRelationColumnDefinition
                       {
                           KeyColumnName = (string)dmRow["ColumnName"],
                           ReferenceColumnName = (string)dmRow["ReferenceColumnName"],
                           Order = Convert.ToInt32(dmRow["ForeignKeyOrder"])
                       }));

                    relations.Add(relationDefinition);
                }
            }

            return relations.ToArray();
        }

        public async Task<IEnumerable<SyncColumn>> GetPrimaryKeysAsync()
        {
            // Get PrimaryKey
            var keys = await MySqlManagementUtils.GetPrimaryKeysForTableAsync(this.sqlConnection, this.sqlTransaction, this.tableName).ConfigureAwait(false);

            var lstKeys = new List<SyncColumn>();

            foreach (var key in keys.Rows)
            {
                var keyColumn = new SyncColumn((string)key["COLUMN_NAME"], typeof(string));
                keyColumn.Ordinal = Convert.ToInt32(key["ORDINAL_POSITION"]);
                lstKeys.Add(keyColumn);
            }

            return lstKeys;
        }
    }
}
