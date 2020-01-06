using Dotmim.Sync.Manager;
using Microsoft.Data.Sqlite;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;

namespace Dotmim.Sync.Sqlite.Manager
{
    public class SqliteManagerTable : IDbManagerTable
    {
        private string tableName;
        private string schemaName;
        private readonly SqliteTransaction sqlTransaction;
        private readonly SqliteConnection sqlConnection;

        public string TableName { set => this.tableName = value; }
        public string SchemaName { set => this.schemaName = value; }

        public SqliteManagerTable(DbConnection connection, DbTransaction transaction = null)
        {
            this.sqlConnection = connection as SqliteConnection;
            this.sqlTransaction = transaction as SqliteTransaction;
        }

        public SyncTable GetTable()
        {
            var dmTable = SqliteManagementUtils.Table(this.sqlConnection, this.sqlTransaction, this.tableName);

            if (dmTable == null || dmTable.Rows == null || dmTable.Rows.Count <= 0)
                return null;

            // Get Table
            var dmRow = dmTable.Rows[0];
            var tblName = dmRow["name"].ToString();

            return new SyncTable(tblName);
        }

        public IEnumerable<DbRelationDefinition> GetRelations()
        {

            var relations = new List<DbRelationDefinition>();
            var dmRelations = SqliteManagementUtils.RelationsForTable(this.sqlConnection, this.sqlTransaction, this.tableName);

            if (dmRelations != null && dmRelations.Rows.Count > 0)
            {

                foreach (var fk in dmRelations.Rows.GroupBy(row =>
                new
                {
                    Name = row["id"].ToString(),
                    TableName = tableName,
                    ReferenceTableName = (string)row["table"],
                }))
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
                           KeyColumnName = dmRow["from"].ToString(),
                           ReferenceColumnName = dmRow["to"].ToString(),
                           Order = Convert.ToInt32(dmRow["seq"])
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
            var dmColumnsList = SqliteManagementUtils.ColumnsForTable(this.sqlConnection, this.sqlTransaction, this.tableName);
            var sqlDbMetadata = new SqliteDbMetadata();

            foreach (var c in dmColumnsList.Rows.OrderBy(r => Convert.ToInt32(r["cid"])))
            {
                var typeName = c["type"].ToString();
                var name = c["name"].ToString();

                // Gets the datastore owner dbType 
                var datastoreDbType = (DbType)sqlDbMetadata.ValidateOwnerDbType(typeName, false, false, 0);

                // once we have the datastore type, we can have the managed type
                var columnType = sqlDbMetadata.ValidateType(datastoreDbType);

                var sColumn = new SyncColumn(name, columnType);
                sColumn.OriginalDbType = datastoreDbType.ToString();
                sColumn.Ordinal = Convert.ToInt32(c["cid"]);
                sColumn.OriginalTypeName = c["type"].ToString();
                sColumn.AllowDBNull = !Convert.ToBoolean(c["notnull"]);

                // No unsigned type in SQL Server
                sColumn.IsUnsigned = false;

                columns.Add(sColumn);
            }

            return columns;
        }

        public IEnumerable<SyncColumn> GetPrimaryKeys()
        {
            var dmTableKeys = SqliteManagementUtils.PrimaryKeysForTable(this.sqlConnection, this.sqlTransaction, this.tableName);

            var lstKeys = new List<SyncColumn>();

            foreach (var dmKey in dmTableKeys.Rows)
            {
                var keyColumn = SyncColumn.Create<string>((string)dmKey["name"]);
                keyColumn.Ordinal = Convert.ToInt32(dmKey["cid"]);
                lstKeys.Add(keyColumn);
            }

            return lstKeys;
        }
    }
}
