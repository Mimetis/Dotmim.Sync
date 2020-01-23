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
    public class SqliteManagerTable : IDbTableManager
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
            var syncTable = SqliteManagementUtils.Table(this.sqlConnection, this.sqlTransaction, this.tableName);

            if (syncTable == null || syncTable.Rows == null || syncTable.Rows.Count <= 0)
                return null;

            // Get Table
            var row = syncTable.Rows[0];
            var tblName = row["name"].ToString();

            return new SyncTable(tblName);
        }

        public IEnumerable<DbRelationDefinition> GetRelations()
        {

            var relations = new List<DbRelationDefinition>();
            var relationsTable = SqliteManagementUtils.RelationsForTable(this.sqlConnection, this.sqlTransaction, this.tableName);

            if (relationsTable != null && relationsTable.Rows.Count > 0)
            {

                foreach (var fk in relationsTable.Rows.GroupBy(row =>
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
            var columnsList = SqliteManagementUtils.ColumnsForTable(this.sqlConnection, this.sqlTransaction, this.tableName);
            var sqlDbMetadata = new SqliteDbMetadata();

            foreach (var c in columnsList.Rows.OrderBy(r => Convert.ToInt32(r["cid"])))
            {
                var typeName = c["type"].ToString();
                var name = c["name"].ToString();

                // Gets the datastore owner dbType 
                var datastoreDbType = (SqliteType)sqlDbMetadata.ValidateOwnerDbType(typeName, false, false, 0);

                // once we have the datastore type, we can have the managed type
                var columnType = sqlDbMetadata.ValidateType(datastoreDbType);

                var sColumn = new SyncColumn(name, columnType);
                sColumn.OriginalDbType = datastoreDbType.ToString();
                sColumn.Ordinal = Convert.ToInt32(c["cid"]);
                sColumn.OriginalTypeName = c["type"].ToString();
                sColumn.AllowDBNull = !Convert.ToBoolean(c["notnull"]);
                sColumn.DefaultValue = c["dflt_value"].ToString();

                // No unsigned type in SQL Server
                sColumn.IsUnsigned = false;

                columns.Add(sColumn);
            }

            return columns;
        }

        public IEnumerable<SyncColumn> GetPrimaryKeys()
        {
            var keys = SqliteManagementUtils.PrimaryKeysForTable(this.sqlConnection, this.sqlTransaction, this.tableName);

            var lstKeys = new List<SyncColumn>();

            foreach (var key in keys.Rows)
            {
                var keyColumn = SyncColumn.Create<string>((string)key["name"]);
                keyColumn.Ordinal = Convert.ToInt32(key["cid"]);
                lstKeys.Add(keyColumn);
            }

            return lstKeys;
        }
    }
}
