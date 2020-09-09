using Dotmim.Sync.Builders;


using Dotmim.Sync.SqlServer.Manager;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.SqlServer.Builders
{
    public class SqlBuilderTable : IDbBuilderTableHelper
    {
        private ParserName tableName;
        private ParserName trackingName;
        private SyncTable tableDescription;
        private readonly SyncSetup setup;
        private SqlDbMetadata sqlDbMetadata;


        public SqlBuilderTable(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup)
        {
            this.tableDescription = tableDescription;
            this.setup = setup;
            this.tableName = tableName;
            this.trackingName = trackingName;
            this.sqlDbMetadata = new SqlDbMetadata();
        }


        private static Dictionary<string, string> createdRelationNames = new Dictionary<string, string>();

        private static string GetRandomString() =>
            Path.GetRandomFileName().Replace(".", "").ToLowerInvariant();

        /// <summary>
        /// Ensure the relation name is correct to be created in MySql
        /// </summary>
        public static string NormalizeRelationName(string relation)
        {
            if (createdRelationNames.ContainsKey(relation))
                return createdRelationNames[relation];

            var name = relation;

            if (relation.Length > 128)
                name = $"{relation.Substring(0, 110)}_{GetRandomString()}";

            // MySql could have a special character in its relation names
            name = name.Replace("~", "").Replace("#", "");

            createdRelationNames.Add(relation, name);

            return name;
        }

        //public async Task<bool> NeedToCreateForeignKeyConstraintsAsync(SyncRelation relation, DbConnection connection, DbTransaction transaction)
        //{
        //    // Don't want foreign key on same table since it could be a problem on first 
        //    // sync. We are not sure that parent row will be inserted in first position
        //    //if (relation.GetParentTable() == relation.GetTable())
        //    //    return false;

        //    string tableName = relation.GetTable().TableName;
        //    string schemaName = relation.GetTable().SchemaName;
        //    string fullName = string.IsNullOrEmpty(schemaName) ? tableName : $"{schemaName}.{tableName}";
        //    var relationName = NormalizeRelationName(relation.RelationName);

        //    var syncTable = await SqlManagementUtils.GetRelationsForTableAsync((SqlConnection)connection, (SqlTransaction)transaction, tableName, schemaName).ConfigureAwait(false);

        //    var foreignKeyExist = syncTable.Rows.Any(r =>
        //       string.Equals(r["ForeignKey"].ToString(), relationName, SyncGlobalization.DataSourceStringComparison));

        //    return !foreignKeyExist;

        //}
        
        //private SqlCommand BuildForeignKeyConstraintsCommand(SyncRelation constraint, DbConnection connection, DbTransaction transaction)
        //{
        //    var sqlCommand = new SqlCommand();
        //    sqlCommand.Connection = (SqlConnection)connection;
        //    sqlCommand.Transaction = (SqlTransaction)transaction;


        //    var stringBuilder = new StringBuilder();

        //    sqlCommand.CommandText = stringBuilder.ToString();
        //    return sqlCommand;
        //}
      
        //public async Task CreateForeignKeyConstraintsAsync(SyncRelation constraint, DbConnection connection, DbTransaction transaction)
        //{
        //    using (var command = BuildForeignKeyConstraintsCommand(constraint, connection, transaction))
        //    {
        //        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
        //    }
        //}

        //private SqlCommand BuildPkCommand(DbConnection connection, DbTransaction transaction)
        //{
        //    var stringBuilder = new StringBuilder();
        //    var tableNameString = tableName.Schema().Quoted().ToString();
        //    var primaryKeyNameString = tableName.Schema().Unquoted().Normalized().ToString();


        //    return new SqlCommand(stringBuilder.ToString(), (SqlConnection)connection, (SqlTransaction)transaction);
        //}
        //public async Task CreatePrimaryKeyAsync(DbConnection connection, DbTransaction transaction)
        //{
        //    using (var command = BuildPkCommand(connection, transaction))
        //    {
        //        await command.ExecuteNonQueryAsync().ConfigureAwait(false);
        //    }
        //}

        private SqlCommand BuildTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var stringBuilder = new StringBuilder();
            var tbl = tableName.ToString();
            var schema = SqlManagementUtils.GetUnquotedSqlSchemaName(tableName);

            stringBuilder.AppendLine("IF NOT EXISTS (SELECT t.name FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE t.name = @tableName AND s.name = @schemaName) ");
            stringBuilder.AppendLine("BEGIN");
            stringBuilder.AppendLine($"CREATE TABLE {tableName.Schema().Quoted().ToString()} (");
            string empty = string.Empty;
            stringBuilder.AppendLine();
            foreach (var column in this.tableDescription.Columns)
            {
                var columnName = ParserName.Parse(column).Quoted().ToString();

                var columnTypeString = this.sqlDbMetadata.TryGetOwnerDbTypeString(column.OriginalDbType, column.GetDbType(), false, false, column.MaxLength, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
                var columnPrecisionString = this.sqlDbMetadata.TryGetOwnerDbTypePrecision(column.OriginalDbType, column.GetDbType(), false, false, column.MaxLength, column.Precision, column.Scale, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
                var columnType = $"{columnTypeString} {columnPrecisionString}";
                var identity = string.Empty;

                if (column.IsAutoIncrement)
                {
                    var s = column.GetAutoIncrementSeedAndStep();
                    identity = $"IDENTITY({s.Seed},{s.Step})";
                }
                var nullString = column.AllowDBNull ? "NULL" : "NOT NULL";

                // if we have a computed column, we should allow null
                if (column.IsReadOnly)
                    nullString = "NULL";

                string defaultValue = string.Empty;
                if (this.tableDescription.OriginalProvider == SqlSyncProvider.ProviderType)
                {
                    if (!string.IsNullOrEmpty(column.DefaultValue))
                    {
                        defaultValue = "DEFAULT " + column.DefaultValue;
                    }
                }

                stringBuilder.AppendLine($"\t{empty}{columnName} {columnType} {identity} {nullString} {defaultValue}");
                empty = ",";
            }
            stringBuilder.AppendLine(");");

            // Primary Keys
            var primaryKeyNameString = tableName.Schema().Unquoted().Normalized().ToString();
            stringBuilder.AppendLine($"ALTER TABLE {tableName.Schema().Quoted().ToString()} ADD CONSTRAINT [PK_{primaryKeyNameString}] PRIMARY KEY(");
            for (int i = 0; i < this.tableDescription.PrimaryKeys.Count; i++)
            {
                var pkColumn = this.tableDescription.PrimaryKeys[i];
                var quotedColumnName = ParserName.Parse(pkColumn).Quoted().ToString();
                stringBuilder.Append(quotedColumnName);

                if (i < this.tableDescription.PrimaryKeys.Count - 1)
                    stringBuilder.Append(", ");
            }
            stringBuilder.AppendLine(");");

            // Foreign Keys
            foreach (var constraint in this.tableDescription.GetRelations())
            {
                var tableName = ParserName.Parse(constraint.GetTable()).Quoted().Schema().ToString();
                var parentTableName = ParserName.Parse(constraint.GetParentTable()).Quoted().Schema().ToString();
                var relationName = NormalizeRelationName(constraint.RelationName);

                stringBuilder.Append("ALTER TABLE ");
                stringBuilder.Append(tableName);
                stringBuilder.AppendLine(" WITH NOCHECK");
                stringBuilder.Append("ADD CONSTRAINT ");
                stringBuilder.AppendLine($"[{relationName}]");
                stringBuilder.Append("FOREIGN KEY (");
                empty = string.Empty;
                foreach (var column in constraint.Keys)
                {
                    var childColumnName = ParserName.Parse(column.ColumnName).Quoted().ToString();
                    stringBuilder.Append($"{empty} {childColumnName}");
                    empty = ", ";
                }
                stringBuilder.AppendLine(" )");
                stringBuilder.Append("REFERENCES ");
                stringBuilder.Append(parentTableName).Append(" (");
                empty = string.Empty;
                foreach (var parentdColumn in constraint.ParentKeys)
                {
                    var parentColumnName = ParserName.Parse(parentdColumn.ColumnName).Quoted().ToString();
                    stringBuilder.Append($"{empty} {parentColumnName}");
                    empty = ", ";
                }
                stringBuilder.Append(" ) ");
            }

            stringBuilder.AppendLine("END");
            string createTableCommandString = stringBuilder.ToString();

            var command =  new SqlCommand(createTableCommandString, (SqlConnection)connection, (SqlTransaction)transaction);

            SqlParameter sqlParameter = new SqlParameter()
            {
                ParameterName = "@tableName",
                Value = tbl
            };
            command.Parameters.Add(sqlParameter);

            sqlParameter = new SqlParameter()
            {
                ParameterName = "@schemaName",
                Value = schema
            };
            command.Parameters.Add(sqlParameter);

            return command;
        }

        private SqlCommand BuildDeleteTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var tbl = tableName.ToString();
            var schema = SqlManagementUtils.GetUnquotedSqlSchemaName(tableName);

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("IF EXISTS (SELECT t.name FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE t.name = @tableName AND s.name = @schemaName) ");
            stringBuilder.AppendLine("BEGIN");
            stringBuilder.AppendLine($"ALTER TABLE {tableName.Schema().Quoted().ToString()} NOCHECK CONSTRAINT ALL; DROP TABLE {tableName.Schema().Quoted().ToString()};");
            stringBuilder.AppendLine("END");

            var command = new SqlCommand(stringBuilder.ToString(), (SqlConnection)connection, (SqlTransaction)transaction);

            SqlParameter sqlParameter = new SqlParameter()
            {
                ParameterName = "@tableName",
                Value = tbl
            };
            command.Parameters.Add(sqlParameter);

            sqlParameter = new SqlParameter()
            {
                ParameterName = "@schemaName",
                Value = schema
            };
            command.Parameters.Add(sqlParameter);

            return command;
        }

        public async Task CreateSchemaAsync(DbConnection connection, DbTransaction transaction)
        {
            if (string.IsNullOrEmpty(tableName.SchemaName) || tableName.SchemaName.ToLowerInvariant() == "dbo")
                return;

            var schemaExists = await SqlManagementUtils.SchemaExistsAsync((SqlConnection)connection, (SqlTransaction)transaction, tableName.SchemaName);

            if (schemaExists)
                return;

            var schemaCommand = $"Create Schema {tableName.SchemaName}";

            using var command = new SqlCommand(schemaCommand, (SqlConnection)connection, (SqlTransaction)transaction);
            
            await command.ExecuteNonQueryAsync().ConfigureAwait(false);
        }

        public async Task CreateTableAsync(DbConnection connection, DbTransaction transaction)
        {
            using (var command = BuildTableCommand(connection, transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }

        public async Task DropTableAsync(DbConnection connection, DbTransaction transaction)
        {
            using (var command = BuildDeleteTableCommand(connection, transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }
    }
}
