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
using Dotmim.Sync.Manager;

namespace Dotmim.Sync.SqlServer.Builders
{
    public class SqlBuilderTable
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




        private Dictionary<string, string> createdRelationNames = new Dictionary<string, string>();

        private static string GetRandomString() =>
            Path.GetRandomFileName().Replace(".", "").ToLowerInvariant();

        /// <summary>
        /// Ensure the relation name is correct to be created in MySql
        /// </summary>
        public string NormalizeRelationName(string relation)
        {
            if (createdRelationNames.TryGetValue(relation, out var name))
                return name;

            name = relation;

            if (relation.Length > 128)
                name = $"{relation.Substring(0, 110)}_{GetRandomString()}";

            // MySql could have a special character in its relation names
            name = name.Replace("~", "").Replace("#", "");

            createdRelationNames.Add(relation, name);

            return name;
        }

        private SqlCommand BuildCreateTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var stringBuilder = new StringBuilder();
            var tbl = tableName.ToString();
            var schema = SqlManagementUtils.GetUnquotedSqlSchemaName(tableName);

            stringBuilder.AppendLine($"CREATE TABLE {tableName.Schema().Quoted().ToString()} (");
            string empty = string.Empty;
            stringBuilder.AppendLine();
            foreach (var column in this.tableDescription.Columns)
            {
                var columnName = ParserName.Parse(column).Quoted().ToString();
                var columnType = this.sqlDbMetadata.GetCompatibleColumnTypeDeclarationString(column, this.tableDescription.OriginalProvider);
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
            string createTableCommandString = stringBuilder.ToString();

            var command = new SqlCommand(createTableCommandString, (SqlConnection)connection, (SqlTransaction)transaction);

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

        internal async Task<IEnumerable<SyncColumn>> GetPrimaryKeysAsync(DbConnection connection, DbTransaction transaction)
        {
            var schema = SqlManagementUtils.GetUnquotedSqlSchemaName(tableName);
            var syncTableKeys = await SqlManagementUtils.GetPrimaryKeysForTableAsync(this.tableName.ToString(), schema, (SqlConnection)connection, (SqlTransaction)transaction).ConfigureAwait(false);

            var lstKeys = new List<SyncColumn>();

            foreach (var dmKey in syncTableKeys.Rows)
            {
                var keyColumn = SyncColumn.Create<string>((string)dmKey["columnName"]);
                keyColumn.Ordinal = Convert.ToInt32(dmKey["column_id"]);
                lstKeys.Add(keyColumn);
            }

            return lstKeys;

        }
        internal async Task<IEnumerable<DbRelationDefinition>> GetRelationsAsync(DbConnection connection, DbTransaction transaction)
        {
            var schema = SqlManagementUtils.GetUnquotedSqlSchemaName(tableName);
            var relations = new List<DbRelationDefinition>();
            var tableRelations = await SqlManagementUtils.GetRelationsForTableAsync((SqlConnection)connection, (SqlTransaction)transaction, this.tableName.ToString(), schema).ConfigureAwait(false);

            if (tableRelations != null && tableRelations.Rows.Count > 0)
            {
                foreach (var fk in tableRelations.Rows.GroupBy(row =>
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
            return relations.OrderBy(t => t.ForeignKey).ToArray();

        }
        internal async Task<IEnumerable<SyncColumn>> GetColumnsAsync(DbConnection connection, DbTransaction transaction)
        {
            var schema = SqlManagementUtils.GetUnquotedSqlSchemaName(tableName);
            var columns = new List<SyncColumn>();
            // Get the columns definition
            var syncTableColumnsList = await SqlManagementUtils.GetColumnsForTableAsync(this.tableName.ToString(), schema, (SqlConnection)connection, (SqlTransaction)transaction).ConfigureAwait(false);

            foreach (var c in syncTableColumnsList.Rows.OrderBy(r => (int)r["column_id"]))
            {
                var typeName = c["type"].ToString();
                var name = c["name"].ToString();
                var maxLengthLong = Convert.ToInt64(c["max_length"]);

                //// Gets the datastore owner dbType 
                //var datastoreDbType = (SqlDbType)sqlDbMetadata.ValidateOwnerDbType(typeName, false, false, maxLengthLong);

                //// once we have the datastore type, we can have the managed type
                //var columnType = sqlDbMetadata.ValidateType(datastoreDbType);

                var sColumn = new SyncColumn(name)
                {
                    OriginalDbType = typeName,
                    Ordinal = (int)c["column_id"],
                    OriginalTypeName = c["type"].ToString(),
                    MaxLength = maxLengthLong > int.MaxValue ? int.MaxValue : (int)maxLengthLong,
                    Precision = (byte)c["precision"],
                    Scale = (byte)c["scale"],
                    AllowDBNull = (bool)c["is_nullable"],
                    IsAutoIncrement = (bool)c["is_identity"],
                    IsUnique = c["is_unique"] != DBNull.Value ? (bool)c["is_unique"] : false,
                    IsCompute = (bool)c["is_computed"],
                    DefaultValue = c["defaultvalue"] != DBNull.Value ? c["defaultvalue"].ToString() : null
                };

                if (sColumn.IsAutoIncrement)
                {
                    sColumn.AutoIncrementSeed = Convert.ToInt64(c["seed"]);
                    sColumn.AutoIncrementStep = Convert.ToInt64(c["step"]);
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
        public Task<DbCommand> GetCreateSchemaCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var schema = SqlManagementUtils.GetUnquotedSqlSchemaName(tableName);

            if (schema == "dbo")
                return Task.FromResult<DbCommand>(null);

            var command = connection.CreateCommand();

            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = $"Create Schema {schema}";

            return Task.FromResult(command);
        }
        public Task<DbCommand> GetCreateTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var command = BuildCreateTableCommand(connection, transaction);
            return Task.FromResult((DbCommand)command);
        }
        public Task<DbCommand> GetExistsTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var tbl = tableName.ToString();
            var schema = SqlManagementUtils.GetUnquotedSqlSchemaName(tableName);

            var command = connection.CreateCommand();

            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = $"IF EXISTS (SELECT t.name FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE t.name = @tableName AND s.name = @schemaName) SELECT 1 ELSE SELECT 0;";

            var parameter = command.CreateParameter();
            parameter.ParameterName = "@tableName";
            parameter.Value = tbl;
            command.Parameters.Add(parameter);

            parameter = command.CreateParameter();
            parameter.ParameterName = "@schemaName";
            parameter.Value = schema;
            command.Parameters.Add(parameter);

            return Task.FromResult(command);
        }
        public Task<DbCommand> GetExistsSchemaCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var schema = SqlManagementUtils.GetUnquotedSqlSchemaName(tableName);

            var command = connection.CreateCommand();

            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = "IF EXISTS (SELECT sch.name FROM sys.schemas sch WHERE sch.name = @schemaName) SELECT 1 ELSE SELECT 0";

            var parameter = command.CreateParameter();
            parameter.ParameterName = "@schemaName";
            parameter.Value = schema;
            command.Parameters.Add(parameter);

            return Task.FromResult(command);
        }

        public Task<DbCommand> GetDropTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();
            var schema = SqlManagementUtils.GetUnquotedSqlSchemaName(tableName);
            var tbl = tableName.ToString();

            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = $"IF EXISTS (SELECT t.name FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE t.name = @tableName AND s.name = @schemaName) " +
                $"BEGIN " +
                $"ALTER TABLE {tableName.Schema().Quoted().ToString()} NOCHECK CONSTRAINT ALL; " +
                $"DROP TABLE {tableName.Schema().Quoted().ToString()}; " +
                $"END";


            var parameter = command.CreateParameter();
            parameter.ParameterName = "@tableName";
            parameter.Value = tbl;
            command.Parameters.Add(parameter);

            parameter = command.CreateParameter();
            parameter.ParameterName = "@schemaName";
            parameter.Value = schema;
            command.Parameters.Add(parameter);

            return Task.FromResult(command);
        }

        public Task<DbCommand> GetAddColumnCommandAsync(string columnName, DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();

            command.Connection = connection;
            command.Transaction = transaction;

            var stringBuilder = new StringBuilder($"ALTER TABLE {tableName.Schema().Quoted().ToString()} WITH NOCHECK ");

            var column = this.tableDescription.Columns[columnName];
            var columnNameString = ParserName.Parse(column).Quoted().ToString();
            var columnType = this.sqlDbMetadata.GetCompatibleColumnTypeDeclarationString(column, this.tableDescription.OriginalProvider);

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

            stringBuilder.AppendLine($"ADD {columnNameString} {columnType} {identity} {nullString} {defaultValue}");

            command.CommandText = stringBuilder.ToString();

            return Task.FromResult(command);
        }

        public Task<DbCommand> GetDropColumnCommandAsync(string columnName, DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();

            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = $"ALTER TABLE {tableName.Schema().Quoted().ToString()} WITH NOCHECK DROP COLUMN {columnName};";

            return Task.FromResult(command);
        }

        public Task<DbCommand> GetExistsColumnCommandAsync(string columnName, DbConnection connection, DbTransaction transaction)
        {
            var tbl = tableName.ToString();
            var schema = SqlManagementUtils.GetUnquotedSqlSchemaName(tableName);

            var command = connection.CreateCommand();

            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = $"IF EXISTS (" +
                $"SELECT col.* " +
                $"FROM sys.columns as col " +
                $"JOIN sys.tables as t on t.object_id = col.object_id " +
                $"JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE t.name = @tableName AND s.name = @schemaName and col.name=@columnName) SELECT 1 ELSE SELECT 0;";

            var parameter = command.CreateParameter();
            parameter.ParameterName = "@tableName";
            parameter.Value = tbl;
            command.Parameters.Add(parameter);

            parameter = command.CreateParameter();
            parameter.ParameterName = "@schemaName";
            parameter.Value = schema;
            command.Parameters.Add(parameter);

            parameter = command.CreateParameter();
            parameter.ParameterName = "@columnName";
            parameter.Value = columnName;
            command.Parameters.Add(parameter);

            return Task.FromResult(command);
        }

    }


}
