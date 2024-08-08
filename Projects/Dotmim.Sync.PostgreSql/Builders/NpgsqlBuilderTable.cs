using Dotmim.Sync.DatabaseStringParsers;
using Dotmim.Sync.Manager;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.PostgreSql.Builders
{
    /// <summary>
    /// Represents a table builder for PostgreSQL and provides various methods to interact with the table.
    /// </summary>
    public class NpgsqlBuilderTable
    {

        private Dictionary<string, string> createdRelationNames = [];

        /// <summary>
        /// Gets the table description.
        /// </summary>
        protected SyncTable TableDescription { get; }

        /// <summary>
        /// Gets the ,npgsql object names.
        /// </summary>
        protected NpgsqlObjectNames NpgsqlObjectNames { get; }

        /// <summary>
        /// Gets the npgsql database metadata.
        /// </summary>
        protected NpgsqlDbMetadata NpgsqlDbMetadata { get; }

        /// <inheritdoc cref="NpgsqlBuilderTable" />
        public NpgsqlBuilderTable(SyncTable tableDescription, NpgsqlObjectNames npgsqlObjectNames, NpgsqlDbMetadata npgsqlDbMetadata)
        {
            this.TableDescription = tableDescription;
            this.NpgsqlObjectNames = npgsqlObjectNames;
            this.NpgsqlDbMetadata = npgsqlDbMetadata;
        }

        /// <summary>
        /// Returns a command to add a column to a table.
        /// </summary>
        public Task<DbCommand> GetAddColumnCommandAsync(string columnName, DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();

            command.Connection = connection;
            command.Transaction = transaction;

            var stringBuilder = new StringBuilder($"alter table if exists {this.NpgsqlObjectNames.TableQuotedFullName}");

            var column = this.TableDescription.Columns[columnName];
            var columnParser = new ObjectParser(column.ColumnName);
            var columnNameString = columnParser.QuotedShortName;
            var columnType = this.NpgsqlDbMetadata.GetNpgsqlDbType(column);

            var identity = string.Empty;

            if (column.IsAutoIncrement)
                identity = $"SERIAL";

            var nullString = column.AllowDBNull ? "NULL" : "NOT NULL";

            // if we have a computed column, we should allow null
            if (column.IsReadOnly)
                nullString = "NULL";

            string defaultValue = string.Empty;
            if (this.TableDescription.OriginalProvider == NpgsqlSyncProvider.ProviderType)
            {
                if (!string.IsNullOrEmpty(column.DefaultValue))
                    defaultValue = "DEFAULT " + column.DefaultValue;
            }

            stringBuilder.AppendLine($"ADD {columnNameString} {columnType} {identity} {nullString} {defaultValue}");

            command.CommandText = stringBuilder.ToString();

            return Task.FromResult(command);
        }

        /// <summary>
        /// Returns a command to get columns.
        /// </summary>
        public async Task<IEnumerable<SyncColumn>> GetColumnsAsync(DbConnection connection, DbTransaction transaction)
        {
            var schema = this.NpgsqlObjectNames.TableSchemaName;
            var columns = new List<SyncColumn>();

            // Get the columns definition
            var syncTableColumnsList = await NpgsqlManagementUtils.GetColumnsForTableAsync((NpgsqlConnection)connection, (NpgsqlTransaction)transaction,
                this.NpgsqlObjectNames.TableName, schema).ConfigureAwait(false);

            foreach (var c in syncTableColumnsList.Rows.OrderBy(r => (int)r["ordinal_position"]))
            {
                var typeName = c["data_type"].ToString();
                var udt_name = c["udt_name"].ToString();
                var name = c["column_name"].ToString();
                var maxLengthLong = c["character_maximum_length"] != DBNull.Value ? (int)c["character_maximum_length"] : 0;
                byte precision = c["numeric_precision"] != DBNull.Value ? Convert.ToByte(c["numeric_precision"].ToString()) : byte.MinValue;
                byte numeric_scale = c["numeric_scale"] != DBNull.Value ? Convert.ToByte(c["numeric_scale"].ToString()) : byte.MinValue;
                var sColumn = new SyncColumn(name)
                {
                    OriginalDbType = udt_name,
                    Ordinal = (int)c["ordinal_position"],
                    OriginalTypeName = typeName,
                    MaxLength = maxLengthLong,
                    Precision = precision,
                    Scale = numeric_scale,
                    AllowDBNull = c["is_nullable"].ToString() == "YES",
                    IsAutoIncrement = c["is_identity"].ToString() == "YES",
                    IsUnique = false,

                    // IsUnique = c["is_identity"] != DBNull.Value ? (bool)c["is_identity"] : false, //Todo: need to join to get index info
                    IsCompute = c["is_generated"].ToString() != "NEVER",
                    DefaultValue = c["column_default"] != DBNull.Value ? c["column_default"].ToString() : null,
                };

                if (sColumn.IsAutoIncrement)
                {
                    sColumn.AutoIncrementSeed = Convert.ToInt32(c["identity_start"]);
                    sColumn.AutoIncrementStep = Convert.ToInt32(c["identity_increment"]);
                }

                sColumn.IsUnicode = sColumn.OriginalTypeName.ToLowerInvariant() switch
                {
                    "varchar" => true,
                    _ => false,
                };

                // No unsigned type in Postgres Server
                sColumn.IsUnsigned = false;

                columns.Add(sColumn);
            }

            return columns;
        }

        /// <summary>
        /// Returns a command to create a schema if it does not exist.
        /// </summary>
        public Task<DbCommand> GetCreateSchemaCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = $"CREATE SCHEMA IF NOT EXISTS \"{this.NpgsqlObjectNames.TableSchemaName}\";";
            return Task.FromResult(command);
        }

        /// <summary>
        /// Returns a command to drop a column from a table.
        /// </summary>
        public Task<DbCommand> GetDropColumnCommandAsync(string columnName, DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = $"alter table if exists {this.NpgsqlObjectNames.TableQuotedFullName} drop column {columnName};";

            return Task.FromResult(command);
        }

        /// <summary>
        /// Returns a command to drop a table.
        /// </summary>
        public Task<DbCommand> GetDropTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = $"drop table {this.NpgsqlObjectNames.TableQuotedFullName}";

            return Task.FromResult(command);
        }

        /// <summary>
        /// Returns a command to check if a column exists.
        /// </summary>
        public Task<DbCommand> GetExistsColumnCommandAsync(string columnName, DbConnection connection, DbTransaction transaction)
        {

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = @"select exists (select from information_schema.columns 
                                        where table_schema=@schemaname 
                                        and table_name=@tablename 
                                        and column_name=@columnname);";

            var parameter = command.CreateParameter();
            parameter.ParameterName = "@tableName";
            parameter.Value = this.NpgsqlObjectNames.TableName;
            command.Parameters.Add(parameter);

            parameter = command.CreateParameter();
            parameter.ParameterName = "@schemaname";
            parameter.Value = this.NpgsqlObjectNames.TableSchemaName;
            command.Parameters.Add(parameter);

            parameter = command.CreateParameter();
            parameter.ParameterName = "@columnname";
            parameter.Value = columnName;
            command.Parameters.Add(parameter);

            return Task.FromResult(command);
        }

        /// <summary>
        /// Returns a command to check if a schema exists.
        /// </summary>
        public Task<DbCommand> GetExistsSchemaCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            if (string.IsNullOrEmpty(this.NpgsqlObjectNames.TableSchemaName))
                return null;

            var schemaCommand = $"select exists (select from information_schema.schemata where schema_name = @schemaname)";

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = schemaCommand;

            var parameter = command.CreateParameter();
            parameter.ParameterName = "@schemaname";
            parameter.Value = this.NpgsqlObjectNames.TableSchemaName;
            command.Parameters.Add(parameter);

            return Task.FromResult(command);
        }

        /// <summary>
        /// Returns a command to check if a table exists.
        /// </summary>
        public Task<DbCommand> GetExistsTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {

            // Todo: update command text for postgresql
            var dbCommand = connection.CreateCommand();
            dbCommand.Connection = connection;
            dbCommand.Transaction = transaction;
            dbCommand.CommandText = @"select exists (select from pg_tables 
                                          where schemaname=@schemaname 
                                            and tablename=@tablename)";

            var parameter = dbCommand.CreateParameter();

            parameter.ParameterName = "@tablename";
            parameter.Value = this.NpgsqlObjectNames.TableName;
            dbCommand.Parameters.Add(parameter);

            parameter = dbCommand.CreateParameter();
            parameter.ParameterName = "@schemaname";
            parameter.Value = this.NpgsqlObjectNames.TableSchemaName;
            dbCommand.Parameters.Add(parameter);

            return Task.FromResult(dbCommand);
        }

        /// <summary>
        /// Returns a list of primary keys columns.
        /// </summary>
        public async Task<IEnumerable<SyncColumn>> GetPrimaryKeysAsync(DbConnection connection, DbTransaction transaction)
        {
            var syncTableKeys = await NpgsqlManagementUtils.GetPrimaryKeysForTableAsync((NpgsqlConnection)connection, (NpgsqlTransaction)transaction,
                this.NpgsqlObjectNames.TableName, this.NpgsqlObjectNames.TableSchemaName).ConfigureAwait(false);

            var lstKeys = new List<SyncColumn>();

            foreach (var dmKey in syncTableKeys.Rows)
            {

                var keyColumn = SyncColumn.Create<string>((string)dmKey["column_name"]);
                keyColumn.Ordinal = Convert.ToInt32(dmKey["ordinal_position"]);
                lstKeys.Add(keyColumn);
            }

            return lstKeys;
        }

        /// <summary>
        /// Returns a list of relations.
        /// </summary>
        public async Task<IEnumerable<DbRelationDefinition>> GetRelationsAsync(DbConnection connection, DbTransaction transaction)
        {
            var relations = new List<DbRelationDefinition>();
            var tableRelations = await NpgsqlManagementUtils.GetRelationsForTableAsync((NpgsqlConnection)connection, (NpgsqlTransaction)transaction,
                this.NpgsqlObjectNames.TableName, this.NpgsqlObjectNames.TableSchemaName).ConfigureAwait(false);

            if (tableRelations != null && tableRelations.Rows.Count > 0)
            {
                foreach (var fk in tableRelations.Rows.GroupBy(row =>
                new
                {
                    Name = (string)row["ForeignKey"],
                    TableName = (string)row["TableName"],
                    SchemaName = (string)row["SchemaName"] == "public" ? string.Empty : (string)row["SchemaName"],
                    ReferenceTableName = (string)row["ReferenceTableName"],
                    ReferenceSchemaName = (string)row["ReferenceSchemaName"] == "public" ? string.Empty : (string)row["ReferenceSchemaName"],
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
                           Order = (int)dmRow["ForeignKeyOrder"],
                       }));

                    relations.Add(relationDefinition);
                }
            }

            return [.. relations.OrderBy(t => t.ForeignKey)];
        }

        /// <summary>
        /// Ensure the relation name is correct to be created in MySql.
        /// </summary>
        public string NormalizeRelationName(string relation)
        {
            if (this.createdRelationNames.TryGetValue(relation, out var name))
                return name;

            name = relation;

            if (relation.Length > 128)
                name = $"{relation.Substring(0, 110)}_{GetRandomString()}";

            // MySql could have a special character in its relation names
            name = name.Replace("~", string.Empty, SyncGlobalization.DataSourceStringComparison).Replace("#", string.Empty, SyncGlobalization.DataSourceStringComparison);

            this.createdRelationNames.Add(relation, name);

            return name;
        }

        private static string GetRandomString() => Path.GetRandomFileName().Replace(".", string.Empty, SyncGlobalization.DataSourceStringComparison).ToLowerInvariant();

        /// <summary>
        /// Returns a command to create a table if it does not exist.
        /// </summary>
        public Task<DbCommand> GetCreateTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {

            var stringBuilder = new StringBuilder($"CREATE TABLE IF NOT EXISTS \"{this.NpgsqlObjectNames.TableSchemaName}\".\"{this.NpgsqlObjectNames.TableName}\" (");
            string empty = string.Empty;
            stringBuilder.AppendLine();
            foreach (var column in this.TableDescription.Columns)
            {
                var columnParser = new ObjectParser(column.ColumnName);

                var columnType = this.NpgsqlDbMetadata.GetCompatibleColumnTypeDeclarationString(column, this.TableDescription.OriginalProvider);

                if (column.IsAutoIncrement)
                    columnType = $"SERIAL";

                var nullString = column.AllowDBNull ? "NULL" : "NOT NULL";

                // if we have a computed column, we should allow null
                if (column.IsReadOnly)
                    nullString = "NULL";

                // && !column.DefaultValue.StartsWith("nextval")
                string defaultValue = string.Empty;
                if (this.TableDescription.OriginalProvider == NpgsqlSyncProvider.ProviderType && !string.IsNullOrEmpty(column.DefaultValue))
                {
                    if (column.DefaultValue.StartsWith("nextval", SyncGlobalization.DataSourceStringComparison))
                        columnType = $"SERIAL";
                    else
                        defaultValue = "DEFAULT " + column.DefaultValue;
                }

                stringBuilder.AppendLine($"\t{empty}{columnParser.QuotedShortName} {columnType} {nullString} {defaultValue}");
                empty = ",";
            }

            stringBuilder.Append(");");

            // Primary Keys
            var primaryKeyNameString = this.NpgsqlObjectNames.TableNormalizedFullName;
            stringBuilder.AppendLine();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"ALTER TABLE \"{this.NpgsqlObjectNames.TableSchemaName}\".\"{this.NpgsqlObjectNames.TableName}\" ADD CONSTRAINT \"PK_{primaryKeyNameString}\" PRIMARY KEY(");
            for (int i = 0; i < this.TableDescription.PrimaryKeys.Count; i++)
            {
                var pkColumn = this.TableDescription.PrimaryKeys[i];
                var pkColumnParser = new ObjectParser(pkColumn);
                stringBuilder.Append(pkColumnParser.QuotedShortName);

                if (i < this.TableDescription.PrimaryKeys.Count - 1)
                    stringBuilder.Append(", ");
            }

            stringBuilder.AppendLine(");");
            stringBuilder.AppendLine();

            // Foreign Keys
            foreach (var constraint in this.TableDescription.GetRelations())
            {

                var tableParser = new TableParser(constraint.GetTable().GetFullName(), NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);
                var schemaName = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(tableParser);

                var parentTableParser = new TableParser(constraint.GetParentTable().GetFullName(), NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);
                var parentSchemaName = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(parentTableParser);

                // var relationName = NormalizeRelationName(constraint.RelationName);
                var relationName = constraint.RelationName;
                stringBuilder.AppendLine();
                stringBuilder.Append($"ALTER TABLE \"{schemaName}\".\"{tableParser.TableName}\" ");
                stringBuilder.Append("ADD CONSTRAINT ");
                stringBuilder.AppendLine($"\"{relationName}\"");
                stringBuilder.Append("FOREIGN KEY (");
                empty = string.Empty;
                foreach (var column in constraint.Keys)
                {
                    var childColumnParser = new ObjectParser(column.ColumnName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);
                    stringBuilder.Append($"{empty} {childColumnParser.QuotedShortName}");
                    empty = ", ";
                }

                stringBuilder.AppendLine(" )");
                stringBuilder.Append("REFERENCES ");
                stringBuilder.Append($"\"{parentSchemaName}\".\"{parentTableParser.TableName}\"").Append(" (");
                empty = string.Empty;
                foreach (var parentdColumn in constraint.ParentKeys)
                {
                    var parentColumnParser = new ObjectParser(parentdColumn.ColumnName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);
                    stringBuilder.Append($"{empty} {parentColumnParser.QuotedShortName}");
                    empty = ", ";
                }

                stringBuilder.Append(" ); ");
            }

            string createTableCommandString = stringBuilder.ToString();

            var command = new NpgsqlCommand(createTableCommandString, (NpgsqlConnection)connection, (NpgsqlTransaction)transaction);

            return Task.FromResult(command as DbCommand);
        }
    }
}