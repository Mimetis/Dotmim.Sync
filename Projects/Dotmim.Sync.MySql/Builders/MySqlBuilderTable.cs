using Dotmim.Sync.Builders;
using Dotmim.Sync.Manager;
#if NET6_0 || NET8_0 
using MySqlConnector;
#elif NETSTANDARD
using MySql.Data.MySqlClient;
#endif
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

#if MARIADB
namespace Dotmim.Sync.MariaDB.Builders
#elif MYSQL
namespace Dotmim.Sync.MySql.Builders
#endif
{
    public class MySqlBuilderTable
    {
        private readonly ParserName tableName;
        private readonly ParserName trackingName;
        private readonly SyncTable tableDescription;
        private readonly SyncSetup setup;
        private readonly MySqlDbMetadata dbMetadata;

        private static Dictionary<string, string> createdRelationNames = new Dictionary<string, string>();

        private static string GetRandomString()
            => Path.GetRandomFileName().Replace(".", "").ToLowerInvariant();

        /// <summary>
        /// Ensure the relation name is correct to be created in MySql
        /// </summary>
        public static string NormalizeRelationName(string relation)
        {
            if (createdRelationNames.TryGetValue(relation, out var name))
                return name;

            name = relation;

            if (relation.Length > 65)
                name = $"{relation.Substring(0, 50)}_{GetRandomString()}";

            createdRelationNames.Add(relation, name);

            return name;
        }

        public MySqlBuilderTable(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup)
        {
            this.tableDescription = tableDescription;
            this.setup = setup;
            this.tableName = tableName;
            this.trackingName = trackingName;
            this.dbMetadata = new MySqlDbMetadata();
        }


        public Task<DbCommand> GetCreateTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {

#if MARIADB
            var originalProvider = MariaDBSyncProvider.ProviderType;
#elif MYSQL
            var originalProvider = MySqlSyncProvider.ProviderType;
#endif

            var stringBuilder = new StringBuilder();
            string empty = string.Empty;

            stringBuilder.AppendLine("SET FOREIGN_KEY_CHECKS=0;");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"CREATE TABLE {this.tableName.Quoted().ToString()} (");
            stringBuilder.AppendLine();
            foreach (var column in this.tableDescription.Columns)
            {
                var columnName = ParserName.Parse(column, "`").Quoted().ToString();
                var columnType = this.dbMetadata.GetCompatibleColumnTypeDeclarationString(column, this.tableDescription.OriginalProvider);
                var identity = string.Empty;

                if (column.IsAutoIncrement)
                {
                    var s = column.GetAutoIncrementSeedAndStep();
                    if (s.Seed > 1 || s.Step > 1)
                        throw new NotSupportedException("can't establish a seed / step in MySql autoinc value");

                    identity = $"AUTO_INCREMENT";
                }
                var nullString = column.AllowDBNull ? "NULL" : "NOT NULL";

                // if we have a readonly column, we may have a computed one, so we need to allow null
                // if we are not on the same provider with a default value existing
                if (column.IsReadOnly)
                {
                    if (this.tableDescription.OriginalProvider != originalProvider || string.IsNullOrEmpty(column.DefaultValue))
                        nullString = "NULL";
                }

                string defaultValue = string.Empty;

                if (this.tableDescription.OriginalProvider == originalProvider && !string.IsNullOrEmpty(column.DefaultValue) && column.IsCompute)
                {
                    defaultValue = column.DefaultValue;
                    nullString = "";
                }

                stringBuilder.AppendLine($"\t{empty}{columnName} {columnType} {identity} {defaultValue} {nullString}");
                empty = ",";
            }

            if (this.tableDescription.GetMutableColumns().Any(mc => mc.IsAutoIncrement))
                stringBuilder.Append("\t, KEY (");

            empty = string.Empty;
            foreach (var column in this.tableDescription.GetMutableColumns().Where(c => c.IsAutoIncrement))
            {
                var columnName = ParserName.Parse(column, "`").Quoted().ToString();
                stringBuilder.Append($"{empty} {columnName}");
                empty = ",";
            }

            if (this.tableDescription.GetMutableColumns().Any(mc => mc.IsAutoIncrement))
                stringBuilder.AppendLine(")");

            stringBuilder.Append("\t,PRIMARY KEY (");

            int i = 0;
            // It seems we need to specify the increment column in first place
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns().OrderByDescending(pk => pk.IsAutoIncrement))
            {
                var columnName = ParserName.Parse(pkColumn, "`").Quoted().ToString();

                stringBuilder.Append(columnName);

                if (i < this.tableDescription.PrimaryKeys.Count - 1)
                    stringBuilder.Append(", ");
                i++;
            }

            stringBuilder.AppendLine(")");

            foreach (var constraint in this.tableDescription.GetRelations())
            {
                var tableName = ParserName.Parse(constraint.GetTable(), "`").Quoted().ToString();
                var parentTableName = ParserName.Parse(constraint.GetParentTable(), "`").Quoted().ToString();
                var relationName = NormalizeRelationName(constraint.RelationName);
                var keyColumns = constraint.Keys;
                var referencesColumns = constraint.ParentKeys;
                stringBuilder.Append($"\t,CONSTRAINT ");

                stringBuilder.Append($"`{relationName}` ");
                stringBuilder.Append("FOREIGN KEY (");
                empty = string.Empty;
                foreach (var keyColumn in keyColumns)
                {
                    var foreignKeyColumnName = ParserName.Parse(keyColumn.ColumnName, "`").Quoted().ToString();
                    stringBuilder.Append($"{empty} {foreignKeyColumnName}");
                    empty = ", ";
                }
                stringBuilder.Append(" ) ");
                stringBuilder.Append("REFERENCES ");
                stringBuilder.Append(parentTableName).Append(" (");
                empty = string.Empty;
                foreach (var referencesColumn in referencesColumns)
                {
                    var referencesColumnName = ParserName.Parse(referencesColumn.ColumnName, "`").Quoted().ToString();
                    stringBuilder.Append($"{empty} {referencesColumnName}");
                    empty = ", ";
                }
                stringBuilder.AppendLine(" )");
            }


            stringBuilder.Append(");");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("SET FOREIGN_KEY_CHECKS=1;");

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = stringBuilder.ToString();

            return Task.FromResult(command);

        }

        public Task<DbCommand> GetCreateSchemaCommandAsync(DbConnection connection, DbTransaction transaction) => Task.FromResult<DbCommand>(null);
        public Task<DbCommand> GetExistsSchemaCommandAsync(DbConnection connection, DbTransaction transaction) => Task.FromResult<DbCommand>(null);

        public Task<DbCommand> GetDropTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var commandText = $"drop table {this.tableName.Quoted().ToString()}";

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = commandText;

            return Task.FromResult(command);
        }

        public Task<DbCommand> GetExistsTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var commandText = "select COUNT(*) from information_schema.TABLES where TABLE_NAME = @tableName and TABLE_SCHEMA = schema() and TABLE_TYPE = 'BASE TABLE'";

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = commandText;

            var parameter = command.CreateParameter();
            parameter.ParameterName = "@tableName";
            parameter.Value = tableName.Unquoted().ToString();

            command.Parameters.Add(parameter);

            return Task.FromResult(command);
        }



        public Task<DbCommand> GetAddColumnCommandAsync(string columnName, DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();

            command.Connection = connection;
            command.Transaction = transaction;

            var stringBuilder = new StringBuilder($"ALTER TABLE {this.tableName.Quoted().ToString()}  ");

            var column = this.tableDescription.Columns[columnName];
            var columnNameString = ParserName.Parse(columnName, "`").Quoted().ToString();
            var columnType = this.dbMetadata.GetCompatibleColumnTypeDeclarationString(column, this.tableDescription.OriginalProvider);
            var identity = string.Empty;

            if (column.IsAutoIncrement)
            {
                var s = column.GetAutoIncrementSeedAndStep();
                if (s.Seed > 1 || s.Step > 1)
                    throw new NotSupportedException("can't establish a seed / step in MySql autoinc value");

                identity = $"AUTO_INCREMENT";
            }
            var nullString = column.AllowDBNull ? "NULL" : "NOT NULL";

            // if we have a readonly column, we may have a computed one, so we need to allow null
            if (column.IsReadOnly)
                nullString = "NULL";

            stringBuilder.AppendLine($"ADD {columnNameString} {columnType} {identity} {nullString};");

            command.CommandText = stringBuilder.ToString();

            return Task.FromResult(command);
        }

        public Task<DbCommand> GetDropColumnCommandAsync(string columnName, DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();

            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = $"ALTER TABLE {tableName.Quoted().ToString()}  {columnName};";

            return Task.FromResult(command);
        }

        public Task<DbCommand> GetExistsColumnCommandAsync(string columnName, DbConnection connection, DbTransaction transaction)
        {
            var tbl = tableName.ToString();
            var command = connection.CreateCommand();

            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = $"select count(*) from information_schema.COLUMNS where table_schema = schema() and table_name = @tableName and column_name = @columnName;";

            var parameter = command.CreateParameter();
            parameter.ParameterName = "@tableName";
            parameter.Value = tbl;
            command.Parameters.Add(parameter);

            parameter = command.CreateParameter();
            parameter.ParameterName = "@columnName";
            parameter.Value = columnName;
            command.Parameters.Add(parameter);

            return Task.FromResult(command);
        }


        public async Task<IEnumerable<SyncColumn>> GetColumnsAsync(DbConnection connection, DbTransaction transaction)
        {
            string commandColumn = "select * from information_schema.COLUMNS where table_schema = schema() and table_name = @tableName";

            var columns = new List<SyncColumn>();

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = commandColumn;

            var parameter = command.CreateParameter();
            parameter.ParameterName = "@tableName";
            parameter.Value = tableName.Unquoted().ToString();

            command.Parameters.Add(parameter);

            bool alreadyOpened = connection.State == ConnectionState.Open;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            var syncTable = new SyncTable(this.tableName.Unquoted().ToString());

            using var reader = await command.ExecuteReaderAsync().ConfigureAwait(false);
            syncTable.Load(reader);

            reader.Close();

            var mySqlDbMetadata = new MySqlDbMetadata();

            foreach (var c in syncTable.Rows.OrderBy(r => Convert.ToUInt64(r["ordinal_position"])))
            {
                var maxLengthLong = c["character_maximum_length"] != DBNull.Value ? Convert.ToInt64(c["character_maximum_length"]) : 0;

                var sColumn = new SyncColumn(c["column_name"].ToString())
                {
                    OriginalTypeName = c["data_type"].ToString(),
                    Ordinal = Convert.ToInt32(c["ordinal_position"]),
                    MaxLength = maxLengthLong > int.MaxValue ? int.MaxValue : (int)maxLengthLong,
                    Precision = c["numeric_precision"] != DBNull.Value ? Convert.ToByte(c["numeric_precision"]) : (byte)0,
                    Scale = c["numeric_scale"] != DBNull.Value ? Convert.ToByte(c["numeric_scale"]) : (byte)0,
                    AllowDBNull = (string)c["is_nullable"] != "NO",
                    DefaultValue = c["COLUMN_DEFAULT"].ToString(),
                    ExtraProperty1 = c["column_type"] != DBNull.Value ? c["column_type"].ToString() : null,
                    IsUnsigned = c["column_type"] != DBNull.Value && ((string)c["column_type"]).Contains("unsigned"),
#if NETSTANDARD2_0
                    IsUnique = c["column_key"] != DBNull.Value && ((string)c["column_key"]).ToLowerInvariant().Contains("uni")
#else
                    IsUnique = c["column_key"] != DBNull.Value && ((string)c["column_key"]).Contains("uni", SyncGlobalization.DataSourceStringComparison)
#endif
                };

                var extra = c["extra"] != DBNull.Value ? ((string)c["extra"]).ToLowerInvariant() : null;

                if (!string.IsNullOrEmpty(extra) && (extra.Contains("auto increment") || extra.Contains("auto_increment")))
                {
                    sColumn.IsAutoIncrement = true;
                    sColumn.AutoIncrementSeed = 1;
                    sColumn.AutoIncrementStep = 1;
                }

                if (!string.IsNullOrEmpty(extra) && extra.Contains("generated"))
                {
                    var generationExpression = c["generation_expression"] != DBNull.Value ? ((string)c["generation_expression"]) : null;

                    if (!string.IsNullOrEmpty(generationExpression) && !string.IsNullOrEmpty(extra) && extra.Contains("generated"))
                    {
                        var virtualOrStored = extra.Contains("virtual") ? "VIRTUAL" : "STORED";
                        var exp = $"GENERATED ALWAYS AS ({generationExpression}) {virtualOrStored}";
                        sColumn.DefaultValue = exp;
                        sColumn.IsCompute = true;
                        sColumn.AllowDBNull = false;
                    }

                }
                columns.Add(sColumn);

            }
            if (!alreadyOpened)
                connection.Close();

            return columns.ToArray();

        }

        public async Task<IEnumerable<SyncColumn>> GetPrimaryKeysAsync(DbConnection connection, DbTransaction transaction)
        {
            var commandColumn = @"select * from information_schema.COLUMNS where table_schema = schema() and table_name = @tableName and column_key='PRI'";

            var keys = new SyncTable(tableName.Unquoted().ToString());

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = commandColumn;

            var parameter = command.CreateParameter();
            parameter.ParameterName = "@tableName";
            parameter.Value = tableName.Unquoted().ToString();

            command.Parameters.Add(parameter);

            bool alreadyOpened = connection.State == ConnectionState.Open;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
            {
                keys.Load(reader);
            }

            if (!alreadyOpened)
                connection.Close();

            var lstKeys = new List<SyncColumn>();

            foreach (var key in keys.Rows)
            {
                var keyColumn = new SyncColumn((string)key["COLUMN_NAME"], typeof(string));
                keyColumn.Ordinal = Convert.ToInt32(key["ORDINAL_POSITION"]);
                lstKeys.Add(keyColumn);
            }

            return lstKeys;


        }

        public async Task<IEnumerable<DbRelationDefinition>> GetRelationsAsync(DbConnection connection, DbTransaction transaction)
        {
            var relations = new List<DbRelationDefinition>();

            var commandRelations = @"
            SELECT
              ke.CONSTRAINT_NAME as ForeignKey,
              ke.POSITION_IN_UNIQUE_CONSTRAINT as ForeignKeyOrder,
              ke.referenced_table_name as ReferenceTableName,
              ke.REFERENCED_COLUMN_NAME as ReferenceColumnName,
              ke.table_name TableName,
              ke.COLUMN_NAME ColumnName
            FROM
              information_schema.KEY_COLUMN_USAGE ke
            WHERE
              ke.referenced_table_name IS NOT NULL
              and ke.table_schema = schema()
              AND ke.table_name = @tableName
            ORDER BY
              ke.referenced_table_name;";

            var relationsList = new SyncTable(tableName.Unquoted().ToString());

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = commandRelations;

            var parameter = command.CreateParameter();
            parameter.ParameterName = "@tableName";
            parameter.Value = tableName.Unquoted().ToString();

            command.Parameters.Add(parameter);

            bool alreadyOpened = connection.State == ConnectionState.Open;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
            {
                relationsList.Load(reader);
            }

            if (!alreadyOpened)
                connection.Close();

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

            return relations.OrderBy(t => t.ForeignKey).ToArray();
        }
    }
}
