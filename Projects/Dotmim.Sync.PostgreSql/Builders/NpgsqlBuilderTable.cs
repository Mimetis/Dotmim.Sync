using Dotmim.Sync.Builders;
using Dotmim.Sync.Manager;
using Dotmim.Sync.PostgreSql.Builders;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.PostgreSql.Builders
{
    public class NpgsqlBuilderTable
    {
        private SyncSetup setup;
        private SyncTable tableDescription;
        private ParserName tableName;
        private ParserName trackingTableName;
        public NpgsqlBuilderTable(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup)
        {
            this.tableDescription = tableDescription;
            this.tableName = tableName;
            this.trackingTableName = trackingTableName;
            this.setup = setup;
            this.NpgsqlDbMetadata = new NpgsqlDbMetadata();
        }

        public NpgsqlDbMetadata NpgsqlDbMetadata { get; set; }
        public Task<DbCommand> GetAddColumnCommandAsync(string columnName, DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();

            command.Connection = connection;
            command.Transaction = transaction;

            var stringBuilder = new StringBuilder($"ALTER TABLE IF EXISTS {tableName.Schema().Quoted().ToString()}");

            var column = this.tableDescription.Columns[columnName];
            var columnNameString = ParserName.Parse(column).Quoted().ToString();
            var columnType = this.NpgsqlDbMetadata.GetNpgsqlDbType(column);

            var identity = string.Empty;

            if (column.IsAutoIncrement)
            {
                //var s = column.GetAutoIncrementSeedAndStep();
                identity = $"SERIAL";
            }
            var nullString = column.AllowDBNull ? "NULL" : "NOT NULL";

            // if we have a computed column, we should allow null
            if (column.IsReadOnly)
                nullString = "NULL";

            string defaultValue = string.Empty;
            if (this.tableDescription.OriginalProvider == NpgsqlSyncProvider.ProviderType)
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

        public async Task<IEnumerable<SyncColumn>> GetColumnsAsync(DbConnection connection, DbTransaction transaction)
        {
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(tableName);
            var columns = new List<SyncColumn>();
            // Get the columns definition
            var syncTableColumnsList = await NpgsqlManagementUtils.GetColumnsForTableAsync((NpgsqlConnection)connection, (NpgsqlTransaction)transaction, this.tableName.ToString(), schema).ConfigureAwait(false);

            foreach (var c in syncTableColumnsList.Rows.OrderBy(r => (int)r["ordinal_position"]))
            {
                var typeName = c["data_type"].ToString();
                var name = c["column_name"].ToString();
                var maxLengthLong = c["character_maximum_length"] != DBNull.Value ? Convert.ToInt64(c["character_maximum_length"]) : 0;
                byte precision = c["numeric_precision"] != DBNull.Value ? Convert.ToByte(c["numeric_precision"].ToString()) : byte.MinValue;
                byte numeric_scale = c["numeric_scale"] != DBNull.Value ? Convert.ToByte(c["numeric_scale"].ToString()) : byte.MinValue;
                var sColumn = new SyncColumn(name)
                {
                    OriginalDbType = typeName,
                    Ordinal = (int)c["ordinal_position"],
                    OriginalTypeName = c["udt_name"].ToString(),
                    MaxLength = maxLengthLong > int.MaxValue ? int.MaxValue : (int)maxLengthLong,
                    Precision = precision,
                    Scale = numeric_scale,
                    AllowDBNull = c["is_nullable"].ToString() == "YES" ? true : false,
                    IsAutoIncrement = c["is_identity"].ToString() == "YES" ? true : false,
                    IsUnique = false,
                    //IsUnique = c["is_identity"] != DBNull.Value ? (bool)c["is_identity"] : false, //Todo: need to join to get index info
                    IsCompute = c["is_generated"].ToString() == "NEVER" ? false : true,
                    DefaultValue = c["column_default"] != DBNull.Value ? c["column_default"].ToString() : null
                };

                if (sColumn.IsAutoIncrement)
                {
                    sColumn.AutoIncrementSeed = Convert.ToInt32(c["identity_start"]);
                    sColumn.AutoIncrementStep = Convert.ToInt32(c["identity_increment"]);
                }

                switch (sColumn.OriginalTypeName.ToLowerInvariant())
                {
                    case "varchar":
                        sColumn.IsUnicode = true;
                        break;
                    default:
                        sColumn.IsUnicode = false;
                        break;
                }

                // No unsigned type in Postgres Server
                sColumn.IsUnsigned = false;

                columns.Add(sColumn);
            }

            return columns;
        }

        public Task<DbCommand> GetCreateSchemaCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(tableName);
            if (schema == "public")
                return null;

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = $"CREATE SCHEMA {schema}";
            //Testing
            Console.WriteLine(command.CommandText);
            return Task.FromResult(command);
        }
        public Task<DbCommand> GetCreateTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var command = BuildCreateTableCommand(connection, transaction);
            //Testing
            Console.WriteLine(command.CommandText);
            return Task.FromResult((DbCommand)command);
        }

        public Task<DbCommand> GetDropColumnCommandAsync(string columnName, DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = $"ALTER TABLE IF EXISTS {tableName.Schema().Unquoted().ToString()} DROP COLUMN {columnName};";

            //Testing
            Console.WriteLine(command.CommandText);
            return Task.FromResult(command);
        }

        public Task<DbCommand> GetDropTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var table = tableName.Unquoted().ToString();
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(tableName);

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = $"DROP TABLE {schema}.{table};";

            return Task.FromResult(command);
        }

        public Task<DbCommand> GetExistsColumnCommandAsync(string columnName, DbConnection connection, DbTransaction transaction)
        {
            var tbl = tableName.ToString();
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(tableName);

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = @"SELECT EXISTS (SELECT FROM INFORMATION_SCHEMA.COLUMNS 
                                        WHERE TABLE_SCHEMA=@schemaName 
                                        AND TABLE_NAME=@tableName 
                                        AND COLUMN_NAME =@columnName);";

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

        public Task<DbCommand> GetExistsSchemaCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            if (string.IsNullOrEmpty(tableName.SchemaName))
                return null;

            var schemaCommand = $"SELECT EXISTS (SELECT FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = @schemaName)";

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = schemaCommand;

            var parameter = command.CreateParameter();
            parameter.ParameterName = "@schemaName";
            parameter.Value = tableName.SchemaName;
            command.Parameters.Add(parameter);

            return Task.FromResult(command);
        }

        public Task<DbCommand> GetExistsTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var pTableName = tableName.ToString();
            var pSchemaName = tableName.SchemaName;
            pSchemaName = string.IsNullOrEmpty(tableName.ToString()) ? "public" : pSchemaName;

            //Todo: update command text for postgresql
            var dbCommand = connection.CreateCommand();
            dbCommand.Connection = connection;
            dbCommand.Transaction = transaction;
            dbCommand.CommandText = @"SELECT EXISTS (SELECT FROM PG_TABLES 
                                          WHERE SCHEMANAME=@schemaName 
                                            AND TABLENAME=@tableName)";


            var parameter = dbCommand.CreateParameter();

            parameter.ParameterName = "@tableName";
            parameter.Value = pTableName;
            dbCommand.Parameters.Add(parameter);

            parameter = dbCommand.CreateParameter();
            parameter.ParameterName = "@schemaName";
            parameter.Value = pSchemaName;
            dbCommand.Parameters.Add(parameter);

            return Task.FromResult(dbCommand);
        }

        public async Task<IEnumerable<SyncColumn>> GetPrimaryKeysAsync(DbConnection connection, DbTransaction transaction)
        {
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(tableName);
            if (string.IsNullOrEmpty(schema))
                schema = "public";
            var syncTableKeys = await NpgsqlManagementUtils.GetPrimaryKeysForTableAsync((NpgsqlConnection)connection, (NpgsqlTransaction)transaction, this.tableName.ToString(), schema).ConfigureAwait(false);

            var lstKeys = new List<SyncColumn>();

            foreach (var dmKey in syncTableKeys.Rows)
            {

                var keyColumn = SyncColumn.Create<string>((string)dmKey["column_name"]);
                keyColumn.Ordinal = Convert.ToInt32(dmKey["ordinal_position"]);
                lstKeys.Add(keyColumn);
            }

            return lstKeys;
        }

        public async Task<IEnumerable<DbRelationDefinition>> GetRelationsAsync(DbConnection connection, DbTransaction transaction)
        {
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(tableName);
            var relations = new List<DbRelationDefinition>();
            var tableRelations = await NpgsqlManagementUtils.GetRelationsForTableAsync((NpgsqlConnection)connection, (NpgsqlTransaction)transaction, this.tableName.ToString(), schema).ConfigureAwait(false);

            if (tableRelations != null && tableRelations.Rows.Count > 0)
            {
                foreach (var fk in tableRelations.Rows.GroupBy(row =>
                new
                {
                    Name = (string)row["ForeignKey"],
                    TableName = (string)row["TableName"],
                    SchemaName = (string)row["SchemaName"] == "public" ? "" : (string)row["SchemaName"],
                    ReferenceTableName = (string)row["ReferenceTableName"],
                    ReferenceSchemaName = (string)row["ReferenceSchemaName"] == "public" ? "" : (string)row["ReferenceSchemaName"],
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

        private NpgsqlCommand BuildCreateTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var table = this.tableName.Unquoted().ToString();
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(tableName);

            var stringBuilder = new StringBuilder($"CREATE TABLE IF NOT EXISTS {schema}.{table} (");
            string empty = string.Empty;
            stringBuilder.AppendLine();
            foreach (var column in this.tableDescription.Columns)
            {
                var columnName = ParserName.Parse(column).Unquoted().ToString();

                var columnType = this.NpgsqlDbMetadata.GetCompatibleColumnTypeDeclarationString(column, this.tableDescription.OriginalProvider);
                var identity = string.Empty;

                if (column.IsAutoIncrement)
                {
                    identity = $"SERIAL";
                }
                var nullString = column.AllowDBNull ? "NULL" : "NOT NULL";

                // if we have a computed column, we should allow null
                if (column.IsReadOnly)
                    nullString = "NULL";
                //&& !column.DefaultValue.StartsWith("nextval")
                string defaultValue = string.Empty;
                if (this.tableDescription.OriginalProvider == NpgsqlSyncProvider.ProviderType)
                {
                    if (!string.IsNullOrEmpty(column.DefaultValue) )
                    {
                        defaultValue = "DEFAULT " + column.DefaultValue;
                    }
                }

                stringBuilder.AppendLine($"\t{empty}{columnName} {columnType} {identity} {nullString} {defaultValue}");
                empty = ",";
            }
            stringBuilder.Append(")");
            string createTableCommandString = stringBuilder.ToString();
            var command = new NpgsqlCommand(createTableCommandString, (NpgsqlConnection)connection, (NpgsqlTransaction)transaction);
           
            return command;
        }
    }
}
