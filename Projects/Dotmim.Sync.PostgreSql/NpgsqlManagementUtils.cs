using Dotmim.Sync.Builders;
using Dotmim.Sync.DatabaseStringParsers;
using Dotmim.Sync.PostgreSql.Builders;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.PostgreSql
{
    /// <summary>
    /// Represents a class that contains utility methods for managing a PostgreSql database.
    /// </summary>
    public static class NpgsqlManagementUtils
    {
        /// <summary>
        /// return columns = parameters string for a command.
        /// </summary>
        public static string ColumnsAndParameters(IEnumerable<string> columns, string fromPrefix, string sqlPrefix)
        {
            StringBuilder stringBuilder = new StringBuilder();
            string strFromPrefix = string.IsNullOrEmpty(fromPrefix) ? string.Empty : string.Concat(fromPrefix, ".");
            string str1 = string.Empty;
            foreach (var column in columns)
            {
                var columnParser = new ObjectParser(column, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);

                stringBuilder.Append(str1);
                stringBuilder.Append(strFromPrefix);
                stringBuilder.Append(columnParser.QuotedShortName);
                stringBuilder.Append(" = ");
                stringBuilder.Append($"\"{sqlPrefix}{columnParser.NormalizedShortName}\"");
                str1 = " AND ";
            }

            return stringBuilder.ToString();
        }

        /// <summary>
        /// Return a boolean expression checking if a database exists.
        /// </summary>
        public static async Task<bool> DatabaseExistsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction)
        {
            bool tableExist;

            using (var dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = "SELECT count(*) FROM pg_database where pg_database.datname = @databaseName";

                var sqlParameter = new NpgsqlParameter()
                {
                    ParameterName = "@databaseName",
                    Value = connection.Database,
                };
                dbCommand.Parameters.Add(sqlParameter);

                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                if (transaction != null)
                    dbCommand.Transaction = transaction;

                var result = await dbCommand.ExecuteScalarAsync().ConfigureAwait(false);

                tableExist = (long)result != 0;

                if (!alreadyOpened)
                    await connection.CloseAsync().ConfigureAwait(false);
            }

            return tableExist;
        }

        /// <summary>
        /// Returns the database version.
        /// </summary>
        public static async Task<int> DatabaseVersionAsync(NpgsqlConnection connection, NpgsqlTransaction transaction)
        {
            int v = 0;

            using (var dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = "SHOW server_version_num;";

                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                if (transaction != null)
                    dbCommand.Transaction = transaction;

                var result = await dbCommand.ExecuteScalarAsync().ConfigureAwait(false);

                if (result != DBNull.Value)
                {
                    if (!int.TryParse(result.ToString(), out v))
                        v = -1;
                }

                if (!alreadyOpened)
                    await connection.CloseAsync().ConfigureAwait(false);
            }

            return v;
        }

        /// <summary>
        /// Drop a table if exists.
        /// </summary>
        public static async Task DropTableIfExistsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string quotedTableName, string schemaName)
        {
            using var sqlCommand = new NpgsqlCommand(
                string.Format(CultureInfo.InvariantCulture, "DROP TABLE IF EXISTS {0}", quotedTableName), connection);
            bool alreadyOpened = connection.State == ConnectionState.Open;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            if (transaction != null)
                sqlCommand.Transaction = transaction;

            await sqlCommand.ExecuteNonQueryAsync().ConfigureAwait(false);

            if (!alreadyOpened)
                await connection.CloseAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Return a sync setup with all tables.
        /// </summary>
        public static async Task<SyncSetup> GetAllTablesAsync(NpgsqlConnection connection, NpgsqlTransaction transaction)
        {
            var command = $@"
                            select table_name,
	                               table_schema
                            from information_schema.tables
                            where lower(table_type) = 'base table'
	                              and table_schema not in ('information_schema','pg_catalog');
                           ";

            var syncSetup = new SyncSetup();

            using (var npgsqlCommand = new NpgsqlCommand(command, connection))
            {
                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                npgsqlCommand.Transaction = transaction;

                using (var reader = await npgsqlCommand.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    while (await reader.ReadAsync().ConfigureAwait(false))
                    {
                        var tableName = reader.GetString(0);
                        var schemaName = reader.GetString(1);
                        var setupTable = new SetupTable(tableName, schemaName);
                        syncSetup.Tables.Add(setupTable);
                    }
                }

                foreach (var setupTable in syncSetup.Tables)
                {
                    var syncTableColumnsList = await GetColumnsForTableAsync(connection, transaction, setupTable.TableName, setupTable.SchemaName).ConfigureAwait(false);

                    foreach (var column in syncTableColumnsList.Rows)
                        setupTable.Columns.Add(column["column_name"].ToString());
                }

                if (!alreadyOpened)
                    await connection.CloseAsync().ConfigureAwait(false);
            }

            return syncSetup;
        }

        /// <summary>
        /// Gets all columns for a table.
        /// </summary>
        public static async Task<SyncTable> GetColumnsForTableAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string tableName, string schemaName)
        {
            var commandColumn = @"
                                select column_name,
	                                ordinal_position,
	                                data_type,
	                                udt_name,
	                                character_maximum_length,
	                                numeric_precision,
	                                numeric_scale,
	                                is_nullable,
	                                is_generated,
	                                is_identity,
	                                column_default,
	                                identity_start,
	                                identity_increment
                                from information_schema.columns
                                where table_name = @tablename
	                                and table_schema = @schemaname;";

            var schemaNameString = string.IsNullOrWhiteSpace(schemaName) ? "public" : schemaName;

            var syncTable = new SyncTable(tableName, schemaNameString);
            using (var sqlCommand = new NpgsqlCommand(commandColumn, connection))
            {

                var parameter = sqlCommand.CreateParameter();
                parameter.ParameterName = "@tablename";
                parameter.Value = tableName;
                sqlCommand.Parameters.Add(parameter);

                parameter = sqlCommand.CreateParameter();
                parameter.ParameterName = "@schemaname";
                parameter.Value = schemaNameString;
                sqlCommand.Parameters.Add(parameter);

                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                if (transaction != null)
                    sqlCommand.Transaction = transaction;

                using (var reader = await sqlCommand.ExecuteReaderAsync().ConfigureAwait(false))
                    syncTable.Load(reader);

                if (!alreadyOpened)
                    await connection.CloseAsync().ConfigureAwait(false);
            }

            return syncTable;
        }

        /// <summary>
        /// Returns database version and name, ensuring database is available.
        /// </summary>
        public static async Task<(string DatabaseName, string EngineVersion)> GetHelloAsync(NpgsqlConnection connection, NpgsqlTransaction transaction)
        {
            string dbName = null;
            string dbVersion = null;

            using (var dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = "SELECT version(), pg_database.datname FROM pg_database WHERE datname = @databaseName;";

                var sqlParameter = new NpgsqlParameter()
                {
                    ParameterName = "@databaseName",
                    Value = connection.Database,
                };
                dbCommand.Parameters.Add(sqlParameter);

                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                if (transaction != null)
                    dbCommand.Transaction = transaction;

                using (var reader = await dbCommand.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    if (reader.HasRows)
                    {
                        await reader.ReadAsync().ConfigureAwait(false);

                        dbVersion = reader.GetString(0);
                        dbName = reader.GetString(1);
                    }
                }

                if (!alreadyOpened)
                    await connection.CloseAsync().ConfigureAwait(false);
            }

            return (dbName, dbVersion);
        }

        /// <summary>
        /// Returns primary keys for a table.
        /// </summary>
        public static async Task<SyncTable> GetPrimaryKeysForTableAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string tableName, string schemaName)
        {
            var commandColumn = @"
                SELECT CCU.COLUMN_NAME,
	                TC.CONSTRAINT_NAME,
	                C.ORDINAL_POSITION
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC
                JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE AS CCU ON CCU.CONSTRAINT_SCHEMA = TC.CONSTRAINT_SCHEMA
                AND CCU.CONSTRAINT_NAME = TC.CONSTRAINT_NAME
                JOIN INFORMATION_SCHEMA.COLUMNS AS C ON C.TABLE_SCHEMA = TC.CONSTRAINT_SCHEMA
                AND TC.TABLE_NAME = C.TABLE_NAME
                AND CCU.COLUMN_NAME = C.COLUMN_NAME
                WHERE TC.CONSTRAINT_TYPE = 'PRIMARY KEY'
	                AND TC.TABLE_NAME = @TABLENAME
	                AND TC.TABLE_SCHEMA = @SCHEMANAME;";

            var schemaNameString = string.IsNullOrWhiteSpace(schemaName) ? "public" : schemaName;

            var syncTable = new SyncTable(tableName, schemaNameString);
            using (var command = new NpgsqlCommand(commandColumn, connection))
            {

                var parameter = command.CreateParameter();
                parameter.ParameterName = "@TABLENAME";
                parameter.Value = tableName;
                command.Parameters.Add(parameter);

                parameter = command.CreateParameter();
                parameter.ParameterName = "@SCHEMANAME";
                parameter.Value = schemaNameString;
                command.Parameters.Add(parameter);

                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                if (transaction != null)
                    command.Transaction = transaction;

                using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                    syncTable.Load(reader);

                if (!alreadyOpened)
                    await connection.CloseAsync().ConfigureAwait(false);
            }

            return syncTable;
        }

        /// <summary>
        /// Gets relations for a table.
        /// </summary>
        public static async Task<SyncTable> GetRelationsForTableAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string tableName, string schemaName)
        {
            // Todo: get from metadata
            var commandRelations = @"
            Select 	kcu.ordinal_position as ForeignKeyOrder, 
		            kcu.constraint_name as ForeignKey,
		            kcu.table_name as TableName,
		            kcu.table_schema as SchemaName,
		            kcu.column_name as ColumnName,
		            rel_kcu.table_name as ReferenceTableName,
		            rel_kcu.table_schema as ReferenceSchemaName,
		            rel_kcu.column_name as ReferenceColumnName
            from information_schema.table_constraints AS tc
            Join information_schema.key_column_usage AS kcu on kcu.constraint_name = tc.constraint_name and tc.constraint_schema = kcu.constraint_schema
            join information_schema.referential_constraints rco on tc.constraint_schema = rco.constraint_schema and tc.constraint_name = rco.constraint_name
            join information_schema.table_constraints rel_tco on rco.unique_constraint_schema = rel_tco.constraint_schema and rco.unique_constraint_name = rel_tco.constraint_name
            Join information_schema.key_column_usage AS rel_kcu on rel_kcu.constraint_name = rel_tco.constraint_name and rel_kcu.constraint_schema = rel_tco.constraint_schema and rel_kcu.ordinal_position = kcu.ordinal_position
            Where tc.constraint_type='FOREIGN KEY' and tc.table_name = @tableName AND tc.table_schema = @schemaName";

            var schemaNameString = string.IsNullOrWhiteSpace(schemaName) ? "public" : schemaName;
            var syncTable = new SyncTable(tableName, schemaNameString);

            using (var sqlCommand = new NpgsqlCommand(commandRelations, connection))
            {
                var parameter = sqlCommand.CreateParameter();
                parameter.ParameterName = "@tableName";
                parameter.Value = tableName;
                sqlCommand.Parameters.Add(parameter);

                parameter = sqlCommand.CreateParameter();
                parameter.ParameterName = "@schemaName";
                parameter.Value = schemaNameString;
                sqlCommand.Parameters.Add(parameter);

                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                if (transaction != null)
                    sqlCommand.Transaction = transaction;

                using (var reader = await sqlCommand.ExecuteReaderAsync().ConfigureAwait(false))
                    syncTable.Load(reader);

                if (!alreadyOpened)
                    await connection.CloseAsync().ConfigureAwait(false);
            }

            return syncTable;
        }

        /// <summary>
        /// Get a table rows.
        /// </summary>
        public static async Task<SyncTable> GetTableAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string tableName, string schemaName)
        {

            var schemaNameString = string.IsNullOrWhiteSpace(schemaName) ? "public" : schemaName;
            var command = $"Select * from \"{schemaNameString}\".\"{tableName}\"";
            var syncTable = new SyncTable(tableName, schemaNameString);

            using var npgCommand = new NpgsqlCommand(command, connection);

            bool alreadyOpened = connection.State == ConnectionState.Open;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            npgCommand.Transaction = transaction;

            using (var reader = await npgCommand.ExecuteReaderAsync().ConfigureAwait(false))
                syncTable.Load(reader);

            if (!alreadyOpened)
                await connection.CloseAsync().ConfigureAwait(false);

            return syncTable;
        }

        /// <summary>
        /// Gets the quoted table schema. if empty, return public.
        /// </summary>
        public static string GetUnquotedSqlSchemaName(string schema) => string.IsNullOrEmpty(schema) ? "public" : schema;

        /// <summary>
        /// Gets the quoted table schema. if empty, return public.
        /// </summary>
        public static string GetUnquotedSqlSchemaName(TableParser parser) => string.IsNullOrEmpty(parser.SchemaName) ? "public" : parser.SchemaName;

        /// <summary>
        /// Gets the quoted table schema. if empty, return public.
        /// </summary>
        public static string GetUnquotedSqlSchemaName(ObjectParser parser) => string.IsNullOrEmpty(parser.OwnerName) ? "public" : parser.OwnerName;

        /// <summary>
        /// Returns a string joining two tables on clause.
        /// </summary>
        public static string JoinTwoTablesOnClause(IEnumerable<string> columns, string leftName, string rightName)
        {
            var stringBuilder = new StringBuilder();
            string strRightName = string.IsNullOrEmpty(rightName) ? string.Empty : string.Concat(rightName, ".");
            string strLeftName = string.IsNullOrEmpty(leftName) ? string.Empty : string.Concat(leftName, ".");

            string str = string.Empty;
            foreach (var column in columns)
            {
                var quotedColumn = new ObjectParser(column, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);

                stringBuilder.Append(str);
                stringBuilder.Append(strLeftName);
                stringBuilder.Append(quotedColumn.QuotedShortName);
                stringBuilder.Append(" = ");
                stringBuilder.Append(strRightName);
                stringBuilder.Append(quotedColumn.QuotedShortName);

                str = " AND ";
            }

            return stringBuilder.ToString();
        }

        /// <summary>
        /// Renames a table.
        /// </summary>
        public static async Task RenameTableAsync(string tableName, string schemaName, string newTableName, string newSchemaName, NpgsqlConnection connection, NpgsqlTransaction transaction)
        {

            var quotedTableName = string.IsNullOrEmpty(schemaName) ? tableName : $"{schemaName}.{tableName}";
            var quotedNewTableName = string.IsNullOrEmpty(newSchemaName) ? newTableName : $"{newSchemaName}.{newTableName}";

            var commandText = $"exec sp_rename '{quotedTableName}', '{quotedNewTableName}';";

            using var npgsqlCommand = new NpgsqlCommand(commandText, connection);

            bool alreadyOpened = connection.State == ConnectionState.Open;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            npgsqlCommand.Transaction = transaction;

            await npgsqlCommand.ExecuteNonQueryAsync().ConfigureAwait(false);

            if (!alreadyOpened)
                await connection.CloseAsync().ConfigureAwait(false);
        }

        /// <summary>
        /// Checks if a schema exists.
        /// </summary>
        public static async Task<bool> SchemaExistsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string schemaName)
        {
            bool schemaExist;
            using (var dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = "SELECT count(*) FROM information_schema.schemata where schema_name = @schemaName";

                var sqlParameter = new NpgsqlParameter()
                {
                    ParameterName = "@schemaName",
                    Value = schemaName,
                };
                dbCommand.Parameters.Add(sqlParameter);

                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                if (transaction != null)
                    dbCommand.Transaction = transaction;

                var result = await dbCommand.ExecuteScalarAsync().ConfigureAwait(false);

                schemaExist = (long)result != 0;

                if (!alreadyOpened)
                    await connection.CloseAsync().ConfigureAwait(false);
            }

            return schemaExist;
        }

        /// <summary>
        /// Checks if a table exists.
        /// </summary>
        public static async Task<bool> TableExistsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string tableName, string schemaName)
        {
            bool tableExist;
            var pSchemaName = string.IsNullOrEmpty(schemaName) ? "public" : schemaName;

            using (var dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = "SELECT count(*) FROM information_schema.tables " +
                                        "WHERE table_type = 'BASE TABLE' AND table_schema != 'pg_catalog' AND table_schema != 'information_schema' " +
                                        "AND table_name = @tableName AND table_schema = @schemaName";

                NpgsqlParameter sqlParameter = new NpgsqlParameter()
                {
                    ParameterName = "@tableName",
                    DbType = DbType.String,
                    Value = tableName,
                };
                dbCommand.Parameters.Add(sqlParameter);

                sqlParameter = new NpgsqlParameter()
                {
                    ParameterName = "@schemaName",
                    DbType = DbType.String,
                    Value = pSchemaName,
                };
                dbCommand.Parameters.Add(sqlParameter);

                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                if (transaction != null)
                    dbCommand.Transaction = transaction;

                var result = await dbCommand.ExecuteScalarAsync().ConfigureAwait(false);

                tableExist = (long)result != 0;

                if (!alreadyOpened)
                    await connection.CloseAsync().ConfigureAwait(false);
            }

            return tableExist;
        }
    }
}