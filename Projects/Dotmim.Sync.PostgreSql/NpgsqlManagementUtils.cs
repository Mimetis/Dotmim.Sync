using Dotmim.Sync.Builders;

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Text;
using System.Threading.Tasks;
using Npgsql;
using Dotmim.Sync.PostgreSql.Builders;

namespace Dotmim.Sync.PostgreSql
{
    public static class NpgsqlManagementUtils
    {
        public static string ColumnsAndParameters(IEnumerable<string> columns, string fromPrefix, string sql_prefix)
        {
            StringBuilder stringBuilder = new StringBuilder();
            string strFromPrefix = (string.IsNullOrEmpty(fromPrefix) ? string.Empty : string.Concat(fromPrefix, "."));
            string str1 = "";
            foreach (var column in columns)
            {
                var unquotedColumn = ParserName.Parse(column, "\"").Unquoted().Normalized().ToString();
                var quotedColumn = ParserName.Parse(column, "\"").Quoted().ToString();

                stringBuilder.Append(str1);
                stringBuilder.Append(strFromPrefix);
                stringBuilder.Append(quotedColumn);
                stringBuilder.Append(" = ");
                stringBuilder.Append($"\"{sql_prefix}{unquotedColumn}\"");
                str1 = " AND ";
            }
            return stringBuilder.ToString();
        }

        public static async Task<bool> DatabaseExistsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction)
        {
            bool tableExist;

            using (DbCommand dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = "SELECT count(*) FROM pg_database where pg_database.datname = @databaseName";

                var sqlParameter = new NpgsqlParameter()
                {
                    ParameterName = "@databaseName",
                    Value = connection.Database
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
                    connection.Close();
            }
            return tableExist;
        }


        public static async Task<int> DatabaseVersionAsync(NpgsqlConnection connection, NpgsqlTransaction transaction)
        {
            int v = 0;

            using (DbCommand dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = "SHOW server_version_num;";

                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                if (transaction != null)
                    dbCommand.Transaction = transaction;

                var result = await dbCommand.ExecuteScalarAsync().ConfigureAwait(false);

                if (result != DBNull.Value)
                    if (!int.TryParse(result.ToString(), out v))
                        v = -1;

                if (!alreadyOpened)
                    connection.Close();
            }
            return v;
        }

        public static async Task DropProcedureIfExistsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, int commandTimout, string quotedProcedureName)
        {
            var procName = ParserName.Parse(quotedProcedureName).ToString();
            //var schemaName = GetUnquotedSqlSchemaName(ParserName.Parse(quotedProcedureName));

            using (var sqlCommand = new NpgsqlCommand($"drop procedure if exists {procName}", connection))
            {
                sqlCommand.CommandTimeout = commandTimout;

                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                sqlCommand.Transaction = transaction;
                await sqlCommand.ExecuteNonQueryAsync().ConfigureAwait(false);

                if (!alreadyOpened)
                    connection.Close();
            }
        }
        public static async Task DropTableIfExistsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string quotedTableName, string schemaName)
        {
            var tableName = ParserName.Parse(quotedTableName, "\"").ToString();

            using (var sqlCommand = new NpgsqlCommand(
                string.Format(CultureInfo.InvariantCulture, "DROP TABLE IF EXISTS {0}", quotedTableName), connection))
            {
                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                if (transaction != null)
                    sqlCommand.Transaction = transaction;

                await sqlCommand.ExecuteNonQueryAsync().ConfigureAwait(false);

                if (!alreadyOpened)
                    connection.Close();
            }
        }

        public static async Task DropTriggerIfExistsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, int commandTimeout, string quotedTriggerName, string quotedTableName)
        {
            var triggerName = ParserName.Parse(quotedTriggerName, "\"").ToString();

            using (var sqlCommand = new NpgsqlCommand(string.Format(CultureInfo.InvariantCulture,
                "DROP TRIGGER IF EXISTS {0} ON TABLE {1}", quotedTriggerName, quotedTableName), connection))
            {
                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                if (transaction != null)
                    sqlCommand.Transaction = transaction;


                await sqlCommand.ExecuteNonQueryAsync().ConfigureAwait(false);

                if (!alreadyOpened)
                    connection.Close();
            }
        }

        public static Task DropTypeIfExistsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, int commandTimeout, string quotedTypeName)
        {
            throw new NotImplementedException();
        }

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

            using (var NpgsqlCommand = new NpgsqlCommand(command, connection))
            {
                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                NpgsqlCommand.Transaction = transaction;

                using (var reader = await NpgsqlCommand.ExecuteReaderAsync().ConfigureAwait(false))
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
                    connection.Close();

            }
            return syncSetup;
        }

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

            var tableNameNormalized = ParserName.Parse(tableName, "\"").Unquoted().Normalized().ToString();
            var tableNameString = ParserName.Parse(tableName, "\"").Unquoted().ToString();

            var schemaNameString = "public";
            if (!string.IsNullOrEmpty(schemaName))
            {
                schemaNameString = ParserName.Parse(schemaName, "\"").ToString();
                schemaNameString = string.IsNullOrWhiteSpace(schemaNameString) ? "public" : schemaNameString;
            }

            var syncTable = new SyncTable(tableNameNormalized);
            using (var sqlCommand = new NpgsqlCommand(commandColumn, connection))
            {

                var parameter = sqlCommand.CreateParameter();
                parameter.ParameterName = "@tablename";
                parameter.Value = tableNameString;
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
                    connection.Close();

            }
            return syncTable;
        }

        public static async Task<(string DatabaseName, string EngineVersion)> GetHelloAsync(NpgsqlConnection connection, NpgsqlTransaction transaction)
        {
            string dbName = null;
            string dbVersion = null;

            using (DbCommand dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = "SELECT version(), pg_database.datname FROM pg_database WHERE datname = @databaseName;";

                var sqlParameter = new NpgsqlParameter()
                {
                    ParameterName = "@databaseName",
                    Value = connection.Database
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
                    connection.Close();
            }
            return (dbName, dbVersion);
        }

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

            var tableNameNormalized = ParserName.Parse(tableName, "\"").Quoted().Normalized().ToString();
            var tableNameString = ParserName.Parse(tableName, "\"").ToString();

            var schemaNameString = "public";
            if (!string.IsNullOrEmpty(schemaName))
            {
                schemaNameString = ParserName.Parse(schemaName, "\"").ToString();
                schemaNameString = string.IsNullOrWhiteSpace(schemaNameString) ? "public" : schemaNameString;
            }

            var syncTable = new SyncTable(tableNameNormalized);
            using (var command = new NpgsqlCommand(commandColumn, connection))
            {

                var parameter = command.CreateParameter();
                parameter.ParameterName = "@TABLENAME";
                parameter.Value = tableNameString;
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
                    connection.Close();
            }
            return syncTable;
        }

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

            var tableNameNormalized = ParserName.Parse(tableName, "\"").Quoted().Normalized().ToString();
            var tableNameString = ParserName.Parse(tableName, "\"").ToString();

            var schemaNameString = ParserName.Parse(schemaName, "\"").ToString();
            // default as public
            schemaNameString = string.IsNullOrEmpty(schemaNameString) ? "public" : schemaNameString;

            var syncTable = new SyncTable(tableNameNormalized, schemaNameString);

            using (var sqlCommand = new NpgsqlCommand(commandRelations, connection))
            {

                var parameter = sqlCommand.CreateParameter();
                parameter.ParameterName = "@tableName";
                parameter.Value = tableNameString;
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
                    connection.Close();
            }
            return syncTable;
        }

        public static async Task<SyncTable> GetTableAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string tableName, string schemaName)
        {

            var tableNameNormalized = ParserName.Parse(tableName, "\"").Quoted().Normalized().ToString();
            var tableNameString = ParserName.Parse(tableName, "\"");

            var schemaNameString = "public";
            if (!string.IsNullOrEmpty(schemaName))
            {
                schemaNameString = ParserName.Parse(schemaName, "\"").ToString();
                schemaNameString = string.IsNullOrWhiteSpace(schemaNameString) ? "public" : schemaNameString;
            }

            var command = $"Select * from \"{schemaNameString}\".\"{tableNameString}\"";

            var syncTable = new SyncTable(tableNameNormalized, schemaNameString);

            using var npgCommand = new NpgsqlCommand(command, connection);

            bool alreadyOpened = connection.State == ConnectionState.Open;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            npgCommand.Transaction = transaction;

            using (var reader = await npgCommand.ExecuteReaderAsync().ConfigureAwait(false))
                syncTable.Load(reader);

            if (!alreadyOpened)
                connection.Close();

            return syncTable;
        }
        public static async Task<SyncTable> GetTableDefinitionAsync(string tableName, string schemaName, NpgsqlConnection connection, NpgsqlTransaction transaction)
        {
            var command = @"
                            SELECT TABLE_NAME as TableName,
	                            TABLE_SCHEMA as SchemaName
                            FROM INFORMATION_SCHEMA.TABLES
                            WHERE TABLE_TYPE = 'BASE TABLE'
                                and table_name=@tableName and table_schema=@schemaName
	                            AND TABLE_SCHEMA not in ('information_schema','pg_catalog');";

            var tableNameNormalized = ParserName.Parse(tableName).Quoted().Normalized().ToString();
            var tableNameString = ParserName.Parse(tableName).ToString();

            var schemaNameString = "public";
            if (!string.IsNullOrEmpty(schemaName))
            {
                schemaNameString = ParserName.Parse(schemaName).ToString();
                schemaNameString = string.IsNullOrWhiteSpace(schemaNameString) ? "public" : schemaNameString;
            }

            var syncTable = new SyncTable(tableNameNormalized, schemaNameString);

            using (var npgsqlCommand = new NpgsqlCommand(command, connection))
            {
                var parameter = npgsqlCommand.CreateParameter();
                parameter.ParameterName = "@tableName";
                parameter.Value = tableNameString;
                npgsqlCommand.Parameters.Add(parameter);

                parameter = npgsqlCommand.CreateParameter();
                parameter.ParameterName = "@schemaName";
                parameter.Value = schemaNameString;
                npgsqlCommand.Parameters.Add(parameter);

                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                npgsqlCommand.Transaction = transaction;

                using (var reader = await npgsqlCommand.ExecuteReaderAsync().ConfigureAwait(false))
                    syncTable.Load(reader);

                if (!alreadyOpened)
                    connection.Close();

            }
            return syncTable;
        }
        public static async Task<SyncTable> GetTriggerAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string triggerName, string schemaName)
        {
            var command = @"
                            SELECT TRIGGER_NAME
                            FROM INFORMATION_SCHEMA.TRIGGERS
                            WHERE TRIGGER_NAME = @TRIGGERNAME
	                            AND TRIGGER_SCHEMA = @SCHEMANAME;";


            var triggerNameNormalized = ParserName.Parse(triggerName, "\"").Quoted().Normalized().ToString();
            var triggerNameString = ParserName.Parse(triggerName, "\"").ToString();

            var schemaNameString = "public";
            if (!string.IsNullOrEmpty(schemaName))
            {
                schemaNameString = ParserName.Parse(schemaName, "\"").ToString();
                schemaNameString = string.IsNullOrWhiteSpace(schemaNameString) ? "public" : schemaNameString;
            }

            var syncTable = new SyncTable(triggerNameNormalized, schemaNameString);

            using (var npgsqlCommand = new NpgsqlCommand(command, connection))
            {
                var parameter = npgsqlCommand.CreateParameter();
                parameter.ParameterName = "@TRIGGERNAME";
                parameter.Value = triggerNameString;
                npgsqlCommand.Parameters.Add(parameter);

                parameter = npgsqlCommand.CreateParameter();
                parameter.ParameterName = "@SCHEMANAME";
                parameter.Value = schemaNameString;
                npgsqlCommand.Parameters.Add(parameter);


                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                if (transaction != null)
                    npgsqlCommand.Transaction = transaction;

                using (var reader = await npgsqlCommand.ExecuteReaderAsync().ConfigureAwait(false))
                    syncTable.Load(reader);

                if (!alreadyOpened)
                    connection.Close();

            }
            return syncTable;
        }

        public static string GetUnquotedSqlSchemaName(ParserName parser)
        {
            if (string.IsNullOrEmpty(parser.SchemaName))
                return "public";

            return parser.SchemaName;
        }

        public static string JoinTwoTablesOnClause(IEnumerable<string> columns, string leftName, string rightName)
        {
            var stringBuilder = new StringBuilder();
            string strRightName = (string.IsNullOrEmpty(rightName) ? string.Empty : string.Concat(rightName, "."));
            string strLeftName = (string.IsNullOrEmpty(leftName) ? string.Empty : string.Concat(leftName, "."));

            string str = "";
            foreach (var column in columns)
            {
                var quotedColumn = ParserName.Parse(column, "\"").Quoted().ToString();

                stringBuilder.Append(str);
                stringBuilder.Append(strLeftName);
                stringBuilder.Append(quotedColumn);
                stringBuilder.Append(" = ");
                stringBuilder.Append(strRightName);
                stringBuilder.Append(quotedColumn);

                str = " AND ";
            }
            return stringBuilder.ToString();
        }

        public static async Task<bool> ProcedureExistsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string quotedProcedureName)
        {
            bool tableExist;

            var proname = ParserName.Parse(quotedProcedureName, "\"").ObjectName;


            using (DbCommand dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = "select count(*) from pg_proc where proname = @proname;";

                NpgsqlParameter sqlParameter = new NpgsqlParameter()
                {
                    ParameterName = "@proname",
                    Value = proname
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
                    connection.Close();
            }
            return tableExist;
        }

        public static async Task RenameTableAsync(string tableName, string schemaName, string newTableName, string newSchemaName, NpgsqlConnection connection, NpgsqlTransaction transaction)
        {
            var pTableName = ParserName.Parse(tableName).Quoted().ToString();
            var pSchemaName = ParserName.Parse(schemaName).Quoted().ToString();

            var pNewTableName = ParserName.Parse(newTableName).Quoted().ToString();
            var pNewSchemaName = ParserName.Parse(newSchemaName).Quoted().ToString();

            var quotedTableName = string.IsNullOrEmpty(pSchemaName) ? pTableName : $"{pSchemaName}.{pTableName}";
            var quotedNewTableName = string.IsNullOrEmpty(pNewSchemaName) ? pNewTableName : $"{pNewSchemaName}.{pNewTableName}";

            var commandText = $"exec sp_rename '{quotedTableName}', '{quotedNewTableName}';";

            using var NpgsqlCommand = new NpgsqlCommand(commandText, connection);

            bool alreadyOpened = connection.State == ConnectionState.Open;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            NpgsqlCommand.Transaction = transaction;

            await NpgsqlCommand.ExecuteNonQueryAsync().ConfigureAwait(false);

            if (!alreadyOpened)
                connection.Close();
        }
        public static async Task<bool> SchemaExistsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string schemaName)
        {
            bool schemaExist;
            using (DbCommand dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = "SELECT count(*) FROM information_schema.schemata where schema_name = @schemaName";

                var sqlParameter = new NpgsqlParameter()
                {
                    ParameterName = "@schemaName",
                    Value = schemaName
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
                    connection.Close();

            }
            return schemaExist;
        }

        public static async Task<bool> TableExistsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string quotedTableName, string schemaName)
        {
            bool tableExist;
            var tableName = ParserName.Parse(quotedTableName, "\"").ObjectName;
            var pSchemaName = string.IsNullOrEmpty(schemaName) ? "public" : schemaName;

            using (DbCommand dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = "SELECT count(*) FROM information_schema.tables " +
                                        "WHERE table_type = 'BASE TABLE' AND table_schema != 'pg_catalog' AND table_schema != 'information_schema' " +
                                        "AND table_name = @tableName AND table_schema = @schemaName";

                NpgsqlParameter sqlParameter = new NpgsqlParameter()
                {
                    ParameterName = "@tableName",
                    DbType = DbType.String,
                    Value = tableName
                };
                dbCommand.Parameters.Add(sqlParameter);

                sqlParameter = new NpgsqlParameter()
                {
                    ParameterName = "@schemaName",
                    DbType = DbType.String,
                    Value = pSchemaName
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
                    connection.Close();



            }
            return tableExist;
        }
        public static async Task<bool> TriggerExistsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string quotedTriggerName)
        {
            bool triggerExist;
            var triggerName = ParserName.Parse(quotedTriggerName, "\"").ToString();

            var commandText = "Select count(*) from Information_schema.triggers " +
                              "Where trigger_name = @triggerName and trigger_schema = @schemaName";

            using (var sqlCommand = new NpgsqlCommand(commandText, connection))
            {

                var parameter = sqlCommand.CreateParameter();
                parameter.ParameterName = "@triggerName";
                parameter.Value = triggerName;
                sqlCommand.Parameters.Add(parameter);

                parameter = sqlCommand.CreateParameter();
                parameter.ParameterName = "@schemaName";
                parameter.Value = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(ParserName.Parse(quotedTriggerName, "\""));
                sqlCommand.Parameters.Add(parameter);


                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                if (transaction != null)
                    sqlCommand.Transaction = transaction;

                var result = await sqlCommand.ExecuteScalarAsync().ConfigureAwait(false);

                triggerExist = (long)result != 0;

                if (!alreadyOpened)
                    connection.Close();


            }
            return triggerExist;
        }
        public static Task<bool> TypeExistsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string quotedTypeName)
        {
            throw new NotImplementedException();
        }

        public static string WhereColumnAndParameters(IEnumerable<SyncColumn> primaryKeys, string fromPrefix, string mysql_prefix)
        {
            StringBuilder stringBuilder = new StringBuilder();
            string strFromPrefix = string.IsNullOrEmpty(fromPrefix) ? string.Empty : string.Concat(fromPrefix, ".");
            string str1 = "";
            foreach (var column in primaryKeys)
            {
                var unquotedColumn = ParserName.Parse(column, "\"").Quoted().Normalized().ToString();
                var paramUnquotedColumn = ParserName.Parse($"{mysql_prefix}{column.ColumnName}", "\"").Quoted().Normalized().ToString();

                stringBuilder.Append(str1);
                stringBuilder.Append(strFromPrefix);
                stringBuilder.Append(unquotedColumn);
                stringBuilder.Append(" = ");
                stringBuilder.Append($"{paramUnquotedColumn}");
                str1 = " AND ";
            }
            return stringBuilder.ToString();
        }
    }
}
