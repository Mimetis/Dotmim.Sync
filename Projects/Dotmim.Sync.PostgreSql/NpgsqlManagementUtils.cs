using Dotmim.Sync.Builders;

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Text;
using System.Threading.Tasks;
using Npgsql;

namespace Dotmim.Sync.Postgres
{
    public static class NpgsqlManagementUtils
    {


        /// <summary>
        /// Get Table
        /// </summary>
        public static async Task<SyncTable> GetTableAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string tableName, string schemaName)
        {

            var command = $"SELECT * " +
                           " FROM information_schema.tables " +
                           " WHERE table_type = 'BASE TABLE' " +
                           " AND table_schema != 'pg_catalog' AND table_schema != 'information_schema' " +
                           " AND table_name=@tableName AND table_schema=@schemaName " +
                           " ORDER BY table_schema, table_name " +
                           " LIMIT 1";

            var tableNameNormalized = ParserName.Parse(tableName).Unquoted().Normalized().ToString();
            var tableNameString = ParserName.Parse(tableName).ToString();

            var schemaNameString = "public";
            if (!string.IsNullOrEmpty(schemaName))
            {
                schemaNameString = ParserName.Parse(schemaName).ToString();
                schemaNameString = string.IsNullOrWhiteSpace(schemaNameString) ? "public" : schemaNameString;
            }

            var syncTable = new SyncTable(tableNameNormalized, schemaNameString);

            using (var sqlCommand = new NpgsqlCommand(command, connection))
            {
                sqlCommand.Parameters.AddWithValue("@tableName", tableNameString);
                sqlCommand.Parameters.AddWithValue("@schemaName", schemaNameString);

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

        /// <summary>
        /// Get Table
        /// </summary>
        public static async Task<SyncTable> GetTriggerAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string triggerName, string schemaName)
        {
            var command = $"Select trigger_name from Information_schema.triggers " +
                           "Where trigger_name = @triggerName and trigger_schema = @schemaName";


            var triggerNameNormalized = ParserName.Parse(triggerName).Unquoted().Normalized().ToString();
            var triggerNameString = ParserName.Parse(triggerName).ToString();

            var schemaNameString = "public";
            if (!string.IsNullOrEmpty(schemaName))
            {
                schemaNameString = ParserName.Parse(schemaName).ToString();
                schemaNameString = string.IsNullOrWhiteSpace(schemaNameString) ? "public" : schemaNameString;
            }

            var syncTable = new SyncTable(triggerNameNormalized, schemaNameString);

            using (var npgsqlCommand = new NpgsqlCommand(command, connection))
            {
                npgsqlCommand.Parameters.AddWithValue("@triggerName", triggerNameString);
                npgsqlCommand.Parameters.AddWithValue("@schemaName", schemaNameString);

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


        /// <summary>
        /// Get columns for table
        /// </summary>
        public static async Task<SyncTable> GetColumnsForTableAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string tableName, string schemaName)
        {
            var commandColumn = $"Select column_name, " +
                                $"ordinal_position,  " +
                                $"data_type,  " +
                                $"character_maximum_length,  " +
                                $"numeric_precision,  " +
                                $"numeric_scale,  " +
                                $"is_nullable,  " +
                                $"is_generated,  " +
                                $"is_identity,  " +
                                $"ind.is_unique,  " +
                                $"identity_start, " +
                                $"identity_increment " +
                                $"from information_schema.columns " +
                                $"where table_name = @tableName and table_schema = @schemaName ";

            var tableNameNormalized = ParserName.Parse(tableName).Unquoted().Normalized().ToString();
            var tableNameString = ParserName.Parse(tableName).ToString();

            var schemaNameString = "public";
            if (!string.IsNullOrEmpty(schemaName))
            {
                schemaNameString = ParserName.Parse(schemaName).ToString();
                schemaNameString = string.IsNullOrWhiteSpace(schemaNameString) ? "public" : schemaNameString;
            }

            var syncTable = new SyncTable(tableNameNormalized);
            using (var sqlCommand = new NpgsqlCommand(commandColumn, connection))
            {
                sqlCommand.Parameters.AddWithValue("@tableName", tableNameString);
                sqlCommand.Parameters.AddWithValue("@schemaName", schemaNameString);

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

        /// <summary>
        /// Get primary keys columns
        /// </summary>
        public static async Task<SyncTable> GetPrimaryKeysForTableAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string tableName, string schemaName)
        {

            var commandColumn = @"SELECT ccu.column_name, tc.constraint_name, c.ordinal_position
                                  FROM information_schema.table_constraints tc 
                                  JOIN information_schema.constraint_column_usage AS ccu on ccu.constraint_schema = tc.constraint_schema 
                                    AND ccu.constraint_name = tc.constraint_name
                                  JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema
                                    AND tc.table_name = c.table_name AND ccu.column_name = c.column_name
                                  WHERE tc.constraint_type = 'PRIMARY KEY' and tc.table_name = @tableName and tc.table_schema=@schemaName;";

            var tableNameNormalized = ParserName.Parse(tableName).Unquoted().Normalized().ToString();
            var tableNameString = ParserName.Parse(tableName).ToString();

            var schemaNameString = "public";
            if (!string.IsNullOrEmpty(schemaName))
            {
                schemaNameString = ParserName.Parse(schemaName).ToString();
                schemaNameString = string.IsNullOrWhiteSpace(schemaNameString) ? "public" : schemaNameString;
            }

            var syncTable = new SyncTable(tableNameNormalized);
            using (var command = new NpgsqlCommand(commandColumn, connection))
            {
                command.Parameters.AddWithValue("@tableName", tableNameString);
                command.Parameters.AddWithValue("@schemaName", schemaNameString);

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

            var commandRelations = @"
                SELECT tc.constraint_name AS ForeignKey,
                    tc.table_schema  AS SchemaName,
                    tc.table_name AS TableName,
                    kcu.column_name AS ColumnName,
                    ccu.table_schema AS ReferenceSchemaName,
                    ccu.table_name  AS ReferenceTableName,
                    ccu.column_name AS ReferenceColumnName
                FROM information_schema.table_constraints AS tc 
                JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name  AND ccu.table_schema = tc.table_schema
                WHERE tc.constraint_type = 'FOREIGN KEY' and tc.table_name = @tableName AND tc.table_schema = @schemaName";

            var tableNameNormalized = ParserName.Parse(tableName).Unquoted().Normalized().ToString();
            var tableNameString = ParserName.Parse(tableName).ToString();

            var schemaNameString = ParserName.Parse(schemaName).ToString();
            // default as dbo
            schemaNameString = string.IsNullOrEmpty(schemaNameString) ? "dbo" : schemaNameString;

            var syncTable = new SyncTable(tableNameNormalized, schemaNameString);

            using (var sqlCommand = new NpgsqlCommand(commandRelations, connection))
            {
                sqlCommand.Parameters.AddWithValue("@tableName", tableNameString);
                sqlCommand.Parameters.AddWithValue("@schemaName", schemaNameString);

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

        public static async Task DropProcedureIfExistsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, int commandTimout, string quotedProcedureName)
        {
            throw new NotImplementedException();
        }

        public static async Task DropTableIfExistsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, int commandTimeout, string quotedTableName)
        {
            var tableName = ParserName.Parse(quotedTableName).ToString();

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

        public static async Task DropTriggerIfExistsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, int commandTimeout, 
            string quotedTriggerName, string quotedTableName)
        {
            var triggerName = ParserName.Parse(quotedTriggerName).ToString();

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


        public static string GetUnquotedSqlSchemaName(ParserName parser)
        {
            if (string.IsNullOrEmpty(parser.SchemaName))
                return "public";

            return parser.SchemaName;
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
                        reader.Read();

                        dbVersion = reader.GetString(0);
                        dbName = reader.GetString(1);
                    }
                }

                if (!alreadyOpened)
                    connection.Close();
            }
            return (dbName, dbVersion);
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

                tableExist = (int)result != 0;

                if (!alreadyOpened)
                    connection.Close();
            }
            return tableExist;
        }


        public static Task<bool> ProcedureExistsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string quotedProcedureName)
        {
            throw new NotImplementedException();
        }

        public static async Task<bool> TableExistsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string quotedTableName)
        {
            bool tableExist;
            var tableName = ParserName.Parse(quotedTableName).ObjectName;

            using (DbCommand dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = "SELECT count(*) FROM information_schema.tables " +
                                        "WHERE table_type = 'BASE TABLE' AND table_schema != 'pg_catalog' AND table_schema != 'information_schema' " +
                                        "AND table_name = @tableName AND table_schema = @schemaName";

                NpgsqlParameter sqlParameter = new NpgsqlParameter()
                {
                    ParameterName = "@tableName",
                    Value = tableName
                };
                dbCommand.Parameters.Add(sqlParameter);

                sqlParameter = new NpgsqlParameter()
                {
                    ParameterName = "@schemaName",
                    Value = GetUnquotedSqlSchemaName(ParserName.Parse(quotedTableName))
                };
                dbCommand.Parameters.Add(sqlParameter);

                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                if (transaction != null)
                    dbCommand.Transaction = transaction;

                var result = await dbCommand.ExecuteScalarAsync().ConfigureAwait(false);

                tableExist = (int)result != 0;

                if (!alreadyOpened)
                    connection.Close();



            }
            return tableExist;
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

                schemaExist = (int)result != 0;

                if (!alreadyOpened)
                    connection.Close();

            }
            return schemaExist;
        }

        public static async Task<bool> TriggerExistsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string quotedTriggerName)
        {
            bool triggerExist;
            var triggerName = ParserName.Parse(quotedTriggerName).ToString();

            var commandText = "Select count(*) from Information_schema.triggers " +
                              "Where trigger_name = @triggerName and trigger_schema = @schemaName";

            using (var sqlCommand = new NpgsqlCommand(commandText, connection))
            {
                sqlCommand.Parameters.AddWithValue("@triggerName", triggerName);
                sqlCommand.Parameters.AddWithValue("@schemaName", NpgsqlManagementUtils.GetUnquotedSqlSchemaName(ParserName.Parse(quotedTriggerName)));

                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                if (transaction != null)
                    sqlCommand.Transaction = transaction;

                var result = await sqlCommand.ExecuteScalarAsync().ConfigureAwait(false);

                triggerExist = (int)result != 0;

                if (!alreadyOpened)
                    connection.Close();


            }
            return triggerExist;
        }

        public static Task<bool> TypeExistsAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string quotedTypeName)
        {
            throw new NotImplementedException();
        }

        public static string JoinTwoTablesOnClause(IEnumerable<string> columns, string leftName, string rightName)
        {
            var stringBuilder = new StringBuilder();
            string strRightName = (string.IsNullOrEmpty(rightName) ? string.Empty : string.Concat(rightName, "."));
            string strLeftName = (string.IsNullOrEmpty(leftName) ? string.Empty : string.Concat(leftName, "."));

            string str = "";
            foreach (var column in columns)
            {
                var quotedColumn = ParserName.Parse(column).Quoted().ToString();

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

        public static string ColumnsAndParameters(IEnumerable<string> columns, string fromPrefix)
        {
            StringBuilder stringBuilder = new StringBuilder();
            string strFromPrefix = (string.IsNullOrEmpty(fromPrefix) ? string.Empty : string.Concat(fromPrefix, "."));
            string str1 = "";
            foreach (var column in columns)
            {
                var quotedColumn = ParserName.Parse(column).Quoted().ToString();
                var unquotedColumn = ParserName.Parse(column).Unquoted().Normalized().ToString();

                stringBuilder.Append(str1);
                stringBuilder.Append(strFromPrefix);
                stringBuilder.Append(quotedColumn);
                stringBuilder.Append(" = ");
                stringBuilder.Append($"@{unquotedColumn}");
                str1 = " AND ";
            }
            return stringBuilder.ToString();
        }

        public static string CommaSeparatedUpdateFromParameters(SyncTable table, string fromPrefix = "")
        {
            StringBuilder stringBuilder = new StringBuilder();
            string strFromPrefix = (string.IsNullOrEmpty(fromPrefix) ? string.Empty : string.Concat(fromPrefix, "."));
            string strSeparator = "";
            foreach (var mutableColumn in table.GetMutableColumns(false))
            {
                var quotedColumn = ParserName.Parse(mutableColumn).Quoted().ToString();
                var unquotedColumn = ParserName.Parse(mutableColumn).Unquoted().Normalized().ToString();

                stringBuilder.AppendLine($"{strSeparator} {strFromPrefix}{quotedColumn} = @{unquotedColumn}");
                strSeparator = ", ";
            }
            return stringBuilder.ToString();

        }




    }
}
