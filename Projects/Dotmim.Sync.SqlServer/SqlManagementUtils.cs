using Dotmim.Sync.DatabaseStringParsers;
using Dotmim.Sync.SqlServer.Builders;
using Microsoft.Data.SqlClient;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.SqlServer
{
    /// <summary>
    /// Sql Management Utils.
    /// </summary>
    public static class SqlManagementUtils
    {

        /// <summary>
        /// Get all Tables.
        /// </summary>
        public static async Task<SyncSetup> GetAllTablesAsync(SqlConnection connection, SqlTransaction transaction)
        {
            var command = $"Select tbl.name as TableName, " +
                          $"sch.name as SchemaName " +
                          $"  from sys.tables as tbl  " +
                          $"  Inner join sys.schemas as sch on tbl.schema_id = sch.schema_id where tbl.is_ms_shipped = 0;";

            var syncSetup = new SyncSetup();

            using (var sqlCommand = new SqlCommand(command, connection))
            {
                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                sqlCommand.Transaction = transaction;

                using (var reader = await sqlCommand.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    while (await reader.ReadAsync().ConfigureAwait(false))
                    {
                        var tableName = reader.GetString(0);
                        var schemaName = reader.GetString(1) == "dbo" ? null : reader.GetString(1);
                        var setupTable = new SetupTable(tableName, schemaName);
                        syncSetup.Tables.Add(setupTable);
                    }
                }

                foreach (var setupTable in syncSetup.Tables)
                {
                    var syncTableColumnsList = await GetColumnsForTableAsync(setupTable.TableName, setupTable.SchemaName, connection, transaction).ConfigureAwait(false);

                    foreach (var column in syncTableColumnsList.Rows)
                        setupTable.Columns.Add(column["name"].ToString());
                }

                if (!alreadyOpened)
#if NET6_0_OR_GREATER
                    await connection.CloseAsync().ConfigureAwait(false);
#else
                    connection.Close();
#endif
            }

            return syncSetup;
        }

        /// <summary>
        /// Get Table.
        /// </summary>
        public static async Task<SyncTable> GetTableDefinitionAsync(string tableName, string schemaName, SqlConnection connection, SqlTransaction transaction)
        {
            var command = $"Select top 1 tbl.name as TableName, " +
                          $"sch.name as SchemaName " +
                          $"  from sys.tables as tbl  " +
                          $"  Inner join sys.schemas as sch on tbl.schema_id = sch.schema_id " +
                          $"  Where tbl.name = @tableName and sch.name = @schemaName ";

            var schemaNameString = string.IsNullOrEmpty(schemaName) ? "dbo" : schemaName;

            var syncTable = new SyncTable(tableName, schemaNameString);

            using (var sqlCommand = new SqlCommand(command, connection))
            {
                sqlCommand.Parameters.AddWithValue("@tableName", tableName);
                sqlCommand.Parameters.AddWithValue("@schemaName", schemaNameString);

                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                sqlCommand.Transaction = transaction;

                using (var reader = await sqlCommand.ExecuteReaderAsync().ConfigureAwait(false))
                    syncTable.Load(reader);

                if (!alreadyOpened)
#if NET6_0_OR_GREATER
                    await connection.CloseAsync().ConfigureAwait(false);
#else
                    connection.Close();
#endif
            }

            return syncTable;
        }

        /// <summary>
        /// Get Table.
        /// </summary>
        public static async Task<SyncTable> GetTableAsync(string tableName, string schemaName, SqlConnection connection, SqlTransaction transaction)
        {

            var sName = string.IsNullOrEmpty(schemaName) ? "[dbo]" : schemaName;
            var commandText = $"Select * from {sName}.{tableName};";
            var syncTable = new SyncTable(tableName, sName);

            using var sqlCommand = new SqlCommand(commandText, connection);

            bool alreadyOpened = connection.State == ConnectionState.Open;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            sqlCommand.Transaction = transaction;

            using (var reader = await sqlCommand.ExecuteReaderAsync().ConfigureAwait(false))
                syncTable.Load(reader);

            if (!alreadyOpened)
#if NET6_0_OR_GREATER
                await connection.CloseAsync().ConfigureAwait(false);
#else
                connection.Close();
#endif

            return syncTable;
        }

        /// <summary>
        /// Rename Table.
        /// </summary>
        public static async Task RenameTableAsync(string tableName, string schemaName, string newTableName, string newSchemaName, SqlConnection connection, SqlTransaction transaction)
        {
            var quotedTableName = string.IsNullOrEmpty(schemaName) ? tableName : $"{schemaName}.{tableName}";
            var quotedNewTableName = string.IsNullOrEmpty(newSchemaName) ? newTableName : $"{newSchemaName}.{newTableName}";

            var commandText = $"exec sp_rename '{quotedTableName}', '{quotedNewTableName}';";

            using var sqlCommand = new SqlCommand(commandText, connection);

            bool alreadyOpened = connection.State == ConnectionState.Open;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            sqlCommand.Transaction = transaction;

            await sqlCommand.ExecuteNonQueryAsync().ConfigureAwait(false);

            if (!alreadyOpened)
#if NET6_0_OR_GREATER
                await connection.CloseAsync().ConfigureAwait(false);
#else
                connection.Close();
#endif
        }

        /// <summary>
        /// Get Table.
        /// </summary>
        public static async Task<SyncTable> GetTriggerAsync(string triggerName, string schemaName, SqlConnection connection, SqlTransaction transaction)
        {
            var command = $"SELECT tr.name FROM sys.triggers tr " +
                           "JOIN sys.tables t ON tr.parent_id = t.object_id " +
                           "JOIN sys.schemas s ON t.schema_id = s.schema_id " +
                           "WHERE tr.name = @triggerName and s.name = @schemaName";

            var schemaNameString = string.IsNullOrEmpty(schemaName) ? "dbo" : schemaName;

            var syncTable = new SyncTable(triggerName, schemaNameString);

            using (var sqlCommand = new SqlCommand(command, connection))
            {
                sqlCommand.Parameters.AddWithValue("@triggerName", triggerName);
                sqlCommand.Parameters.AddWithValue("@schemaName", schemaNameString);

                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                sqlCommand.Transaction = transaction;

                using (var reader = await sqlCommand.ExecuteReaderAsync().ConfigureAwait(false))
                    syncTable.Load(reader);

                if (!alreadyOpened)
#if NET6_0_OR_GREATER
                    await connection.CloseAsync().ConfigureAwait(false);
#else
                    connection.Close();
#endif
            }

            return syncTable;
        }

        /// <summary>
        /// Get columns for table.
        /// </summary>
        public static async Task<SyncTable> GetColumnsForTableAsync(string tableName, string schemaName, SqlConnection connection, SqlTransaction transaction)
        {

            var commandColumn = $"Select col.name as name, " +
                                $"col.column_id,  " +
                                $"typ.name as [type],  " +
                                $"col.max_length,  " +
                                $"col.precision,  " +
                                $"col.scale,  " +
                                $"col.is_nullable,  " +
                                $"col.is_computed,  " +
                                $"col.is_identity,  " +
                                $"ind.is_unique,  " +
                                $"ident_seed(sch.name + '.' + tbl.name) AS seed, " +
                                $"ident_incr(sch.name + '.' + tbl.name) AS step, " +
                                $"object_definition(col.default_object_id) AS defaultvalue " +
                                $"  from sys.columns as col " +
                                $"  Inner join sys.tables as tbl on tbl.object_id = col.object_id " +
                                $"  Inner join sys.schemas as sch on tbl.schema_id = sch.schema_id " +
                                $"  Inner Join sys.systypes typ on typ.xusertype = col.system_type_id " +
                                $"  Left outer join sys.indexes ind on ind.object_id = col.object_id and ind.index_id = col.column_id " +
                                $"  Where tbl.name = @tableName and sch.name = @schemaName ";

            var schemaNameString = string.IsNullOrEmpty(schemaName) ? "dbo" : schemaName;

            var syncTable = new SyncTable(tableName);
            using (var sqlCommand = new SqlCommand(commandColumn, connection))
            {
                sqlCommand.Parameters.AddWithValue("@tableName", tableName);
                sqlCommand.Parameters.AddWithValue("@schemaName", schemaNameString);

                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                sqlCommand.Transaction = transaction;

                using (var reader = await sqlCommand.ExecuteReaderAsync().ConfigureAwait(false))
                    syncTable.Load(reader);

                if (!alreadyOpened)
#if NET6_0_OR_GREATER
                    await connection.CloseAsync().ConfigureAwait(false);
#else
                    connection.Close();
#endif
            }

            return syncTable;
        }

        /// <summary>
        /// Get columns for table.
        /// </summary>
        public static async Task<SyncTable> GetPrimaryKeysForTableAsync(string tableName, string schemaName, SqlConnection connection, SqlTransaction transaction)
        {

            var commandColumn = @"select ind.name, col.name as columnName, ind_col.column_id, ind_col.key_ordinal 
                                  from sys.indexes ind
                                  left outer join sys.index_columns ind_col on ind_col.object_id = ind.object_id and ind_col.index_id = ind.index_id
                                  inner join sys.columns col on col.object_id = ind_col.object_id and col.column_id = ind_col.column_id
                                  inner join sys.tables tbl on tbl.object_id = ind.object_id
                                  inner join sys.schemas as sch on tbl.schema_id = sch.schema_id
                                  where tbl.name = @tableName and sch.name = @schemaName and ind.index_id >= 0 and ind.type <> 3 and ind.type <> 4 and ind.is_hypothetical = 0 and ind.is_primary_key = 1
                                  order by ind_col.column_id";

            var schemaNameString = string.IsNullOrEmpty(schemaName) ? "dbo" : schemaName;

            var syncTable = new SyncTable(tableName);
            using (var sqlCommand = new SqlCommand(commandColumn, connection))
            {
                sqlCommand.Parameters.AddWithValue("@tableName", tableName);
                sqlCommand.Parameters.AddWithValue("@schemaName", schemaNameString);

                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                sqlCommand.Transaction = transaction;

                using (var reader = await sqlCommand.ExecuteReaderAsync().ConfigureAwait(false))
                    syncTable.Load(reader);

                if (!alreadyOpened)
#if NET6_0_OR_GREATER
                    await connection.CloseAsync().ConfigureAwait(false);
#else
                    connection.Close();
#endif
            }

            return syncTable;
        }

        /// <summary>
        /// Get relations for table.
        /// </summary>
        public static async Task<SyncTable> GetRelationsForTableAsync(SqlConnection connection, SqlTransaction transaction, string tableName, string schemaName)
        {

            var commandRelations = @"
                SELECT f.name AS ForeignKey,
                    constraint_column_id as ForeignKeyOrder,
                    SCHEMA_NAME (f.schema_id)  AS SchemaName,
                    OBJECT_NAME(f.parent_object_id) AS TableName,
                    COL_NAME(fc.parent_object_id, fc.parent_column_id) AS ColumnName,
                    SCHEMA_NAME (reft.schema_id) AS ReferenceSchemaName,
                    OBJECT_NAME (f.referenced_object_id)  AS ReferenceTableName,
                    COL_NAME(fc.referenced_object_id, fc.referenced_column_id) AS ReferenceColumnName
                FROM sys.foreign_keys AS f
                INNER JOIN sys.foreign_key_columns AS fc ON f.OBJECT_ID = fc.constraint_object_id
                INNER JOIN sys.tables reft on reft.object_id =  f.referenced_object_id
                WHERE OBJECT_NAME(f.parent_object_id) = @tableName AND SCHEMA_NAME(f.schema_id) = @schemaName";

            var schemaNameString = string.IsNullOrEmpty(schemaName) ? "dbo" : schemaName;

            var syncTable = new SyncTable(tableName, schemaNameString);

            using (var sqlCommand = new SqlCommand(commandRelations, connection))
            {
                sqlCommand.Parameters.AddWithValue("@tableName", tableName);
                sqlCommand.Parameters.AddWithValue("@schemaName", schemaNameString);

                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                sqlCommand.Transaction = transaction;

                using (var reader = await sqlCommand.ExecuteReaderAsync().ConfigureAwait(false))
                    syncTable.Load(reader);

                if (!alreadyOpened)
#if NET6_0_OR_GREATER
                    await connection.CloseAsync().ConfigureAwait(false);
#else
                    connection.Close();
#endif
            }

            return syncTable;
        }

        /// <summary>
        /// Drop Table if exists.
        /// </summary>
        public static async Task DropTableIfExistsAsync(string tableName, string schemaName, SqlConnection connection, SqlTransaction transaction)
        {
            var schemaNameString = string.IsNullOrEmpty(schemaName) ? "dbo" : schemaName;

            var quotedTableName = $"{schemaName}.{tableName}";

            var commandText = $"IF EXISTS " +
                $"(SELECT t.name FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE t.name = @tableName AND s.name = @schemaName) " +
                $"DROP TABLE {quotedTableName}";

            using var sqlCommand = new SqlCommand(commandText, connection);

            sqlCommand.Parameters.AddWithValue("@tableName", tableName);
            sqlCommand.Parameters.AddWithValue("@schemaName", schemaName);

            bool alreadyOpened = connection.State == ConnectionState.Open;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            sqlCommand.Transaction = transaction;

            await sqlCommand.ExecuteNonQueryAsync().ConfigureAwait(false);

            if (!alreadyOpened)
#if NET6_0_OR_GREATER
                await connection.CloseAsync().ConfigureAwait(false);
#else
                connection.Close();
#endif
        }

        /// <summary>
        /// Gets the quoted table schema. if empty, return dbo.
        /// </summary>
        public static string GetUnquotedSqlSchemaName(TableParser parser) => string.IsNullOrEmpty(parser.SchemaName) ? "dbo" : parser.SchemaName;

        /// <summary>
        /// Gets the quoted table schema. if empty, return dbo.
        /// </summary>
        public static string GetUnquotedSqlSchemaName(ObjectParser parser) => string.IsNullOrEmpty(parser.OwnerName) ? "dbo" : parser.OwnerName;

        /// <summary>
        /// Gets if change tracking is enabled on the database.
        /// </summary>
        public static async Task<bool> IsChangeTrackingEnabledAsync(SqlConnection connection, SqlTransaction transaction)
        {
            bool flag;
            string commandText = @"if (exists(
                                Select* from sys.change_tracking_databases ct
                                Inner join sys.databases d on d.database_id = ct.database_id
                                where d.name = @databaseName)) 
                                Select 1 Else Select 0";

            using (var sqlCommand = new SqlCommand(commandText, connection))
            {
                sqlCommand.Parameters.AddWithValue("@databaseName", connection.Database);

                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                sqlCommand.Transaction = transaction;

                var result = await sqlCommand.ExecuteScalarAsync().ConfigureAwait(false);

                flag = (int)result != 0;

                if (!alreadyOpened)
#if NET6_0_OR_GREATER
                    await connection.CloseAsync().ConfigureAwait(false);
#else
                    connection.Close();
#endif
            }

            return flag;
        }

        /// <summary>
        /// Gets if SQL Server is reachable by trying a simple query on the server.
        /// </summary>
        public static async Task<(string DatabaseName, string EngineVersion)> GetHelloAsync(SqlConnection connection, SqlTransaction transaction)
        {
            string dbName = null;
            string dbVersion = null;

            using (DbCommand dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = "SELECT name, @@VERSION as version FROM sys.databases WHERE name = @databaseName;";

                var sqlParameter = new SqlParameter()
                {
                    ParameterName = "@databaseName",
                    Value = connection.Database,
                };
                dbCommand.Parameters.Add(sqlParameter);

                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                dbCommand.Transaction = transaction;

                using (var reader = await dbCommand.ExecuteReaderAsync().ConfigureAwait(false))
                {
                    if (reader.HasRows)
                    {
                        await reader.ReadAsync().ConfigureAwait(false);

                        dbName = reader.GetString(0);
                        dbVersion = reader.GetString(1);
                    }
                }

                if (!alreadyOpened)
#if NET6_0_OR_GREATER
                    await connection.CloseAsync().ConfigureAwait(false);
#else
                    connection.Close();
#endif
            }

            return (dbName, dbVersion);
        }

        /// <summary>
        /// Check if a database exists.
        /// </summary>
        public static async Task<bool> DatabaseExistsAsync(SqlConnection connection, SqlTransaction transaction)
        {
            bool tableExist;

            using (DbCommand dbCommand = connection.CreateCommand())
            {
                dbCommand.CommandText = "IF EXISTS (SELECT * FROM sys.databases WHERE name = @databaseName) SELECT 1 ELSE SELECT 0";

                var sqlParameter = new SqlParameter()
                {
                    ParameterName = "@databaseName",
                    Value = connection.Database,
                };
                dbCommand.Parameters.Add(sqlParameter);

                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                dbCommand.Transaction = transaction;

                var result = await dbCommand.ExecuteScalarAsync().ConfigureAwait(false);

                tableExist = (int)result != 0;

                if (!alreadyOpened)
#if NET6_0_OR_GREATER
                    await connection.CloseAsync().ConfigureAwait(false);
#else
                    connection.Close();
#endif
            }

            return tableExist;
        }

        /// <summary>
        /// Check if a stored procedure exists.
        /// </summary>
        public static async Task<bool> ProcedureExistsAsync(SqlConnection connection, SqlTransaction transaction,
            string normalizedProcedureName, string normalizedSchemaName)
        {
            bool flag;

            using (var sqlCommand = new SqlCommand("IF EXISTS (SELECT * FROM sys.procedures p JOIN sys.schemas s ON s.schema_id = p.schema_id WHERE p.name = @procName AND s.name = @schemaName) SELECT 1 ELSE SELECT 0", connection))
            {
                sqlCommand.Parameters.AddWithValue("@procName", normalizedProcedureName);
                sqlCommand.Parameters.AddWithValue("@schemaName", normalizedSchemaName);

                bool alreadyOpened = connection.State == ConnectionState.Open;

                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                sqlCommand.Transaction = transaction;

                var result = await sqlCommand.ExecuteScalarAsync().ConfigureAwait(false);

                flag = (int)result != 0;

                if (!alreadyOpened)
#if NET6_0_OR_GREATER
                    await connection.CloseAsync().ConfigureAwait(false);
#else
                    connection.Close();
#endif
            }

            return flag;
        }

        /// <summary>
        /// Check if a table exists.
        /// </summary>
        public static async Task<bool> TableExistsAsync(string tableName, string schemaName, SqlConnection connection, SqlTransaction transaction)
        {
            var pSchemaName = string.IsNullOrEmpty(schemaName) ? "dbo" : schemaName;

            using DbCommand dbCommand = connection.CreateCommand();

            dbCommand.CommandText = "IF EXISTS (SELECT t.name FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE t.name = @tableName AND s.name = @schemaName) SELECT 1 ELSE SELECT 0";

            SqlParameter sqlParameter = new SqlParameter()
            {
                ParameterName = "@tableName",
                Value = tableName,
            };
            dbCommand.Parameters.Add(sqlParameter);

            sqlParameter = new SqlParameter()
            {
                ParameterName = "@schemaName",
                Value = pSchemaName,
            };
            dbCommand.Parameters.Add(sqlParameter);

            bool alreadyOpened = connection.State == ConnectionState.Open;

            if (!alreadyOpened)
                await connection.OpenAsync().ConfigureAwait(false);

            dbCommand.Transaction = transaction;

            var result = await dbCommand.ExecuteScalarAsync().ConfigureAwait(false);

            var tableExist = (int)result != 0;

            if (!alreadyOpened)
#if NET6_0_OR_GREATER
                await connection.CloseAsync().ConfigureAwait(false);
#else
                connection.Close();
#endif

            return tableExist;
        }

        /// <summary>
        /// Returns a join clause for two tables on their primary keys.
        /// </summary>
        public static string JoinTwoTablesOnClause(IEnumerable<string> columns, string leftName, string rightName)
        {
            var stringBuilder = new StringBuilder();
            string strRightName = string.IsNullOrEmpty(rightName) ? string.Empty : string.Concat(rightName, ".");
            string strLeftName = string.IsNullOrEmpty(leftName) ? string.Empty : string.Concat(leftName, ".");

            string str = string.Empty;
            foreach (var column in columns)
            {
                var objectParser = new ObjectParser(column, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);

                stringBuilder.Append(str);
                stringBuilder.Append(strLeftName);
                stringBuilder.Append(objectParser.QuotedShortName);
                stringBuilder.Append(" = ");
                stringBuilder.Append(strRightName);
                stringBuilder.Append(objectParser.QuotedShortName);

                str = " AND ";
            }

            return stringBuilder.ToString();
        }

        /// <summary>
        /// Returns an equality clause for two tables on columns = parameters.
        /// </summary>
        public static string ColumnsAndParameters(IEnumerable<string> columns, string fromPrefix)
        {
            StringBuilder stringBuilder = new StringBuilder();
            string strFromPrefix = string.IsNullOrEmpty(fromPrefix) ? string.Empty : string.Concat(fromPrefix, ".");
            string str1 = string.Empty;
            foreach (var column in columns)
            {
                var objectParser = new ObjectParser(column, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);

                stringBuilder.Append(str1);
                stringBuilder.Append(strFromPrefix);
                stringBuilder.Append(objectParser.QuotedShortName);
                stringBuilder.Append(" = ");
                stringBuilder.Append($"@{objectParser.NormalizedShortName}");
                str1 = " AND ";
            }

            return stringBuilder.ToString();
        }
    }
}