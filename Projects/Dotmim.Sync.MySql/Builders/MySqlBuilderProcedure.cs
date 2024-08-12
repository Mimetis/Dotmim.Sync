using Dotmim.Sync.Builders;
using Dotmim.Sync.DatabaseStringParsers;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

#if NET6_0 || NET8_0
using MySqlConnector;
#elif NETSTANDARD
using MySql.Data.MySqlClient;
#endif

#if MARIADB
namespace Dotmim.Sync.MariaDB.Builders
#elif MYSQL
namespace Dotmim.Sync.MySql.Builders
#endif
{

    /// <summary>
    /// MySqlBuilderProcedure is an object used to create Metadatas for MySql stored procedures.
    /// </summary>
    public class MySqlBuilderProcedure
    {
        /// <summary>
        /// Gets the scope info.
        /// </summary>
        public ScopeInfo ScopeInfo { get; }

        /// <summary>
        /// Gets the table description.
        /// </summary>
        protected SyncTable TableDescription { get; }

        /// <summary>
        /// Gets the sql object names.
        /// </summary>
        protected MySqlObjectNames MySqlObjectNames { get; }

        /// <summary>
        /// Gets the sql database metadata.
        /// </summary>
        protected MySqlDbMetadata MySqlDbMetadata { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="MySqlBuilderProcedure"/> class.
        /// </summary>
        public MySqlBuilderProcedure(SyncTable tableDescription, MySqlObjectNames mysqlObjectNames, MySqlDbMetadata mysqlDbMetadata, ScopeInfo scopeInfo)
        {
            this.ScopeInfo = scopeInfo;
            this.TableDescription = tableDescription;
            this.MySqlObjectNames = mysqlObjectNames;
            this.MySqlDbMetadata = mysqlDbMetadata;
        }

        /// <summary>
        /// Gets the MySql prefix parameter.
        /// </summary>
        public const string MYSQLPREFIXPARAMETER = "in_";

        /// <summary>
        /// Get the command to create a stored procedure.
        /// </summary>
        public Task<DbCommand> GetCreateStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
        {
            var command = storedProcedureType switch
            {
                DbStoredProcedureType.UpdateRow => this.CreateUpdateStoredProcedureCommand(connection, transaction),
                DbStoredProcedureType.DeleteRow => this.CreateDeleteStoredProcedureCommand(connection, transaction),
                _ => null,
            };

            return Task.FromResult(command);
        }

        /// <summary>
        /// Returns the DbCommand to update a row in the table.
        /// </summary>
        public DbCommand CreateUpdateStoredProcedureCommand(DbConnection connection, DbTransaction transaction)
        {

            var scopeNameWithoutDefaultScope = this.ScopeInfo.Name == SyncOptions.DefaultScopeName ? string.Empty : $"{this.ScopeInfo.Name}_";
            var storedProcedureNormalizedName = $"{this.ScopeInfo.Setup?.StoredProceduresPrefix}{this.MySqlObjectNames.TableNormalizedShortName}{this.ScopeInfo.Setup?.StoredProceduresSuffix}_";
            var storedProcedureName = string.Format(MySqlObjectNames.UpdateProcName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope);

            // Check if we have mutables columns
            var hasMutableColumns = this.TableDescription.GetMutableColumns(false).Any();

            var stringBuilder = new StringBuilder();
            var lstParameters = new List<MySqlParameter>();

            foreach (var column in this.TableDescription.Columns.Where(c => !c.IsReadOnly))
                lstParameters.Add(this.GetMySqlParameter(column));

            var sqlParameter = new MySqlParameter();
            sqlParameter.ParameterName = "sync_scope_id";
            sqlParameter.MySqlDbType = MySqlDbType.Guid;
            sqlParameter.Size = 36;
            lstParameters.Add(sqlParameter);

            sqlParameter = new MySqlParameter();
            sqlParameter.ParameterName = "sync_force_write";
            sqlParameter.MySqlDbType = MySqlDbType.Int32;
            lstParameters.Add(sqlParameter);

            sqlParameter = new MySqlParameter();
            sqlParameter.ParameterName = "sync_min_timestamp";
            sqlParameter.MySqlDbType = MySqlDbType.Int64;
            lstParameters.Add(sqlParameter);

            sqlParameter = new MySqlParameter();
            sqlParameter.ParameterName = "sync_row_count";
            sqlParameter.MySqlDbType = MySqlDbType.Int32;
            sqlParameter.Direction = ParameterDirection.Output;
            lstParameters.Add(sqlParameter);

            var listQuotedPrimaryKeys = new StringBuilder();
            var listColumnsTmp2 = new StringBuilder();
            var listColumnsTmp3 = new StringBuilder();


            stringBuilder.Append("CREATE PROCEDURE ");
            stringBuilder.Append(storedProcedureName);
            stringBuilder.Append(" (");
            stringBuilder.AppendLine();
            string str = "\n\t";

            foreach (MySqlParameter parameter in lstParameters)
            {
                stringBuilder.Append(string.Concat(str, this.CreateParameterDeclaration(parameter)));
                str = ",\n\t";
            }

            stringBuilder.Append("\n)\nBEGIN\n");


            var and = string.Empty;
            foreach (var column in this.TableDescription.GetPrimaryKeysColumns())
            {
                var param = this.GetMySqlParameter(column);
                param.ParameterName = $"t_{param.ParameterName}";

                var declar = this.CreateParameterDeclaration(param);

                var columnParser = new ObjectParser(column.ColumnName, MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote);

                var parameterNameQuoted = ParserName.Parse(param.ParameterName, "`").Quoted().ToString();

                // Primary keys column name, with quote
                listQuotedPrimaryKeys.Append($"{columnParser.QuotedShortName}, ");

                // param name without type
                listColumnsTmp2.Append($"{parameterNameQuoted}, ");

                // param name with type
                stringBuilder.AppendLine($"DECLARE {declar};");

                // Param equal IS NULL
                listColumnsTmp3.Append($"{and}{parameterNameQuoted} IS NULL");

                and = " AND ";
            }

            stringBuilder.AppendLine("DECLARE ts BIGINT;");
            stringBuilder.AppendLine("DECLARE t_update_scope_id VARCHAR(36);");
            stringBuilder.AppendLine($"SELECT {listQuotedPrimaryKeys}");
            stringBuilder.AppendLine($"`timestamp`, `update_scope_id` FROM {this.MySqlObjectNames.TrackingTableQuotedShortName} ");
            stringBuilder.AppendLine($"WHERE {MySqlManagementUtils.WhereColumnAndParameters(this.TableDescription.GetPrimaryKeysColumns(), this.MySqlObjectNames.TrackingTableQuotedShortName)} LIMIT 1 ");
            stringBuilder.AppendLine($"INTO {listColumnsTmp2} ts, t_update_scope_id;");
            stringBuilder.AppendLine();

            if (hasMutableColumns)
            {
                stringBuilder.AppendLine($"UPDATE {this.MySqlObjectNames.TableQuotedShortName}");
                stringBuilder.Append($"SET {MySqlManagementUtils.CommaSeparatedUpdateFromParameters(this.TableDescription)}");
                stringBuilder.Append($"WHERE {MySqlManagementUtils.WhereColumnAndParameters(this.TableDescription.GetPrimaryKeysColumns(), string.Empty)}");
                stringBuilder.AppendLine($" AND (ts <= sync_min_timestamp OR ts IS NULL OR t_update_scope_id  = sync_scope_id OR sync_force_write = 1);");
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"SELECT ROW_COUNT() INTO sync_row_count;"); // [AB] LIMIT 1 removed to be compatible with MariaDB 10.3.x
                stringBuilder.AppendLine($"IF (sync_row_count = 0) THEN");
            }

            string empty = string.Empty;
            var stringBuilderArguments = new StringBuilder();
            var stringBuilderParameters = new StringBuilder();
            foreach (var mutableColumn in this.TableDescription.Columns.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn, "`").Quoted().ToString();
                var parameterName = ParserName.Parse(mutableColumn, "`").Quoted().ToString();

                var paramQuotedColumn = ParserName.Parse($"{MYSQLPREFIXPARAMETER}{mutableColumn.ColumnName}", "`");

                stringBuilderArguments.Append(string.Concat(empty, columnName));
                stringBuilderParameters.Append(string.Concat(empty, paramQuotedColumn.Quoted().Normalized().ToString()));
                empty = ", ";
            }

            // If we don't have any mutable column, we can't update, and the Insert
            // will fail if we don't ignore the insert (on Reinitialize for example)
            var ignoreKeyWord = hasMutableColumns ? string.Empty : "IGNORE";
            stringBuilder.AppendLine($"\tINSERT {ignoreKeyWord} INTO {this.MySqlObjectNames.TableQuotedShortName}");
            stringBuilder.AppendLine($"\t({stringBuilderArguments})");
            stringBuilder.AppendLine($"\tSELECT * FROM ( SELECT {stringBuilderParameters}) as TMP ");
            stringBuilder.AppendLine($"\tWHERE ( {listColumnsTmp3} )");
            stringBuilder.AppendLine($"\tOR (ts <= sync_min_timestamp OR ts IS NULL OR t_update_scope_id = sync_scope_id OR sync_force_write = 1)");
            stringBuilder.AppendLine($"\tLIMIT 1;");
            stringBuilder.AppendLine($"");
            stringBuilder.AppendLine($"SELECT ROW_COUNT() INTO sync_row_count;"); // [AB] LIMIT 1 removed to be compatible with MariaDB 10.3.x
            stringBuilder.AppendLine($"");

            if (hasMutableColumns)
                stringBuilder.AppendLine("END IF;");

            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"IF (sync_row_count > 0) THEN");
            stringBuilder.AppendLine($"\tUPDATE {this.MySqlObjectNames.TrackingTableQuotedShortName}");
            stringBuilder.AppendLine($"\tSET `update_scope_id` = sync_scope_id, ");
            stringBuilder.AppendLine($"\t\t `sync_row_is_tombstone` = 0, ");
            stringBuilder.AppendLine($"\t\t `timestamp` = {MySqlObjectNames.TimestampValue}, ");
            stringBuilder.AppendLine($"\t\t `last_change_datetime` = now() ");
            stringBuilder.AppendLine($"\tWHERE {MySqlManagementUtils.WhereColumnAndParameters(this.TableDescription.GetPrimaryKeysColumns(), string.Empty)};");
            stringBuilder.AppendLine($"END IF;");
            stringBuilder.Append("\nEND");

            var sqlCommand = new MySqlCommand();
            sqlCommand.CommandText = stringBuilder.ToString();
            sqlCommand.Connection = connection as MySqlConnection;
            sqlCommand.Transaction = transaction as MySqlTransaction;

            return sqlCommand;
        }

        /// <summary>
        /// Returns the DbCommand to delete a row in the table.
        /// </summary>
        public DbCommand CreateDeleteStoredProcedureCommand(DbConnection connection, DbTransaction transaction)
        {

            var scopeNameWithoutDefaultScope = this.ScopeInfo.Name == SyncOptions.DefaultScopeName ? string.Empty : $"{this.ScopeInfo.Name}_";
            var storedProcedureNormalizedName = $"{this.ScopeInfo.Setup?.StoredProceduresPrefix}{this.MySqlObjectNames.TableNormalizedShortName}{this.ScopeInfo.Setup?.StoredProceduresSuffix}_";
            var storedProcedureName = string.Format(MySqlObjectNames.DeleteProcName, storedProcedureNormalizedName, scopeNameWithoutDefaultScope);

            var lstParameters = new List<MySqlParameter>();

            foreach (var column in this.TableDescription.Columns.Where(c => !c.IsReadOnly))
                lstParameters.Add(this.GetMySqlParameter(column));

            var sqlParameter1 = new MySqlParameter();
            sqlParameter1.ParameterName = "sync_scope_id";
            sqlParameter1.MySqlDbType = MySqlDbType.Guid;
            sqlParameter1.Size = 36;
            lstParameters.Add(sqlParameter1);

            var sqlParameter = new MySqlParameter();
            sqlParameter.ParameterName = "sync_force_write";
            sqlParameter.MySqlDbType = MySqlDbType.Int32;
            lstParameters.Add(sqlParameter);

            var sqlParameter2 = new MySqlParameter();
            sqlParameter2.ParameterName = "sync_min_timestamp";
            sqlParameter2.MySqlDbType = MySqlDbType.Int64;
            lstParameters.Add(sqlParameter2);

            sqlParameter = new MySqlParameter();
            sqlParameter.ParameterName = "sync_row_count";
            sqlParameter.MySqlDbType = MySqlDbType.Int32;
            sqlParameter.Direction = ParameterDirection.Output;
            lstParameters.Add(sqlParameter);

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.Append("CREATE PROCEDURE ");
            stringBuilder.Append(storedProcedureName);
            stringBuilder.Append(" (");
            stringBuilder.AppendLine();
            string str = "\n\t";

            foreach (MySqlParameter parameter in lstParameters)
            {
                stringBuilder.Append(string.Concat(str, this.CreateParameterDeclaration(parameter)));
                str = ",\n\t";
            }

            stringBuilder.Append("\n)\nBEGIN\n");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("DECLARE ts BIGINT;");
            stringBuilder.AppendLine("DECLARE t_update_scope_id VARCHAR(36);");
            stringBuilder.AppendLine($"SELECT `timestamp`, `update_scope_id` FROM {this.MySqlObjectNames.TrackingTableQuotedFullName} WHERE {MySqlManagementUtils.WhereColumnAndParameters(this.TableDescription.GetPrimaryKeysColumns(), this.MySqlObjectNames.TrackingTableQuotedFullName)} LIMIT 1 INTO ts, t_update_scope_id;");
            stringBuilder.AppendLine($"DELETE FROM {this.MySqlObjectNames.TableQuotedFullName} WHERE");
            stringBuilder.AppendLine(MySqlManagementUtils.WhereColumnAndParameters(this.TableDescription.GetPrimaryKeysColumns(), string.Empty));
            stringBuilder.AppendLine("AND (ts <= sync_min_timestamp OR ts IS NULL OR t_update_scope_id = sync_scope_id OR sync_force_write = 1);");

            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"SELECT ROW_COUNT() INTO sync_row_count;"); // [AB] LIMIT 1 removed to be compatible with MariaDB 10.3.x
            stringBuilder.AppendLine();

            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"IF (sync_row_count > 0) THEN");
            stringBuilder.AppendLine($"\tUPDATE {this.MySqlObjectNames.TrackingTableQuotedFullName}");
            stringBuilder.AppendLine($"\tSET `update_scope_id` = sync_scope_id, ");
            stringBuilder.AppendLine($"\t\t `sync_row_is_tombstone` = 1, ");
            stringBuilder.AppendLine($"\t\t `timestamp` = {MySqlObjectNames.TimestampValue}, ");
            stringBuilder.AppendLine($"\t\t `last_change_datetime` = now() ");
            stringBuilder.AppendLine($"\tWHERE {MySqlManagementUtils.WhereColumnAndParameters(this.TableDescription.GetPrimaryKeysColumns(), string.Empty)};");
            stringBuilder.AppendLine($"END IF;");
            stringBuilder.Append("\nEND");

            MySqlCommand sqlCommand = new MySqlCommand();
            sqlCommand.CommandText = stringBuilder.ToString();
            sqlCommand.Connection = connection as MySqlConnection;
            sqlCommand.Transaction = transaction as MySqlTransaction;

            return sqlCommand;
        }

        /// <summary>
        /// Returns the DbCommand to check if a stored procedure exists.
        /// </summary>
        public Task<DbCommand> GetExistsStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
        {
            if (filter == null && (storedProcedureType == DbStoredProcedureType.SelectChangesWithFilters || storedProcedureType == DbStoredProcedureType.SelectInitializedChangesWithFilters))
                return Task.FromResult<DbCommand>(null);

            var quotedProcedureName = this.MySqlObjectNames.GetStoredProcedureCommandName(storedProcedureType, filter);

            if (string.IsNullOrEmpty(quotedProcedureName))
                return Task.FromResult<DbCommand>(null);

            var procedureFilter = new ObjectParser(quotedProcedureName, MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote);

            var command = connection.CreateCommand();

            command.CommandText = @"select count(*) from information_schema.ROUTINES
                                        where ROUTINE_TYPE = 'PROCEDURE'
                                        and ROUTINE_SCHEMA = schema()
                                        and ROUTINE_NAME = @procName limit 1";

            command.Transaction = transaction;

            var p = command.CreateParameter();
            p.ParameterName = "@procName";
            p.Value = procedureFilter.ObjectName;
            command.Parameters.Add(p);

            return Task.FromResult(command);
        }

        /// <summary>
        /// Returns a DbCommand to drop a stored procedure.
        /// </summary>
        public Task<DbCommand> GetDropStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
        {
            if (filter == null && (storedProcedureType == DbStoredProcedureType.SelectChangesWithFilters || storedProcedureType == DbStoredProcedureType.SelectInitializedChangesWithFilters))
                return Task.FromResult<DbCommand>(null);

            if (storedProcedureType == DbStoredProcedureType.BulkDeleteRows ||
                storedProcedureType == DbStoredProcedureType.BulkUpdateRows || storedProcedureType == DbStoredProcedureType.BulkTableType)
                return Task.FromResult<DbCommand>(null);

            var quotedProcedureName = this.MySqlObjectNames.GetStoredProcedureCommandName(storedProcedureType, filter);

            if (string.IsNullOrEmpty(quotedProcedureName))
                return Task.FromResult<DbCommand>(null);

            var commandText = $"drop procedure {quotedProcedureName}";

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = commandText;

            return Task.FromResult(command);
        }

        internal MySqlParameter GetMySqlParameter(SyncColumn column)
        {
#if MARIADB
            var originalProvider = MariaDBSyncProvider.ProviderType;
#elif MYSQL
            var originalProvider = MySqlSyncProvider.ProviderType;
#endif
            var parameterParser = new ObjectParser(column.ColumnName, MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote);

            // Get the good SqlDbType (even if we are not from Sql Server def)
            var mySqlDbType = this.TableDescription.OriginalProvider == originalProvider ?
                this.MySqlDbMetadata.GetMySqlDbType(column) : this.MySqlDbMetadata.GetOwnerDbTypeFromDbType(column);

            var sqlParameter = new MySqlParameter
            {
                ParameterName = $"{MYSQLPREFIXPARAMETER}{parameterParser.NormalizedShortName}",
                DbType = column.GetDbType(),
                IsNullable = column.AllowDBNull,
                MySqlDbType = mySqlDbType,
                SourceColumn = string.IsNullOrEmpty(column.ExtraProperty1) ? null : column.ExtraProperty1,
            };

            (byte precision, byte scale) = this.MySqlDbMetadata.GetCompatibleColumnPrecisionAndScale(column, this.TableDescription.OriginalProvider);

            if ((sqlParameter.DbType == DbType.Decimal || sqlParameter.DbType == DbType.Double
                 || sqlParameter.DbType == DbType.Single || sqlParameter.DbType == DbType.VarNumeric) && precision > 0)
            {
                sqlParameter.Precision = precision;
                if (scale > 0)
                    sqlParameter.Scale = scale;
            }
            else
            {
                sqlParameter.Size = column.MaxLength > 0 ? column.MaxLength : sqlParameter.DbType == DbType.Guid ? 36 : -1;
            }

            return sqlParameter;
        }

        /// <summary>
        /// From a SqlParameter, create the declaration.
        /// </summary>
        internal string CreateParameterDeclaration(MySqlParameter param)
        {

            var tmpColumn = new SyncColumn(param.ParameterName)
            {
                OriginalDbType = param.MySqlDbType.ToString(),
                OriginalTypeName = param.MySqlDbType.ToString().ToLowerInvariant(),
                MaxLength = param.Size,
                Precision = param.Precision,
                Scale = param.Scale,
                DbType = (int)param.DbType,
                ExtraProperty1 = string.IsNullOrEmpty(param.SourceColumn) ? null : param.SourceColumn,
            };

            var stringBuilder3 = new StringBuilder();
            string columnDeclarationString = this.MySqlDbMetadata.GetCompatibleColumnTypeDeclarationString(tmpColumn, this.TableDescription.OriginalProvider);

            string output = string.Empty;
            string isNull = string.Empty;
            string defaultValue = string.Empty;

            if (param.Direction == ParameterDirection.Output || param.Direction == ParameterDirection.InputOutput)
                output = "OUT ";

            var parameterParser = new ObjectParser(param.ParameterName, MySqlObjectNames.LeftQuote, MySqlObjectNames.RightQuote);

            stringBuilder3.Append($"{output}{parameterParser.QuotedShortName} {columnDeclarationString} {isNull} {defaultValue}");

            return stringBuilder3.ToString();
        }
    }
}