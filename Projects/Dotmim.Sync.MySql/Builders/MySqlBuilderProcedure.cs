using Dotmim.Sync.Builders;
using System;
using System.Data;
using System.Data.Common;
using System.Text;

#if NET6_0 || NET8_0
using MySqlConnector;
#elif NETSTANDARD
using MySql.Data.MySqlClient;
#endif

using System.Threading.Tasks;

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
        /// <param name="tableDescription"></param>
        /// <param name="mysqlObjectNames"></param>
        /// <param name="mysqlDbMetadata"></param>
        public MySqlBuilderProcedure(SyncTable tableDescription, MySqlObjectNames mysqlObjectNames, MySqlDbMetadata mysqlDbMetadata)
        {
            this.TableDescription = tableDescription;
            this.MySqlObjectNames = mysqlObjectNames;
            this.MySqlDbMetadata = mysqlDbMetadata;
        }

        private void AddPkColumnParametersToCommand(MySqlCommand sqlCommand)
        {
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
                sqlCommand.Parameters.Add(this.GetMySqlParameter(pkColumn));
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
            string columnDeclarationString = this.dbMetadata.GetCompatibleColumnTypeDeclarationString(tmpColumn, this.tableDescription.OriginalProvider);

            string output = string.Empty;
            string isNull = string.Empty;
            string defaultValue = string.Empty;

            if (param.Direction == ParameterDirection.Output || param.Direction == ParameterDirection.InputOutput)
                output = "OUT ";

            var parameterName = ParserName.Parse(param.ParameterName, "`").Quoted().ToString();

            stringBuilder3.Append($"{output}{parameterName} {columnDeclarationString} {isNull} {defaultValue}");

            return stringBuilder3.ToString();
        }

        /// <summary>
        /// From a SqlCommand, create a stored procedure string.
        /// </summary>
        private string CreateProcedureCommandText(MySqlCommand cmd, string procName)
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.Append("create procedure ");
            stringBuilder.Append(procName);
            stringBuilder.Append(" (");
            stringBuilder.AppendLine();
            string str = "\n\t";
            foreach (MySqlParameter parameter in cmd.Parameters)
            {
                stringBuilder.Append(string.Concat(str, this.CreateParameterDeclaration(parameter)));
                str = ",\n\t";
            }

            stringBuilder.Append("\n)\nBEGIN\n");
            stringBuilder.Append(cmd.CommandText);
            stringBuilder.Append("\nEND");
            return stringBuilder.ToString();
        }

        /// <summary>
        /// Create a stored procedure.
        /// </summary>
        private DbCommand CreateProcedureCommand(Func<MySqlCommand> buildCommand, string procName, DbConnection connection, DbTransaction transaction)
        {
            var str = this.CreateProcedureCommandText(buildCommand(), procName);

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = str;

            return command;
        }

        private DbCommand CreateProcedureCommand<T>(Func<T, MySqlCommand> buildCommand, string procName, T t, DbConnection connection, DbTransaction transaction)
        {
            var str = this.CreateProcedureCommandText(buildCommand(t), procName);

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = str;

            return command;
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="storedProcedureType"></param>
        /// <param name="filter"></param>
        /// <param name="connection"></param>
        /// <param name="transaction"></param>
        /// <returns>A <see cref="Task{TResult}"/> representing the result of the asynchronous operation.</returns>
        public Task<DbCommand> GetCreateStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
        {
            var command = storedProcedureType switch
            {
                DbStoredProcedureType.SelectRow => this.CreateSelectRowCommand(connection, transaction),
                DbStoredProcedureType.UpdateRow => this.CreateUpdateCommand(connection, transaction),
                DbStoredProcedureType.DeleteRow => this.CreateDeleteCommand(connection, transaction),
                DbStoredProcedureType.Reset => this.CreateResetCommand(connection, transaction),
                _ => null,
            };

            return Task.FromResult(command);
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="storedProcedureType"></param>
        /// <param name="filter"></param>
        /// <param name="connection"></param>
        /// <param name="transaction"></param>
        /// <returns>A <see cref="Task{TResult}"/> representing the result of the asynchronous operation.</returns>
        public Task<DbCommand> GetExistsStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
        {
            if (filter == null && (storedProcedureType == DbStoredProcedureType.SelectChangesWithFilters || storedProcedureType == DbStoredProcedureType.SelectInitializedChangesWithFilters))
                return Task.FromResult<DbCommand>(null);

            if (storedProcedureType == DbStoredProcedureType.BulkDeleteRows ||
                storedProcedureType == DbStoredProcedureType.BulkUpdateRows || storedProcedureType == DbStoredProcedureType.BulkTableType)
                return Task.FromResult<DbCommand>(null);

            var quotedProcedureName = this.objectNames.GetStoredProcedureCommandName(storedProcedureType, filter);

            if (string.IsNullOrEmpty(quotedProcedureName))
                return Task.FromResult<DbCommand>(null);

            var procedureName = ParserName.Parse(quotedProcedureName, "`").ToString();

            var command = connection.CreateCommand();

            command.CommandText = @"select count(*) from information_schema.ROUTINES
                                        where ROUTINE_TYPE = 'PROCEDURE'
                                        and ROUTINE_SCHEMA = schema()
                                        and ROUTINE_NAME = @procName limit 1";

            command.Transaction = transaction;

            var p = command.CreateParameter();
            p.ParameterName = "@procName";
            p.Value = procedureName;
            command.Parameters.Add(p);

            return Task.FromResult(command);
        }

        /// <summary>
        ///
        /// </summary>
        /// <param name="storedProcedureType"></param>
        /// <param name="filter"></param>
        /// <param name="connection"></param>
        /// <param name="transaction"></param>
        /// <returns>A <see cref="Task{TResult}"/> representing the result of the asynchronous operation.</returns>
        public Task<DbCommand> GetDropStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
        {
            if (filter == null && (storedProcedureType == DbStoredProcedureType.SelectChangesWithFilters || storedProcedureType == DbStoredProcedureType.SelectInitializedChangesWithFilters))
                return Task.FromResult<DbCommand>(null);

            if (storedProcedureType == DbStoredProcedureType.BulkDeleteRows ||
                storedProcedureType == DbStoredProcedureType.BulkUpdateRows || storedProcedureType == DbStoredProcedureType.BulkTableType)
                return Task.FromResult<DbCommand>(null);

            var quotedProcedureName = this.objectNames.GetStoredProcedureCommandName(storedProcedureType, filter);

            if (string.IsNullOrEmpty(quotedProcedureName))
                return Task.FromResult<DbCommand>(null);

            var commandText = $"drop procedure {quotedProcedureName}";

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = commandText;

            return Task.FromResult(command);
        }

        public DbCommand CreateResetCommand(DbConnection connection, DbTransaction transaction) => null;

        //------------------------------------------------------------------
        // Delete command
        //------------------------------------------------------------------
        private MySqlCommand BuildDeleteCommand()
        {
            MySqlCommand sqlCommand = new MySqlCommand();
            this.AddPkColumnParametersToCommand(sqlCommand);

            var sqlParameter1 = new MySqlParameter();
            sqlParameter1.ParameterName = "sync_scope_id";
            sqlParameter1.MySqlDbType = MySqlDbType.Guid;
            sqlParameter1.Size = 36;
            sqlCommand.Parameters.Add(sqlParameter1);

            var sqlParameter = new MySqlParameter();
            sqlParameter.ParameterName = "sync_force_write";
            sqlParameter.MySqlDbType = MySqlDbType.Int32;
            sqlCommand.Parameters.Add(sqlParameter);

            var sqlParameter2 = new MySqlParameter();
            sqlParameter2.ParameterName = "sync_min_timestamp";
            sqlParameter2.MySqlDbType = MySqlDbType.Int64;
            sqlCommand.Parameters.Add(sqlParameter2);

            sqlParameter = new MySqlParameter();
            sqlParameter.ParameterName = "sync_row_count";
            sqlParameter.MySqlDbType = MySqlDbType.Int32;
            sqlParameter.Direction = ParameterDirection.Output;
            sqlCommand.Parameters.Add(sqlParameter);

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("DECLARE ts BIGINT;");
            stringBuilder.AppendLine("DECLARE t_update_scope_id VARCHAR(36);");
            stringBuilder.AppendLine($"SELECT `timestamp`, `update_scope_id` FROM {this.trackingName.Quoted()} WHERE {MySqlManagementUtils.WhereColumnAndParameters(this.tableDescription.GetPrimaryKeysColumns(), this.trackingName.Quoted().ToString())} LIMIT 1 INTO ts, t_update_scope_id;");
            stringBuilder.AppendLine($"DELETE FROM {this.tableName.Quoted()} WHERE");
            stringBuilder.AppendLine(MySqlManagementUtils.WhereColumnAndParameters(this.tableDescription.GetPrimaryKeysColumns(), string.Empty));
            stringBuilder.AppendLine("AND (ts <= sync_min_timestamp OR ts IS NULL OR t_update_scope_id = sync_scope_id OR sync_force_write = 1);");

            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"SELECT ROW_COUNT() INTO sync_row_count;"); // [AB] LIMIT 1 removed to be compatible with MariaDB 10.3.x
            stringBuilder.AppendLine();

            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"IF (sync_row_count > 0) THEN");
            stringBuilder.AppendLine($"\tUPDATE {this.trackingName.Quoted()}");
            stringBuilder.AppendLine($"\tSET `update_scope_id` = sync_scope_id, ");
            stringBuilder.AppendLine($"\t\t `sync_row_is_tombstone` = 1, ");
            stringBuilder.AppendLine($"\t\t `timestamp` = {MySqlObjectNames.TimestampValue}, ");
            stringBuilder.AppendLine($"\t\t `last_change_datetime` = now() ");
            stringBuilder.AppendLine($"\tWHERE {MySqlManagementUtils.WhereColumnAndParameters(this.tableDescription.GetPrimaryKeysColumns(), string.Empty)};");
            stringBuilder.AppendLine($"END IF;");

            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        public DbCommand CreateDeleteCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandName = this.objectNames.GetStoredProcedureCommandName(DbStoredProcedureType.DeleteRow);
            return this.CreateProcedureCommand(this.BuildDeleteCommand, commandName, connection, transaction);

            // return null;
        }

        public DbCommand CreateDeleteMetadataCommand(DbConnection connection, DbTransaction transaction) => null;

        public DbCommand CreateSelectRowCommand(DbConnection connection, DbTransaction transaction) => null;

        //------------------------------------------------------------------
        // Select changes command
        //------------------------------------------------------------------

        /// <summary>
        /// Add all sql parameters.
        /// </summary>
        protected void CreateFilterParameters(MySqlCommand sqlCommand, SyncFilter filter)
        {
#if MARIADB
            var originalProvider = MariaDBSyncProvider.ProviderType;
#elif MYSQL
            var originalProvider = MySqlSyncProvider.ProviderType;
#endif
            var parameters = filter.Parameters;

            if (parameters.Count == 0)
                return;

            foreach (var param in parameters)
            {
                if (param.DbType.HasValue)
                {
                    // Get column name and type
                    var columnName = ParserName.Parse(param.Name, "`").Unquoted().Normalized().ToString();
                    var sqlDbType = this.dbMetadata.GetOwnerDbTypeFromDbType(new SyncColumn(columnName) { DbType = (int)param.DbType, MaxLength = param.MaxLength });

                    var customParameterFilter = new MySqlParameter($"in_{columnName}", sqlDbType);
                    customParameterFilter.Size = param.MaxLength;
                    customParameterFilter.IsNullable = param.AllowNull;
                    customParameterFilter.Value = param.DefaultValue;
                    sqlCommand.Parameters.Add(customParameterFilter);
                }
                else
                {
                    var tableFilter = this.tableDescription.Schema.Tables[param.TableName, param.SchemaName];
                    if (tableFilter == null)
                        throw new FilterParamTableNotExistsException(param.TableName);

                    var columnFilter = tableFilter.Columns[param.Name];
                    if (columnFilter == null)
                        throw new FilterParamColumnNotExistsException(param.Name, param.TableName);

                    // Get column name and type
                    var columnName = ParserName.Parse(columnFilter, "`").Unquoted().Normalized().ToString();

                    var sqlDbType = tableFilter.OriginalProvider == originalProvider ?
                        this.dbMetadata.GetMySqlDbType(columnFilter) : this.dbMetadata.GetOwnerDbTypeFromDbType(columnFilter);

                    // Add it as parameter
                    var sqlParamFilter = new MySqlParameter($"in_{columnName}", sqlDbType);
                    sqlParamFilter.Size = columnFilter.MaxLength;
                    sqlParamFilter.IsNullable = param.AllowNull;
                    sqlParamFilter.Value = param.DefaultValue;
                    sqlParamFilter.SourceColumn = columnFilter.ExtraProperty1;
                    sqlCommand.Parameters.Add(sqlParamFilter);
                }
            }
        }

        /// <summary>
        /// Create all custom joins from within a filter.
        /// </summary>
        protected string CreateFilterCustomJoins(SyncFilter filter)
        {
            var customJoins = filter.Joins;

            if (customJoins.Count == 0)
                return string.Empty;

            var stringBuilder = new StringBuilder();

            stringBuilder.AppendLine();
            foreach (var customJoin in customJoins)
            {
                switch (customJoin.JoinEnum)
                {
                    case Join.Left:
                        stringBuilder.Append("LEFT JOIN ");
                        break;
                    case Join.Right:
                        stringBuilder.Append("RIGHT JOIN ");
                        break;
                    case Join.Outer:
                        stringBuilder.Append("OUTER JOIN ");
                        break;
                    case Join.Inner:
                    default:
                        stringBuilder.Append("INNER JOIN ");
                        break;
                }

                var filterTableName = ParserName.Parse(filter.TableName, "`").Quoted().ToString();

                var joinTableName = ParserName.Parse(customJoin.TableName, "`").Quoted().ToString();

                var leftTableName = ParserName.Parse(customJoin.LeftTableName, "`").Quoted().ToString();
                if (string.Equals(filterTableName, leftTableName, SyncGlobalization.DataSourceStringComparison))
                    leftTableName = "`base`";

                var rightTableName = ParserName.Parse(customJoin.RightTableName, "`").Quoted().ToString();
                if (string.Equals(filterTableName, rightTableName, SyncGlobalization.DataSourceStringComparison))
                    rightTableName = "`base`";

                var leftColumName = ParserName.Parse(customJoin.LeftColumnName, "`").Quoted().ToString();
                var rightColumName = ParserName.Parse(customJoin.RightColumnName, "`").Quoted().ToString();

                stringBuilder.AppendLine($"{joinTableName} ON {leftTableName}.{leftColumName} = {rightTableName}.{rightColumName}");
            }

            return stringBuilder.ToString();
        }

        /// <summary>
        /// Create all side where criteria from within a filter.
        /// </summary>
        protected string CreateFilterWhereSide(SyncFilter filter, bool checkTombstoneRows = false)
        {
            var sideWhereFilters = filter.Wheres;

            if (sideWhereFilters.Count == 0)
                return string.Empty;

            var stringBuilder = new StringBuilder();

            // Managing when state is tombstone
            if (checkTombstoneRows)
                stringBuilder.AppendLine($"(");

            stringBuilder.AppendLine($" (");

            var and2 = "   ";

            foreach (var whereFilter in sideWhereFilters)
            {
                var tableFilter = this.tableDescription.Schema.Tables[whereFilter.TableName, whereFilter.SchemaName];
                if (tableFilter == null)
                    throw new FilterParamTableNotExistsException(whereFilter.TableName);

                var columnFilter = tableFilter.Columns[whereFilter.ColumnName];
                if (columnFilter == null)
                    throw new FilterParamColumnNotExistsException(whereFilter.ColumnName, whereFilter.TableName);

                var tableName = ParserName.Parse(tableFilter, "`").Unquoted().ToString();
                tableName = string.Equals(tableName, filter.TableName, SyncGlobalization.DataSourceStringComparison)
                    ? "`base`"
                    : ParserName.Parse(tableFilter, "`").Quoted().ToString();

                var columnName = ParserName.Parse(columnFilter, "`").Quoted().ToString();
                var parameterName = ParserName.Parse(whereFilter.ParameterName, "`").Unquoted().Normalized().ToString();

                var param = filter.Parameters[parameterName];

                if (param == null)
                    throw new FilterParamColumnNotExistsException(columnName, whereFilter.TableName);

                stringBuilder.Append($"{and2}({tableName}.{columnName} = in_{parameterName}");

                if (param.AllowNull)
                    stringBuilder.Append($" OR in_{parameterName} IS NULL");

                stringBuilder.Append($")");

                and2 = " AND ";
            }

            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"  )");

            if (checkTombstoneRows)
            {
                stringBuilder.AppendLine($" OR `side`.`sync_row_is_tombstone` = 1");
                stringBuilder.AppendLine($")");
            }

            // Managing when state is tombstone
            return stringBuilder.ToString();
        }

        /// <summary>
        /// Create all custom wheres from witing a filter.
        /// </summary>
        protected string CreateFilterCustomWheres(SyncFilter filter)
        {
            var customWheres = filter.CustomWheres;

            if (customWheres.Count == 0)
                return string.Empty;

            var stringBuilder = new StringBuilder();
            var and2 = "  ";
            stringBuilder.AppendLine($"(");

            foreach (var customWhere in customWheres)
            {
                // IF coming from Postgres
                var customWhereIteration = customWhere.Replace("\"", "`");

                // If coming from SQL
                customWhereIteration = customWhereIteration.Replace("[", "`");
                customWhereIteration = customWhereIteration.Replace("]", "`");

                // if template
                customWhereIteration = customWhereIteration.Replace("{{{", "`");
                customWhereIteration = customWhereIteration.Replace("}}}", "`");

                stringBuilder.Append($"{and2}{customWhereIteration}");
                and2 = " AND ";
            }

            stringBuilder.AppendLine();
            stringBuilder.AppendLine($")");

            return stringBuilder.ToString();
        }
    }
}