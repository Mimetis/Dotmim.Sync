using Dotmim.Sync.Builders;
using System;
using System.Text;
using System.Data.Common;
using System.Data;
using System.Linq;
using MySql.Data.MySqlClient;
using Dotmim.Sync.MySql.Builders;
using System.Diagnostics;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Dotmim.Sync.MySql
{
    public class MySqlBuilderProcedure : IDbBuilderProcedureHelper
    {
        private ParserName tableName;
        private ParserName trackingName;
        private MySqlConnection connection;
        private MySqlTransaction transaction;
        private SyncTable tableDescription;
        private MySqlObjectNames mySqlObjectNames;
        private MySqlDbMetadata mySqlDbMetadata;
        internal const string MYSQL_PREFIX_PARAMETER = "in_";

        public MySqlBuilderProcedure(SyncTable tableDescription, DbConnection connection, DbTransaction transaction = null)
        {
            this.connection = connection as MySqlConnection;
            this.transaction = transaction as MySqlTransaction;

            this.tableDescription = tableDescription;
            (this.tableName, this.trackingName) = MyTableSqlBuilder.GetParsers(tableDescription);
            this.mySqlObjectNames = new MySqlObjectNames(this.tableDescription);
            this.mySqlDbMetadata = new MySqlDbMetadata();
        }

        private void AddPkColumnParametersToCommand(MySqlCommand sqlCommand)
        {
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
                sqlCommand.Parameters.Add(GetMySqlParameter(pkColumn));
        }
        private void AddColumnParametersToCommand(MySqlCommand sqlCommand)
        {
            foreach (var column in this.tableDescription.Columns.Where(c => !c.IsReadOnly))
                sqlCommand.Parameters.Add(GetMySqlParameter(column));
        }

        internal MySqlParameter GetMySqlParameter(SyncColumn column)
        {
            var mySqlDbMetadata = new MySqlDbMetadata();

            var parameterName = ParserName.Parse(column).Unquoted().Normalized().ToString();

            var sqlParameter = new MySqlParameter
            {
                ParameterName = $"{MySqlBuilderProcedure.MYSQL_PREFIX_PARAMETER}{parameterName}",
                DbType = column.GetDbType(),
                IsNullable = column.AllowDBNull
            };

            (byte precision, byte scale) = mySqlDbMetadata.TryGetOwnerPrecisionAndScale(column.OriginalDbType, column.GetDbType(), false, false, column.MaxLength, column.Precision, column.Scale, this.tableDescription.OriginalProvider, MySqlSyncProvider.ProviderType);

            if ((sqlParameter.DbType == DbType.Decimal || sqlParameter.DbType == DbType.Double
                 || sqlParameter.DbType == DbType.Single || sqlParameter.DbType == DbType.VarNumeric) && precision > 0)
            {
                sqlParameter.Precision = precision;
                if (scale > 0)
                    sqlParameter.Scale = scale;
            }
            else if (column.MaxLength > 0)
            {
                sqlParameter.Size = (int)column.MaxLength;
            }
            else if (sqlParameter.DbType == DbType.Guid)
            {
                sqlParameter.Size = 36;
            }
            else
            {
                sqlParameter.Size = -1;
            }

            return sqlParameter;
        }


        /// <summary>
        /// From a SqlParameter, create the declaration
        /// </summary>
        internal string CreateParameterDeclaration(MySqlParameter param)
        {
            var stringBuilder3 = new StringBuilder();
            var sqlDbType = param.MySqlDbType;

            string empty = string.Empty;
            var stringType = this.mySqlDbMetadata.GetStringFromDbType(param.DbType);
            string precision = this.mySqlDbMetadata.GetPrecisionStringFromDbType(param.DbType, param.Size, param.Precision, param.Scale);
            string output = string.Empty;
            string isNull = string.Empty;
            string defaultValue = string.Empty;

            if (param.Direction == ParameterDirection.Output || param.Direction == ParameterDirection.InputOutput)
                output = "OUT ";

            // MySql does not accept default value or Is Nullable

            //if (param.IsNullable)
            //    isNull="NULL";

            //if (param.Value != null)
            //    defaultValue = $"= {param.Value.ToString()}";

            stringBuilder3.Append($"{output}{param.ParameterName} {stringType}{precision} {isNull} {defaultValue}");

            return stringBuilder3.ToString();

        }

        /// <summary>
        /// From a SqlCommand, create a stored procedure string
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
                stringBuilder.Append(string.Concat(str, CreateParameterDeclaration(parameter)));
                str = ",\n\t";
            }
            stringBuilder.Append("\n)\nBEGIN\n");
            stringBuilder.Append(cmd.CommandText);
            stringBuilder.Append("\nEND");
            return stringBuilder.ToString();
        }

        /// <summary>
        /// Create a stored procedure
        /// </summary>
        private async Task CreateProcedureCommandAsync(Func<MySqlCommand> BuildCommand, string procName)
        {

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                var str = CreateProcedureCommandText(BuildCommand(), procName);
                using (var command = new MySqlCommand(str, connection))
                {
                    if (transaction != null)
                        command.Transaction = transaction;

                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during CreateProcedureCommand : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

            }

        }

        private async Task CreateProcedureCommandAsync<T>(Func<T, MySqlCommand> BuildCommand, string procName, T t)
        {

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                var str = CreateProcedureCommandText(BuildCommand(t), procName);
                using (var command = new MySqlCommand(str, connection))
                {
                    if (transaction != null)
                        command.Transaction = transaction;

                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during CreateProcedureCommand : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

            }

        }

        private async Task<string> CreateProcedureCommandScriptTextAsync(Func<MySqlCommand> BuildCommand, string procName)
        {

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                var str1 = $"Command {procName} for table {tableName.Quoted().ToString()}";
                var str = CreateProcedureCommandText(BuildCommand(), procName);
                return MyTableSqlBuilder.WrapScriptTextWithComments(str, str1);


            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during CreateProcedureCommand : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

            }
        }
        private async Task<string> CreateProcedureCommandScriptTextAsync<T>(Func<T, MySqlCommand> BuildCommand, string procName, T t)
        {

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                var str1 = $"Command {procName} for table {tableName.Quoted().ToString()}";
                var str = CreateProcedureCommandText(BuildCommand(t), procName);
                return MyTableSqlBuilder.WrapScriptTextWithComments(str, str1);
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during CreateProcedureCommand : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

            }
        }

        /// <summary>
        /// Check if we need to create the stored procedure
        /// </summary>
        public async Task<bool> NeedToCreateProcedureAsync(DbCommandType commandType)
        {
            if (connection.State != ConnectionState.Open)
                throw new ArgumentException("Here, we need an opened connection please");

            var commandName = this.mySqlObjectNames.GetCommandName(commandType).name;

            return !(await MySqlManagementUtils.ProcedureExistsAsync(connection, transaction, commandName).ConfigureAwait(false));
        }

        /// <summary>
        /// Check if we need to create the TVP Type
        /// </summary>
        public Task<bool> NeedToCreateTypeAsync(DbCommandType commandType) => Task.FromResult(false);

        //------------------------------------------------------------------
        // Reset command
        //------------------------------------------------------------------
        private MySqlCommand BuildResetCommand()
        {

            var sqlCommand = new MySqlCommand();
            var stringBuilder = new StringBuilder();

            stringBuilder.AppendLine($"DELETE FROM {tableName.Quoted().ToString()};");
            stringBuilder.AppendLine($"DELETE FROM {trackingName.Quoted().ToString()};");
            stringBuilder.AppendLine();

            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
        public async Task CreateResetAsync()
        {
            var commandName = this.mySqlObjectNames.GetCommandName(DbCommandType.Reset).name;
            await CreateProcedureCommandAsync(BuildResetCommand, commandName).ConfigureAwait(false);
        }

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
            stringBuilder.AppendLine("SET ts = 0;");
            stringBuilder.AppendLine($"SELECT `timestamp`, `update_scope_id` FROM {trackingName.Quoted().ToString()} WHERE {MySqlManagementUtils.WhereColumnAndParameters(this.tableDescription.PrimaryKeys, trackingName.Quoted().ToString())} LIMIT 1 INTO ts, t_update_scope_id;");
            stringBuilder.AppendLine($"DELETE FROM {tableName.Quoted().ToString()} WHERE");
            stringBuilder.AppendLine(MySqlManagementUtils.WhereColumnAndParameters(this.tableDescription.PrimaryKeys, ""));
            stringBuilder.AppendLine("AND (ts <= sync_min_timestamp OR ts IS NULL OR t_update_scope_id = sync_scope_id OR sync_force_write = 1);");

            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"SELECT ROW_COUNT() LIMIT 1 INTO sync_row_count;");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"IF (sync_row_count > 0) THEN");
            stringBuilder.AppendLine($"\tUPDATE {trackingName.Quoted().ToString()}");
            stringBuilder.AppendLine($"\tSET `update_scope_id` = sync_scope_id, ");
            stringBuilder.AppendLine($"\t\t `sync_row_is_tombstone` = 1, ");
            stringBuilder.AppendLine($"\t\t `timestamp` = {MySqlObjectNames.TimestampValue}, ");
            stringBuilder.AppendLine($"\t\t `last_change_datetime` = now() ");
            stringBuilder.AppendLine($"\tWHERE {MySqlManagementUtils.WhereColumnAndParameters(this.tableDescription.PrimaryKeys, "")};");
            stringBuilder.AppendLine($"END IF;");


            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        public async Task CreateDeleteAsync()
        {
            var commandName = this.mySqlObjectNames.GetCommandName(DbCommandType.DeleteRow).name;
            await CreateProcedureCommandAsync(BuildDeleteCommand, commandName).ConfigureAwait(false);
        }

        //------------------------------------------------------------------
        // Delete Metadata command
        //------------------------------------------------------------------
        private MySqlCommand BuildDeleteMetadataCommand()
        {
            var sqlCommand = new MySqlCommand();

            var sqlParameter1 = new MySqlParameter
            {
                ParameterName = "sync_row_timestamp",
                MySqlDbType = MySqlDbType.Int64
            };
            sqlCommand.Parameters.Add(sqlParameter1);

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"DELETE FROM {trackingName.Quoted().ToString()} WHERE `timestamp` < sync_row_timestamp;");
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        public async Task CreateDeleteMetadataAsync()
        {
            var commandName = this.mySqlObjectNames.GetCommandName(DbCommandType.DeleteMetadata).name;
            await CreateProcedureCommandAsync(BuildDeleteMetadataCommand, commandName).ConfigureAwait(false);
        }


        //------------------------------------------------------------------
        // Select Row command
        //------------------------------------------------------------------
        private MySqlCommand BuildSelectRowCommand()
        {
            MySqlCommand sqlCommand = new MySqlCommand();
            this.AddPkColumnParametersToCommand(sqlCommand);

            MySqlParameter sqlParameter = new MySqlParameter();
            sqlParameter.ParameterName = "sync_scope_id";
            sqlParameter.MySqlDbType = MySqlDbType.Guid;
            sqlParameter.Size = 36;
            sqlCommand.Parameters.Add(sqlParameter);

            StringBuilder stringBuilder = new StringBuilder("SELECT ");
            stringBuilder.AppendLine();
            StringBuilder stringBuilder1 = new StringBuilder();
            string empty = string.Empty;
            foreach (var pkColumn in this.tableDescription.PrimaryKeys)
            {
                var columnName = ParserName.Parse(pkColumn, "`").Quoted().ToString();
                var parameterName = ParserName.Parse(pkColumn, "`").Unquoted().Normalized().ToString();

                stringBuilder.AppendLine($"\t`side`.{columnName}, ");
                stringBuilder1.Append($"{empty}`side`.{columnName} = {MYSQL_PREFIX_PARAMETER}{parameterName}");
                empty = " AND ";
            }
            foreach (var mutableColumn in this.tableDescription.GetMutableColumns())
            {
                var nonPkColumnName = ParserName.Parse(mutableColumn, "`").Quoted().ToString();
                stringBuilder.AppendLine($"\t`base`.{nonPkColumnName}, ");
            }
            stringBuilder.AppendLine("\t`side`.`sync_row_is_tombstone`, ");
            stringBuilder.AppendLine("\t`side`.`update_scope_id` ");
            stringBuilder.AppendLine($"FROM {tableName.Quoted().ToString()} `base`");
            stringBuilder.AppendLine($"RIGHT JOIN {trackingName.Quoted().ToString()} `side` ON");

            string str = string.Empty;
            foreach (var pkColumn in this.tableDescription.PrimaryKeys)
            {
                var columnName = ParserName.Parse(pkColumn, "`").Quoted().ToString();

                stringBuilder.Append($"{str}`base`.{columnName} = `side`.{columnName}");
                str = " AND ";
            }
            stringBuilder.AppendLine();
            stringBuilder.Append("WHERE ");
            stringBuilder.Append(stringBuilder1.ToString());
            stringBuilder.Append(";");
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        public async Task CreateSelectRowAsync()
        {
            var commandName = this.mySqlObjectNames.GetCommandName(DbCommandType.SelectRow).name;
            await CreateProcedureCommandAsync(BuildSelectRowCommand, commandName).ConfigureAwait(false);
        }

        //------------------------------------------------------------------
        // Update command
        //------------------------------------------------------------------
        private MySqlCommand BuildUpdateCommand(bool hasMutableColumns)
        {
            var sqlCommand = new MySqlCommand();

            var stringBuilder = new StringBuilder();
            this.AddColumnParametersToCommand(sqlCommand);

            var sqlParameter = new MySqlParameter();
            sqlParameter.ParameterName = "sync_scope_id";
            sqlParameter.MySqlDbType = MySqlDbType.Guid;
            sqlParameter.Size = 36;
            sqlCommand.Parameters.Add(sqlParameter);

            sqlParameter = new MySqlParameter();
            sqlParameter.ParameterName = "sync_force_write";
            sqlParameter.MySqlDbType = MySqlDbType.Int32;
            sqlCommand.Parameters.Add(sqlParameter);

            sqlParameter = new MySqlParameter();
            sqlParameter.ParameterName = "sync_min_timestamp";
            sqlParameter.MySqlDbType = MySqlDbType.Int64;
            sqlCommand.Parameters.Add(sqlParameter);

            sqlParameter = new MySqlParameter();
            sqlParameter.ParameterName = "sync_row_count";
            sqlParameter.MySqlDbType = MySqlDbType.Int32;
            sqlParameter.Direction = ParameterDirection.Output;
            sqlCommand.Parameters.Add(sqlParameter);

            var listColumnsTmp = new StringBuilder();
            var listColumnsTmp2 = new StringBuilder();
            var listColumnsTmp3 = new StringBuilder();
            var and = " AND ";

            var lstPrimaryKeysColumns = this.tableDescription.GetPrimaryKeysColumns().ToList();

            foreach (var column in lstPrimaryKeysColumns)
            {
                if (lstPrimaryKeysColumns.IndexOf(column) == lstPrimaryKeysColumns.Count - 1)
                    and = "";

                var param = GetMySqlParameter(column);
                var declar = CreateParameterDeclaration(param);
                var columnName = ParserName.Parse(column, "`").Quoted().ToString();

                // Primary keys column name, with quote
                listColumnsTmp.Append($"{columnName}, ");

                // param name without type
                listColumnsTmp2.Append($"t_{param.ParameterName}, ");

                // param name with type
                stringBuilder.AppendLine($"DECLARE t_{declar};");

                // Param equal IS NULL
                listColumnsTmp3.Append($"t_{param.ParameterName} IS NULL {and}");
            }



            stringBuilder.AppendLine("DECLARE ts BIGINT;");

            stringBuilder.AppendLine("DECLARE t_update_scope_id VARCHAR(36);");
            stringBuilder.AppendLine("SET ts = 0;");
            stringBuilder.AppendLine($"SELECT {listColumnsTmp.ToString()}");
            stringBuilder.AppendLine($"`timestamp`, `update_scope_id` FROM {trackingName.Quoted().ToString()} ");
            stringBuilder.AppendLine($"WHERE {MySqlManagementUtils.WhereColumnAndParameters(this.tableDescription.PrimaryKeys, trackingName.Quoted().ToString())} LIMIT 1 ");
            stringBuilder.AppendLine($"INTO {listColumnsTmp2.ToString()} ts, t_update_scope_id;");
            stringBuilder.AppendLine();

            if (hasMutableColumns)
            {
                stringBuilder.AppendLine($"UPDATE {tableName.Quoted().ToString()}");
                stringBuilder.Append($"SET {MySqlManagementUtils.CommaSeparatedUpdateFromParameters(this.tableDescription)}");
                stringBuilder.Append($"WHERE {MySqlManagementUtils.WhereColumnAndParameters(this.tableDescription.PrimaryKeys, "")}");
                stringBuilder.AppendLine($" AND (ts <= sync_min_timestamp OR ts IS NULL OR t_update_scope_id  = sync_scope_id OR sync_force_write = 1);");
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"SELECT ROW_COUNT() LIMIT 1 INTO sync_row_count;");
                stringBuilder.AppendLine($"IF (sync_row_count = 0) THEN");

            }

            string empty = string.Empty;
            var stringBuilderArguments = new StringBuilder();
            var stringBuilderParameters = new StringBuilder();
            foreach (var mutableColumn in this.tableDescription.Columns.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn, "`").Quoted().ToString();
                var parameterName = ParserName.Parse(mutableColumn, "`").Unquoted().Normalized().ToString();

                stringBuilderArguments.Append(string.Concat(empty, columnName));
                stringBuilderParameters.Append(string.Concat(empty, $"{MYSQL_PREFIX_PARAMETER}{parameterName}"));
                empty = ", ";
            }

            // If we don't have any mutable column, we can't update, and the Insert
            // will fail if we don't ignore the insert (on Reinitialize for example)
            var ignoreKeyWord = hasMutableColumns ? "" : "IGNORE";

            stringBuilder.AppendLine($"\tINSERT {ignoreKeyWord} INTO {tableName.Quoted().ToString()}");
            stringBuilder.AppendLine($"\t({stringBuilderArguments.ToString()})");
            stringBuilder.AppendLine($"\tSELECT * FROM ( SELECT {stringBuilderParameters.ToString()}) as TMP ");
            stringBuilder.AppendLine($"\tWHERE ( {listColumnsTmp3.ToString()} )");
            stringBuilder.AppendLine($"\tOR (ts <= sync_min_timestamp OR ts IS NULL OR t_update_scope_id = sync_scope_id OR sync_force_write = 1)");
            stringBuilder.AppendLine($"\tLIMIT 1;");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"SELECT ROW_COUNT() LIMIT 1 INTO sync_row_count;");
            stringBuilder.AppendLine();

            if (hasMutableColumns)
                stringBuilder.AppendLine("END IF;");

            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"IF (sync_row_count > 0) THEN");
            stringBuilder.AppendLine($"\tUPDATE {trackingName.Quoted().ToString()}");
            stringBuilder.AppendLine($"\tSET `update_scope_id` = sync_scope_id, ");
            stringBuilder.AppendLine($"\t\t `sync_row_is_tombstone` = 0, ");
            stringBuilder.AppendLine($"\t\t `timestamp` = {MySqlObjectNames.TimestampValue}, ");
            stringBuilder.AppendLine($"\t\t `last_change_datetime` = now() ");
            stringBuilder.AppendLine($"\tWHERE {MySqlManagementUtils.WhereColumnAndParameters(this.tableDescription.PrimaryKeys, "")};");
            stringBuilder.AppendLine($"END IF;");

            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        public async Task CreateUpdateAsync(bool hasMutableColumns)
        {
            var commandName = this.mySqlObjectNames.GetCommandName(DbCommandType.UpdateRow).name;
            await this.CreateProcedureCommandAsync(BuildUpdateCommand, commandName, hasMutableColumns).ConfigureAwait(false);
        }

        /// <summary>
        /// Add all sql parameters
        /// </summary>
        protected void CreateFilterParameters(MySqlCommand sqlCommand, SyncFilter filter)
        {
            var parameters = filter.Parameters;

            if (parameters.Count == 0)
                return;

            foreach (var param in parameters)
            {
                if (param.DbType.HasValue)
                {
                    // Get column name and type
                    var columnName = ParserName.Parse(param.Name, "`").Unquoted().Normalized().ToString();
                    var sqlDbType = (MySqlDbType)this.mySqlDbMetadata.TryGetOwnerDbType(null, param.DbType.Value, false, false, param.MaxLength, MySqlSyncProvider.ProviderType, MySqlSyncProvider.ProviderType);

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
                    var sqlDbType = (SqlDbType)this.mySqlDbMetadata.TryGetOwnerDbType(columnFilter.OriginalDbType, columnFilter.GetDbType(), false, false, columnFilter.MaxLength, tableFilter.OriginalProvider, MySqlSyncProvider.ProviderType);

                    // Add it as parameter
                    var sqlParamFilter = new MySqlParameter($"in_{columnName}", sqlDbType);
                    sqlParamFilter.Size = columnFilter.MaxLength;
                    sqlParamFilter.IsNullable = param.AllowNull;
                    sqlParamFilter.Value = param.DefaultValue;
                    sqlCommand.Parameters.Add(sqlParamFilter);
                }

            }
        }

        /// <summary>
        /// Create all custom joins from within a filter 
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
        /// Create all side where criteria from within a filter
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
                if (string.Equals(tableName, filter.TableName, SyncGlobalization.DataSourceStringComparison))
                    tableName = "`base`";
                else
                    tableName = ParserName.Parse(tableFilter, "`").Quoted().ToString();

                var columnName = ParserName.Parse(columnFilter, "`").Quoted().ToString();
                var parameterName = ParserName.Parse(whereFilter.ParameterName, "`").Unquoted().Normalized().ToString();
                var sqlDbType = (MySqlDbType)this.mySqlDbMetadata.TryGetOwnerDbType(columnFilter.OriginalDbType, columnFilter.GetDbType(), false, false, columnFilter.MaxLength, tableFilter.OriginalProvider, MySqlSyncProvider.ProviderType);

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
        /// Create all custom wheres from witing a filter
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
                stringBuilder.Append($"{and2}{customWhere}");
                and2 = " AND ";
            }

            stringBuilder.AppendLine();
            stringBuilder.AppendLine($")");

            return stringBuilder.ToString();
        }



        //------------------------------------------------------------------
        // Select changes command
        //------------------------------------------------------------------
        private MySqlCommand BuildSelectIncrementalChangesCommand(SyncFilter filter = null)
        {
            var sqlCommand = new MySqlCommand();
            var sqlParameter1 = new MySqlParameter();
            sqlParameter1.ParameterName = "sync_min_timestamp";
            sqlParameter1.MySqlDbType = MySqlDbType.Int64;
            sqlParameter1.Value = 0;

            sqlCommand.Parameters.Add(sqlParameter1);

            var sqlParameter3 = new MySqlParameter();
            sqlParameter3.ParameterName = "sync_scope_id";
            sqlParameter3.MySqlDbType = MySqlDbType.Guid;
            sqlParameter3.Size = 36;
            sqlParameter3.Value = "NULL";
            sqlCommand.Parameters.Add(sqlParameter3);

            // Add filter parameters
            if (filter != null)
                CreateFilterParameters(sqlCommand, filter);


            var stringBuilder = new StringBuilder("SELECT DISTINCT");

            // ----------------------------------
            // Add all columns
            // ----------------------------------

            foreach (var pkColumn in this.tableDescription.PrimaryKeys)
            {
                var pkColumnName = ParserName.Parse(pkColumn, "`").Quoted().ToString();
                stringBuilder.AppendLine($"\t`side`.{pkColumnName}, ");
            }
            foreach (var mutableColumn in this.tableDescription.GetMutableColumns())
            {
                var columnName = ParserName.Parse(mutableColumn, "`").Quoted().ToString();
                stringBuilder.AppendLine($"\t`base`.{columnName}, ");
            }
            stringBuilder.AppendLine($"\t`side`.`sync_row_is_tombstone`, ");
            stringBuilder.AppendLine($"\t`side`.`update_scope_id` ");
            stringBuilder.AppendLine($"FROM {tableName.Quoted().ToString()} `base`");
            // ----------------------------------
            // Make Right Join
            // ----------------------------------
            stringBuilder.Append($"RIGHT JOIN {trackingName.Quoted().ToString()} `side` ON ");

            string empty = "";
            foreach (var pkColumn in this.tableDescription.PrimaryKeys)
            {
                var pkColumnName = ParserName.Parse(pkColumn, "`").Quoted().ToString();
                stringBuilder.Append($"{empty}`base`.{pkColumnName} = `side`.{pkColumnName}");
                empty = " AND ";
            }

            // ----------------------------------
            // Custom Joins
            // ----------------------------------
            if (filter != null)
                stringBuilder.Append(CreateFilterCustomJoins(filter));

            stringBuilder.AppendLine();
            stringBuilder.AppendLine("WHERE (");

            // ----------------------------------
            // Where filters and Custom Where string
            // ----------------------------------
            if (filter != null)
            {
                var createFilterWhereSide = CreateFilterWhereSide(filter, true);
                stringBuilder.Append(createFilterWhereSide);

                if (!string.IsNullOrEmpty(createFilterWhereSide))
                    stringBuilder.AppendLine($"AND ");

                var createFilterCustomWheres = CreateFilterCustomWheres(filter);
                stringBuilder.Append(createFilterCustomWheres);

                if (!string.IsNullOrEmpty(createFilterCustomWheres))
                    stringBuilder.AppendLine($"AND ");
            }
            // ----------------------------------


            stringBuilder.AppendLine("\t`side`.`timestamp` > sync_min_timestamp");
            stringBuilder.AppendLine("\tAND (`side`.`update_scope_id` <> sync_scope_id OR `side`.`update_scope_id` IS NULL) ");
            stringBuilder.AppendLine(");");

            sqlCommand.CommandText = stringBuilder.ToString();

            return sqlCommand;
        }

        public async Task CreateSelectIncrementalChangesAsync(SyncFilter filter = null)
        {
            var commandName = this.mySqlObjectNames.GetCommandName(DbCommandType.SelectChanges).name;
            Func<MySqlCommand> cmdWithoutFilter = () => BuildSelectIncrementalChangesCommand(null);
            await CreateProcedureCommandAsync(cmdWithoutFilter, commandName).ConfigureAwait(false);

            if (filter != null)
            {
                filter.ValidateColumnFilters(this.tableDescription);
                commandName = this.mySqlObjectNames.GetCommandName(DbCommandType.SelectChangesWithFilters, filter).name;
                Func<MySqlCommand> cmdWithFilter = () => BuildSelectIncrementalChangesCommand(filter);
                await CreateProcedureCommandAsync(cmdWithFilter, commandName).ConfigureAwait(false);
            }
        }



        public async Task CreateTVPTypeAsync() => throw new NotImplementedException();


        public async Task CreateBulkUpdateAsync(bool hasMutableColumns) => throw new NotImplementedException();


        public async Task CreateBulkDeleteAsync() => throw new NotImplementedException();


        private async Task DropProcedureAsync(DbCommandType procType, SyncFilter filter = null)
        {
            var commandName = this.mySqlObjectNames.GetCommandName(procType).name;
            var commandText = $"drop procedure if exists {commandName}";

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                using (var command = new MySqlCommand(commandText, connection))
                {
                    if (transaction != null)
                        command.Transaction = transaction;

                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                }

                if (filter != null)
                {
                    using (var command = new MySqlCommand())
                    {
                        if (!alreadyOpened)
                            await connection.OpenAsync().ConfigureAwait(false);

                        if (this.transaction != null)
                            command.Transaction = this.transaction;

                        filter.ValidateColumnFilters(this.tableDescription);

                        var commandNameWithFilter = this.mySqlObjectNames.GetCommandName(DbCommandType.SelectChangesWithFilters, filter).name;

                        command.CommandText = $"DROP PROCEDURE {commandNameWithFilter};";
                        command.Connection = this.connection;
                        await command.ExecuteNonQueryAsync().ConfigureAwait(false);

                    }
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during DropProcedureCommand : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

            }

        }

        public Task DropSelectInitializedChangesAsync(SyncFilter filter) => this.DropProcedureAsync(DbCommandType.SelectInitializedChanges, filter);
        public Task DropSelectRowAsync() => this.DropProcedureAsync(DbCommandType.SelectRow);
        public Task DropSelectIncrementalChangesAsync(SyncFilter filter) => this.DropProcedureAsync(DbCommandType.SelectChanges, filter);
        public Task DropUpdateAsync() => this.DropProcedureAsync(DbCommandType.UpdateRow);
        public Task DropDeleteAsync() => this.DropProcedureAsync(DbCommandType.DeleteRow);
        public Task DropDeleteMetadataAsync() => this.DropProcedureAsync(DbCommandType.DeleteMetadata);
        public Task DropResetAsync() => this.DropProcedureAsync(DbCommandType.Reset);
        public Task DropTVPTypeAsync() => Task.CompletedTask;
        public Task DropBulkUpdateAsync() => Task.CompletedTask;
        public Task DropBulkDeleteAsync() => Task.CompletedTask;

        //------------------------------------------------------------------
        // Select changes command
        //------------------------------------------------------------------
        private MySqlCommand BuildSelectInitializedChangesCommand(SyncFilter filter)
        {
            var sqlCommand = new MySqlCommand();

            // Add filter parameters
            if (filter != null)
                CreateFilterParameters(sqlCommand, filter);

            var stringBuilder = new StringBuilder("SELECT DISTINCT");
            var columns = this.tableDescription.GetMutableColumns(false, true).ToList();

            for (var i = 0; i < columns.Count; i++)
            {
                var mutableColumn = columns[i];
                var columnName = ParserName.Parse(mutableColumn, "`").Quoted().ToString();
                stringBuilder.AppendLine($"\t`base`.{columnName}");

                if (i < columns.Count - 1)
                    stringBuilder.Append(", ");
            }
            stringBuilder.AppendLine($"FROM {tableName.Quoted().ToString()} `base`");

            if (filter != null)
            {
                // ----------------------------------
                // Custom Joins
                // ----------------------------------
                stringBuilder.Append(CreateFilterCustomJoins(filter));

                // ----------------------------------
                // Where filters on [side]
                // ----------------------------------

                var whereString = CreateFilterWhereSide(filter);
                var customWhereString = CreateFilterCustomWheres(filter);

                if (!string.IsNullOrEmpty(whereString) || !string.IsNullOrEmpty(customWhereString))
                {
                    stringBuilder.AppendLine("WHERE");

                    if (!string.IsNullOrEmpty(whereString))
                        stringBuilder.AppendLine(whereString);

                    if (!string.IsNullOrEmpty(whereString) && !string.IsNullOrEmpty(customWhereString))
                        stringBuilder.AppendLine("AND");

                    if (!string.IsNullOrEmpty(customWhereString))
                        stringBuilder.AppendLine(customWhereString);
                }
            }
            // ----------------------------------

            stringBuilder.Append(";");
            sqlCommand.CommandText = stringBuilder.ToString();

            return sqlCommand;
        }

        public async Task CreateSelectInitializedChangesAsync(SyncFilter filter)
        {
            var commandName = this.mySqlObjectNames.GetCommandName(DbCommandType.SelectInitializedChanges).name;
            Func<MySqlCommand> cmdWithoutFilter = () => BuildSelectInitializedChangesCommand(null);
            await CreateProcedureCommandAsync(cmdWithoutFilter, commandName).ConfigureAwait(false);

            if (filter != null)
            {
                filter.ValidateColumnFilters(this.tableDescription);
                commandName = this.mySqlObjectNames.GetCommandName(DbCommandType.SelectInitializedChangesWithFilters, filter).name;
                Func<MySqlCommand> cmdWithFilter = () => BuildSelectInitializedChangesCommand(filter);
                await CreateProcedureCommandAsync(cmdWithFilter, commandName).ConfigureAwait(false);

            }

        }

    }
}
