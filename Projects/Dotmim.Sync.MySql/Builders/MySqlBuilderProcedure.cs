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
    public class MySqlBuilderProcedure
    {
        private ParserName tableName;
        private ParserName trackingName;
        private SyncTable tableDescription;
        private SyncSetup setup;
        private MySqlObjectNames mySqlObjectNames;
        private MySqlDbMetadata mySqlDbMetadata;
        internal const string MYSQL_PREFIX_PARAMETER = "in_";

        public MySqlBuilderProcedure(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup)
        {
            this.tableDescription = tableDescription;
            this.setup = setup;
            this.tableName = tableName;
            this.trackingName = trackingName;
            this.mySqlObjectNames = new MySqlObjectNames(this.tableDescription, tableName, trackingName, this.setup);
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

            var parameterName = ParserName.Parse(column, "`").Unquoted().Normalized().ToString();

            var sqlParameter = new MySqlParameter
            {
                ParameterName = $"{MySqlBuilderProcedure.MYSQL_PREFIX_PARAMETER}{parameterName}",
                DbType = column.GetDbType(),
                IsNullable = column.AllowDBNull
            };

#if MARIADB
            (byte precision, byte scale) = mySqlDbMetadata.TryGetOwnerPrecisionAndScale(column.OriginalDbType, column.GetDbType(), false, false, column.MaxLength, column.Precision, column.Scale, this.tableDescription.OriginalProvider, MariaDB.MariaDBSyncProvider.ProviderType);
#elif MYSQL
            (byte precision, byte scale) = mySqlDbMetadata.TryGetOwnerPrecisionAndScale(column.OriginalDbType, column.GetDbType(), false, false, column.MaxLength, column.Precision, column.Scale, this.tableDescription.OriginalProvider, MySqlSyncProvider.ProviderType);
#endif

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
        private DbCommand CreateProcedureCommand(Func<MySqlCommand> BuildCommand, string procName, DbConnection connection, DbTransaction transaction)
        {
            var str = CreateProcedureCommandText(BuildCommand(), procName);

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = str;

            return command;

        }

        private DbCommand CreateProcedureCommand<T>(Func<T, MySqlCommand> BuildCommand, string procName, T t, DbConnection connection, DbTransaction transaction)
        {
            var str = CreateProcedureCommandText(BuildCommand(t), procName);

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = str;

            return command;
        }

        public Task<DbCommand> GetCreateStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
        {
            var command = storedProcedureType switch
            {
                DbStoredProcedureType.SelectChanges => this.CreateSelectIncrementalChangesCommand(connection, transaction),
                DbStoredProcedureType.SelectChangesWithFilters => this.CreateSelectIncrementalChangesWithFilterCommand(filter, connection, transaction),
                DbStoredProcedureType.SelectInitializedChanges => this.CreateSelectInitializedChangesCommand(connection, transaction),
                DbStoredProcedureType.SelectInitializedChangesWithFilters => this.CreateSelectInitializedChangesWithFilterCommand(filter, connection, transaction),
                DbStoredProcedureType.SelectRow => this.CreateSelectRowCommand(connection, transaction),
                DbStoredProcedureType.UpdateRow => this.CreateUpdateCommand(connection, transaction),
                DbStoredProcedureType.DeleteRow => this.CreateDeleteCommand(connection, transaction),
                DbStoredProcedureType.DeleteMetadata => this.CreateDeleteMetadataCommand(connection, transaction),
                DbStoredProcedureType.Reset => this.CreateResetCommand(connection, transaction),
                _ => throw new NotImplementedException($"this DbStoredProcedureType {storedProcedureType} has no command to create.")
            };

            return Task.FromResult(command);
        }

        public Task<DbCommand> GetExistsStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
        {
            if (filter == null && (storedProcedureType == DbStoredProcedureType.SelectChangesWithFilters || storedProcedureType == DbStoredProcedureType.SelectInitializedChangesWithFilters))
                return Task.FromResult<DbCommand>(null);

            var quotedProcedureName = this.mySqlObjectNames.GetStoredProcedureCommandName(storedProcedureType, filter);

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

        public Task<DbCommand> GetDropStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
        {
            if (filter == null && (storedProcedureType == DbStoredProcedureType.SelectChangesWithFilters || storedProcedureType == DbStoredProcedureType.SelectInitializedChangesWithFilters))
                return Task.FromResult<DbCommand>(null);

            var quotedProcedureName = this.mySqlObjectNames.GetStoredProcedureCommandName(storedProcedureType, filter);
            var commandText = $"drop procedure {quotedProcedureName}";

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = commandText;

            return Task.FromResult(command);
        }

        //------------------------------------------------------------------
        // Reset command
        //------------------------------------------------------------------
        private MySqlCommand BuildResetCommand()
        {

            var sqlCommand = new MySqlCommand();
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"SET FOREIGN_KEY_CHECKS=0;"); 
            stringBuilder.AppendLine($"DELETE FROM {tableName.Quoted().ToString()};");
            stringBuilder.AppendLine($"DELETE FROM {trackingName.Quoted().ToString()};");
            stringBuilder.AppendLine($"SET FOREIGN_KEY_CHECKS=1;");
            stringBuilder.AppendLine();

            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
        public DbCommand CreateResetCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandName = this.mySqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.Reset);
            return CreateProcedureCommand(BuildResetCommand, commandName, connection, transaction);
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
            stringBuilder.AppendLine($"SELECT `timestamp`, `update_scope_id` FROM {trackingName.Quoted().ToString()} WHERE {MySqlManagementUtils.WhereColumnAndParameters(this.tableDescription.GetPrimaryKeysColumns(), trackingName.Quoted().ToString())} LIMIT 1 INTO ts, t_update_scope_id;");
            stringBuilder.AppendLine($"DELETE FROM {tableName.Quoted().ToString()} WHERE");
            stringBuilder.AppendLine(MySqlManagementUtils.WhereColumnAndParameters(this.tableDescription.GetPrimaryKeysColumns(), ""));
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
            stringBuilder.AppendLine($"\tWHERE {MySqlManagementUtils.WhereColumnAndParameters(this.tableDescription.GetPrimaryKeysColumns(), "")};");
            stringBuilder.AppendLine($"END IF;");


            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        public DbCommand CreateDeleteCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandName = this.mySqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.DeleteRow);
            return CreateProcedureCommand(BuildDeleteCommand, commandName, connection, transaction);
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

        public DbCommand CreateDeleteMetadataCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandName = this.mySqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.DeleteMetadata);
            return CreateProcedureCommand(BuildDeleteMetadataCommand, commandName, connection, transaction);
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

        public DbCommand CreateSelectRowCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandName = this.mySqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectRow);
            return CreateProcedureCommand(BuildSelectRowCommand, commandName, connection, transaction);
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


            var and = "";
            foreach (var column in this.tableDescription.GetPrimaryKeysColumns())
            {
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
                listColumnsTmp3.Append($"{and}t_{param.ParameterName} IS NULL");

                and = " AND ";

            }



            stringBuilder.AppendLine("DECLARE ts BIGINT;");

            stringBuilder.AppendLine("DECLARE t_update_scope_id VARCHAR(36);");
            stringBuilder.AppendLine("SET ts = 0;");
            stringBuilder.AppendLine($"SELECT {listColumnsTmp.ToString()}");
            stringBuilder.AppendLine($"`timestamp`, `update_scope_id` FROM {trackingName.Quoted().ToString()} ");
            stringBuilder.AppendLine($"WHERE {MySqlManagementUtils.WhereColumnAndParameters(this.tableDescription.GetPrimaryKeysColumns(), trackingName.Quoted().ToString())} LIMIT 1 ");
            stringBuilder.AppendLine($"INTO {listColumnsTmp2.ToString()} ts, t_update_scope_id;");
            stringBuilder.AppendLine();

            if (hasMutableColumns)
            {
                stringBuilder.AppendLine($"UPDATE {tableName.Quoted().ToString()}");
                stringBuilder.Append($"SET {MySqlManagementUtils.CommaSeparatedUpdateFromParameters(this.tableDescription)}");
                stringBuilder.Append($"WHERE {MySqlManagementUtils.WhereColumnAndParameters(this.tableDescription.GetPrimaryKeysColumns(), "")}");
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
            stringBuilder.AppendLine($"\tWHERE {MySqlManagementUtils.WhereColumnAndParameters(this.tableDescription.GetPrimaryKeysColumns(), "")};");
            stringBuilder.AppendLine($"END IF;");

            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        public DbCommand CreateUpdateCommand(DbConnection connection, DbTransaction transaction)
        {
            // Check if we have mutables columns
            var hasMutableColumns = this.tableDescription.GetMutableColumns(false).Any();

            var commandName = this.mySqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.UpdateRow);
            return this.CreateProcedureCommand(BuildUpdateCommand, commandName, hasMutableColumns, connection, transaction);
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
#if MARIADB
                    var sqlDbType = (MySqlDbType)this.mySqlDbMetadata.TryGetOwnerDbType(null, param.DbType.Value, false, false, param.MaxLength, MariaDB.MariaDBSyncProvider.ProviderType, MariaDB.MariaDBSyncProvider.ProviderType);
#elif MYSQL
                    var sqlDbType = (MySqlDbType)this.mySqlDbMetadata.TryGetOwnerDbType(null, param.DbType.Value, false, false, param.MaxLength, MySqlSyncProvider.ProviderType, MySqlSyncProvider.ProviderType);
#endif


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
#if MARIADB
                    var sqlDbType = (SqlDbType)this.mySqlDbMetadata.TryGetOwnerDbType(columnFilter.OriginalDbType, columnFilter.GetDbType(), false, false, columnFilter.MaxLength, tableFilter.OriginalProvider, MariaDB.MariaDBSyncProvider.ProviderType);
#elif MYSQL
                    var sqlDbType = (SqlDbType)this.mySqlDbMetadata.TryGetOwnerDbType(columnFilter.OriginalDbType, columnFilter.GetDbType(), false, false, columnFilter.MaxLength, tableFilter.OriginalProvider, MySqlSyncProvider.ProviderType);
#endif

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
#if MARIADB
                var sqlDbType = (MySqlDbType)this.mySqlDbMetadata.TryGetOwnerDbType(columnFilter.OriginalDbType, columnFilter.GetDbType(), false, false, columnFilter.MaxLength, tableFilter.OriginalProvider, MariaDB.MariaDBSyncProvider.ProviderType);
#elif MYSQL
                var sqlDbType = (MySqlDbType)this.mySqlDbMetadata.TryGetOwnerDbType(columnFilter.OriginalDbType, columnFilter.GetDbType(), false, false, columnFilter.MaxLength, tableFilter.OriginalProvider, MySqlSyncProvider.ProviderType);
#endif

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

            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
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

        public DbCommand CreateSelectIncrementalChangesCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandName = this.mySqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectChanges);
            Func<MySqlCommand> cmdWithoutFilter = () => BuildSelectIncrementalChangesCommand(null);
            return CreateProcedureCommand(cmdWithoutFilter, commandName, connection, transaction);

        }

        public DbCommand CreateSelectIncrementalChangesWithFilterCommand(SyncFilter filter, DbConnection connection, DbTransaction transaction)
        {
            if (filter == null)
                return null;

            var commandName = this.mySqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectChangesWithFilters, filter);
            Func<MySqlCommand> cmdWithFilter = () => BuildSelectIncrementalChangesCommand(filter);
            return CreateProcedureCommand(cmdWithFilter, commandName, connection, transaction);
        }

        //------------------------------------------------------------------
        // Select changes command
        //------------------------------------------------------------------
        private MySqlCommand BuildSelectInitializedChangesCommand(SyncFilter filter)
        {
            var sqlCommand = new MySqlCommand();

            var syncMinParameter = new MySqlParameter
            {
                ParameterName = "sync_min_timestamp",
                MySqlDbType = MySqlDbType.Int64,
                Value = 0
            };

            sqlCommand.Parameters.Add(syncMinParameter);

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

            // ----------------------------------
            // Make Left Join
            // ----------------------------------
            stringBuilder.Append($"LEFT JOIN {trackingName.Quoted().ToString()} `side` ON ");


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

            stringBuilder.AppendLine("\t(`side`.`timestamp` > sync_min_timestamp or sync_min_timestamp IS NULL));");
            sqlCommand.CommandText = stringBuilder.ToString();

            return sqlCommand;
        }
        public DbCommand CreateSelectInitializedChangesCommand(DbConnection connection, DbTransaction transaction)
        {
            var commandName = this.mySqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectInitializedChanges);
            Func<MySqlCommand> cmdWithoutFilter = () => BuildSelectInitializedChangesCommand(null);
            return CreateProcedureCommand(cmdWithoutFilter, commandName, connection, transaction);
        }

        public DbCommand CreateSelectInitializedChangesWithFilterCommand(SyncFilter filter, DbConnection connection, DbTransaction transaction)
        {
            if (filter == null)
                return null;

            var commandName = this.mySqlObjectNames.GetStoredProcedureCommandName(DbStoredProcedureType.SelectInitializedChangesWithFilters, filter);
            Func<MySqlCommand> cmdWithFilter = () => BuildSelectInitializedChangesCommand(filter);
            return CreateProcedureCommand(cmdWithFilter, commandName, connection, transaction);
        }
    }
}
