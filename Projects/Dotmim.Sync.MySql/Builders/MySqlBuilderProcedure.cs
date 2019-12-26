using Dotmim.Sync.Builders;
using System;
using System.Text;
using Dotmim.Sync.Data;
using System.Data.Common;
using System.Data;
using Dotmim.Sync.Log;
using System.Linq;
using Dotmim.Sync.Filter;
using MySql.Data.MySqlClient;
using Dotmim.Sync.MySql;
using Dotmim.Sync.MySql.Builders;
using System.Diagnostics;
using System.Collections.Generic;

namespace Dotmim.Sync.MySql
{
    public class MySqlBuilderProcedure : IDbBuilderProcedureHelper
    {
        private ParserName tableName;
        private ParserName trackingName;
        private MySqlConnection connection;
        private MySqlTransaction transaction;
        private SyncTable tableDescription;
        private MySqlObjectNames sqlObjectNames;
        private MySqlDbMetadata mySqlDbMetadata;
        internal const string MYSQL_PREFIX_PARAMETER = "in_";

        public SyncFilters Filters { get; set; }

        public MySqlBuilderProcedure(SyncTable tableDescription, DbConnection connection, DbTransaction transaction = null)
        {
            this.connection = connection as MySqlConnection;
            this.transaction = transaction as MySqlTransaction;

            this.tableDescription = tableDescription;
            (this.tableName, this.trackingName) = MySqlBuilder.GetParsers(tableDescription);
            this.sqlObjectNames = new MySqlObjectNames(this.tableDescription);
            this.mySqlDbMetadata = new MySqlDbMetadata();
        }

        private void AddPkColumnParametersToCommand(MySqlCommand sqlCommand)
        {
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
                sqlCommand.Parameters.Add(pkColumn.GetMySqlParameter());
        }
        private void AddColumnParametersToCommand(MySqlCommand sqlCommand)
        {
            foreach (var column in this.tableDescription.Columns.Where(c => !c.IsReadOnly))
                sqlCommand.Parameters.Add(column.GetMySqlParameter());
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
            stringBuilder3.Append($"{param.ParameterName} {stringType}{precision}");

            return stringBuilder3.ToString();

        }

        /// <summary>
        /// From a SqlCommand, create a stored procedure string
        /// </summary>
        private string CreateProcedureCommandText(MySqlCommand cmd, string procName)
        {
            StringBuilder stringBuilder = new StringBuilder();
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
        private void CreateProcedureCommand(Func<MySqlCommand> BuildCommand, string procName)
        {

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                var str = CreateProcedureCommandText(BuildCommand(), procName);
                using (var command = new MySqlCommand(str, connection))
                {
                    if (transaction != null)
                        command.Transaction = transaction;

                    command.ExecuteNonQuery();
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

        private string CreateProcedureCommandScriptText(Func<MySqlCommand> BuildCommand, string procName)
        {

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                var str1 = $"Command {procName} for table {tableName.Quoted().ToString()}";
                var str = CreateProcedureCommandText(BuildCommand(), procName);
                return MySqlBuilder.WrapScriptTextWithComments(str, str1);


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
        public bool NeedToCreateProcedure(DbCommandType commandType)
        {
            if (connection.State != ConnectionState.Open)
                throw new ArgumentException("Here, we need an opened connection please");

            var commandName = this.sqlObjectNames.GetCommandName(commandType).name;

            return !MySqlManagementUtils.ProcedureExists(connection, transaction, commandName);
        }

        /// <summary>
        /// Check if we need to create the TVP Type
        /// </summary>
        public bool NeedToCreateType(DbCommandType commandType)
        {
            return false;
        }

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
        public void CreateReset()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.Reset).name;
            CreateProcedureCommand(BuildResetCommand, commandName);
        }
        public string CreateResetScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.Reset).name;
            return CreateProcedureCommandScriptText(BuildResetCommand, commandName);
        }

       //------------------------------------------------------------------
        // Delete command
        //------------------------------------------------------------------
        private MySqlCommand BuildDeleteCommand()
        {
            MySqlCommand sqlCommand = new MySqlCommand();
            this.AddPkColumnParametersToCommand(sqlCommand);
            MySqlParameter sqlParameter = new MySqlParameter();
            sqlParameter.ParameterName = "sync_force_write";
            sqlParameter.MySqlDbType = MySqlDbType.Int32;
            sqlCommand.Parameters.Add(sqlParameter);

            MySqlParameter sqlParameter1 = new MySqlParameter();
            sqlParameter1.ParameterName = "sync_min_timestamp";
            sqlParameter1.MySqlDbType = MySqlDbType.Int64;
            sqlCommand.Parameters.Add(sqlParameter1);

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("DECLARE ts BIGINT;");
            stringBuilder.AppendLine("SET ts = 0;");
            stringBuilder.AppendLine($"SELECT `timestamp`, `update_scope_id` FROM {trackingName.Quoted().ToString()} WHERE {MySqlManagementUtils.WhereColumnAndParameters(this.tableDescription.PrimaryKeys, trackingName.Quoted().ToString())} LIMIT 1 INTO ts, updatescopeid;");
            stringBuilder.AppendLine($"DELETE FROM {tableName.Quoted().ToString()} WHERE");
            stringBuilder.AppendLine(MySqlManagementUtils.WhereColumnAndParameters(this.tableDescription.PrimaryKeys, ""));
            stringBuilder.AppendLine("AND (ts <= sync_min_timestamp OR updatescopeid IS NULL OR sync_force_write = 1);");
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        public void CreateDelete()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteRow).name;
            CreateProcedureCommand(BuildDeleteCommand, commandName);
        }

        public string CreateDeleteScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteRow).name;
            return CreateProcedureCommandScriptText(BuildDeleteCommand, commandName);
        }


        //------------------------------------------------------------------
        // Delete Metadata command
        //------------------------------------------------------------------
        private MySqlCommand BuildDeleteMetadataCommand()
        {
            MySqlCommand sqlCommand = new MySqlCommand();
            this.AddPkColumnParametersToCommand(sqlCommand);
            MySqlParameter sqlParameter = new MySqlParameter();
            sqlParameter.ParameterName = "sync_check_concurrency";
            sqlParameter.MySqlDbType = MySqlDbType.Int32;
            sqlCommand.Parameters.Add(sqlParameter);

            MySqlParameter sqlParameter1 = new MySqlParameter();
            sqlParameter1.ParameterName = "sync_row_timestamp";
            sqlParameter1.MySqlDbType = MySqlDbType.Int64;
            sqlCommand.Parameters.Add(sqlParameter1);

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"DELETE FROM {trackingName.Quoted().ToString()} ");
            stringBuilder.Append($"WHERE ");
            stringBuilder.AppendLine(MySqlManagementUtils.ColumnsAndParameters(this.tableDescription.PrimaryKeys, ""));
            stringBuilder.Append(";");
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        public void CreateDeleteMetadata()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteMetadata).name;
            CreateProcedureCommand(BuildDeleteMetadataCommand, commandName);
        }

        public string CreateDeleteMetadataScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteMetadata).name;
            return CreateProcedureCommandScriptText(BuildDeleteMetadataCommand, commandName);
        }


        //------------------------------------------------------------------
        // Insert command
        //------------------------------------------------------------------
        private MySqlCommand BuildInsertCommand()
        {
            MySqlCommand sqlCommand = new MySqlCommand();
            StringBuilder stringBuilder = new StringBuilder();
            StringBuilder stringBuilderArguments = new StringBuilder();
            StringBuilder stringBuilderParameters = new StringBuilder();

            this.AddColumnParametersToCommand(sqlCommand);

            //stringBuilder.Append(string.Concat("IF ((SELECT COUNT(*) FROM ", trackingName.Quoted().ToString(), " WHERE "));
            //stringBuilder.Append(MySqlManagementUtils.ColumnsAndParameters(this.tableDescription.PrimaryKeys, string.Empty));
            //stringBuilder.AppendLine(") <= 0) THEN");

            string empty = string.Empty;
            foreach (var mutableColumn in this.tableDescription.Columns.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn, "`").Quoted().ToString();
                var parameterName = ParserName.Parse(mutableColumn, "`").Unquoted().Normalized().ToString();

                stringBuilderArguments.Append(string.Concat(empty, columnName));
                stringBuilderParameters.Append(string.Concat(empty, $"{MYSQL_PREFIX_PARAMETER}{parameterName}"));
                empty = ", ";
            }
            stringBuilder.AppendLine($"\tINSERT INTO {tableName.Quoted().ToString()}");
            stringBuilder.AppendLine($"\t({stringBuilderArguments.ToString()})");
            stringBuilder.AppendLine($"\tVALUES ({stringBuilderParameters.ToString()});");
            stringBuilder.AppendLine();

            //stringBuilder.AppendLine("END IF;");

            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        public void CreateInsert()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertRow).name;
            CreateProcedureCommand(BuildInsertCommand, commandName);
        }

        public string CreateInsertScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertRow).name;
            return CreateProcedureCommandScriptText(BuildInsertCommand, commandName);
        }

        //------------------------------------------------------------------
        // Insert Metadata command
        //------------------------------------------------------------------
        private MySqlCommand BuildInsertMetadataCommand()
        {
            StringBuilder stringBuilderArguments = new StringBuilder();
            StringBuilder stringBuilderParameters = new StringBuilder();
            MySqlCommand sqlCommand = new MySqlCommand();

            StringBuilder stringBuilder = new StringBuilder();
            this.AddPkColumnParametersToCommand(sqlCommand);
            MySqlParameter sqlParameter = new MySqlParameter();
            sqlParameter.ParameterName = "sync_scope_id";
            sqlParameter.MySqlDbType = MySqlDbType.Guid;
            sqlParameter.Size = 36;
            sqlCommand.Parameters.Add(sqlParameter);

            MySqlParameter sqlParameter1 = new MySqlParameter();
            sqlParameter1.ParameterName = "sync_row_is_tombstone";
            sqlParameter1.MySqlDbType = MySqlDbType.Int32;
            sqlCommand.Parameters.Add(sqlParameter1);

            MySqlParameter sqlParameter3 = new MySqlParameter();
            sqlParameter3.ParameterName = "create_timestamp";
            sqlParameter3.MySqlDbType = MySqlDbType.Int64;

            sqlCommand.Parameters.Add(sqlParameter3);
            MySqlParameter sqlParameter4 = new MySqlParameter();
            sqlParameter4.ParameterName = "update_timestamp";
            sqlParameter4.MySqlDbType = MySqlDbType.Int64;
            sqlCommand.Parameters.Add(sqlParameter4);

            stringBuilder.AppendLine($"\tINSERT INTO {trackingName.Quoted().ToString()}");

            string empty = string.Empty;
            foreach (var pkColumn in this.tableDescription.PrimaryKeys)
            {
                var columnName = ParserName.Parse(pkColumn, "`").Quoted().ToString();
                var parameterName = ParserName.Parse(pkColumn, "`").Unquoted().Normalized().ToString();

                stringBuilderArguments.Append(string.Concat(empty, columnName));
                stringBuilderParameters.Append(string.Concat(empty, $"{MYSQL_PREFIX_PARAMETER}{parameterName}"));
                empty = ", ";
            }
            stringBuilder.Append($"\t({stringBuilderArguments.ToString()}, ");
            stringBuilder.AppendLine($"`create_scope_id`, `create_timestamp`, `update_scope_id`, `update_timestamp`,");
            stringBuilder.AppendLine($"\t`sync_row_is_tombstone`, `timestamp`, `last_change_datetime`)");
            stringBuilder.Append($"\tVALUES ({stringBuilderParameters.ToString()}, ");
            stringBuilder.AppendLine($"\tcreate_scope_id, create_timestamp, update_scope_id, update_timestamp, ");
            stringBuilder.AppendLine($"\tsync_row_is_tombstone, {MySqlObjectNames.TimestampValue}, now())");
            stringBuilder.AppendLine($"\tON DUPLICATE KEY UPDATE");
            stringBuilder.AppendLine($"\t `create_scope_id` = sync_scope_id, ");
            stringBuilder.AppendLine($"\t `create_timestamp` = create_timestamp, ");
            stringBuilder.AppendLine($"\t `update_scope_id` = sync_scope_id, ");
            stringBuilder.AppendLine($"\t `update_timestamp` = update_timestamp, ");
            stringBuilder.AppendLine($"\t `sync_row_is_tombstone` = sync_row_is_tombstone, ");
            stringBuilder.AppendLine($"\t `timestamp` = {MySqlObjectNames.TimestampValue}, ");
            stringBuilder.AppendLine($"\t `last_change_datetime` = now(); ");

            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        public void CreateInsertMetadata()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertMetadata).name;
            CreateProcedureCommand(BuildInsertMetadataCommand, commandName);
        }

        public string CreateInsertMetadataScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertMetadata).name;
            return CreateProcedureCommandScriptText(BuildInsertMetadataCommand, commandName);
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
            stringBuilder.AppendLine("\t`side`.`sync_row_is_tombstone`,");
            stringBuilder.AppendLine("\t`side`.`create_scope_id`,");
            stringBuilder.AppendLine("\t`side`.`create_timestamp`,");
            stringBuilder.AppendLine("\t`side`.`update_scope_id`,");
            stringBuilder.AppendLine("\t`side`.`update_timestamp`");

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

        public void CreateSelectRow()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.SelectRow).name;
            CreateProcedureCommand(BuildSelectRowCommand, commandName);
        }

        public string CreateSelectRowScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.SelectRow).name;
            return CreateProcedureCommandScriptText(BuildSelectRowCommand, commandName);
        }


        //------------------------------------------------------------------
        // Update command
        //------------------------------------------------------------------
        private MySqlCommand BuildUpdateCommand()
        {
            MySqlCommand sqlCommand = new MySqlCommand();

            StringBuilder stringBuilder = new StringBuilder();
            this.AddColumnParametersToCommand(sqlCommand);

            MySqlParameter sqlParameter = new MySqlParameter();
            sqlParameter.ParameterName = "sync_force_write";
            sqlParameter.MySqlDbType = MySqlDbType.Int32;
            sqlCommand.Parameters.Add(sqlParameter);

            MySqlParameter sqlParameter1 = new MySqlParameter();
            sqlParameter1.ParameterName = "sync_min_timestamp";
            sqlParameter1.MySqlDbType = MySqlDbType.Int64;
            sqlCommand.Parameters.Add(sqlParameter1);


            stringBuilder.AppendLine("DECLARE ts BIGINT;");
            stringBuilder.AppendLine("SET ts = 0;");
            stringBuilder.AppendLine($"SELECT `timestamp`, `update_scope_id` FROM {trackingName.Quoted().ToString()} WHERE {MySqlManagementUtils.WhereColumnAndParameters(this.tableDescription.PrimaryKeys, trackingName.Quoted().ToString())} LIMIT 1 INTO ts, updatescopeid;");

            stringBuilder.AppendLine($"UPDATE {tableName.Quoted().ToString()}");
            stringBuilder.Append($"SET {MySqlManagementUtils.CommaSeparatedUpdateFromParameters(this.tableDescription)}");
            stringBuilder.Append($"WHERE {MySqlManagementUtils.WhereColumnAndParameters(this.tableDescription.PrimaryKeys, "")}");
            stringBuilder.AppendLine($" AND (ts <= sync_min_timestamp OR updatescopeid IS NULL OR sync_force_write = 1);");
            stringBuilder.AppendLine();

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
            stringBuilder.AppendLine($"INSERT INTO {tableName.Quoted().ToString()}");
            stringBuilder.AppendLine($"({stringBuilderArguments.ToString()})");
            stringBuilder.AppendLine($"SELECT * FROM ( SELECT {stringBuilderParameters.ToString()}) as TMP ");
            stringBuilder.AppendLine($"WHERE NOT EXISTS ( ");
            stringBuilder.AppendLine($"  SELECT * FROM {tableName.Quoted().ToString()} ");
            stringBuilder.AppendLine($"  WHERE {MySqlManagementUtils.WhereColumnAndParameters(this.tableDescription.PrimaryKeys, "")}");
            stringBuilder.AppendLine($") LIMIT 1");
            stringBuilder.AppendLine();

            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        public void CreateUpdate()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateRow).name;
            this.CreateProcedureCommand(BuildUpdateCommand, commandName);
        }

        public string CreateUpdateScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateRow).name;
            return CreateProcedureCommandScriptText(BuildUpdateCommand, commandName);
        }

        //------------------------------------------------------------------
        // Update Metadata command
        //------------------------------------------------------------------
        private MySqlCommand BuildUpdateMetadataCommand()
        {
            MySqlCommand sqlCommand = new MySqlCommand();
            StringBuilder stringBuilder = new StringBuilder();
            this.AddPkColumnParametersToCommand(sqlCommand);
            MySqlParameter sqlParameter = new MySqlParameter();
            sqlParameter.ParameterName = "sync_scope_id";
            sqlParameter.MySqlDbType = MySqlDbType.Guid;
            sqlParameter.Size = 36;
            sqlCommand.Parameters.Add(sqlParameter);

            MySqlParameter sqlParameter1 = new MySqlParameter();
            sqlParameter1.ParameterName = "sync_row_is_tombstone";
            sqlParameter1.MySqlDbType = MySqlDbType.Int32;
            sqlCommand.Parameters.Add(sqlParameter1);

            MySqlParameter sqlParameter3 = new MySqlParameter();
            sqlParameter3.ParameterName = "create_timestamp";
            sqlParameter3.MySqlDbType = MySqlDbType.Int64;
            sqlCommand.Parameters.Add(sqlParameter3);

            MySqlParameter sqlParameter5 = new MySqlParameter();
            sqlParameter5.ParameterName = "update_timestamp";
            sqlParameter5.MySqlDbType = MySqlDbType.Int64;
            sqlCommand.Parameters.Add(sqlParameter5);

            stringBuilder.AppendLine($"DECLARE was_tombstone int;");
            stringBuilder.AppendLine($"SET was_tombstone = 1;");
            stringBuilder.AppendLine($"SELECT `sync_row_is_tombstone` " +
                                     $"FROM {trackingName.Quoted().ToString()} " +
                                     $"WHERE {MySqlManagementUtils.WhereColumnAndParameters(this.tableDescription.PrimaryKeys, trackingName.Quoted().ToString())} " +
                                     $"LIMIT 1 INTO was_tombstone;");
            stringBuilder.AppendLine($"IF (was_tombstone is not null and was_tombstone = 1 and sync_row_is_tombstone = 0) THEN");

            stringBuilder.AppendLine($"UPDATE {trackingName.Quoted().ToString()}");
            stringBuilder.AppendLine($"SET `create_scope_id` = sync_scope_id, ");
            stringBuilder.AppendLine($"\t `update_scope_id` = sync_scope_id, ");
            stringBuilder.AppendLine($"\t `create_timestamp` = create_timestamp, ");
            stringBuilder.AppendLine($"\t `update_timestamp` = update_timestamp, ");
            stringBuilder.AppendLine($"\t `sync_row_is_tombstone` = sync_row_is_tombstone, ");
            stringBuilder.AppendLine($"\t `timestamp` = {MySqlObjectNames.TimestampValue}, ");
            stringBuilder.AppendLine($"\t `last_change_datetime` = now() ");
            stringBuilder.AppendLine($"WHERE {MySqlManagementUtils.WhereColumnAndParameters(this.tableDescription.PrimaryKeys, "")};");

            stringBuilder.AppendLine($"ELSE");

            stringBuilder.AppendLine($"UPDATE {trackingName.Quoted().ToString()}");
            stringBuilder.AppendLine($"SET `update_scope_id` = sync_scope_id, ");
            stringBuilder.AppendLine($"\t `update_timestamp` = update_timestamp, ");
            stringBuilder.AppendLine($"\t `sync_row_is_tombstone` = sync_row_is_tombstone, ");
            stringBuilder.AppendLine($"\t `timestamp` = {MySqlObjectNames.TimestampValue}, ");
            stringBuilder.AppendLine($"\t `last_change_datetime` = now() ");
            stringBuilder.AppendLine($"WHERE {MySqlManagementUtils.WhereColumnAndParameters(this.tableDescription.PrimaryKeys, "")};");
            stringBuilder.AppendLine("END IF;");

            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        public void CreateUpdateMetadata()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateMetadata).name;
            CreateProcedureCommand(BuildUpdateMetadataCommand, commandName);
        }

        public string CreateUpdateMetadataScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateMetadata).name;
            return CreateProcedureCommandScriptText(BuildUpdateMetadataCommand, commandName);
        }


        //------------------------------------------------------------------
        // Select changes command
        //------------------------------------------------------------------
        private MySqlCommand BuildSelectIncrementalChangesCommand(bool withFilter = false)
        {
            var sqlCommand = new MySqlCommand();
            var sqlParameter1 = new MySqlParameter();
            sqlParameter1.ParameterName = "sync_min_timestamp";
            sqlParameter1.MySqlDbType = MySqlDbType.Int64;
            sqlCommand.Parameters.Add(sqlParameter1);

            var sqlParameter3 = new MySqlParameter();
            sqlParameter3.ParameterName = "sync_scope_id";
            sqlParameter3.MySqlDbType = MySqlDbType.Guid;
            sqlParameter3.Size = 36;
            sqlCommand.Parameters.Add(sqlParameter3);

            var sqlParameter4 = new MySqlParameter();
            sqlParameter4.ParameterName = "sync_scope_is_new";
            sqlParameter4.MySqlDbType = MySqlDbType.Bit;
            sqlCommand.Parameters.Add(sqlParameter4);

            var sqlParameter5 = new MySqlParameter();
            sqlParameter5.ParameterName = "sync_scope_is_reinit";
            sqlParameter5.MySqlDbType = MySqlDbType.Bit;
            sqlCommand.Parameters.Add(sqlParameter5);


            if (withFilter && this.Filters != null && this.Filters.Count() > 0)
            {
                foreach (var c in this.Filters)
                {
                    if (!c.IsVirtual)
                    {
                        var columnFilter = this.tableDescription.Columns[c.ColumnName];

                        if (columnFilter == null)
                            throw new InvalidExpressionException($"Column {c.ColumnName} does not exist in Table {this.tableDescription.TableName}");

                        var columnName = ParserName.Parse(columnFilter).Unquoted().Normalized().ToString();
                        var mySqlDbType = (MySqlDbType)this.mySqlDbMetadata.TryGetOwnerDbType(columnFilter.OriginalDbType, columnFilter.GetDbType(), false, false, columnFilter.MaxLength, this.tableDescription.OriginalProvider, MySqlSyncProvider.ProviderType);
                        var mySqlParamFilter = new MySqlParameter($"{MYSQL_PREFIX_PARAMETER}{columnName}", mySqlDbType);
                        sqlCommand.Parameters.Add(mySqlParamFilter);
                    }
                    else
                    {
                        var sqlDbType = (MySqlDbType)this.mySqlDbMetadata.TryGetOwnerDbType(null, (DbType)c.ColumnType.Value, false, false, 0, this.tableDescription.OriginalProvider, MySqlSyncProvider.ProviderType);
                        var columnFilterName = ParserName.Parse(c.ColumnName).Unquoted().Normalized().ToString();
                        var sqlParamFilter = new MySqlParameter($"{MYSQL_PREFIX_PARAMETER}{columnFilterName}", sqlDbType);
                        sqlCommand.Parameters.Add(sqlParamFilter);
                    }
                }
            }

            var stringBuilder = new StringBuilder("SELECT ");
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
            stringBuilder.AppendLine($"\t`side`.`create_scope_id`, ");
            stringBuilder.AppendLine($"\t`side`.`create_timestamp`, ");
            stringBuilder.AppendLine($"\t`side`.`update_scope_id`, ");
            stringBuilder.AppendLine($"\t`side`.`update_timestamp` ");
            stringBuilder.AppendLine($"FROM {tableName.Quoted().ToString()} `base`");
            stringBuilder.AppendLine($"RIGHT JOIN {trackingName.Quoted().ToString()} `side`");
            stringBuilder.Append($"ON ");

            string empty = "";
            foreach (var pkColumn in this.tableDescription.PrimaryKeys)
            {
                var pkColumnName = ParserName.Parse(pkColumn, "`").Quoted().ToString();
                stringBuilder.Append($"{empty}`base`.{pkColumnName} = `side`.{pkColumnName}");
                empty = " AND ";
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("WHERE (");
            string str = string.Empty;

            if (withFilter && this.Filters != null && this.Filters.Count() > 0)
            {
                StringBuilder builderFilter = new StringBuilder();
                builderFilter.Append("\t(");
                string filterSeparationString = "";
                foreach (var c in this.Filters)
                {
                    var columnFilter = this.tableDescription.Columns[c.ColumnName];

                    if (columnFilter == null)
                        throw new InvalidExpressionException($"Column {c.ColumnName} does not exist in Table {this.tableDescription.TableName}");

                    var columnFilterName = ParserName.Parse(columnFilter, "`").Quoted().ToString();
                    var unquotedColumnFilterName = ParserName.Parse(columnFilter, "`").Unquoted().Normalized().ToString();

                    builderFilter.Append($"`side`.{columnFilterName} = {MYSQL_PREFIX_PARAMETER}{unquotedColumnFilterName}{filterSeparationString}");
                    filterSeparationString = " AND ";
                }
                builderFilter.AppendLine(")");
                builderFilter.Append("\tOR (");
                builderFilter.AppendLine("(`side`.`update_scope_id` = sync_scope_id or `side`.`update_scope_id` IS NULL)");
                builderFilter.Append("\t\tAND (");

                filterSeparationString = "";
                foreach (var c in this.Filters)
                {
                    var columnFilter = this.tableDescription.Columns[c.ColumnName];
                    var columnFilterName = ParserName.Parse(columnFilter, "`").Quoted().ToString();

                    builderFilter.Append($"`side`.{columnFilterName} IS NULL{filterSeparationString}");
                    filterSeparationString = " OR ";
                }

                builderFilter.AppendLine("))");
                builderFilter.AppendLine("\t)");
                builderFilter.AppendLine("AND (");
                stringBuilder.Append(builderFilter.ToString());
            }

            stringBuilder.AppendLine("\t-- Update made by the local instance");
            stringBuilder.AppendLine("\t`side`.`update_scope_id` IS NULL");
            stringBuilder.AppendLine("\t-- Or Update different from remote");
            stringBuilder.AppendLine("\tOR `side`.`update_scope_id` <> sync_scope_id");
            stringBuilder.AppendLine("\t-- Or we are in reinit mode so we take rows even thoses updated by the scope");
            stringBuilder.AppendLine("\tOR sync_scope_is_reinit = 1");
            stringBuilder.AppendLine("    )");
            stringBuilder.AppendLine("AND (");
            stringBuilder.AppendLine("\t-- And Timestamp is > from remote timestamp");
            stringBuilder.AppendLine("\t`side`.`timestamp` > sync_min_timestamp");
            stringBuilder.AppendLine("\tOR");
            stringBuilder.AppendLine("\t-- remote instance is new, so we don't take the last timestamp");
            stringBuilder.AppendLine("\tsync_scope_is_new = 1");
            stringBuilder.AppendLine("\t)");
            stringBuilder.AppendLine("AND (");
            stringBuilder.AppendLine("\t`side`.`sync_row_is_tombstone` = 1 ");
            stringBuilder.AppendLine("\tOR");
            stringBuilder.Append("\t(`side`.`sync_row_is_tombstone` = 0");

            empty = " AND ";
            foreach (var pkColumn in this.tableDescription.PrimaryKeys)
            {
                var pkColumnName = ParserName.Parse(pkColumn, "`").Quoted().ToString();
                stringBuilder.Append($"{empty}`base`.{pkColumnName} is not null");
            }
            stringBuilder.AppendLine("\t)");
            stringBuilder.AppendLine(");");


            sqlCommand.CommandText = stringBuilder.ToString();

            //if (this._filterParameters != null)
            //{
            //    foreach (MySqlParameter _filterParameter in this._filterParameters)
            //    {
            //        sqlCommand.Parameters.Add(((ICloneable)_filterParameter).Clone());
            //    }
            //}
            return sqlCommand;
        }

        public void CreateSelectIncrementalChanges()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.SelectChanges).name;
            Func<MySqlCommand> cmdWithoutFilter = () => BuildSelectIncrementalChangesCommand(false);
            CreateProcedureCommand(cmdWithoutFilter, commandName);

            if (this.Filters != null && this.Filters.Count() > 0)
            {
                this.Filters.ValidateColumnFilters(this.tableDescription);
                commandName = this.sqlObjectNames.GetCommandName(DbCommandType.SelectChangesWitFilters, this.Filters).name;
                Func<MySqlCommand> cmdWithFilter = () => BuildSelectIncrementalChangesCommand(true);
                CreateProcedureCommand(cmdWithFilter, commandName);

            }

        }

        public string CreateSelectIncrementalChangesScriptText()
        {
            StringBuilder sbSelecteChanges = new StringBuilder();

            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.SelectChanges).name;
            Func<MySqlCommand> cmdWithoutFilter = () => BuildSelectIncrementalChangesCommand(false);
            sbSelecteChanges.AppendLine(CreateProcedureCommandScriptText(cmdWithoutFilter, commandName));


            if (this.Filters != null && this.Filters.Count > 0)
            {
                commandName = this.sqlObjectNames.GetCommandName(DbCommandType.SelectChangesWitFilters).name;
                string name = "";
                string sep = "";
                foreach (var c in this.Filters)
                {
                    var columnFilter = this.tableDescription.Columns[c.ColumnName];

                    if (columnFilter == null)
                        throw new InvalidExpressionException($"Column {c.ColumnName} does not exist in Table {this.tableDescription.TableName}");

                    var unquotedColumnName = ParserName.Parse(columnFilter, "`").Unquoted().Normalized().ToString();
                    name += $"{unquotedColumnName}{sep}";
                    sep = "_";
                }

                commandName = String.Format(commandName, name);
                Func<MySqlCommand> cmdWithFilter = () => BuildSelectIncrementalChangesCommand(true);
                sbSelecteChanges.AppendLine(CreateProcedureCommandScriptText(cmdWithFilter, commandName));

            }
            return sbSelecteChanges.ToString();
        }

        public void CreateTVPType()
        {
            throw new NotImplementedException();
        }

        public void CreateBulkInsert()
        {
            throw new NotImplementedException();
        }

        public void CreateBulkUpdate()
        {
            throw new NotImplementedException();
        }

        public void CreateBulkDelete()
        {
            throw new NotImplementedException();
        }

        public string CreateTVPTypeScriptText()
        {
            throw new NotImplementedException();
        }

        public string CreateBulkInsertScriptText()
        {
            throw new NotImplementedException();
        }

        public string CreateBulkUpdateScriptText()
        {
            throw new NotImplementedException();
        }

        public string CreateBulkDeleteScriptText()
        {
            throw new NotImplementedException();
        }


        private string DropProcedureText(DbCommandType procType)
        {
            var commandName = this.sqlObjectNames.GetCommandName(procType).name;
            var commandText = $"drop procedure if exists {commandName}";

            var str1 = $"Drop procedure {commandName} for table {tableName.Quoted().ToString()}";
            return MySqlBuilder.WrapScriptTextWithComments(commandText, str1);

        }
        private void DropProcedure(DbCommandType procType)
        {
            var commandName = this.sqlObjectNames.GetCommandName(procType).name;
            var commandText = $"drop procedure if exists {commandName}";

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                using (var command = new MySqlCommand(commandText, connection))
                {
                    if (transaction != null)
                        command.Transaction = transaction;

                    command.ExecuteNonQuery();
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

        public void DropSelectRow()
        {
            DropProcedure(DbCommandType.SelectRow);
        }

        public void DropSelectIncrementalChanges()
        {
            DropProcedure(DbCommandType.SelectChanges);

            // filtered 
            if (this.Filters != null && this.Filters.Count > 0)
            {
                bool alreadyOpened = this.connection.State == ConnectionState.Open;

                using (var command = new MySqlCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    foreach (var c in this.Filters)
                    {
                        var columnFilter = this.tableDescription.Columns[c.ColumnName];

                        if (columnFilter == null)
                            throw new InvalidExpressionException($"Column {c.ColumnName} does not exist in Table {this.tableDescription.TableName}");
                    }

                    var commandNameWithFilter = this.sqlObjectNames.GetCommandName(DbCommandType.SelectChangesWitFilters, this.Filters).name;

                    command.CommandText = $"DROP PROCEDURE IF EXISTS {commandNameWithFilter};";
                    command.Connection = this.connection;
                    command.ExecuteNonQuery();

                }

                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();

            }

        }

        public void DropInsert()
        {
            DropProcedure(DbCommandType.InsertRow);
        }

        public void DropUpdate()
        {
            DropProcedure(DbCommandType.UpdateRow);
        }

        public void DropDelete()
        {
            DropProcedure(DbCommandType.DeleteRow);
        }

        public void DropInsertMetadata()
        {
            DropProcedure(DbCommandType.InsertMetadata);
        }

        public void DropUpdateMetadata()
        {
            DropProcedure(DbCommandType.UpdateMetadata);
        }

        public void DropDeleteMetadata()
        {
            DropProcedure(DbCommandType.DeleteMetadata);
        }

        public void DropTVPType()
        {
            return;
        }

        public void DropBulkInsert()
        {
            return;
        }

        public void DropBulkUpdate()
        {
            return;
        }

        public void DropBulkDelete()
        {
            return;
        }

        public void DropReset()
        {
            DropProcedure(DbCommandType.Reset);
        }

        public string DropSelectRowScriptText()
        {
            return DropProcedureText(DbCommandType.SelectRow);
        }

        public string DropSelectIncrementalChangesScriptText()
        {
            return DropProcedureText(DbCommandType.SelectChanges);
        }

        public string DropInsertScriptText()
        {
            return DropProcedureText(DbCommandType.InsertRow);

        }

        public string DropUpdateScriptText()
        {
            return DropProcedureText(DbCommandType.UpdateRow);
        }

        public string DropDeleteScriptText()
        {
            return DropProcedureText(DbCommandType.DeleteRow);
        }

        public string DropInsertMetadataScriptText()
        {
            return DropProcedureText(DbCommandType.InsertMetadata);

        }

        public string DropUpdateMetadataScriptText()
        {
            return DropProcedureText(DbCommandType.UpdateMetadata);
        }

        public string DropDeleteMetadataScriptText()
        {
            return DropProcedureText(DbCommandType.DeleteMetadata);
        }

        public string DropTVPTypeScriptText()
        {
            throw new NotImplementedException();
        }

        public string DropBulkInsertScriptText()
        {
            throw new NotImplementedException();
        }

        public string DropBulkUpdateScriptText()
        {
            throw new NotImplementedException();
        }

        public string DropBulkDeleteScriptText()
        {
            throw new NotImplementedException();
        }

        public string DropResetScriptText()
        {
            return DropProcedureText(DbCommandType.Reset);
        }
    }
}
