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

namespace Dotmim.Sync.MySql
{
    public class MySqlBuilderProcedure : IDbBuilderProcedureHelper
    {
        private ObjectNameParser tableName;
        private ObjectNameParser trackingName;
        private MySqlConnection connection;
        private MySqlTransaction transaction;
        private DmTable tableDescription;
        private MySqlObjectNames sqlObjectNames;

        public FilterClauseCollection Filters { get; set; }

        public MySqlBuilderProcedure(DmTable tableDescription, DbConnection connection, DbTransaction transaction = null)
        {
            this.connection = connection as MySqlConnection;
            this.transaction = transaction as MySqlTransaction;

            this.tableDescription = tableDescription;
            (this.tableName, this.trackingName) = MySqlBuilder.GetParsers(tableDescription);
            this.sqlObjectNames = new MySqlObjectNames(this.tableDescription);
        }

        private void AddPkColumnParametersToCommand(MySqlCommand sqlCommand)
        {
            foreach (DmColumn pkColumn in this.tableDescription.PrimaryKey.Columns)
                sqlCommand.Parameters.Add(pkColumn.GetMySqlParameter());
        }
        private void AddColumnParametersToCommand(MySqlCommand sqlCommand)
        {
            foreach (DmColumn column in this.tableDescription.Columns.Where(c => !c.ReadOnly))
                sqlCommand.Parameters.Add(column.GetMySqlParameter());
        }

        /// <summary>
        /// From a SqlParameter, create the declaration
        /// </summary>
        internal static string CreateParameterDeclaration(MySqlParameter param)
        {
            StringBuilder stringBuilder3 = new StringBuilder();
            var sqlDbType = param.MySqlDbType;

            string empty = string.Empty;

            var stringType = MySqlMetadata.GetStringTypeFromDbType(param.DbType, param.Size);

            string precision = "";

            if (param.Size > 0)
            {
                precision = $"({param.Size})";
            }
            else if (param.Precision > 0 && param.Scale == 0)
            {
                precision = $"({param.Size})";

            }
            else if (param.Precision > 0 && param.Scale > 0)
            {
                precision = $"({param.Size},{param.Scale})";
            }

            stringBuilder3.Append($"{param.ParameterName} {stringType}{precision}");

            return stringBuilder3.ToString();

        }

        /// <summary>
        /// From a SqlCommand, create a stored procedure string
        /// </summary>
        private static string CreateProcedureCommandText(MySqlCommand cmd, string procName)
        {
            StringBuilder stringBuilder = new StringBuilder(string.Concat("CREATE PROCEDURE ", procName));
            string str = "\n\t";
            foreach (MySqlParameter parameter in cmd.Parameters)
            {
                stringBuilder.Append(string.Concat(str, CreateParameterDeclaration(parameter)));
                str = ",\n\t";
            }
            stringBuilder.Append("\nAS\nBEGIN\n");
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
                Logger.Current.Error($"Error during CreateProcedureCommand : {ex}");
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

                var str1 = $"Command {procName} for table {tableName.QuotedString}";
                var str = CreateProcedureCommandText(BuildCommand(), procName);
                return MySqlBuilder.WrapScriptTextWithComments(str, str1);


            }
            catch (Exception ex)
            {
                Logger.Current.Error($"Error during CreateProcedureCommand : {ex}");
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
        public bool NeedToCreateProcedure(DbCommandType commandType, DbBuilderOption option)
        {
            if (connection.State != ConnectionState.Open)
                throw new ArgumentException("Here, we need an opened connection please");

            var commandName = this.sqlObjectNames.GetCommandName(commandType);

            if (option.HasFlag(DbBuilderOption.CreateOrUseExistingSchema))
                return !MySqlManagementUtils.ProcedureExists(connection, transaction, commandName);

            return false;
        }

        /// <summary>
        /// Check if we need to create the TVP Type
        /// </summary>
        public bool NeedToCreateType(DbCommandType commandType, DbBuilderOption option)
        {
            return false;
        }




        //------------------------------------------------------------------
        // Delete command
        //------------------------------------------------------------------
        private MySqlCommand BuildDeleteCommand()
        {
            MySqlCommand sqlCommand = new MySqlCommand();
            this.AddPkColumnParametersToCommand(sqlCommand);
            MySqlParameter sqlParameter = new MySqlParameter("@sync_force_write", MySqlDbType.Int32);
            sqlCommand.Parameters.Add(sqlParameter);
            MySqlParameter sqlParameter1 = new MySqlParameter("@sync_min_timestamp", MySqlDbType.Int64);
            sqlCommand.Parameters.Add(sqlParameter1);

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("DECLARE ts BIGINT;");
            stringBuilder.AppendLine("SET ts = 0;");
            stringBuilder.AppendLine("SELECT `timestamp` FROM `customers_tracking` WHERE `CustomerID` = CustomerID LIMIT 1 INTO ts;");
            stringBuilder.AppendLine($"DELETE {tableName.QuotedString} WHERE");
            stringBuilder.AppendLine(MySqlManagementUtils.WhereColumnAndParameters(this.tableDescription.PrimaryKey.Columns, ""));
            stringBuilder.AppendLine("AND (ts <= @sync_min_timestamp  OR @sync_force_write = 1)");
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
        public void CreateDelete()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteRow);
            CreateProcedureCommand(BuildDeleteCommand, commandName);
        }
        public string CreateDeleteScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteRow);
            return CreateProcedureCommandScriptText(BuildDeleteCommand, commandName);
        }


        //------------------------------------------------------------------
        // Delete Metadata command
        //------------------------------------------------------------------
        private MySqlCommand BuildDeleteMetadataCommand()
        {
            MySqlCommand sqlCommand = new MySqlCommand();
            this.AddPkColumnParametersToCommand(sqlCommand);
            MySqlParameter sqlParameter = new MySqlParameter("@sync_check_concurrency", MySqlDbType.Int32);
            sqlCommand.Parameters.Add(sqlParameter);
            MySqlParameter sqlParameter1 = new MySqlParameter("@sync_row_timestamp", MySqlDbType.Int64);
            sqlCommand.Parameters.Add(sqlParameter1);
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"DELETE {trackingName.QuotedString} ");
            stringBuilder.Append($"WHERE ");
            stringBuilder.AppendLine(MySqlManagementUtils.ColumnsAndParameters(this.tableDescription.PrimaryKey.Columns, ""));
            stringBuilder.AppendLine();
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
        public void CreateDeleteMetadata()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteMetadata);
            CreateProcedureCommand(BuildDeleteMetadataCommand, commandName);
        }
        public string CreateDeleteMetadataScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteMetadata);
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

            stringBuilder.Append(string.Concat("IF ((SELECT COUNT(*) FROM ", trackingName.QuotedString, " WHERE "));
            stringBuilder.Append(MySqlManagementUtils.ColumnsAndParameters(this.tableDescription.PrimaryKey.Columns, string.Empty));
            stringBuilder.AppendLine(") <= 0)");
            stringBuilder.AppendLine("BEGIN ");

            string empty = string.Empty;
            foreach (var mutableColumn in this.tableDescription.Columns.Where(c => !c.ReadOnly))
            {
                ObjectNameParser columnName = new ObjectNameParser(mutableColumn.ColumnName);
                stringBuilderArguments.Append(string.Concat(empty, columnName.QuotedString));
                stringBuilderParameters.Append(string.Concat(empty, $"@{columnName.UnquotedString}"));
                empty = ", ";
            }
            stringBuilder.AppendLine($"\tINSERT INTO {tableName.QuotedString}");
            stringBuilder.AppendLine($"\t({stringBuilderArguments.ToString()})");
            stringBuilder.AppendLine($"\tVALUES ({stringBuilderParameters.ToString()});");
            stringBuilder.AppendLine();
            
            stringBuilder.AppendLine("END ");
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
        public void CreateInsert()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertRow);
            CreateProcedureCommand(BuildInsertCommand, commandName);
        }
        public string CreateInsertScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertRow);
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
            MySqlParameter sqlParameter = new MySqlParameter("@sync_scope_id", MySqlDbType.Guid);
            sqlCommand.Parameters.Add(sqlParameter);
            MySqlParameter sqlParameter1 = new MySqlParameter("@sync_row_is_tombstone", MySqlDbType.Int32);
            sqlCommand.Parameters.Add(sqlParameter1);
            MySqlParameter sqlParameter3 = new MySqlParameter("@create_timestamp", MySqlDbType.Int64);
            sqlCommand.Parameters.Add(sqlParameter3);
            MySqlParameter sqlParameter4 = new MySqlParameter("@update_timestamp", MySqlDbType.Int64);
            sqlCommand.Parameters.Add(sqlParameter4);
            MySqlParameter sqlParameter8 = new MySqlParameter("@sync_row_count", MySqlDbType.Int32);
            sqlParameter8.Direction = ParameterDirection.Output;
            sqlCommand.Parameters.Add(sqlParameter8);

            stringBuilder.AppendLine($"SET {sqlParameter8.ParameterName} = 0;");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"UPDATE {trackingName.QuotedString} SET ");
            stringBuilder.AppendLine("\t`create_scope_id` = @sync_scope_id, ");
            stringBuilder.AppendLine("\t`create_timestamp` = @create_timestamp, ");
            stringBuilder.AppendLine("\t`update_scope_id` = @sync_scope_id, ");
            stringBuilder.AppendLine("\t`update_timestamp` = @update_timestamp, ");
            stringBuilder.AppendLine("\t`sync_row_is_tombstone` = @sync_row_is_tombstone ");
            stringBuilder.AppendLine($"WHERE ({MySqlManagementUtils.ColumnsAndParameters(this.tableDescription.PrimaryKey.Columns, "")})");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"SET {sqlParameter8.ParameterName} = @@rowcount; ");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"IF ({sqlParameter8.ParameterName} = 0)");
            stringBuilder.AppendLine("BEGIN ");
            stringBuilder.AppendLine($"\tINSERT INTO {trackingName.QuotedString}");

            string empty = string.Empty;
            foreach (var pkColumn in this.tableDescription.PrimaryKey.Columns)
            {
                ObjectNameParser columnName = new ObjectNameParser(pkColumn.ColumnName);
                stringBuilderArguments.Append(string.Concat(empty, columnName.QuotedString));
                stringBuilderParameters.Append(string.Concat(empty, $"@{columnName.UnquotedString}"));
                empty = ", ";
            }
            stringBuilder.AppendLine($"\t({stringBuilderArguments.ToString()}, ");
            stringBuilder.AppendLine($"\t`create_scope_id`, `create_timestamp`, `update_scope_id`, `update_timestamp`,");
            stringBuilder.AppendLine($"\t`sync_row_is_tombstone`, `last_change_datetime`)");
            stringBuilder.AppendLine($"\tVALUES ({stringBuilderParameters.ToString()}, ");
            stringBuilder.AppendLine($"\t@sync_scope_id, @create_timestamp, @sync_scope_id, @update_timestamp, ");
            stringBuilder.AppendLine($"\t@sync_row_is_tombstone, GetDate());");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"\tSET {sqlParameter8.ParameterName} = @@rowcount; ");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("END");

            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
        public void CreateInsertMetadata()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertMetadata);
            CreateProcedureCommand(BuildInsertMetadataCommand, commandName);
        }
        public string CreateInsertMetadataScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertMetadata);
            return CreateProcedureCommandScriptText(BuildInsertMetadataCommand, commandName);
        }


        //------------------------------------------------------------------
        // Select Row command
        //------------------------------------------------------------------
        private MySqlCommand BuildSelectRowCommand()
        {
            MySqlCommand sqlCommand = new MySqlCommand();
            this.AddPkColumnParametersToCommand(sqlCommand);
            MySqlParameter sqlParameter = new MySqlParameter("@sync_scope_id", MySqlDbType.Guid);
            sqlCommand.Parameters.Add(sqlParameter);

            StringBuilder stringBuilder = new StringBuilder("SELECT ");
            stringBuilder.AppendLine();
            StringBuilder stringBuilder1 = new StringBuilder();
            string empty = string.Empty;
            foreach (var pkColumn in this.tableDescription.PrimaryKey.Columns)
            {
                ObjectNameParser pkColumnName = new ObjectNameParser(pkColumn.ColumnName);
                stringBuilder.AppendLine($"\t`side`.{pkColumnName.QuotedString}, ");
                stringBuilder1.Append($"{empty}`side`.{pkColumnName.QuotedString} = @{pkColumnName.UnquotedString}");
                empty = " AND ";
            }
            foreach (DmColumn nonPkMutableColumn in this.tableDescription.NonPkColumns.Where(c => !c.ReadOnly))
            {
                ObjectNameParser nonPkColumnName = new ObjectNameParser(nonPkMutableColumn.ColumnName);
                stringBuilder.AppendLine($"\t`base`.{nonPkColumnName.QuotedString}, ");
            }
            stringBuilder.AppendLine("\t`side`.`sync_row_is_tombstone`,");
            stringBuilder.AppendLine("\t`side`.`create_scope_id`,");
            stringBuilder.AppendLine("\t`side`.`create_timestamp`,");
            stringBuilder.AppendLine("\t`side`.`update_scope_id`,");
            stringBuilder.AppendLine("\t`side`.`update_timestamp`");

            stringBuilder.AppendLine($"FROM {tableName.QuotedString} `base`");
            stringBuilder.AppendLine($"RIGHT JOIN {trackingName.QuotedString} `side` ON");

            string str = string.Empty;
            foreach (var pkColumn in this.tableDescription.PrimaryKey.Columns)
            {
                ObjectNameParser pkColumnName = new ObjectNameParser(pkColumn.ColumnName);
                stringBuilder.Append($"{str}`base`.{pkColumnName.QuotedString} = `side`.{pkColumnName.QuotedString}");
                str = " AND ";
            }
            stringBuilder.AppendLine();
            stringBuilder.Append(string.Concat("WHERE ", stringBuilder1.ToString()));
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
        public void CreateSelectRow()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.SelectRow);
            CreateProcedureCommand(BuildSelectRowCommand, commandName);
        }
        public string CreateSelectRowScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.SelectRow);
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

            MySqlParameter sqlParameter = new MySqlParameter("@sync_force_write", MySqlDbType.Int32);
            sqlCommand.Parameters.Add(sqlParameter);
            MySqlParameter sqlParameter1 = new MySqlParameter("@sync_min_timestamp", MySqlDbType.Int64);
            sqlCommand.Parameters.Add(sqlParameter1);
            MySqlParameter sqlParameter2 = new MySqlParameter("@sync_row_count", MySqlDbType.Int32);
            sqlParameter2.Direction = ParameterDirection.Output;
            sqlCommand.Parameters.Add(sqlParameter2);

            stringBuilder.AppendLine($"SET {sqlParameter2.ParameterName} = 0;");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"UPDATE {tableName.QuotedString}");
            stringBuilder.Append($"SET {MySqlManagementUtils.CommaSeparatedUpdateFromParameters(this.tableDescription)}");
            stringBuilder.AppendLine($"FROM {tableName.QuotedString} `base`");
            stringBuilder.AppendLine($"JOIN {trackingName.QuotedString} `side`");
            stringBuilder.Append($"ON ");
            stringBuilder.AppendLine(MySqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "`base`", "`side`"));
            stringBuilder.AppendLine("WHERE (`side`.`timestamp` <= @sync_min_timestamp OR @sync_force_write = 1)");
            stringBuilder.Append("AND (");
            stringBuilder.Append(MySqlManagementUtils.ColumnsAndParameters(this.tableDescription.PrimaryKey.Columns, "`base`"));
            stringBuilder.AppendLine(");");
            stringBuilder.AppendLine();
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
        public void CreateUpdate()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateRow);
            this.CreateProcedureCommand(BuildUpdateCommand, commandName);
        }
        public string CreateUpdateScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateRow);
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
            MySqlParameter sqlParameter = new MySqlParameter("@sync_scope_id", MySqlDbType.Guid);
            sqlCommand.Parameters.Add(sqlParameter);
            MySqlParameter sqlParameter1 = new MySqlParameter("@sync_row_is_tombstone", MySqlDbType.Int32);
            sqlCommand.Parameters.Add(sqlParameter1);
            MySqlParameter sqlParameter3 = new MySqlParameter("@create_timestamp", MySqlDbType.Int64);
            sqlCommand.Parameters.Add(sqlParameter3);
            MySqlParameter sqlParameter5 = new MySqlParameter("@update_timestamp", MySqlDbType.Int64);
            sqlCommand.Parameters.Add(sqlParameter5);
            MySqlParameter sqlParameter8 = new MySqlParameter("@sync_row_count", MySqlDbType.Int32);
            sqlParameter8.Direction = ParameterDirection.Output;
            sqlCommand.Parameters.Add(sqlParameter8);

            string str1 = MySqlManagementUtils.ColumnsAndParameters(this.tableDescription.PrimaryKey.Columns, "");

            stringBuilder.AppendLine($"SET {sqlParameter8.ParameterName} = 0;");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine("DECLARE @was_tombstone int; ");

            stringBuilder.AppendLine($"SELECT @was_tombstone = `sync_row_is_tombstone`");
            stringBuilder.AppendLine($"FROM {trackingName.QuotedString}");
            stringBuilder.AppendLine($"WHERE ({str1})");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("IF (@was_tombstone IS NOT NULL AND @was_tombstone = 1 AND @sync_row_is_tombstone = 0)");
            stringBuilder.AppendLine("BEGIN ");
            stringBuilder.AppendLine($"UPDATE {trackingName.QuotedString} SET ");
            stringBuilder.AppendLine("\t `create_scope_id` = @sync_scope_id, ");
            stringBuilder.AppendLine("\t `update_scope_id` = @sync_scope_id, ");
            stringBuilder.AppendLine("\t `create_timestamp` = @create_timestamp, ");
            stringBuilder.AppendLine("\t `update_timestamp` = @update_timestamp, ");
            stringBuilder.AppendLine("\t `sync_row_is_tombstone` = @sync_row_is_tombstone ");
            stringBuilder.AppendLine($"WHERE ({str1})");
            stringBuilder.AppendLine("END");
            stringBuilder.AppendLine("ELSE");
            stringBuilder.AppendLine("BEGIN ");
            stringBuilder.AppendLine($"UPDATE {trackingName.QuotedString} SET ");
            stringBuilder.AppendLine("\t `update_scope_id` = @sync_scope_id, ");
            stringBuilder.AppendLine("\t `update_timestamp` = @update_timestamp, ");
            stringBuilder.AppendLine("\t `sync_row_is_tombstone` = @sync_row_is_tombstone ");
            stringBuilder.AppendLine($"WHERE ({str1})");
            stringBuilder.AppendLine("END;");
            stringBuilder.AppendLine();
            stringBuilder.Append($"SET {sqlParameter8.ParameterName} = @@ROWCOUNT;");
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
        public void CreateUpdateMetadata()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateMetadata);
            CreateProcedureCommand(BuildUpdateMetadataCommand, commandName);
        }
        public string CreateUpdateMetadataScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateMetadata);
            return CreateProcedureCommandScriptText(BuildUpdateMetadataCommand, commandName);
        }


        //------------------------------------------------------------------
        // Select changes command
        //------------------------------------------------------------------
        private MySqlCommand BuildSelectIncrementalChangesCommand(bool withFilter = false)
        {
            MySqlCommand sqlCommand = new MySqlCommand();
            MySqlParameter sqlParameter1 = new MySqlParameter("@sync_min_timestamp", MySqlDbType.Int64);
            MySqlParameter sqlParameter3 = new MySqlParameter("@sync_scope_id", MySqlDbType.Guid);
            MySqlParameter sqlParameter4 = new MySqlParameter("@sync_scope_is_new", MySqlDbType.Bit);
            sqlCommand.Parameters.Add(sqlParameter1);
            sqlCommand.Parameters.Add(sqlParameter3);
            sqlCommand.Parameters.Add(sqlParameter4);


            //if (withFilter && this.Filters != null && this.Filters.Count > 0)
            //{
            //    foreach (var c in this.Filters)
            //    {
            //        var columnFilter = this.tableDescription.Columns[c.ColumnName];

            //        if (columnFilter == null)
            //            throw new InvalidExpressionException($"Column {c.ColumnName} does not exist in Table {this.tableDescription.TableName}");

            //        var columnFilterName = new ObjectNameParser(columnFilter.ColumnName, "`", "`");

            //        MySqlParameter sqlParamFilter = new MySqlParameter($"@{columnFilterName.UnquotedString}", columnFilter.GetMySqlDbType());
            //        sqlCommand.Parameters.Add(sqlParamFilter);
            //    }
            //}

            StringBuilder stringBuilder = new StringBuilder("SELECT ");
            foreach (var pkColumn in this.tableDescription.PrimaryKey.Columns)
            {
                var pkColumnName = new ObjectNameParser(pkColumn.ColumnName);
                stringBuilder.AppendLine($"\t`side`.{pkColumnName.QuotedString}, ");
            }
            foreach (var column in this.tableDescription.NonPkColumns.Where(col => !col.ReadOnly))
            {
                var columnName = new ObjectNameParser(column.ColumnName);
                stringBuilder.AppendLine($"\t`base`.{columnName.QuotedString}, ");
            }
            stringBuilder.AppendLine($"\t`side`.`sync_row_is_tombstone`, ");
            stringBuilder.AppendLine($"\t`side`.`create_scope_id`, ");
            stringBuilder.AppendLine($"\t`side`.`create_timestamp`, ");
            stringBuilder.AppendLine($"\t`side`.`update_scope_id`, ");
            stringBuilder.AppendLine($"\t`side`.`update_timestamp` ");
            stringBuilder.AppendLine($"FROM {tableName.QuotedString} `base`");
            stringBuilder.AppendLine($"RIGHT JOIN {trackingName.QuotedString} `side`");
            stringBuilder.Append($"ON ");

            string empty = "";
            foreach (var pkColumn in this.tableDescription.PrimaryKey.Columns)
            {
                var pkColumnName = new ObjectNameParser(pkColumn.ColumnName);
                stringBuilder.Append($"{empty}`base`.{pkColumnName.QuotedString} = `side`.{pkColumnName.QuotedString}");
                empty = " AND ";
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("WHERE (");
            string str = string.Empty;

            //  --Changes where only customerId is identified
            //  (`side`.`CustomerId` = @CustomerId

            //   Or(
            //       --Or row customer is null and it's come from the scope_id who deal the filter 
            //       (`side`.`update_scope_id` = @sync_scope_id or`side`.`update_scope_id` IS NULL)

            //       and`side`.`CustomerId` is null
            //   )
            //)

            if (withFilter && this.Filters != null && this.Filters.Count > 0)
            {
                StringBuilder builderFilter = new StringBuilder();
                builderFilter.Append("\t(");
                string filterSeparationString = "";
                foreach (var c in this.Filters)
                {
                    var columnFilter = this.tableDescription.Columns[c.ColumnName];

                    if (columnFilter == null)
                        throw new InvalidExpressionException($"Column {c.ColumnName} does not exist in Table {this.tableDescription.TableName}");

                    var columnFilterName = new ObjectNameParser(columnFilter.ColumnName, "`", "`");

                    builderFilter.Append($"`side`.{columnFilterName.QuotedObjectName} = @{columnFilterName.UnquotedString}{filterSeparationString}");
                    filterSeparationString = " AND ";
                }
                builderFilter.AppendLine(")");
                builderFilter.Append("\tOR (");
                builderFilter.AppendLine("(`side`.`update_scope_id` = @sync_scope_id or `side`.`update_scope_id` IS NULL)");
                builderFilter.Append("\t\tAND (");

                filterSeparationString = "";
                foreach (var c in this.Filters)
                {
                    var columnFilter = this.tableDescription.Columns[c.ColumnName];
                    var columnFilterName = new ObjectNameParser(columnFilter.ColumnName, "`", "`");

                    builderFilter.Append($"`side`.{columnFilterName.QuotedObjectName} IS NULL{filterSeparationString}");
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
            stringBuilder.AppendLine("\tOR `side`.`update_scope_id` <> @sync_scope_id");
            stringBuilder.AppendLine("    )");
            stringBuilder.AppendLine("AND (");
            stringBuilder.AppendLine("\t-- And Timestamp is > from remote timestamp");
            stringBuilder.AppendLine("\t`side`.`timestamp` > @sync_min_timestamp");
            stringBuilder.AppendLine("\tOR");
            stringBuilder.AppendLine("\t-- remote instance is new, so we don't take the last timestamp");
            stringBuilder.AppendLine("\t@sync_scope_is_new = 1");
            stringBuilder.AppendLine("\t)");


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
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.SelectChanges);
            Func<MySqlCommand> cmdWithoutFilter = () => BuildSelectIncrementalChangesCommand(false);
            CreateProcedureCommand(cmdWithoutFilter, commandName);

            if (this.Filters != null && this.Filters.Count > 0)
            {
                foreach (var c in this.Filters)
                {
                    var columnFilter = this.tableDescription.Columns[c.ColumnName];

                    if (columnFilter == null)
                        throw new InvalidExpressionException($"Column {c.ColumnName} does not exist in Table {this.tableDescription.TableName}");
                }

                var filtersName = this.Filters.Select(f => f.ColumnName);
                commandName = this.sqlObjectNames.GetCommandName(DbCommandType.SelectChangesWitFilters, filtersName);
                Func<MySqlCommand> cmdWithFilter = () => BuildSelectIncrementalChangesCommand(true);
                CreateProcedureCommand(cmdWithFilter, commandName);

            }

        }
        public string CreateSelectIncrementalChangesScriptText()
        {
            StringBuilder sbSelecteChanges = new StringBuilder();

            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.SelectChanges);
            Func<MySqlCommand> cmdWithoutFilter = () => BuildSelectIncrementalChangesCommand(false);
            sbSelecteChanges.AppendLine(CreateProcedureCommandScriptText(cmdWithoutFilter, commandName));


            if (this.Filters != null && this.Filters.Count > 0)
            {
                commandName = this.sqlObjectNames.GetCommandName(DbCommandType.SelectChangesWitFilters);
                string name = "";
                string sep = "";
                foreach (var c in this.Filters)
                {
                    var columnFilter = this.tableDescription.Columns[c.ColumnName];

                    if (columnFilter == null)
                        throw new InvalidExpressionException($"Column {c.ColumnName} does not exist in Table {this.tableDescription.TableName}");

                    var unquotedColumnName = new ObjectNameParser(columnFilter.ColumnName).UnquotedString;
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
    }
}
