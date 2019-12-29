using Dotmim.Sync.Builders;
using System;
using System.Text;
using Dotmim.Sync.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Data;
using Dotmim.Sync.Log;
using System.Linq;
using Dotmim.Sync.Filter;
using Dotmim.Sync.SqlServer.Manager;
using System.Diagnostics;
using System.Collections.Generic;

namespace Dotmim.Sync.SqlServer.Builders
{
    public class SqlBuilderProcedure : IDbBuilderProcedureHelper
    {
        private ParserName tableName;
        private ParserName trackingName;
        private SqlConnection connection;
        private readonly SqlTransaction transaction;
        private readonly SyncTable tableDescription;
        private readonly SqlObjectNames sqlObjectNames;
        private readonly SqlDbMetadata sqlDbMetadata;

        public SyncFilters Filters { get; set; }

        public SqlBuilderProcedure(SyncTable tableDescription, DbConnection connection, DbTransaction transaction = null)
        {
            this.connection = connection as SqlConnection;
            this.transaction = transaction as SqlTransaction;

            this.tableDescription = tableDescription;
            (this.tableName, this.trackingName) = SqlBuilder.GetParsers(tableDescription);
            this.sqlObjectNames = new SqlObjectNames(this.tableDescription);
            this.sqlDbMetadata = new SqlDbMetadata();
        }

        private void AddPkColumnParametersToCommand(SqlCommand sqlCommand)
        {
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
                sqlCommand.Parameters.Add(GetSqlParameter(pkColumn));
        }
        private void AddColumnParametersToCommand(SqlCommand sqlCommand)
        {
            foreach (var column in this.tableDescription.Columns.Where(c => !c.IsReadOnly))
                sqlCommand.Parameters.Add(GetSqlParameter(column));
        }

        private SqlParameter GetSqlParameter(SyncColumn column)
        {
            var sqlParameter = new SqlParameter
            {
                ParameterName = $"@{ParserName.Parse(column).Unquoted().Normalized().ToString()}"
            };

            // Get the good SqlDbType (even if we are not from Sql Server def)
            var sqlDbType = (SqlDbType)this.sqlDbMetadata.TryGetOwnerDbType(column.OriginalDbType, column.GetDbType(), false, false, column.MaxLength, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);

            sqlParameter.SqlDbType = sqlDbType;
            sqlParameter.IsNullable = column.AllowDBNull;

            var (p, s) = this.sqlDbMetadata.TryGetOwnerPrecisionAndScale(column.OriginalDbType, column.GetDbType(), false, false, column.MaxLength, column.Precision, column.Scale, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);

            if (p > 0)
            {
                sqlParameter.Precision = p;
                if (s > 0)
                    sqlParameter.Scale = s;
            }

            var m = this.sqlDbMetadata.TryGetOwnerMaxLength(column.OriginalDbType, column.GetDbType(), false, false, column.MaxLength, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);

            if (m > 0)
                sqlParameter.Size = m;

            return sqlParameter;
        }

        /// <summary>
        /// From a SqlParameter, create the declaration
        /// </summary>
        internal string CreateParameterDeclaration(SqlParameter param)
        {
            var stringBuilder3 = new StringBuilder();
            var sqlDbType = param.SqlDbType;

            string empty = this.sqlDbMetadata.GetPrecisionStringFromOwnerDbType(sqlDbType, param.Size, param.Precision, param.Scale);

            if (sqlDbType == SqlDbType.Structured)
            {
                stringBuilder3.Append(string.Concat(param.ParameterName, " ", param.TypeName, " READONLY"));
            }
            else
            {
                var sqlDbTypeString = this.sqlDbMetadata.GetStringFromOwnerDbType(sqlDbType);

                stringBuilder3.Append(string.Concat(param.ParameterName, " ", sqlDbTypeString, empty));

                if (param.Direction == ParameterDirection.Output || param.Direction == ParameterDirection.InputOutput)
                    stringBuilder3.Append(" OUTPUT");
            }

            return stringBuilder3.ToString();
        }

        /// <summary>
        /// From a SqlCommand, create a stored procedure string
        /// </summary>
        private string CreateProcedureCommandText(SqlCommand cmd, string procName)
        {
            StringBuilder stringBuilder = new StringBuilder(string.Concat("CREATE PROCEDURE ", procName));
            string str = "\n\t";
            foreach (SqlParameter parameter in cmd.Parameters)
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
        private void CreateProcedureCommand(Func<SqlCommand> BuildCommand, string procName)
        {

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                var str = CreateProcedureCommandText(BuildCommand(), procName);
                using (var command = new SqlCommand(str, connection))
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


        private void CreateProcedureCommand<T>(Func<T, SqlCommand> BuildCommand, string procName, T t)
        {

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                var str = CreateProcedureCommandText(BuildCommand(t), procName);
                using (var command = new SqlCommand(str, connection))
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


        private string CreateProcedureCommandScriptText(Func<SqlCommand> BuildCommand, string procName)
        {

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                var str1 = $"Command {procName} for table {tableName.Schema().Quoted().ToString()}";
                var str = CreateProcedureCommandText(BuildCommand(), procName);
                return SqlBuilder.WrapScriptTextWithComments(str, str1);
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

        private string CreateProcedureCommandScriptText<T>(Func<T, SqlCommand> BuildCommand, string procName, T t)
        {

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                var str1 = $"Command {procName} for table {tableName.Schema().Quoted().ToString()}";
                var str = CreateProcedureCommandText(BuildCommand(t), procName);
                return SqlBuilder.WrapScriptTextWithComments(str, str1);
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

            return !SqlManagementUtils.ProcedureExists(connection, transaction, commandName);
        }

        /// <summary>
        /// Check if we need to create the TVP Type
        /// </summary>
        public bool NeedToCreateType(DbCommandType commandType)
        {
            if (connection.State != ConnectionState.Open)
                throw new ArgumentException("Here, we need an opened connection please");

            var commandName = this.sqlObjectNames.GetCommandName(commandType).name;

            return !SqlManagementUtils.TypeExists(connection, transaction, commandName);

        }

        private string BulkSelectUnsuccessfulRows()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("--Select all ids not inserted / deleted / updated as conflict");
            stringBuilder.Append("SELECT ");
            for (int i = 0; i < this.tableDescription.PrimaryKeys.Count; i++)
            {
                var cc = ParserName.Parse(this.tableDescription.PrimaryKeys[i]).Quoted().ToString();

                stringBuilder.Append($"{cc}");
                if (i < this.tableDescription.PrimaryKeys.Count - 1)
                    stringBuilder.Append(", ");
                else
                    stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine($"FROM @changeTable t");
            stringBuilder.AppendLine("WHERE NOT EXISTS (");
            stringBuilder.Append("\t SELECT ");
            for (int i = 0; i < this.tableDescription.PrimaryKeys.Count; i++)
            {
                var cc = ParserName.Parse(this.tableDescription.PrimaryKeys[i])
                                   .Quoted().ToString();

                stringBuilder.Append($"{cc}");
                if (i < this.tableDescription.PrimaryKeys.Count - 1)
                    stringBuilder.Append(", ");
                else
                    stringBuilder.AppendLine();
            }
            stringBuilder.AppendLine("\t FROM @changed i");
            stringBuilder.Append("\t WHERE ");
            for (int i = 0; i < this.tableDescription.PrimaryKeys.Count; i++)
            {
                var cc = ParserName.Parse(this.tableDescription.PrimaryKeys[i])
                                .Quoted().ToString();
                stringBuilder.Append($"t.{cc} = i.{cc}");
                if (i < this.tableDescription.PrimaryKeys.Count - 1)
                    stringBuilder.Append("AND ");
                else
                    stringBuilder.AppendLine();
            }
            stringBuilder.AppendLine("\t)");
            return stringBuilder.ToString();
        }


        //------------------------------------------------------------------
        // Bulk Delete command
        //------------------------------------------------------------------
        private SqlCommand BuildBulkDeleteCommand()
        {
            var sqlCommand = new SqlCommand();

            var sqlParameter = new SqlParameter("@sync_min_timestamp", SqlDbType.BigInt);
            sqlCommand.Parameters.Add(sqlParameter);
            var sqlParameter1 = new SqlParameter("@sync_scope_id", SqlDbType.UniqueIdentifier);
            sqlCommand.Parameters.Add(sqlParameter1);
            var sqlParameter2 = new SqlParameter("@changeTable", SqlDbType.Structured)
            {
                TypeName = this.sqlObjectNames.GetCommandName(DbCommandType.BulkTableType).name
            };
            sqlCommand.Parameters.Add(sqlParameter2);


            string str4 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[p]", "[t]");
            string str5 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[changes]", "[base]");
            string str6 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[t]", "[side]");

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("-- use a temp table to store the list of PKs that successfully got deleted");
            stringBuilder.Append("declare @changed TABLE (");
            foreach (var c in this.tableDescription.GetPrimaryKeysColumns())
            {
                // Get the good SqlDbType (even if we are not from Sql Server def)
                var sqlDbTypeString = this.sqlDbMetadata.TryGetOwnerDbTypeString(c.OriginalDbType, c.GetDbType(), false, false, c.MaxLength, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
                var quotedColumnType = ParserName.Parse(sqlDbTypeString).Quoted().ToString();

                quotedColumnType += this.sqlDbMetadata.TryGetOwnerDbTypePrecision(c.OriginalDbType, c.GetDbType(), false, false, c.MaxLength, c.Precision, c.Scale, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);

                stringBuilder.Append($"{ParserName.Parse(c).Quoted().ToString()} {quotedColumnType}, ");
            }
            stringBuilder.Append(" PRIMARY KEY (");
            for (int i = 0; i < this.tableDescription.PrimaryKeys.Count; i++)
            {
                var cc = ParserName.Parse(this.tableDescription.PrimaryKeys[i]).Quoted().ToString();
                stringBuilder.Append($"{cc}");

                if (i < this.tableDescription.PrimaryKeys.Count - 1)
                    stringBuilder.Append(", ");
            }
            stringBuilder.AppendLine("));");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"-- delete all items where timestamp <= @sync_min_timestamp or update_scope_id is not null (if not null update was not made locally)");

            stringBuilder.AppendLine($"DELETE {tableName.Schema().Quoted().ToString()}");
            stringBuilder.Append($"OUTPUT ");
            for (int i = 0; i < this.tableDescription.PrimaryKeys.Count; i++)
            {
                var cc = ParserName.Parse(this.tableDescription.PrimaryKeys[i]).Quoted().ToString();
                stringBuilder.Append($"DELETED.{cc}");
                if (i < this.tableDescription.PrimaryKeys.Count - 1)
                    stringBuilder.Append(", ");
                else
                    stringBuilder.AppendLine();
            }
            stringBuilder.AppendLine($"INTO @changed ");
            stringBuilder.AppendLine($"FROM {tableName.Quoted().ToString()} [base]");
            stringBuilder.AppendLine("JOIN (");


            stringBuilder.AppendLine($"\tSELECT ");
            string str = "";
            stringBuilder.Append("\t");
            foreach (var c in this.tableDescription.Columns.Where(col => !col.IsReadOnly))
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();
                stringBuilder.Append($"{str}[p].{columnName}");
                str = ", ";
            }
            stringBuilder.AppendLine("\t, [t].[update_scope_id], [t].[timestamp]");
            stringBuilder.AppendLine($"\tFROM @changeTable p ");
            stringBuilder.AppendLine($"\tJOIN {trackingName.Schema().Quoted().ToString()} t ON ");
            stringBuilder.AppendLine($"\t{str4}");
            stringBuilder.AppendLine("\t)");
            stringBuilder.AppendLine("\tAS [changes] ON ");
            stringBuilder.AppendLine(str5);
            stringBuilder.AppendLine("WHERE");
            stringBuilder.AppendLine("-- Last changes was not made locally (update_scope_id is not null means update was made by a remote db)");
            stringBuilder.AppendLine("changes.update_scope_id IS NOT NULL");
            stringBuilder.AppendLine("-- no change since the last sync time");
            stringBuilder.AppendLine("OR [changes].[timestamp] <= @sync_min_timestamp;");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("-- Since the delete trigger is passed, we update the tracking table to reflect the real scope deleter");
            stringBuilder.AppendLine("UPDATE [side] SET");
            stringBuilder.AppendLine("\tsync_row_is_tombstone = 1, ");
            stringBuilder.AppendLine("\tupdate_scope_id = @sync_scope_id,");
            stringBuilder.AppendLine("\tlast_change_datetime = GETUTCDATE()");
            stringBuilder.AppendLine($"FROM {trackingName.Schema().Quoted().ToString()} [side]");
            stringBuilder.AppendLine($"JOIN @changed [t] on {str6}");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine();
            stringBuilder.Append(BulkSelectUnsuccessfulRows());
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        public void CreateBulkDelete()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.BulkDeleteRows).name;
            CreateProcedureCommand(this.BuildBulkDeleteCommand, commandName);
        }

        public string CreateBulkDeleteScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.BulkDeleteRows).name;
            return CreateProcedureCommandScriptText(this.BuildBulkDeleteCommand, commandName);
        }

        public void DropBulkDelete()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqlCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.BulkDeleteRows).name;

                    command.CommandText = $"DROP PROCEDURE {commandName};";
                    command.Connection = this.connection;
                    command.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during DropBulkDelete : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();

            }

        }

        public string DropBulkDeleteScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.BulkDeleteRows).name;

            return $"DROP PROCEDURE {commandName};";
        }

        //------------------------------------------------------------------
        // Bulk Update command
        //------------------------------------------------------------------
        private SqlCommand BuildBulkUpdateCommand(bool hasMutableColumns)
        {
            var sqlCommand = new SqlCommand();
            var stringBuilderArguments = new StringBuilder();
            var stringBuilderParameters = new StringBuilder();
            string empty = string.Empty;

            var sqlParameter = new SqlParameter("@sync_min_timestamp", SqlDbType.BigInt);
            sqlCommand.Parameters.Add(sqlParameter);
            var sqlParameter1 = new SqlParameter("@sync_scope_id", SqlDbType.UniqueIdentifier);
            sqlCommand.Parameters.Add(sqlParameter1);
            var sqlParameter2 = new SqlParameter("@changeTable", SqlDbType.Structured)
            {
                TypeName = this.sqlObjectNames.GetCommandName(DbCommandType.BulkTableType).name
            };
            sqlCommand.Parameters.Add(sqlParameter2);

            string str4 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[p]", "[t]");
            string str5 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[changes]", "[base]");
            string str6 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[t]", "[side]");

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("-- use a temp table to store the list of PKs that successfully got updated/inserted");
            stringBuilder.Append("declare @changed TABLE (");
            foreach (var c in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();

                // Get the good SqlDbType (even if we are not from Sql Server def)
                var sqlDbTypeString = this.sqlDbMetadata.TryGetOwnerDbTypeString(c.OriginalDbType, c.GetDbType(), false, false, c.MaxLength, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
                var quotedColumnType = ParserName.Parse(sqlDbTypeString).Quoted().ToString();
                quotedColumnType += this.sqlDbMetadata.TryGetOwnerDbTypePrecision(c.OriginalDbType, c.GetDbType(), false, false, c.MaxLength, c.Precision, c.Scale, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);

                stringBuilder.Append($"{columnName} {quotedColumnType}, ");
            }
            stringBuilder.Append(" PRIMARY KEY (");
            for (int i = 0; i < this.tableDescription.PrimaryKeys.Count; i++)
            {
                var columnName = ParserName.Parse(this.tableDescription.PrimaryKeys[i]).Quoted().ToString();
                stringBuilder.Append($"{columnName}");
                if (i < this.tableDescription.PrimaryKeys.Count - 1)
                    stringBuilder.Append(", ");
            }
            stringBuilder.AppendLine("));");
            stringBuilder.AppendLine();

            // Check if we have auto inc column
            if (this.tableDescription.HasAutoIncrementColumns)
            {
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"SET IDENTITY_INSERT {tableName.Schema().Quoted().ToString()} ON;");
                stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine("-- update the base table");
            stringBuilder.AppendLine($"MERGE {tableName.Schema().Quoted().ToString()} AS base USING");
            stringBuilder.AppendLine("\t-- join done here against the side table to get the local timestamp for concurrency check\n");

            stringBuilder.AppendLine($"\t(SELECT ");
            string str = "";
            stringBuilder.Append("\t");
            foreach (var c in this.tableDescription.Columns.Where(col => !col.IsReadOnly))
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();
                stringBuilder.Append($"{str}[p].{columnName}");
                str = ", ";
            }
            stringBuilder.AppendLine("\t, [t].[update_scope_id], [t].[timestamp]");
            stringBuilder.AppendLine($"\tFROM @changeTable p ");
            stringBuilder.Append($"\tLEFT JOIN {trackingName.Schema().Quoted().ToString()} t ON ");
            stringBuilder.AppendLine($" {str4}");
            stringBuilder.AppendLine($"\t) AS changes ON {str5}");
            stringBuilder.AppendLine();
            if (hasMutableColumns)
            {
                stringBuilder.AppendLine("WHEN MATCHED AND ([changes].[update_scope_id] IS NOT NULL OR [changes].[timestamp] <= @sync_min_timestamp) THEN");
                foreach (var mutableColumn in this.tableDescription.Columns.Where(c => !c.IsReadOnly))
                {
                    var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                    stringBuilderArguments.Append(string.Concat(empty, columnName));
                    stringBuilderParameters.Append(string.Concat(empty, $"changes.{columnName}"));
                    empty = ", ";
                }
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"\tUPDATE SET");

                string strSeparator = "";
                foreach (var mutableColumn in this.tableDescription.GetMutableColumns(false))
                {
                    var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                    stringBuilder.AppendLine($"\t{strSeparator}{columnName} = [changes].{columnName}");
                    strSeparator = ", ";
                }
            }

            stringBuilder.AppendLine("WHEN NOT MATCHED BY TARGET AND ([changes].[timestamp] <= @sync_min_timestamp OR changes.[timestamp] IS NULL) THEN");


            stringBuilderArguments = new StringBuilder();
            stringBuilderParameters = new StringBuilder();
            empty = string.Empty;

            foreach (var mutableColumn in this.tableDescription.Columns.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();

                stringBuilderArguments.Append(string.Concat(empty, columnName));
                stringBuilderParameters.Append(string.Concat(empty, $"changes.{columnName}"));
                empty = ", ";
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"\tINSERT");
            stringBuilder.AppendLine($"\t({stringBuilderArguments.ToString()})");
            stringBuilder.AppendLine($"\tVALUES ({stringBuilderParameters.ToString()})");


            stringBuilder.Append($"\tOUTPUT ");
            for (int i = 0; i < this.tableDescription.PrimaryKeys.Count; i++)
            {
                var columnName = ParserName.Parse(this.tableDescription.PrimaryKeys[i]).Quoted().ToString();
                stringBuilder.Append($"INSERTED.{columnName}");
                if (i < this.tableDescription.PrimaryKeys.Count - 1)
                    stringBuilder.Append(", ");
                else
                    stringBuilder.AppendLine();
            }
            stringBuilder.AppendLine($"\tINTO @changed; -- populates the temp table with successful PKs");
            stringBuilder.AppendLine();

            // Check if we have auto inc column
            if (this.tableDescription.HasAutoIncrementColumns)
            {
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"SET IDENTITY_INSERT {tableName.Schema().Quoted().ToString()} ON;");
                stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine("-- Since the update trigger is passed, we update the tracking table to reflect the real scope updater");
            stringBuilder.AppendLine("UPDATE side SET");
            stringBuilder.AppendLine("\tupdate_scope_id = @sync_scope_id,");
            stringBuilder.AppendLine("\tsync_row_is_tombstone = 0,");
            stringBuilder.AppendLine("\tlast_change_datetime = GETUTCDATE()");
            stringBuilder.AppendLine($"FROM {trackingName.Schema().Quoted().ToString()} [side]");
            stringBuilder.AppendLine($"JOIN @changed [t] on {str6}");
            stringBuilder.AppendLine();
            stringBuilder.Append(BulkSelectUnsuccessfulRows());

            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
        public void CreateBulkUpdate(bool hasMutableColumns)
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.BulkUpdateRows).name;
            CreateProcedureCommand(BuildBulkUpdateCommand, commandName, hasMutableColumns);
        }
        public string CreateBulkUpdateScriptText(bool hasMutableColumns)
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.BulkUpdateRows).name;
            return CreateProcedureCommandScriptText(BuildBulkUpdateCommand, commandName, hasMutableColumns);
        }
        public void DropBulkUpdate()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqlCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.BulkUpdateRows).name;

                    command.CommandText = $"DROP PROCEDURE {commandName};";
                    command.Connection = this.connection;
                    command.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during Drop BulkUpdate : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();

            }

        }
        public string DropBulkUpdateScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.BulkUpdateRows).name;

            return $"DROP PROCEDURE {commandName};";
        }


        //------------------------------------------------------------------
        // Reset command
        //------------------------------------------------------------------
        private SqlCommand BuildResetCommand()
        {
            var updTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger).name;
            var delTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger).name;
            var insTriggerName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertTrigger).name;

            SqlCommand sqlCommand = new SqlCommand();
            SqlParameter sqlParameter2 = new SqlParameter("@sync_row_count", SqlDbType.Int)
            {
                Direction = ParameterDirection.Output
            };
            sqlCommand.Parameters.Add(sqlParameter2);

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"SET {sqlParameter2.ParameterName} = 0;");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"DISABLE TRIGGER {updTriggerName} ON {tableName.Schema().Quoted().ToString()};");
            stringBuilder.AppendLine($"DISABLE TRIGGER {insTriggerName} ON {tableName.Schema().Quoted().ToString()};");
            stringBuilder.AppendLine($"DISABLE TRIGGER {delTriggerName} ON {tableName.Schema().Quoted().ToString()};");

            stringBuilder.AppendLine($"DELETE FROM {tableName.Schema().Quoted().ToString()};");
            stringBuilder.AppendLine($"DELETE FROM {trackingName.Schema().Quoted().ToString()};");

            stringBuilder.AppendLine($"ENABLE TRIGGER {updTriggerName} ON {tableName.Schema().Quoted().ToString()};");
            stringBuilder.AppendLine($"ENABLE TRIGGER {insTriggerName} ON {tableName.Schema().Quoted().ToString()};");
            stringBuilder.AppendLine($"ENABLE TRIGGER {delTriggerName} ON {tableName.Schema().Quoted().ToString()};");


            stringBuilder.AppendLine();
            stringBuilder.AppendLine(string.Concat("SET ", sqlParameter2.ParameterName, " = @@ROWCOUNT;"));
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
        public void DropReset()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqlCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.Reset).name;

                    command.CommandText = $"DROP PROCEDURE {commandName};";
                    command.Connection = this.connection;
                    command.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during Drop Reset : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();

            }

        }
        public string DropResetScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.Reset).name;

            return $"DROP PROCEDURE {commandName};";
        }


        //------------------------------------------------------------------
        // Delete command
        //------------------------------------------------------------------
        private SqlCommand BuildDeleteCommand()
        {
            SqlCommand sqlCommand = new SqlCommand();
            this.AddPkColumnParametersToCommand(sqlCommand);
            SqlParameter sqlParameter = new SqlParameter("@sync_force_write", SqlDbType.Int);
            sqlCommand.Parameters.Add(sqlParameter);
            SqlParameter sqlParameter1 = new SqlParameter("@sync_min_timestamp", SqlDbType.BigInt);
            sqlCommand.Parameters.Add(sqlParameter1);
            SqlParameter sqlParameter2 = new SqlParameter("@sync_row_count", SqlDbType.Int)
            {
                Direction = ParameterDirection.Output
            };
            sqlCommand.Parameters.Add(sqlParameter2);

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"SET {sqlParameter2.ParameterName} = 0;");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"DELETE {tableName.Schema().Quoted().ToString()}");
            stringBuilder.AppendLine($"FROM {tableName.Schema().Quoted().ToString()} [base]");
            stringBuilder.AppendLine($"JOIN {trackingName.Schema().Quoted().ToString()} [side] ON ");

            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[base]", "[side]"));

            stringBuilder.AppendLine("WHERE ([side].[timestamp] <= @sync_min_timestamp  OR [side].[update_scope_id] IS NOT NULL OR @sync_force_write = 1)");
            stringBuilder.Append("AND ");
            stringBuilder.AppendLine(string.Concat("(", SqlManagementUtils.ColumnsAndParameters(this.tableDescription.PrimaryKeys, "[base]"), ");"));
            stringBuilder.AppendLine();
            stringBuilder.AppendLine(string.Concat("SET ", sqlParameter2.ParameterName, " = @@ROWCOUNT;"));
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
        public void DropDelete()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqlCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteRow).name;

                    command.CommandText = $"DROP PROCEDURE {commandName};";
                    command.Connection = this.connection;
                    command.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during Drop DeleteRow : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();

            }

        }
        public string DropDeleteScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteRow).name;

            return $"DROP PROCEDURE {commandName};";
        }


        //------------------------------------------------------------------
        // Delete Metadata command
        //------------------------------------------------------------------
        private SqlCommand BuildDeleteMetadataCommand()
        {
            SqlCommand sqlCommand = new SqlCommand();
            this.AddPkColumnParametersToCommand(sqlCommand);
            SqlParameter sqlParameter = new SqlParameter("@sync_check_concurrency", SqlDbType.Int);
            sqlCommand.Parameters.Add(sqlParameter);
            SqlParameter sqlParameter1 = new SqlParameter("@sync_row_timestamp", SqlDbType.BigInt);
            sqlCommand.Parameters.Add(sqlParameter1);
            SqlParameter sqlParameter2 = new SqlParameter("@sync_row_count", SqlDbType.Int)
            {
                Direction = ParameterDirection.Output
            };
            sqlCommand.Parameters.Add(sqlParameter2);
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"SET {sqlParameter2.ParameterName} = 0;");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"DELETE [side] FROM {trackingName.Schema().Quoted().ToString()} [side]");
            stringBuilder.Append($"WHERE ");
            stringBuilder.AppendLine(SqlManagementUtils.ColumnsAndParameters(this.tableDescription.PrimaryKeys, ""));
            stringBuilder.AppendLine();
            stringBuilder.AppendLine(string.Concat("SET ", sqlParameter2.ParameterName, " = @@ROWCOUNT;"));
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
        public void DropDeleteMetadata()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqlCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteMetadata).name;

                    command.CommandText = $"DROP PROCEDURE {commandName};";
                    command.Connection = this.connection;
                    command.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during Drop DeleteMetadata : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();

            }

        }
        public string DropDeleteMetadataScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteMetadata).name;

            return $"DROP PROCEDURE {commandName};";
        }

        //------------------------------------------------------------------
        // Select Row command
        //------------------------------------------------------------------
        private SqlCommand BuildSelectRowCommand()
        {
            var sqlCommand = new SqlCommand();
            this.AddPkColumnParametersToCommand(sqlCommand);
            var sqlParameter = new SqlParameter("@sync_scope_id", SqlDbType.UniqueIdentifier);
            sqlCommand.Parameters.Add(sqlParameter);

            var stringBuilder = new StringBuilder("SELECT ");
            stringBuilder.AppendLine();
            var stringBuilder1 = new StringBuilder();
            string empty = string.Empty;
            foreach (var pkColumn in this.tableDescription.PrimaryKeys)
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                var parameterName = ParserName.Parse(pkColumn).Unquoted().Normalized().ToString();

                stringBuilder.AppendLine($"\t[side].{columnName}, ");
                stringBuilder1.Append($"{empty}[side].{columnName} = @{parameterName}");
                empty = " AND ";
            }
            foreach (var mutableColumn in this.tableDescription.GetMutableColumns())
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                stringBuilder.AppendLine($"\t[base].{columnName}, ");
            }
            stringBuilder.AppendLine("\t[side].[sync_row_is_tombstone]");

            stringBuilder.AppendLine($"FROM {tableName.Schema().Quoted().ToString()} [base]");
            stringBuilder.AppendLine($"RIGHT JOIN {trackingName.Schema().Quoted().ToString()} [side] ON");

            string str = string.Empty;
            foreach (var pkColumn in this.tableDescription.PrimaryKeys)
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                stringBuilder.Append($"{str}[base].{columnName} = [side].{columnName}");
                str = " AND ";
            }
            stringBuilder.AppendLine();
            stringBuilder.Append(string.Concat("WHERE ", stringBuilder1.ToString()));
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
        public void DropSelectRow()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqlCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.SelectRow).name;

                    command.CommandText = $"DROP PROCEDURE {commandName};";
                    command.Connection = this.connection;
                    command.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during Drop SelectRow : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();

            }

        }
        public string DropSelectRowScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.SelectRow).name;

            return $"DROP PROCEDURE {commandName};";
        }

        //------------------------------------------------------------------
        // Create TVP command
        //------------------------------------------------------------------
        private string CreateTVPTypeCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.BulkTableType).name;
            stringBuilder.AppendLine($"CREATE TYPE {commandName} AS TABLE (");
            string str = "";
            foreach (var c in this.tableDescription.Columns.Where(col => !col.IsReadOnly))
            {
                var isPrimaryKey = this.tableDescription.IsPrimaryKey(c.ColumnName);

                var columnName = ParserName.Parse(c).Quoted().ToString();
                var nullString = isPrimaryKey ? "NOT NULL" : "NULL";

                // Get the good SqlDbType (even if we are not from Sql Server def)
                var sqlDbTypeString = this.sqlDbMetadata.TryGetOwnerDbTypeString(c.OriginalDbType, c.GetDbType(), false, false, c.MaxLength, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
                var quotedColumnType = ParserName.Parse(sqlDbTypeString).Quoted().ToString();
                quotedColumnType += this.sqlDbMetadata.TryGetOwnerDbTypePrecision(c.OriginalDbType, c.GetDbType(), false, false, c.MaxLength, c.Precision, c.Scale, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);

                stringBuilder.AppendLine($"{str}{columnName} {quotedColumnType} {nullString}");
                str = ", ";
            }
            stringBuilder.Append(string.Concat(str, "PRIMARY KEY ("));
            str = "";
            foreach (var c in this.tableDescription.PrimaryKeys)
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();
                stringBuilder.Append($"{str}{columnName} ASC");
                str = ", ";
            }

            stringBuilder.Append("))");
            return stringBuilder.ToString();
        }
        public void CreateTVPType()
        {
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                using (SqlCommand sqlCommand = new SqlCommand(this.CreateTVPTypeCommandText(),
                    connection))
                {
                    if (transaction != null)
                        sqlCommand.Transaction = transaction;

                    sqlCommand.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during CreateTVPType : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

            }

        }
        public string CreateTVPTypeScriptText()
        {
            string str = string.Concat("Create TVP Type on table ", tableName.Schema().Quoted().ToString());
            return SqlBuilder.WrapScriptTextWithComments(this.CreateTVPTypeCommandText(), str);
        }
        public void DropTVPType()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqlCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.BulkTableType).name;

                    command.CommandText = $"DROP TYPE {commandName};";
                    command.Connection = this.connection;
                    command.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during Drop TVPType : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();

            }

        }
        public string DropTVPTypeScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.BulkTableType).name;

            return $"DROP TYPE {commandName};";
        }

        //------------------------------------------------------------------
        // Update command
        //------------------------------------------------------------------
        private SqlCommand BuildUpdateCommand(bool hasMutableColumns)
        {
            var sqlCommand = new SqlCommand();
            var stringBuilderArguments = new StringBuilder();
            var stringBuilderParameters = new StringBuilder();
            var stringBuilder = new StringBuilder();

            this.AddColumnParametersToCommand(sqlCommand);

            var sqlParameter = new SqlParameter("@sync_force_write", SqlDbType.Int);
            sqlCommand.Parameters.Add(sqlParameter);
            var sqlParameter1 = new SqlParameter("@sync_min_timestamp", SqlDbType.BigInt);
            sqlCommand.Parameters.Add(sqlParameter1);
            var sqlParameter2 = new SqlParameter("@sync_row_count", SqlDbType.Int)
            {
                Direction = ParameterDirection.Output
            };
            sqlCommand.Parameters.Add(sqlParameter2);

            stringBuilder.AppendLine($"SET {sqlParameter2.ParameterName} = 0;");
            stringBuilder.AppendLine();

            if (hasMutableColumns)
            {

                stringBuilder.AppendLine($"UPDATE {tableName.Schema().Quoted().ToString()}");
                stringBuilder.Append($"SET {SqlManagementUtils.CommaSeparatedUpdateFromParameters(this.tableDescription)}");
                stringBuilder.AppendLine($"FROM {tableName.Schema().Quoted().ToString()} [base]");
                stringBuilder.AppendLine($"JOIN {trackingName.Schema().Quoted().ToString()} [side]");
                stringBuilder.Append($"ON ");
                stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKeys, "[base]", "[side]"));
                stringBuilder.AppendLine("WHERE ([side].[timestamp] <= @sync_min_timestamp OR [side].[update_scope_id] IS NOT NULL OR @sync_force_write = 1)");
                stringBuilder.Append("AND (");
                stringBuilder.Append(SqlManagementUtils.ColumnsAndParameters(this.tableDescription.PrimaryKeys, "[base]"));
                stringBuilder.AppendLine(");");
                stringBuilder.AppendLine();

                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"SET {sqlParameter2.ParameterName} = @@ROWCOUNT;");
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"IF ({sqlParameter2.ParameterName} = 0)");
                stringBuilder.AppendLine($"BEGIN");

            }

            if (this.tableDescription.HasAutoIncrementColumns)
            {
                stringBuilder.AppendLine($"\tSET IDENTITY_INSERT {tableName.Schema().Quoted().ToString()} ON;");
                stringBuilder.AppendLine();
            }

            string empty = string.Empty;
            foreach (var mutableColumn in this.tableDescription.Columns.Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                var parameterName = ParserName.Parse(mutableColumn).Unquoted().Normalized().ToString();
                stringBuilderArguments.Append(string.Concat(empty, columnName));
                stringBuilderParameters.Append(string.Concat(empty, $"@{parameterName}"));
                empty = ", ";
            }
            stringBuilder.AppendLine($"\tINSERT INTO {tableName.Schema().Quoted().ToString()}");
            stringBuilder.AppendLine($"\t({stringBuilderArguments.ToString()})");
            stringBuilder.AppendLine($"\tSELECT {stringBuilderParameters.ToString()}");
            stringBuilder.AppendLine($"\tWHERE (");
            stringBuilder.AppendLine($"\t EXISTS (SELECT 1 FROM {trackingName.Schema().Quoted().ToString()} [side]");
            stringBuilder.AppendLine($"\t    WHERE ({SqlManagementUtils.ColumnsAndParameters(this.tableDescription.PrimaryKeys, "[side]")}) ");
            stringBuilder.AppendLine($"\t    AND ([side].[timestamp] <= @sync_min_timestamp OR [side].[update_scope_id] IS NOT NULL))");
            stringBuilder.AppendLine($"\t OR NOT EXISTS (SELECT 1 FROM {trackingName.Schema().Quoted().ToString()} [side]");
            stringBuilder.AppendLine($"\t    WHERE ({SqlManagementUtils.ColumnsAndParameters(this.tableDescription.PrimaryKeys, "[side]")})) ");
            stringBuilder.AppendLine($"\t OR @sync_force_write = 1");
            stringBuilder.AppendLine($"\t )");

            stringBuilder.AppendLine();

            if (this.tableDescription.HasAutoIncrementColumns)
            {
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"\tSET IDENTITY_INSERT {tableName.Schema().Quoted().ToString()} OFF;");
            }

            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"\tSET {sqlParameter2.ParameterName} = @@ROWCOUNT;");

            if (hasMutableColumns)
                stringBuilder.AppendLine($"END");

            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
        public void CreateUpdate(bool hasMutableColumns)
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateRow).name;
            this.CreateProcedureCommand(BuildUpdateCommand, commandName, hasMutableColumns);
        }
        public string CreateUpdateScriptText(bool hasMutableColumns)
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateRow).name;
            return CreateProcedureCommandScriptText(BuildUpdateCommand, commandName, hasMutableColumns);
        }
        public void DropUpdate()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqlCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateRow).name;

                    command.CommandText = $"DROP PROCEDURE {commandName};";
                    command.Connection = this.connection;
                    command.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during Drop Update : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();

            }

        }
        public string DropUpdateScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateRow).name;

            return $"DROP PROCEDURE {commandName};";
        }

        //------------------------------------------------------------------
        // Update Metadata command
        //------------------------------------------------------------------
        private SqlCommand BuildUpdateMetadataCommand(bool hasMutableColumns)
        {
            var sqlCommand = new SqlCommand();
            var stringBuilder = new StringBuilder();
            this.AddPkColumnParametersToCommand(sqlCommand);
            var sqlParameter = new SqlParameter("@sync_scope_id", SqlDbType.UniqueIdentifier);
            sqlCommand.Parameters.Add(sqlParameter);
            var sqlParameter1 = new SqlParameter("@sync_row_is_tombstone", SqlDbType.Int);
            sqlCommand.Parameters.Add(sqlParameter1);
            var sqlParameter3 = new SqlParameter("@create_timestamp", SqlDbType.BigInt);
            sqlCommand.Parameters.Add(sqlParameter3);
            var sqlParameter5 = new SqlParameter("@update_timestamp", SqlDbType.BigInt);
            sqlCommand.Parameters.Add(sqlParameter5);
            var sqlParameter8 = new SqlParameter("@sync_row_count", SqlDbType.Int)
            {
                Direction = ParameterDirection.Output
            };
            sqlCommand.Parameters.Add(sqlParameter8);

            string str1 = SqlManagementUtils.ColumnsAndParameters(this.tableDescription.PrimaryKeys, "");

            stringBuilder.AppendLine($"SET {sqlParameter8.ParameterName} = 0;");
            stringBuilder.AppendLine();

            if (hasMutableColumns)
            {
                stringBuilder.AppendLine($"UPDATE {trackingName.Schema().Quoted().ToString()} SET ");
                stringBuilder.AppendLine("\t [update_scope_id] = @sync_scope_id, ");
                stringBuilder.AppendLine("\t [sync_row_is_tombstone] = @sync_row_is_tombstone, ");
                stringBuilder.AppendLine("\t [last_change_datetime] = GETUTCDATE() ");
                stringBuilder.AppendLine($"WHERE ({str1})");
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"SET {sqlParameter8.ParameterName} = @@ROWCOUNT;");
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"IF ({sqlParameter8.ParameterName} = 0)");
                stringBuilder.AppendLine($"BEGIN");
            }

            stringBuilder.AppendLine($"\tINSERT INTO {trackingName.Schema().Quoted().ToString()}");

            string empty = string.Empty;
            var stringBuilderArguments = new StringBuilder();
            var stringBuilderParameters = new StringBuilder();

            foreach (var pkColumn in this.tableDescription.PrimaryKeys)
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                var parameterName = ParserName.Parse(pkColumn).Unquoted().Normalized().ToString();

                stringBuilderArguments.Append(string.Concat(empty, columnName));
                stringBuilderParameters.Append(string.Concat(empty, $"@{parameterName}"));
                empty = ", ";
            }
            stringBuilder.AppendLine($"\t({stringBuilderArguments.ToString()}, ");
            stringBuilder.AppendLine($"\t[update_scope_id], [sync_row_is_tombstone], [last_change_datetime])");
            stringBuilder.AppendLine($"\tVALUES ({stringBuilderParameters.ToString()}, ");
            stringBuilder.AppendLine($"\t@sync_scope_id, @sync_row_is_tombstone, GetDate());");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"\tSET {sqlParameter8.ParameterName} = @@rowcount; ");
            stringBuilder.AppendLine();

            if (hasMutableColumns)
                stringBuilder.AppendLine($"END");

            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
        public void CreateUpdateMetadata(bool hasMutableColumns)
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateMetadata).name;
            CreateProcedureCommand(BuildUpdateMetadataCommand, commandName, hasMutableColumns);
        }
        public string CreateUpdateMetadataScriptText(bool hasMutableColumns)
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateMetadata).name;
            return CreateProcedureCommandScriptText(BuildUpdateMetadataCommand, commandName, hasMutableColumns);
        }
        public void DropUpdateMetadata()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqlCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateMetadata).name;

                    command.CommandText = $"DROP PROCEDURE {commandName};";
                    command.Connection = this.connection;
                    command.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during Drop UpdateMetadata : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();

            }

        }
        public string DropUpdateMetadataScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateMetadata).name;

            return $"DROP PROCEDURE {commandName};";
        }


        //------------------------------------------------------------------
        // Select changes command
        //------------------------------------------------------------------
        private SqlCommand BuildSelectIncrementalChangesCommand(bool withFilter = false)
        {
            var sqlCommand = new SqlCommand();
            var pTimestamp = new SqlParameter("@sync_min_timestamp", SqlDbType.BigInt);
            var pScopeId = new SqlParameter("@sync_scope_id", SqlDbType.UniqueIdentifier);
            var pScopeNew = new SqlParameter("@sync_scope_is_new", SqlDbType.Bit);
            var pReinit = new SqlParameter("@sync_scope_is_reinit", SqlDbType.Bit);

            sqlCommand.Parameters.Add(pTimestamp);
            sqlCommand.Parameters.Add(pScopeId);
            sqlCommand.Parameters.Add(pScopeNew);
            sqlCommand.Parameters.Add(pReinit);

            if (withFilter && this.Filters != null && this.Filters.Count > 0)
            {
                foreach (var c in this.Filters)
                {
                    if (!c.IsVirtual)
                    {
                        var columnFilter = this.tableDescription.Columns[c.ColumnName];

                        if (columnFilter == null)
                            throw new InvalidExpressionException($"Column {c.ColumnName} does not exist in Table {this.tableDescription.TableName}");

                        var columnName = ParserName.Parse(columnFilter).Unquoted().Normalized().ToString();

                        // Get the good SqlDbType (even if we are not from Sql Server def)

                        var sqlDbType = (SqlDbType)this.sqlDbMetadata.TryGetOwnerDbType(columnFilter.OriginalDbType, columnFilter.GetDbType(), false, false, columnFilter.MaxLength, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
                        var sqlParamFilter = new SqlParameter($"@{columnName}", sqlDbType);
                        sqlCommand.Parameters.Add(sqlParamFilter);
                    }
                    else
                    {
                        var sqlDbType = (SqlDbType)this.sqlDbMetadata.TryGetOwnerDbType(null, c.GetDbType().Value, false, false, 0, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
                        var columnFilterName = ParserName.Parse(c.ColumnName).Unquoted().Normalized().ToString();
                        var sqlParamFilter = new SqlParameter($"@{columnFilterName}", sqlDbType);
                        sqlCommand.Parameters.Add(sqlParamFilter);
                    }
                }
            }

            var stringBuilder = new StringBuilder("SELECT ");
            foreach (var pkColumn in this.tableDescription.PrimaryKeys)
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                stringBuilder.AppendLine($"\t[side].{columnName}, ");
            }
            foreach (var mutableColumn in this.tableDescription.GetMutableColumns())
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                stringBuilder.AppendLine($"\t[base].{columnName}, ");
            }
            stringBuilder.AppendLine($"\t[side].[sync_row_is_tombstone] ");
            stringBuilder.AppendLine($"FROM {tableName.Schema().Quoted().ToString()} [base]");
            stringBuilder.AppendLine($"RIGHT JOIN {trackingName.Schema().Quoted().ToString()} [side]");
            stringBuilder.Append($"ON ");

            string empty = "";
            foreach (var pkColumn in this.tableDescription.PrimaryKeys)
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                stringBuilder.Append($"{empty}[base].{columnName} = [side].{columnName}");
                empty = " AND ";
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("WHERE (");
            string str = string.Empty;

            var columnFilters = this.Filters.GetColumnFilters();
            if (withFilter && columnFilters.Count() != 0)
            {
                StringBuilder builderFilter = new StringBuilder();
                builderFilter.Append("\t(");
                bool isFirst = true;
                foreach (var c in columnFilters)
                {
                    if (!isFirst)
                        builderFilter.Append(" AND ");
                    isFirst = false;

                    var columnFilter = this.tableDescription.Columns[c.ColumnName];

                    if (columnFilter == null)
                        throw new InvalidExpressionException($"Column {c.ColumnName} does not exist in Table {this.tableDescription.TableName}");

                    var columnFilterName = ParserName.Parse(c.ColumnName).Quoted().ToString();
                    var columnFilterParameterName = ParserName.Parse(c.ColumnName).Unquoted().Normalized().ToString();

                    builderFilter.Append($"[side].{columnFilterName} = @{columnFilterParameterName}");
                }
                builderFilter.AppendLine(")");
                builderFilter.Append("\tOR (");
                builderFilter.AppendLine("([side].[update_scope_id] = @sync_scope_id or [side].[update_scope_id] IS NULL)");
                builderFilter.Append("\t\tAND (");

                isFirst = true;

                foreach (var c in columnFilters)
                {
                    if (!isFirst)
                        builderFilter.Append(" OR ");
                    isFirst = false;

                    var columnFilter = this.tableDescription.Columns[c.ColumnName];
                    var columnFilterName = ParserName.Parse(columnFilter).Quoted().ToString();

                    builderFilter.Append($"[side].{columnFilterName} IS NULL");
                }

                builderFilter.AppendLine("))");
                builderFilter.AppendLine("\t)");
                builderFilter.AppendLine("AND (");
                stringBuilder.Append(builderFilter.ToString());
            }

            stringBuilder.AppendLine("\t[side].[timestamp] > @sync_min_timestamp");
            stringBuilder.AppendLine("\tAND ([side].[update_scope_id] <> @sync_scope_id OR [side].[update_scope_id] IS NULL)");
            stringBuilder.AppendLine(")");

            sqlCommand.CommandText = stringBuilder.ToString();

            return sqlCommand;
        }
        public void CreateSelectIncrementalChanges()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.SelectChanges).name;
            SqlCommand cmdWithoutFilter() => BuildSelectIncrementalChangesCommand(false);
            CreateProcedureCommand(cmdWithoutFilter, commandName);

            if (this.Filters != null && this.Filters.Count > 0)
            {
                this.Filters.ValidateColumnFilters(this.tableDescription);

                commandName = this.sqlObjectNames.GetCommandName(DbCommandType.SelectChangesWitFilters, this.Filters).name;
                SqlCommand cmdWithFilter() => BuildSelectIncrementalChangesCommand(true);
                CreateProcedureCommand(cmdWithFilter, commandName);

            }

        }
        public string CreateSelectIncrementalChangesScriptText()
        {
            StringBuilder sbSelecteChanges = new StringBuilder();

            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.SelectChanges).name;
            SqlCommand cmdWithoutFilter() => BuildSelectIncrementalChangesCommand(false);
            sbSelecteChanges.AppendLine(CreateProcedureCommandScriptText(cmdWithoutFilter, commandName));


            if (this.Filters != null && this.Filters.Count > 0)
            {
                commandName = this.sqlObjectNames.GetCommandName(DbCommandType.SelectChangesWitFilters).name;
                string name = "";
                string sep = "";

                foreach (var c in this.Filters)
                {
                    string unquotedColumnName;
                    if (!c.IsVirtual)
                    {
                        var columnFilter = this.tableDescription.Columns[c.ColumnName];

                        if (columnFilter == null)
                            throw new InvalidExpressionException(
                                $"Column {c.ColumnName} does not exist in Table {this.tableDescription.TableName}");


                        unquotedColumnName = ParserName.Parse(columnFilter).Unquoted().Normalized().ToString();
                    }
                    else
                    {
                        unquotedColumnName = ParserName.Parse(c.ColumnName).Unquoted().Normalized().ToString();
                    }

                    name += $"{unquotedColumnName}{sep}";
                    sep = "_";
                }

                commandName = String.Format(commandName, name);
                SqlCommand cmdWithFilter() => BuildSelectIncrementalChangesCommand(true);
                sbSelecteChanges.AppendLine(CreateProcedureCommandScriptText(cmdWithFilter, commandName));

            }
            return sbSelecteChanges.ToString();
        }
        public void DropSelectIncrementalChanges()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqlCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.SelectChanges).name;

                    command.CommandText = $"DROP PROCEDURE {commandName};";
                    command.Connection = this.connection;
                    command.ExecuteNonQuery();

                }

                if (this.Filters != null && this.Filters.Count > 0)
                {

                    using (var command = new SqlCommand())
                    {
                        if (!alreadyOpened)
                            this.connection.Open();

                        if (this.transaction != null)
                            command.Transaction = this.transaction;

                        this.Filters.ValidateColumnFilters(this.tableDescription);

                        var commandNameWithFilter = this.sqlObjectNames.GetCommandName(DbCommandType.SelectChangesWitFilters, this.Filters).name;

                        command.CommandText = $"DROP PROCEDURE {commandNameWithFilter};";
                        command.Connection = this.connection;
                        command.ExecuteNonQuery();

                    }


                }

            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during DropBulkDelete : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();

            }

        }
        public string DropSelectIncrementalChangesScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.SelectChanges).name;

            string dropProcedure = $"DROP PROCEDURE {commandName};";

            if (this.Filters != null && this.Filters.Count > 0)
            {

                using (var command = new SqlCommand())
                {
                    this.Filters.ValidateColumnFilters(this.tableDescription);
                    var commandNameWithFilter = this.sqlObjectNames.GetCommandName(DbCommandType.SelectChangesWitFilters, this.Filters).name;
                    dropProcedure += Environment.NewLine + $"DROP PROCEDURE {commandNameWithFilter};";

                }
            }
            return dropProcedure;
        }
    }
}
