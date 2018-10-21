using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using Dotmim.Sync.Data.Surrogate;
using Dotmim.Sync.Filter;
using Dotmim.Sync.SqlServer.Manager;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace Dotmim.Sync.SqlServer.Builders
{
    public class SqlBuilderProcedure : IDbBuilderProcedureHelper
    {
        private ObjectNameParser tableName;
        private ObjectNameParser trackingName;
        private SqlConnection connection;
        private readonly SqlTransaction transaction;
        private readonly DmTable tableDescription;
        private readonly SqlObjectNames sqlObjectNames;
        private readonly SqlDbMetadata sqlDbMetadata;

        public IList<FilterClause2> Filters { get; set; }

        public SqlBuilderProcedure(DmTable tableDescription, DbConnection connection, DbTransaction transaction = null)
        {
            this.connection = connection as SqlConnection;
            this.transaction = transaction as SqlTransaction;

            this.tableDescription = tableDescription;
            (tableName, trackingName) = SqlBuilder.GetParsers(tableDescription);
            sqlObjectNames = new SqlObjectNames(this.tableDescription);
            sqlDbMetadata = new SqlDbMetadata();
        }

        private void AddPkColumnParametersToCommand(SqlCommand sqlCommand)
        {
            foreach (DmColumn pkColumn in tableDescription.PrimaryKey.Columns)
                sqlCommand.Parameters.Add(GetSqlParameter(pkColumn));
        }
        private void AddColumnParametersToCommand(SqlCommand sqlCommand)
        {
            foreach (DmColumn column in tableDescription.Columns.Where(c => !c.IsReadOnly))
                sqlCommand.Parameters.Add(GetSqlParameter(column));
        }

        private SqlParameter GetSqlParameter(DmColumnSurrogate column)
        {
            return GetSqlParameter(column.ConvertToDmColumn());
        }

        private SqlParameter GetSqlParameter(DmColumn column)
        {
            SqlParameter sqlParameter = new SqlParameter
            {
                ParameterName = $"@{column.ColumnName}"
            };

            // Get the good SqlDbType (even if we are not from Sql Server def)
            SqlDbType sqlDbType = (SqlDbType)sqlDbMetadata.TryGetOwnerDbType(column.OriginalDbType, column.DbType, column.IsUnsigned, column.IsUnicode, column.MaxLength, tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);

            sqlParameter.SqlDbType = sqlDbType;
            sqlParameter.IsNullable = column.AllowDBNull;

            if (column.DefaultValue != null)
                sqlParameter.Value = column.DefaultValue;


            var (p, s) = sqlDbMetadata.TryGetOwnerPrecisionAndScale(column.OriginalDbType, column.DbType, column.IsUnsigned, column.IsUnicode, column.MaxLength, column.Precision, column.Scale, tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);

            if (p > 0)
            {
                sqlParameter.Precision = p;
                if (s > 0)
                    sqlParameter.Scale = s;
            }

            var m = sqlDbMetadata.TryGetOwnerMaxLength(column.OriginalDbType, column.DbType, false, false, column.MaxLength, tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);

            if (m > 0)
                sqlParameter.Size = m;

            return sqlParameter;
        }

        /// <summary>
        /// From a SqlParameter, create the declaration
        /// </summary>
        internal string CreateParameterDeclaration(SqlParameter param)
        {
            StringBuilder stringBuilder = new StringBuilder();
            SqlDbType sqlDbType = param.SqlDbType;

            string empty = sqlDbMetadata.GetPrecisionStringFromOwnerDbType(sqlDbType, param.Size, param.Precision, param.Scale);

            if (sqlDbType == SqlDbType.Structured)
            {
                stringBuilder.Append(string.Concat(param.ParameterName, " ", param.TypeName, " READONLY"));
            }
            else
            {
                var sqlDbTypeString = sqlDbMetadata.GetStringFromOwnerDbType(sqlDbType);

                stringBuilder.Append(string.Concat(param.ParameterName, " ", sqlDbTypeString, empty));

                if (param.IsNullable)
                    stringBuilder.Append(" NULL");

                if (param.Direction == ParameterDirection.Output || param.Direction == ParameterDirection.InputOutput)
                    stringBuilder.Append(" OUTPUT");
            }

            return stringBuilder.ToString();
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

        private string CreateProcedureCommandScriptText(Func<SqlCommand> BuildCommand, string procName)
        {

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                var str1 = $"Command {procName} for table {tableName.FullQuotedString}";
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

        /// <summary>
        /// Check if we need to create the stored procedure
        /// </summary>
        public bool NeedToCreateProcedure(DbCommandType commandType)
        {
            if (connection.State != ConnectionState.Open)
                throw new ArgumentException("Here, we need an opened connection please");

            var commandName = sqlObjectNames.GetCommandName(commandType);

            return !SqlManagementUtils.ProcedureExists(connection, transaction, commandName);
        }

        /// <summary>
        /// Check if we need to create the TVP Type
        /// </summary>
        public bool NeedToCreateType(DbCommandType commandType)
        {
            if (connection.State != ConnectionState.Open)
                throw new ArgumentException("Here, we need an opened connection please");

            var commandName = sqlObjectNames.GetCommandName(commandType);

            return !SqlManagementUtils.TypeExists(connection, transaction, commandName);

        }

        private string BulkSelectUnsuccessfulRows()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("--Select all ids not deleted for conflict");
            stringBuilder.Append("SELECT ");
            for (int i = 0; i < tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(tableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"{cc.FullQuotedString}");
                if (i < tableDescription.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
                else
                    stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine($"FROM @changeTable t");
            stringBuilder.AppendLine("WHERE NOT EXISTS (");
            stringBuilder.Append("\t SELECT ");
            for (int i = 0; i < tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(tableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"{cc.FullQuotedString}");
                if (i < tableDescription.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
                else
                    stringBuilder.AppendLine();
            }
            stringBuilder.AppendLine("\t FROM @changed i");
            stringBuilder.Append("\t WHERE ");
            for (int i = 0; i < tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(tableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"t.{cc.FullQuotedString} = i.{cc.FullQuotedString}");
                if (i < tableDescription.PrimaryKey.Columns.Length - 1)
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
            SqlCommand sqlCommand = new SqlCommand();

            SqlParameter sqlParameter = new SqlParameter("@sync_min_timestamp", SqlDbType.BigInt);
            sqlCommand.Parameters.Add(sqlParameter);
            SqlParameter sqlParameter1 = new SqlParameter("@sync_scope_id", SqlDbType.UniqueIdentifier);
            sqlCommand.Parameters.Add(sqlParameter1);
            SqlParameter sqlParameter2 = new SqlParameter("@changeTable", SqlDbType.Structured)
            {
                TypeName = sqlObjectNames.GetCommandName(DbCommandType.BulkTableType)
            };
            sqlCommand.Parameters.Add(sqlParameter2);


            string str4 = SqlManagementUtils.JoinTwoTablesOnClause(tableDescription.PrimaryKey.Columns, "p", "t");
            string str5 = SqlManagementUtils.JoinTwoTablesOnClause(tableDescription.PrimaryKey.Columns, "changes", "base");
            string str6 = SqlManagementUtils.JoinTwoTablesOnClause(tableDescription.PrimaryKey.Columns, "changes", "side");
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("-- use a temp table to store the list of PKs that successfully got updated/inserted");
            stringBuilder.Append("declare @changed TABLE (");
            foreach (var c in tableDescription.PrimaryKey.Columns)
            {
                var cc = new ObjectNameParser(c.ColumnName);

                // Get the good SqlDbType (even if we are not from Sql Server def)
                var sqlDbTypeString = sqlDbMetadata.TryGetOwnerDbTypeString(c.OriginalDbType, c.DbType, false, false, c.MaxLength, tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
                var quotedColumnType = new ObjectNameParser(sqlDbTypeString, "[", "]").FullQuotedString;
                quotedColumnType += sqlDbMetadata.TryGetOwnerDbTypePrecision(c.OriginalDbType, c.DbType, false, false, c.MaxLength, c.Precision, c.Scale, tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);

                stringBuilder.Append($"{cc.FullQuotedString} {quotedColumnType}, ");
            }
            stringBuilder.Append(" PRIMARY KEY (");
            for (int i = 0; i < tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(tableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"{cc.FullQuotedString}");
                if (i < tableDescription.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
            }
            stringBuilder.AppendLine("));");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"-- delete all items where timestamp <= @sync_min_timestamp or update_scope_id = @sync_scope_id");

            stringBuilder.AppendLine($"DELETE {tableName.FullQuotedString}");
            stringBuilder.Append($"OUTPUT ");
            for (int i = 0; i < tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(tableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"DELETED.{cc.FullQuotedString}");
                if (i < tableDescription.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
                else
                    stringBuilder.AppendLine();
            }
            stringBuilder.AppendLine($"INTO @changed ");
            stringBuilder.AppendLine($"FROM {tableName.FullQuotedString} [base]");
            stringBuilder.AppendLine("JOIN (");


            stringBuilder.AppendLine($"\tSELECT ");
            string str = "";
            stringBuilder.Append("\t");
            foreach (var c in tableDescription.Columns.Where(col => !col.IsReadOnly))
            {
                var columnName = new ObjectNameParser(c.ColumnName);
                stringBuilder.Append($"{str}[p].{columnName.FullQuotedString}");
                str = ", ";
            }
            stringBuilder.AppendLine("\t, [p].[create_timestamp], [p].[update_timestamp]");
            stringBuilder.AppendLine("\t, [t].[update_scope_id], [t].[timestamp]");

            stringBuilder.AppendLine($"\tFROM @changeTable p ");


            //stringBuilder.AppendLine($"\tSELECT p.*, t.update_scope_id, t.timestamp");
            //stringBuilder.AppendLine($"\tFROM @changeTable p ");
            stringBuilder.AppendLine($"\tJOIN {trackingName.FullQuotedString} t ON ");
            stringBuilder.AppendLine($"\t{str4}");
            stringBuilder.AppendLine("\t)");
            stringBuilder.AppendLine("\tAS changes ON ");
            stringBuilder.AppendLine(str5);
            stringBuilder.AppendLine("WHERE");
            stringBuilder.AppendLine("-- Last changes was from the current scope, so we can delete it since we are sure no one else edit it ");
            stringBuilder.AppendLine("changes.update_scope_id = @sync_scope_id");
            stringBuilder.AppendLine("-- no change since the last time the current scope has sync (so no one has update the row)");
            stringBuilder.AppendLine("OR [changes].[timestamp] <= @sync_min_timestamp;");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("-- Since the delete trigger is passed, we update the tracking table to reflect the real scope deleter");
            stringBuilder.AppendLine("UPDATE [side] SET");
            stringBuilder.AppendLine("\tsync_row_is_tombstone = 1, ");
            stringBuilder.AppendLine("\tupdate_scope_id = @sync_scope_id,");
            stringBuilder.AppendLine("\tupdate_timestamp = [changes].update_timestamp");
            stringBuilder.AppendLine($"FROM {trackingName.FullQuotedString} side");
            stringBuilder.AppendLine("JOIN (");
            stringBuilder.Append("\tSELECT ");
            for (int i = 0; i < tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(tableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"p.{cc.FullQuotedString}, ");
            }
            stringBuilder.AppendLine(" p.update_timestamp, p.create_timestamp ");
            stringBuilder.AppendLine("\tFROM @changed t JOIN @changeTable p ON ");
            stringBuilder.AppendLine($"\t{str4}");
            stringBuilder.Append("\t) AS changes ON ");
            stringBuilder.Append(str6);
            stringBuilder.AppendLine();
            stringBuilder.AppendLine();
            stringBuilder.Append(BulkSelectUnsuccessfulRows());
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }



        public void CreateBulkDelete()
        {
            var commandName = sqlObjectNames.GetCommandName(DbCommandType.BulkDeleteRows);
            CreateProcedureCommand(BuildBulkDeleteCommand, commandName);
        }


        public string CreateBulkDeleteScriptText()
        {
            var commandName = sqlObjectNames.GetCommandName(DbCommandType.BulkDeleteRows);
            return CreateProcedureCommandScriptText(BuildBulkDeleteCommand, commandName);
        }

        public void DropBulkDelete()
        {
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqlCommand())
                {
                    if (!alreadyOpened)
                        connection.Open();

                    if (transaction != null)
                        command.Transaction = transaction;

                    var commandName = sqlObjectNames.GetCommandName(DbCommandType.BulkDeleteRows);

                    command.CommandText = $"DROP PROCEDURE {commandName};";
                    command.Connection = connection;
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
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

            }

        }

        public string DropBulkDeleteScriptText()
        {
            var commandName = sqlObjectNames.GetCommandName(DbCommandType.BulkDeleteRows);

            return $"DROP PROCEDURE {commandName};";
        }


        //------------------------------------------------------------------
        // Bulk Insert command
        //------------------------------------------------------------------
        private SqlCommand BuildBulkInsertCommand()
        {
            SqlCommand sqlCommand = new SqlCommand();

            SqlParameter sqlParameter = new SqlParameter("@sync_min_timestamp", SqlDbType.BigInt);
            sqlCommand.Parameters.Add(sqlParameter);
            SqlParameter sqlParameter1 = new SqlParameter("@sync_scope_id", SqlDbType.UniqueIdentifier);
            sqlCommand.Parameters.Add(sqlParameter1);
            SqlParameter sqlParameter2 = new SqlParameter("@changeTable", SqlDbType.Structured)
            {
                TypeName = sqlObjectNames.GetCommandName(DbCommandType.BulkTableType)
            };
            sqlCommand.Parameters.Add(sqlParameter2);


            string str4 = SqlManagementUtils.JoinTwoTablesOnClause(tableDescription.PrimaryKey.Columns, "[p]", "[t]");
            string str5 = SqlManagementUtils.JoinTwoTablesOnClause(tableDescription.PrimaryKey.Columns, "[changes]", "[base]");
            string str6 = SqlManagementUtils.JoinTwoTablesOnClause(tableDescription.PrimaryKey.Columns, "[changes]", "[side]");

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("-- use a temp table to store the list of PKs that successfully got updated/inserted");
            stringBuilder.Append("declare @changed TABLE (");
            foreach (var c in tableDescription.PrimaryKey.Columns)
            {
                var cc = new ObjectNameParser(c.ColumnName);

                // Get the good SqlDbType (even if we are not from Sql Server def)
                var sqlDbTypeString = sqlDbMetadata.TryGetOwnerDbTypeString(c.OriginalDbType, c.DbType, false, false, c.MaxLength, tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
                var quotedColumnType = new ObjectNameParser(sqlDbTypeString, "[", "]").FullQuotedString;
                quotedColumnType += sqlDbMetadata.TryGetOwnerDbTypePrecision(c.OriginalDbType, c.DbType, false, false, c.MaxLength, c.Precision, c.Scale, tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);

                stringBuilder.Append($"{cc.FullQuotedString} {quotedColumnType}, ");
            }
            stringBuilder.Append(" PRIMARY KEY (");
            for (int i = 0; i < tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(tableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"{cc.FullQuotedString}");
                if (i < tableDescription.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
            }
            stringBuilder.AppendLine("));");
            stringBuilder.AppendLine();

            // Check if we have auto inc column
            if (tableDescription.HasAutoIncrementColumns)
            {
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"SET IDENTITY_INSERT {tableName.FullQuotedString} ON;");
                stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine("-- update/insert into the base table");
            stringBuilder.AppendLine($"MERGE {tableName.FullQuotedString} AS base USING");
            stringBuilder.AppendLine("\t-- join done here against the side table to get the local timestamp for concurrency check\n");


            //   stringBuilder.AppendLine("\t(SELECT p.*, t.timestamp FROM @changeTable p ");

            stringBuilder.AppendLine($"\t(SELECT ");
            string str = "";
            stringBuilder.Append("\t");
            foreach (var c in tableDescription.Columns.Where(col => !col.IsReadOnly))
            {
                var columnName = new ObjectNameParser(c.ColumnName);
                stringBuilder.Append($"{str}[p].{columnName.FullQuotedString}");
                str = ", ";
            }
            stringBuilder.AppendLine("\t, [p].[create_timestamp], [p].[update_timestamp]");
            stringBuilder.AppendLine("\t, [t].[update_scope_id], [t].[timestamp]");

            stringBuilder.AppendLine($"\tFROM @changeTable p ");


            stringBuilder.Append($"\tLEFT JOIN {trackingName.FullQuotedString} t ON ");
            stringBuilder.AppendLine($"\t{str4}");
            stringBuilder.AppendLine($"\t) AS changes ON {str5}");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("-- Si la ligne n'existe pas en local et qu'elle a été créé avant le timestamp de référence");
            stringBuilder.Append("WHEN NOT MATCHED BY TARGET AND (changes.[timestamp] <= @sync_min_timestamp OR changes.[timestamp] IS NULL) THEN");

            StringBuilder stringBuilderArguments = new StringBuilder();
            StringBuilder stringBuilderParameters = new StringBuilder();

            string empty = string.Empty;
            foreach (var mutableColumn in tableDescription.Columns.Where(c => !c.IsReadOnly))
            {
                ObjectNameParser columnName = new ObjectNameParser(mutableColumn.ColumnName);
                stringBuilderArguments.Append(string.Concat(empty, columnName.FullQuotedString));
                stringBuilderParameters.Append(string.Concat(empty, $"changes.{columnName.FullQuotedString}"));
                empty = ", ";
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"\tINSERT");
            stringBuilder.AppendLine($"\t({stringBuilderArguments.ToString()})");
            stringBuilder.AppendLine($"\tVALUES ({stringBuilderParameters.ToString()})");
            stringBuilder.Append($"\tOUTPUT ");
            for (int i = 0; i < tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(tableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"INSERTED.{cc.FullQuotedString}");
                if (i < tableDescription.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
                else
                    stringBuilder.AppendLine();
            }
            stringBuilder.AppendLine($"\tINTO @changed; -- populates the temp table with successful PKs");
            stringBuilder.AppendLine();

            if (tableDescription.HasAutoIncrementColumns)
            {
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"SET IDENTITY_INSERT {tableName.FullQuotedString} OFF;");
                stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine("-- Since the insert trigger is passed, we update the tracking table to reflect the real scope inserter");
            stringBuilder.AppendLine("UPDATE side SET");
            stringBuilder.AppendLine("\tupdate_scope_id = @sync_scope_id,");
            stringBuilder.AppendLine("\tcreate_scope_id = @sync_scope_id,");
            stringBuilder.AppendLine("\tupdate_timestamp = changes.update_timestamp,");
            stringBuilder.AppendLine("\tcreate_timestamp = changes.create_timestamp");
            stringBuilder.AppendLine($"FROM {trackingName.FullQuotedString} side");
            stringBuilder.AppendLine("JOIN (");
            stringBuilder.Append("\tSELECT ");
            for (int i = 0; i < tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(tableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"p.{cc.FullQuotedString}, ");
            }

            stringBuilder.AppendLine(" p.update_timestamp, p.create_timestamp ");
            stringBuilder.AppendLine("\tFROM @changed t");
            stringBuilder.AppendLine("\tJOIN @changeTable p ON ");
            stringBuilder.Append(str4);
            stringBuilder.AppendLine(") AS [changes] ON ");
            stringBuilder.AppendLine(str6);
            stringBuilder.Append(BulkSelectUnsuccessfulRows());


            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
        public void CreateBulkInsert()
        {
            var commandName = sqlObjectNames.GetCommandName(DbCommandType.BulkInsertRows);
            CreateProcedureCommand(BuildBulkInsertCommand, commandName);
        }
        public string CreateBulkInsertScriptText()
        {
            var commandName = sqlObjectNames.GetCommandName(DbCommandType.BulkInsertRows);
            return CreateProcedureCommandScriptText(BuildBulkInsertCommand, commandName);
        }
        public void DropBulkInsert()
        {
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqlCommand())
                {
                    if (!alreadyOpened)
                        connection.Open();

                    if (transaction != null)
                        command.Transaction = transaction;

                    var commandName = sqlObjectNames.GetCommandName(DbCommandType.BulkInsertRows);

                    command.CommandText = $"DROP PROCEDURE {commandName};";
                    command.Connection = connection;
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
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

            }

        }
        public string DropBulkInsertScriptText()
        {
            var commandName = sqlObjectNames.GetCommandName(DbCommandType.BulkInsertRows);

            return $"DROP PROCEDURE {commandName};";
        }

        //------------------------------------------------------------------
        // Bulk Update command
        //------------------------------------------------------------------
        private SqlCommand BuildBulkUpdateCommand()
        {
            SqlCommand sqlCommand = new SqlCommand();

            SqlParameter sqlParameter = new SqlParameter("@sync_min_timestamp", SqlDbType.BigInt);
            sqlCommand.Parameters.Add(sqlParameter);
            SqlParameter sqlParameter1 = new SqlParameter("@sync_scope_id", SqlDbType.UniqueIdentifier);
            sqlCommand.Parameters.Add(sqlParameter1);
            SqlParameter sqlParameter2 = new SqlParameter("@changeTable", SqlDbType.Structured)
            {
                TypeName = sqlObjectNames.GetCommandName(DbCommandType.BulkTableType)
            };
            sqlCommand.Parameters.Add(sqlParameter2);


            string str4 = SqlManagementUtils.JoinTwoTablesOnClause(tableDescription.PrimaryKey.Columns, "[p]", "[t]");
            string str5 = SqlManagementUtils.JoinTwoTablesOnClause(tableDescription.PrimaryKey.Columns, "[changes]", "[base]");
            string str6 = SqlManagementUtils.JoinTwoTablesOnClause(tableDescription.PrimaryKey.Columns, "[changes]", "[side]");

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("-- use a temp table to store the list of PKs that successfully got updated/inserted");
            stringBuilder.Append("declare @changed TABLE (");
            foreach (var c in tableDescription.PrimaryKey.Columns)
            {
                var cc = new ObjectNameParser(c.ColumnName);

                // Get the good SqlDbType (even if we are not from Sql Server def)
                var sqlDbTypeString = sqlDbMetadata.TryGetOwnerDbTypeString(c.OriginalDbType, c.DbType, false, false, c.MaxLength, tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
                var quotedColumnType = new ObjectNameParser(sqlDbTypeString, "[", "]").FullQuotedString;
                quotedColumnType += sqlDbMetadata.TryGetOwnerDbTypePrecision(c.OriginalDbType, c.DbType, false, false, c.MaxLength, c.Precision, c.Scale, tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);

                stringBuilder.Append($"{cc.FullQuotedString} {quotedColumnType}, ");
            }
            stringBuilder.Append(" PRIMARY KEY (");
            for (int i = 0; i < tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(tableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"{cc.FullQuotedString}");
                if (i < tableDescription.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
            }
            stringBuilder.AppendLine("));");
            stringBuilder.AppendLine();


            stringBuilder.AppendLine("-- update the base table");
            stringBuilder.AppendLine($"MERGE {tableName.FullQuotedString} AS base USING");
            stringBuilder.AppendLine("\t-- join done here against the side table to get the local timestamp for concurrency check\n");

            //stringBuilder.AppendLine("\t(SELECT p.*, t.update_scope_id, t.timestamp FROM @changeTable p ");

            stringBuilder.AppendLine($"\t(SELECT ");
            string str = "";
            stringBuilder.Append("\t");
            foreach (var c in tableDescription.Columns.Where(col => !col.IsReadOnly))
            {
                var columnName = new ObjectNameParser(c.ColumnName);
                stringBuilder.Append($"{str}[p].{columnName.FullQuotedString}");
                str = ", ";
            }
            stringBuilder.AppendLine("\t, [p].[create_timestamp], [p].[update_timestamp]");
            stringBuilder.AppendLine("\t, [t].[update_scope_id], [t].[timestamp]");

            stringBuilder.AppendLine($"\tFROM @changeTable p ");
            stringBuilder.Append($"\tLEFT JOIN {trackingName.FullQuotedString} t ON ");
            stringBuilder.AppendLine($" {str4}");
            stringBuilder.AppendLine($"\t) AS changes ON {str5}");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine("WHEN MATCHED AND ([changes].[update_scope_id] = @sync_scope_id OR [changes].[timestamp] <= @sync_min_timestamp) THEN");

            StringBuilder stringBuilderArguments = new StringBuilder();
            StringBuilder stringBuilderParameters = new StringBuilder();

            string empty = string.Empty;
            foreach (var mutableColumn in tableDescription.Columns.Where(c => !c.IsReadOnly))
            {
                ObjectNameParser columnName = new ObjectNameParser(mutableColumn.ColumnName);
                stringBuilderArguments.Append(string.Concat(empty, columnName.FullQuotedString));
                stringBuilderParameters.Append(string.Concat(empty, $"changes.{columnName.FullQuotedString}"));
                empty = ", ";
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"\tUPDATE SET");

            string strSeparator = "";
            foreach (DmColumn mutableColumn in tableDescription.MutableColumnsAndNotAutoInc)
            {
                ObjectNameParser quotedColumn = new ObjectNameParser(mutableColumn.ColumnName);
                stringBuilder.AppendLine($"\t{strSeparator}{quotedColumn.FullQuotedString} = [changes].{quotedColumn.FullQuotedString}");
                strSeparator = ", ";
            }
            stringBuilder.Append($"\tOUTPUT ");
            for (int i = 0; i < tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(tableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"INSERTED.{cc.FullQuotedString}");
                if (i < tableDescription.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
                else
                    stringBuilder.AppendLine();
            }
            stringBuilder.AppendLine($"\tINTO @changed; -- populates the temp table with successful PKs");
            stringBuilder.AppendLine();


            stringBuilder.AppendLine("-- Since the update trigger is passed, we update the tracking table to reflect the real scope updater");
            stringBuilder.AppendLine("UPDATE side SET");
            stringBuilder.AppendLine("\tupdate_scope_id = @sync_scope_id,");
            stringBuilder.AppendLine("\tupdate_timestamp = changes.update_timestamp");
            stringBuilder.AppendLine($"FROM {trackingName.FullQuotedString} side");
            stringBuilder.AppendLine("JOIN (");
            stringBuilder.Append("\tSELECT ");
            for (int i = 0; i < tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(tableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"p.{cc.FullQuotedString}, ");
            }

            stringBuilder.AppendLine(" p.update_timestamp, p.create_timestamp ");
            stringBuilder.AppendLine("\tFROM @changed t");
            stringBuilder.AppendLine("\tJOIN @changeTable p ON ");
            stringBuilder.AppendLine($"\t{str4}");
            stringBuilder.AppendLine(") AS [changes] ON ");
            stringBuilder.AppendLine($"\t{str6}");
            stringBuilder.AppendLine();
            stringBuilder.Append(BulkSelectUnsuccessfulRows());

            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
        public void CreateBulkUpdate()
        {
            var commandName = sqlObjectNames.GetCommandName(DbCommandType.BulkUpdateRows);
            CreateProcedureCommand(BuildBulkUpdateCommand, commandName);
        }
        public string CreateBulkUpdateScriptText()
        {
            var commandName = sqlObjectNames.GetCommandName(DbCommandType.BulkUpdateRows);
            return CreateProcedureCommandScriptText(BuildBulkUpdateCommand, commandName);
        }
        public void DropBulkUpdate()
        {
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqlCommand())
                {
                    if (!alreadyOpened)
                        connection.Open();

                    if (transaction != null)
                        command.Transaction = transaction;

                    var commandName = sqlObjectNames.GetCommandName(DbCommandType.BulkUpdateRows);

                    command.CommandText = $"DROP PROCEDURE {commandName};";
                    command.Connection = connection;
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
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

            }

        }
        public string DropBulkUpdateScriptText()
        {
            var commandName = sqlObjectNames.GetCommandName(DbCommandType.BulkUpdateRows);

            return $"DROP PROCEDURE {commandName};";
        }


        //------------------------------------------------------------------
        // Reset command
        //------------------------------------------------------------------
        private SqlCommand BuildResetCommand()
        {
            var updTriggerName = sqlObjectNames.GetCommandName(DbCommandType.UpdateTrigger);
            var delTriggerName = sqlObjectNames.GetCommandName(DbCommandType.DeleteTrigger);
            var insTriggerName = sqlObjectNames.GetCommandName(DbCommandType.InsertTrigger);

            SqlCommand sqlCommand = new SqlCommand();
            SqlParameter sqlParameter2 = new SqlParameter("@sync_row_count", SqlDbType.Int)
            {
                Direction = ParameterDirection.Output
            };
            sqlCommand.Parameters.Add(sqlParameter2);

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"SET {sqlParameter2.ParameterName} = 0;");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"DISABLE TRIGGER {updTriggerName} ON {tableName.FullQuotedString};");
            stringBuilder.AppendLine($"DISABLE TRIGGER {insTriggerName} ON {tableName.FullQuotedString};");
            stringBuilder.AppendLine($"DISABLE TRIGGER {delTriggerName} ON {tableName.FullQuotedString};");

            stringBuilder.AppendLine($"DELETE FROM {tableName.FullQuotedString};");
            stringBuilder.AppendLine($"DELETE FROM {trackingName.FullQuotedString};");

            stringBuilder.AppendLine($"ENABLE TRIGGER {updTriggerName} ON {tableName.FullQuotedString};");
            stringBuilder.AppendLine($"ENABLE TRIGGER {insTriggerName} ON {tableName.FullQuotedString};");
            stringBuilder.AppendLine($"ENABLE TRIGGER {delTriggerName} ON {tableName.FullQuotedString};");


            stringBuilder.AppendLine();
            stringBuilder.AppendLine(string.Concat("SET ", sqlParameter2.ParameterName, " = @@ROWCOUNT;"));
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
        public void CreateReset()
        {
            var commandName = sqlObjectNames.GetCommandName(DbCommandType.Reset);
            CreateProcedureCommand(BuildResetCommand, commandName);
        }
        public string CreateResetScriptText()
        {
            var commandName = sqlObjectNames.GetCommandName(DbCommandType.Reset);
            return CreateProcedureCommandScriptText(BuildResetCommand, commandName);
        }
        public void DropReset()
        {
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqlCommand())
                {
                    if (!alreadyOpened)
                        connection.Open();

                    if (transaction != null)
                        command.Transaction = transaction;

                    var commandName = sqlObjectNames.GetCommandName(DbCommandType.Reset);

                    command.CommandText = $"DROP PROCEDURE {commandName};";
                    command.Connection = connection;
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
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

            }

        }
        public string DropResetScriptText()
        {
            var commandName = sqlObjectNames.GetCommandName(DbCommandType.Reset);

            return $"DROP PROCEDURE {commandName};";
        }


        //------------------------------------------------------------------
        // Delete command
        //------------------------------------------------------------------
        private SqlCommand BuildDeleteCommand()
        {
            SqlCommand sqlCommand = new SqlCommand();
            AddPkColumnParametersToCommand(sqlCommand);
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
            stringBuilder.AppendLine($"DELETE {tableName.FullQuotedString}");
            stringBuilder.AppendLine($"FROM {tableName.FullQuotedString} [base]");
            stringBuilder.AppendLine($"JOIN {trackingName.FullQuotedString} [side] ON ");

            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(tableDescription.PrimaryKey.Columns, "[base]", "[side]"));

            stringBuilder.AppendLine("WHERE ([side].[timestamp] <= @sync_min_timestamp  OR @sync_force_write = 1)");
            stringBuilder.Append("AND ");
            stringBuilder.AppendLine(string.Concat("(", SqlManagementUtils.ColumnsAndParameters(tableDescription.PrimaryKey.Columns, "[base]"), ");"));
            stringBuilder.AppendLine();
            stringBuilder.AppendLine(string.Concat("SET ", sqlParameter2.ParameterName, " = @@ROWCOUNT;"));
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
        public void CreateDelete()
        {
            var commandName = sqlObjectNames.GetCommandName(DbCommandType.DeleteRow);
            CreateProcedureCommand(BuildDeleteCommand, commandName);
        }
        public string CreateDeleteScriptText()
        {
            var commandName = sqlObjectNames.GetCommandName(DbCommandType.DeleteRow);
            return CreateProcedureCommandScriptText(BuildDeleteCommand, commandName);
        }
        public void DropDelete()
        {
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqlCommand())
                {
                    if (!alreadyOpened)
                        connection.Open();

                    if (transaction != null)
                        command.Transaction = transaction;

                    var commandName = sqlObjectNames.GetCommandName(DbCommandType.DeleteRow);

                    command.CommandText = $"DROP PROCEDURE {commandName};";
                    command.Connection = connection;
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
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

            }

        }
        public string DropDeleteScriptText()
        {
            var commandName = sqlObjectNames.GetCommandName(DbCommandType.DeleteRow);

            return $"DROP PROCEDURE {commandName};";
        }


        //------------------------------------------------------------------
        // Delete Metadata command
        //------------------------------------------------------------------
        private SqlCommand BuildDeleteMetadataCommand()
        {
            SqlCommand sqlCommand = new SqlCommand();
            AddPkColumnParametersToCommand(sqlCommand);
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
            stringBuilder.AppendLine($"DELETE [side] FROM {trackingName.FullQuotedString} [side]");
            stringBuilder.Append($"WHERE ");
            stringBuilder.AppendLine(SqlManagementUtils.ColumnsAndParameters(tableDescription.PrimaryKey.Columns, ""));
            stringBuilder.AppendLine();
            stringBuilder.AppendLine(string.Concat("SET ", sqlParameter2.ParameterName, " = @@ROWCOUNT;"));
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
        public void CreateDeleteMetadata()
        {
            var commandName = sqlObjectNames.GetCommandName(DbCommandType.DeleteMetadata);
            CreateProcedureCommand(BuildDeleteMetadataCommand, commandName);
        }
        public string CreateDeleteMetadataScriptText()
        {
            var commandName = sqlObjectNames.GetCommandName(DbCommandType.DeleteMetadata);
            return CreateProcedureCommandScriptText(BuildDeleteMetadataCommand, commandName);
        }
        public void DropDeleteMetadata()
        {
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqlCommand())
                {
                    if (!alreadyOpened)
                        connection.Open();

                    if (transaction != null)
                        command.Transaction = transaction;

                    var commandName = sqlObjectNames.GetCommandName(DbCommandType.DeleteMetadata);

                    command.CommandText = $"DROP PROCEDURE {commandName};";
                    command.Connection = connection;
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
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

            }

        }
        public string DropDeleteMetadataScriptText()
        {
            var commandName = sqlObjectNames.GetCommandName(DbCommandType.DeleteMetadata);

            return $"DROP PROCEDURE {commandName};";
        }


        //------------------------------------------------------------------
        // Insert command
        //------------------------------------------------------------------
        private SqlCommand BuildInsertCommand()
        {
            SqlCommand sqlCommand = new SqlCommand();
            StringBuilder stringBuilder = new StringBuilder();
            StringBuilder stringBuilderArguments = new StringBuilder();
            StringBuilder stringBuilderParameters = new StringBuilder();

            AddColumnParametersToCommand(sqlCommand);
            SqlParameter sqlParameter = new SqlParameter("@sync_row_count", SqlDbType.Int)
            {
                Direction = ParameterDirection.Output
            };
            sqlCommand.Parameters.Add(sqlParameter);

            stringBuilder.AppendLine($"SET {sqlParameter.ParameterName} = 0;");

            stringBuilder.Append(string.Concat("IF NOT EXISTS (SELECT * FROM ", trackingName.FullQuotedString, " WHERE "));
            stringBuilder.Append(SqlManagementUtils.ColumnsAndParameters(tableDescription.PrimaryKey.Columns, string.Empty));
            stringBuilder.AppendLine(") ");
            stringBuilder.AppendLine("BEGIN ");

            if (tableDescription.HasAutoIncrementColumns)
            {
                stringBuilder.AppendLine($"\tSET IDENTITY_INSERT {tableName.FullQuotedString} ON;");
                stringBuilder.AppendLine();
            }

            string empty = string.Empty;
            foreach (var mutableColumn in tableDescription.Columns.Where(c => !c.IsReadOnly))
            {
                ObjectNameParser columnName = new ObjectNameParser(mutableColumn.ColumnName);
                stringBuilderArguments.Append(string.Concat(empty, columnName.FullQuotedString));
                stringBuilderParameters.Append(string.Concat(empty, $"@{columnName.FullUnquotedString}"));
                empty = ", ";
            }
            stringBuilder.AppendLine($"\tINSERT INTO {tableName.FullQuotedString}");
            stringBuilder.AppendLine($"\t({stringBuilderArguments.ToString()})");
            stringBuilder.AppendLine($"\tVALUES ({stringBuilderParameters.ToString()});");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine(string.Concat("\tSET ", sqlParameter.ParameterName, " = @@rowcount; "));

            if (tableDescription.HasAutoIncrementColumns)
            {
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"\tSET IDENTITY_INSERT {tableName.FullQuotedString} OFF;");
            }

            stringBuilder.AppendLine("END ");
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
        public void CreateInsert()
        {
            var commandName = sqlObjectNames.GetCommandName(DbCommandType.InsertRow);
            CreateProcedureCommand(BuildInsertCommand, commandName);
        }
        public string CreateInsertScriptText()
        {
            var commandName = sqlObjectNames.GetCommandName(DbCommandType.InsertRow);
            return CreateProcedureCommandScriptText(BuildInsertCommand, commandName);
        }
        public void DropInsert()
        {
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqlCommand())
                {
                    if (!alreadyOpened)
                        connection.Open();

                    if (transaction != null)
                        command.Transaction = transaction;

                    var commandName = sqlObjectNames.GetCommandName(DbCommandType.InsertRow);

                    command.CommandText = $"DROP PROCEDURE {commandName};";
                    command.Connection = connection;
                    command.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during Drop InsertRow : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

            }

        }
        public string DropInsertScriptText()
        {
            var commandName = sqlObjectNames.GetCommandName(DbCommandType.InsertRow);

            return $"DROP PROCEDURE {commandName};";
        }

        //------------------------------------------------------------------
        // Insert Metadata command
        //------------------------------------------------------------------
        private SqlCommand BuildInsertMetadataCommand()
        {
            StringBuilder stringBuilderArguments = new StringBuilder();
            StringBuilder stringBuilderParameters = new StringBuilder();
            SqlCommand sqlCommand = new SqlCommand();

            StringBuilder stringBuilder = new StringBuilder();
            AddPkColumnParametersToCommand(sqlCommand);
            SqlParameter sqlParameter = new SqlParameter("@sync_scope_id", SqlDbType.UniqueIdentifier);
            sqlCommand.Parameters.Add(sqlParameter);
            SqlParameter sqlParameter1 = new SqlParameter("@sync_row_is_tombstone", SqlDbType.Int);
            sqlCommand.Parameters.Add(sqlParameter1);
            SqlParameter sqlParameter3 = new SqlParameter("@create_timestamp", SqlDbType.BigInt);
            sqlCommand.Parameters.Add(sqlParameter3);
            SqlParameter sqlParameter4 = new SqlParameter("@update_timestamp", SqlDbType.BigInt);
            sqlCommand.Parameters.Add(sqlParameter4);
            SqlParameter sqlParameter8 = new SqlParameter("@sync_row_count", SqlDbType.Int)
            {
                Direction = ParameterDirection.Output
            };
            sqlCommand.Parameters.Add(sqlParameter8);

            stringBuilder.AppendLine($"SET {sqlParameter8.ParameterName} = 0;");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"UPDATE {trackingName.FullQuotedString} SET ");
            stringBuilder.AppendLine("\t[create_scope_id] = @sync_scope_id, ");
            stringBuilder.AppendLine("\t[create_timestamp] = @create_timestamp, ");
            stringBuilder.AppendLine("\t[update_scope_id] = @sync_scope_id, ");
            stringBuilder.AppendLine("\t[update_timestamp] = @update_timestamp, ");
            stringBuilder.AppendLine("\t[sync_row_is_tombstone] = @sync_row_is_tombstone ");
            stringBuilder.AppendLine($"WHERE ({SqlManagementUtils.ColumnsAndParameters(tableDescription.PrimaryKey.Columns, "")})");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"SET {sqlParameter8.ParameterName} = @@rowcount; ");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"IF ({sqlParameter8.ParameterName} = 0)");
            stringBuilder.AppendLine("BEGIN ");
            stringBuilder.AppendLine($"\tINSERT INTO {trackingName.FullQuotedString}");

            string empty = string.Empty;
            foreach (var pkColumn in tableDescription.PrimaryKey.Columns)
            {
                ObjectNameParser columnName = new ObjectNameParser(pkColumn.ColumnName);
                stringBuilderArguments.Append(string.Concat(empty, columnName.FullQuotedString));
                stringBuilderParameters.Append(string.Concat(empty, $"@{columnName.FullUnquotedString}"));
                empty = ", ";
            }
            stringBuilder.AppendLine($"\t({stringBuilderArguments.ToString()}, ");
            stringBuilder.AppendLine($"\t[create_scope_id], [create_timestamp], [update_scope_id], [update_timestamp],");
            stringBuilder.AppendLine($"\t[sync_row_is_tombstone], [last_change_datetime])");
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
            var commandName = sqlObjectNames.GetCommandName(DbCommandType.InsertMetadata);
            CreateProcedureCommand(BuildInsertMetadataCommand, commandName);
        }
        public string CreateInsertMetadataScriptText()
        {
            var commandName = sqlObjectNames.GetCommandName(DbCommandType.InsertMetadata);
            return CreateProcedureCommandScriptText(BuildInsertMetadataCommand, commandName);
        }
        public void DropInsertMetadata()
        {
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqlCommand())
                {
                    if (!alreadyOpened)
                        connection.Open();

                    if (transaction != null)
                        command.Transaction = transaction;

                    var commandName = sqlObjectNames.GetCommandName(DbCommandType.InsertMetadata);

                    command.CommandText = $"DROP PROCEDURE {commandName};";
                    command.Connection = connection;
                    command.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during Drop InsertMetadata : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

            }

        }
        public string DropInsertMetadataScriptText()
        {
            var commandName = sqlObjectNames.GetCommandName(DbCommandType.InsertMetadata);

            return $"DROP PROCEDURE {commandName};";
        }


        //------------------------------------------------------------------
        // Select Row command
        //------------------------------------------------------------------
        private SqlCommand BuildSelectRowCommand()
        {
            SqlCommand sqlCommand = new SqlCommand();
            AddPkColumnParametersToCommand(sqlCommand);
            SqlParameter sqlParameter = new SqlParameter("@sync_scope_id", SqlDbType.UniqueIdentifier);
            sqlCommand.Parameters.Add(sqlParameter);

            StringBuilder stringBuilder = new StringBuilder("SELECT ");
            stringBuilder.AppendLine();
            StringBuilder stringBuilder1 = new StringBuilder();
            string empty = string.Empty;
            foreach (var pkColumn in tableDescription.PrimaryKey.Columns)
            {
                ObjectNameParser pkColumnName = new ObjectNameParser(pkColumn.ColumnName);
                stringBuilder.AppendLine($"\t[side].{pkColumnName.FullQuotedString}, ");
                stringBuilder1.Append($"{empty}[side].{pkColumnName.FullQuotedString} = @{pkColumnName.FullUnquotedString}");
                empty = " AND ";
            }
            foreach (DmColumn mutableColumn in tableDescription.MutableColumns)
            {
                ObjectNameParser nonPkColumnName = new ObjectNameParser(mutableColumn.ColumnName);
                stringBuilder.AppendLine($"\t[base].{nonPkColumnName.FullQuotedString}, ");
            }
            stringBuilder.AppendLine("\t[side].[sync_row_is_tombstone],");
            stringBuilder.AppendLine("\t[side].[create_scope_id],");
            stringBuilder.AppendLine("\t[side].[create_timestamp],");
            stringBuilder.AppendLine("\t[side].[update_scope_id],");
            stringBuilder.AppendLine("\t[side].[update_timestamp]");

            stringBuilder.AppendLine($"FROM {tableName.FullQuotedString} [base]");
            stringBuilder.AppendLine($"RIGHT JOIN {trackingName.FullQuotedString} [side] ON");

            string str = string.Empty;
            foreach (var pkColumn in tableDescription.PrimaryKey.Columns)
            {
                ObjectNameParser pkColumnName = new ObjectNameParser(pkColumn.ColumnName);
                stringBuilder.Append($"{str}[base].{pkColumnName.FullQuotedString} = [side].{pkColumnName.FullQuotedString}");
                str = " AND ";
            }
            stringBuilder.AppendLine();
            stringBuilder.Append(string.Concat("WHERE ", stringBuilder1.ToString()));
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
        public void CreateSelectRow()
        {
            var commandName = sqlObjectNames.GetCommandName(DbCommandType.SelectRow);
            CreateProcedureCommand(BuildSelectRowCommand, commandName);
        }
        public string CreateSelectRowScriptText()
        {
            var commandName = sqlObjectNames.GetCommandName(DbCommandType.SelectRow);
            return CreateProcedureCommandScriptText(BuildSelectRowCommand, commandName);
        }
        public void DropSelectRow()
        {
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqlCommand())
                {
                    if (!alreadyOpened)
                        connection.Open();

                    if (transaction != null)
                        command.Transaction = transaction;

                    var commandName = sqlObjectNames.GetCommandName(DbCommandType.SelectRow);

                    command.CommandText = $"DROP PROCEDURE {commandName};";
                    command.Connection = connection;
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
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

            }

        }
        public string DropSelectRowScriptText()
        {
            var commandName = sqlObjectNames.GetCommandName(DbCommandType.SelectRow);

            return $"DROP PROCEDURE {commandName};";
        }

        //------------------------------------------------------------------
        // Create TVP command
        //------------------------------------------------------------------
        private string CreateTVPTypeCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            var commandName = sqlObjectNames.GetCommandName(DbCommandType.BulkTableType);
            stringBuilder.AppendLine($"CREATE TYPE {commandName} AS TABLE (");
            string str = "";
            foreach (var c in tableDescription.Columns.Where(col => !col.IsReadOnly))
            {
                var isPrimaryKey = tableDescription.PrimaryKey.Columns.Any(cc => tableDescription.IsEqual(cc.ColumnName, c.ColumnName));
                var columnName = new ObjectNameParser(c.ColumnName);
                var nullString = isPrimaryKey ? "NOT NULL" : "NULL";

                // Get the good SqlDbType (even if we are not from Sql Server def)
                var sqlDbTypeString = sqlDbMetadata.TryGetOwnerDbTypeString(c.OriginalDbType, c.DbType, false, false, c.MaxLength, tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
                var quotedColumnType = new ObjectNameParser(sqlDbTypeString, "[", "]").FullQuotedString;
                quotedColumnType += sqlDbMetadata.TryGetOwnerDbTypePrecision(c.OriginalDbType, c.DbType, false, false, c.MaxLength, c.Precision, c.Scale, tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);

                stringBuilder.AppendLine($"{str}{columnName.FullQuotedString} {quotedColumnType} {nullString}");
                str = ", ";
            }
            stringBuilder.AppendLine(", [create_scope_id] [uniqueidentifier] NULL");
            stringBuilder.AppendLine(", [create_timestamp] [bigint] NULL");
            stringBuilder.AppendLine(", [update_scope_id] [uniqueidentifier] NULL");
            stringBuilder.AppendLine(", [update_timestamp] [bigint] NULL");
            stringBuilder.Append(string.Concat(str, "PRIMARY KEY ("));
            str = "";
            foreach (var c in tableDescription.PrimaryKey.Columns)
            {
                var columnName = new ObjectNameParser(c.ColumnName);
                stringBuilder.Append($"{str}{columnName.FullQuotedString} ASC");
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

                using (SqlCommand sqlCommand = new SqlCommand(CreateTVPTypeCommandText(),
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
            string str = string.Concat("Create TVP Type on table ", tableName.FullQuotedString);
            return SqlBuilder.WrapScriptTextWithComments(CreateTVPTypeCommandText(), str);
        }
        public void DropTVPType()
        {
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqlCommand())
                {
                    if (!alreadyOpened)
                        connection.Open();

                    if (transaction != null)
                        command.Transaction = transaction;

                    var commandName = sqlObjectNames.GetCommandName(DbCommandType.BulkTableType);

                    command.CommandText = $"DROP TYPE {commandName};";
                    command.Connection = connection;
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
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

            }

        }
        public string DropTVPTypeScriptText()
        {
            var commandName = sqlObjectNames.GetCommandName(DbCommandType.BulkTableType);

            return $"DROP TYPE {commandName};";
        }

        //------------------------------------------------------------------
        // Update command
        //------------------------------------------------------------------
        private SqlCommand BuildUpdateCommand()
        {
            SqlCommand sqlCommand = new SqlCommand();

            StringBuilder stringBuilder = new StringBuilder();
            AddColumnParametersToCommand(sqlCommand);

            SqlParameter sqlParameter = new SqlParameter("@sync_force_write", SqlDbType.Int);
            sqlCommand.Parameters.Add(sqlParameter);
            SqlParameter sqlParameter1 = new SqlParameter("@sync_min_timestamp", SqlDbType.BigInt);
            sqlCommand.Parameters.Add(sqlParameter1);
            SqlParameter sqlParameter2 = new SqlParameter("@sync_row_count", SqlDbType.Int)
            {
                Direction = ParameterDirection.Output
            };
            sqlCommand.Parameters.Add(sqlParameter2);

            stringBuilder.AppendLine($"SET {sqlParameter2.ParameterName} = 0;");
            stringBuilder.AppendLine();


            stringBuilder.AppendLine($"UPDATE {tableName.FullQuotedString}");
            stringBuilder.Append($"SET {SqlManagementUtils.CommaSeparatedUpdateFromParameters(tableDescription)}");
            stringBuilder.AppendLine($"FROM {tableName.FullQuotedString} [base]");
            stringBuilder.AppendLine($"JOIN {trackingName.FullQuotedString} [side]");
            stringBuilder.Append($"ON ");
            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(tableDescription.PrimaryKey.Columns, "[base]", "[side]"));
            stringBuilder.AppendLine("WHERE ([side].[timestamp] <= @sync_min_timestamp OR @sync_force_write = 1)");
            stringBuilder.Append("AND (");
            stringBuilder.Append(SqlManagementUtils.ColumnsAndParameters(tableDescription.PrimaryKey.Columns, "[base]"));
            stringBuilder.AppendLine(");");
            stringBuilder.AppendLine();


            stringBuilder.Append(string.Concat("SET ", sqlParameter2.ParameterName, " = @@ROWCOUNT;"));
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
        public void CreateUpdate()
        {
            var commandName = sqlObjectNames.GetCommandName(DbCommandType.UpdateRow);
            CreateProcedureCommand(BuildUpdateCommand, commandName);
        }
        public string CreateUpdateScriptText()
        {
            var commandName = sqlObjectNames.GetCommandName(DbCommandType.UpdateRow);
            return CreateProcedureCommandScriptText(BuildUpdateCommand, commandName);
        }
        public void DropUpdate()
        {
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqlCommand())
                {
                    if (!alreadyOpened)
                        connection.Open();

                    if (transaction != null)
                        command.Transaction = transaction;

                    var commandName = sqlObjectNames.GetCommandName(DbCommandType.UpdateRow);

                    command.CommandText = $"DROP PROCEDURE {commandName};";
                    command.Connection = connection;
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
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

            }

        }
        public string DropUpdateScriptText()
        {
            var commandName = sqlObjectNames.GetCommandName(DbCommandType.UpdateRow);

            return $"DROP PROCEDURE {commandName};";
        }

        //------------------------------------------------------------------
        // Update Metadata command
        //------------------------------------------------------------------
        private SqlCommand BuildUpdateMetadataCommand()
        {
            SqlCommand sqlCommand = new SqlCommand();
            StringBuilder stringBuilder = new StringBuilder();
            AddPkColumnParametersToCommand(sqlCommand);
            SqlParameter sqlParameter = new SqlParameter("@sync_scope_id", SqlDbType.UniqueIdentifier);
            sqlCommand.Parameters.Add(sqlParameter);
            SqlParameter sqlParameter1 = new SqlParameter("@sync_row_is_tombstone", SqlDbType.Int);
            sqlCommand.Parameters.Add(sqlParameter1);
            SqlParameter sqlParameter3 = new SqlParameter("@create_timestamp", SqlDbType.BigInt);
            sqlCommand.Parameters.Add(sqlParameter3);
            SqlParameter sqlParameter5 = new SqlParameter("@update_timestamp", SqlDbType.BigInt);
            sqlCommand.Parameters.Add(sqlParameter5);
            SqlParameter sqlParameter8 = new SqlParameter("@sync_row_count", SqlDbType.Int)
            {
                Direction = ParameterDirection.Output
            };
            sqlCommand.Parameters.Add(sqlParameter8);

            string str1 = SqlManagementUtils.ColumnsAndParameters(tableDescription.PrimaryKey.Columns, "");

            stringBuilder.AppendLine($"SET {sqlParameter8.ParameterName} = 0;");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine("DECLARE @was_tombstone int; ");

            stringBuilder.AppendLine($"SELECT @was_tombstone = [sync_row_is_tombstone]");
            stringBuilder.AppendLine($"FROM {trackingName.FullQuotedString}");
            stringBuilder.AppendLine($"WHERE ({str1})");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("IF (@was_tombstone IS NOT NULL AND @was_tombstone = 1 AND @sync_row_is_tombstone = 0)");
            stringBuilder.AppendLine("BEGIN ");
            stringBuilder.AppendLine($"UPDATE {trackingName.FullQuotedString} SET ");
            stringBuilder.AppendLine("\t [create_scope_id] = @sync_scope_id, ");
            stringBuilder.AppendLine("\t [update_scope_id] = @sync_scope_id, ");
            stringBuilder.AppendLine("\t [create_timestamp] = @create_timestamp, ");
            stringBuilder.AppendLine("\t [update_timestamp] = @update_timestamp, ");
            stringBuilder.AppendLine("\t [sync_row_is_tombstone] = @sync_row_is_tombstone ");
            stringBuilder.AppendLine($"WHERE ({str1})");
            stringBuilder.AppendLine("END");
            stringBuilder.AppendLine("ELSE");
            stringBuilder.AppendLine("BEGIN ");
            stringBuilder.AppendLine($"UPDATE {trackingName.FullQuotedString} SET ");
            stringBuilder.AppendLine("\t [update_scope_id] = @sync_scope_id, ");
            stringBuilder.AppendLine("\t [update_timestamp] = @update_timestamp, ");
            stringBuilder.AppendLine("\t [sync_row_is_tombstone] = @sync_row_is_tombstone ");
            stringBuilder.AppendLine($"WHERE ({str1})");
            stringBuilder.AppendLine("END;");
            stringBuilder.AppendLine();
            stringBuilder.Append($"SET {sqlParameter8.ParameterName} = @@ROWCOUNT;");
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
        public void CreateUpdateMetadata()
        {
            var commandName = sqlObjectNames.GetCommandName(DbCommandType.UpdateMetadata);
            CreateProcedureCommand(BuildUpdateMetadataCommand, commandName);
        }
        public string CreateUpdateMetadataScriptText()
        {
            var commandName = sqlObjectNames.GetCommandName(DbCommandType.UpdateMetadata);
            return CreateProcedureCommandScriptText(BuildUpdateMetadataCommand, commandName);
        }
        public void DropUpdateMetadata()
        {
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqlCommand())
                {
                    if (!alreadyOpened)
                        connection.Open();

                    if (transaction != null)
                        command.Transaction = transaction;

                    var commandName = sqlObjectNames.GetCommandName(DbCommandType.UpdateMetadata);

                    command.CommandText = $"DROP PROCEDURE {commandName};";
                    command.Connection = connection;
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
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

            }

        }
        public string DropUpdateMetadataScriptText()
        {
            var commandName = sqlObjectNames.GetCommandName(DbCommandType.UpdateMetadata);

            return $"DROP PROCEDURE {commandName};";
        }


        /// <summary>
        /// Get the list of all columns to select
        /// </summary>
        /// <returns></returns>
        private string GetSICStandardColumns()
        {
            StringBuilder stringBuilder = new StringBuilder();
            foreach (var pkColumn in tableDescription.PrimaryKey.Columns)
            {
                var pkColumnName = new ObjectNameParser(pkColumn.ColumnName);
                stringBuilder.AppendLine($"\t[side].{pkColumnName.FullQuotedString}, ");
            }
            foreach (var mutableColumn in tableDescription.MutableColumns)
            {
                var columnName = new ObjectNameParser(mutableColumn.ColumnName);
                stringBuilder.AppendLine($"\t[base].{columnName.FullQuotedString}, ");
            }
            stringBuilder.AppendLine($"\t[side].[sync_row_is_tombstone], ");
            stringBuilder.AppendLine($"\t[side].[create_scope_id], ");
            stringBuilder.AppendLine($"\t[side].[create_timestamp], ");
            stringBuilder.AppendLine($"\t[side].[update_scope_id], ");
            stringBuilder.AppendLine($"\t[side].[update_timestamp] ");
            return stringBuilder.ToString();
        }

        private string GetSICFirstLevelFrom()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"FROM {tableName.FullQuotedString} [base]");
            stringBuilder.AppendLine($"RIGHT JOIN {trackingName.FullQuotedString} [side]");
            stringBuilder.Append($"ON ");

            string empty = "";
            foreach (var pkColumn in tableDescription.PrimaryKey.Columns)
            {
                var pkColumnName = new ObjectNameParser(pkColumn.ColumnName);
                stringBuilder.Append($"{empty}[base].{pkColumnName.FullQuotedString} = [side].{pkColumnName.FullQuotedString}");
                empty = " AND ";
            }

            return stringBuilder.ToString();
        }

        private string GetSICStandardClause()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("(");
            stringBuilder.AppendLine("\t-- Update made by the local instance");
            stringBuilder.AppendLine("\t[side].[update_scope_id] IS NULL");
            stringBuilder.AppendLine("\t-- Or Update different from remote");
            stringBuilder.AppendLine("\tOR [side].[update_scope_id] <> @sync_scope_id");
            stringBuilder.AppendLine("\t-- Or we are in reinit mode so we take rows even thoses updated by the scope");
            stringBuilder.AppendLine("\tOR @sync_scope_is_reinit = 1");
            stringBuilder.AppendLine("    )");
            stringBuilder.AppendLine("AND (");
            stringBuilder.AppendLine("\t-- And Timestamp is > from remote timestamp");
            stringBuilder.AppendLine("\t[side].[timestamp] > @sync_min_timestamp");
            stringBuilder.AppendLine("\tOR");
            stringBuilder.AppendLine("\t-- remote instance is new, so we don't take the last timestamp");
            stringBuilder.AppendLine("\t@sync_scope_is_new = 1");
            stringBuilder.AppendLine("\t)");
            stringBuilder.AppendLine("AND (");
            stringBuilder.AppendLine("\t[side].[sync_row_is_tombstone] = 1 ");
            stringBuilder.AppendLine("\tOR");
            stringBuilder.Append("\t([side].[sync_row_is_tombstone] = 0");

            var empty = " AND ";
            foreach (var pkColumn in tableDescription.PrimaryKey.Columns)
            {
                var pkColumnName = new ObjectNameParser(pkColumn.ColumnName, "[", "]");
                stringBuilder.Append($"{empty}[base].{pkColumnName.FullQuotedString} is not null");
            }
            stringBuilder.AppendLine("\t)");
            stringBuilder.AppendLine(")");

            return stringBuilder.ToString();
        }

        private List<SqlParameter> GetSICAdditionalParameter()
        {
            var lstParams = new List<SqlParameter>();

            if (Filters != null && Filters.Count > 0)
            {
                foreach (var f in Filters)
                {
                    if (f.InParameter == null)
                        throw new InvalidExpressionException($"filter parameter has not been set for table {tableDescription.TableName}");

                    // parse it
                    var columnFilterName = new ObjectNameParser(f.InParameter.ColumnName, "[", "]");

                    // generate the param declaration
                    SqlParameter sqlParamFilter = GetSqlParameter(f.InParameter);
                    if (!lstParams.Any(pf => pf.ParameterName == sqlParamFilter.ParameterName))
                        lstParams.Add(sqlParamFilter);
                }
            }

            return lstParams;
        }

        private string GetSICWhereConditionForFilter(FilterClause2 filter)
        {

            string alias = "[side]";
            StringBuilder whereFilterBuilder = new StringBuilder();

            var columnFilter = tableDescription.DmSet.Tables[filter.FilterTable.TableName.ObjectNameNormalized].Columns[filter.FilterTable.ColumnName];

            if (columnFilter == null)
                throw new InvalidExpressionException($"Column {filter.FilterTable.ColumnName} does not exist in Table {this.tableDescription.TableName}");

            var quotedColumnName = new ObjectNameParser(columnFilter.ColumnName, "[", "]").FullQuotedString;
            whereFilterBuilder.AppendLine("(");
            whereFilterBuilder.Append($"\t{alias}.{quotedColumnName} = @{filter.InParameter.ColumnName} ");
            whereFilterBuilder.Append($"OR {alias}.{quotedColumnName} IS NULL ");
            whereFilterBuilder.AppendLine($"OR @{filter.InParameter.ColumnName} IS NULL ");
            whereFilterBuilder.AppendLine(")");
            whereFilterBuilder.AppendLine("AND ");

            var str = whereFilterBuilder.ToString();
            return str;

        }


        private SqlCommand BuildSICWithFilter()
        {
            SqlCommand sqlCommand = new SqlCommand();

            SqlParameter pTimestamp = new SqlParameter("@sync_min_timestamp", SqlDbType.BigInt);
            SqlParameter pScopeId = new SqlParameter("@sync_scope_id", SqlDbType.UniqueIdentifier);
            SqlParameter pScopeNew = new SqlParameter("@sync_scope_is_new", SqlDbType.Bit);
            SqlParameter pReinit = new SqlParameter("@sync_scope_is_reinit", SqlDbType.Bit);
            sqlCommand.Parameters.Add(pTimestamp);
            sqlCommand.Parameters.Add(pScopeId);
            sqlCommand.Parameters.Add(pScopeNew);
            sqlCommand.Parameters.Add(pReinit);

            // add In params if any (for filters)
            var lstInParams = GetSICAdditionalParameter();
            lstInParams.ForEach(p => sqlCommand.Parameters.Add(p));

            StringBuilder stringBuilder = new StringBuilder();

            // we will have to make UNION of all filters
            for (int filterIndex = 0; filterIndex < Filters.Count; filterIndex++)
            {
                var filter = Filters[filterIndex];

                // append the correct SELECT (DISTINCT or NOT)
                stringBuilder.AppendLine("SELECT DISTINCT");

                // Get all the Columns from table
                stringBuilder.AppendLine(GetSICStandardColumns());

                // get the first FROM on the inner table 
                stringBuilder.AppendLine(GetSICFirstLevelFrom());

                // add Where
                stringBuilder.AppendLine("WHERE ");

                // add condition for current filter
                stringBuilder.AppendLine(GetSICWhereConditionForFilter(filter));

                // append all clauses (timestamp, reinit, and so on)
                stringBuilder.AppendLine(GetSICStandardClause());

                // make the UNION
                if (filterIndex < Filters.Count - 1)
                    stringBuilder.AppendLine("UNION");
            }

            sqlCommand.CommandText = stringBuilder.ToString();


            return sqlCommand;
        }


        private SqlCommand BuildSICWithoutFilter()
        {
            SqlCommand sqlCommand = new SqlCommand();

            SqlParameter pTimestamp = new SqlParameter("@sync_min_timestamp", SqlDbType.BigInt);
            SqlParameter pScopeId = new SqlParameter("@sync_scope_id", SqlDbType.UniqueIdentifier);
            SqlParameter pScopeNew = new SqlParameter("@sync_scope_is_new", SqlDbType.Bit);
            SqlParameter pReinit = new SqlParameter("@sync_scope_is_reinit", SqlDbType.Bit);
            sqlCommand.Parameters.Add(pTimestamp);
            sqlCommand.Parameters.Add(pScopeId);
            sqlCommand.Parameters.Add(pScopeNew);
            sqlCommand.Parameters.Add(pReinit);

            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.AppendLine("SELECT ");

            // Get all the Columns from table
            stringBuilder.AppendLine(GetSICStandardColumns());

            // get the first FROM on the inner table 
            stringBuilder.AppendLine(GetSICFirstLevelFrom());

            // add Where
            stringBuilder.AppendLine("WHERE ");


            // append all clauses (timestamp, reinit, and so on)
            stringBuilder.AppendLine(GetSICStandardClause());

            // make the UNION


            sqlCommand.CommandText = stringBuilder.ToString();


            return sqlCommand;
        }



        //------------------------------------------------------------------
        // Select changes command
        //------------------------------------------------------------------

        private SqlCommand BuildSelectIncrementalChangesCommand(bool withFilter = false)
        {
            return withFilter ? BuildSICWithFilter() : BuildSICWithoutFilter();
        }

        public void CreateSelectIncrementalChanges()
        {
            var commandName = sqlObjectNames.GetCommandName(DbCommandType.SelectChanges);
            SqlCommand cmdWithoutFilter() => BuildSelectIncrementalChangesCommand(false);
            CreateProcedureCommand(cmdWithoutFilter, commandName);

            if (Filters != null && Filters.Count > 0)
            {
                var filtersName = Filters.GroupBy(fc => fc.InParameter.ColumnName).Select(f => f.Key);

                commandName = sqlObjectNames.GetCommandName(DbCommandType.SelectChangesWitFilters, filtersName);
                SqlCommand cmdWithFilter() => BuildSelectIncrementalChangesCommand(true);
                CreateProcedureCommand(cmdWithFilter, commandName);

            }

        }
        public string CreateSelectIncrementalChangesScriptText()
        {
            StringBuilder sbSelecteChanges = new StringBuilder();

            var commandName = sqlObjectNames.GetCommandName(DbCommandType.SelectChanges);
            SqlCommand cmdWithoutFilter() => BuildSelectIncrementalChangesCommand(false);
            sbSelecteChanges.AppendLine(CreateProcedureCommandScriptText(cmdWithoutFilter, commandName));


            if (Filters != null && Filters.Count > 0)
            {
                commandName = sqlObjectNames.GetCommandName(DbCommandType.SelectChangesWitFilters);
                SqlCommand cmdWithFilter() => BuildSelectIncrementalChangesCommand(true);
                sbSelecteChanges.AppendLine(CreateProcedureCommandScriptText(cmdWithFilter, commandName));

            }
            return sbSelecteChanges.ToString();
        }
        public void DropSelectIncrementalChanges()
        {
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqlCommand())
                {
                    if (!alreadyOpened)
                        connection.Open();

                    if (transaction != null)
                        command.Transaction = transaction;

                    var commandName = sqlObjectNames.GetCommandName(DbCommandType.SelectChanges);

                    command.CommandText = $"DROP PROCEDURE {commandName};";
                    command.Connection = connection;
                    command.ExecuteNonQuery();

                }

                if (Filters != null && Filters.Count > 0)
                {

                    using (var command = new SqlCommand())
                    {
                        if (!alreadyOpened)
                            connection.Open();

                        if (transaction != null)
                            command.Transaction = transaction;

                        //this.Filters.ValidateColumnFilters(this.tableDescription);

                        var filtersName = Filters.GroupBy(fc => fc.InParameter.ColumnName).Select(f => f.Key);
                        var commandNameWithFilter = sqlObjectNames.GetCommandName(DbCommandType.SelectChangesWitFilters, filtersName);

                        command.CommandText = $"DROP PROCEDURE {commandNameWithFilter};";
                        command.Connection = connection;
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
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

            }

        }
        public string DropSelectIncrementalChangesScriptText()
        {
            var commandName = sqlObjectNames.GetCommandName(DbCommandType.SelectChanges);

            string dropProcedure = $"DROP PROCEDURE {commandName};";

            if (Filters != null && Filters.Count > 0)
            {

                using (var command = new SqlCommand())
                {
                    var filtersName = Filters.GroupBy(fc => fc.InParameter.ColumnName).Select(f => f.Key);
                    var commandNameWithFilter = sqlObjectNames.GetCommandName(DbCommandType.SelectChangesWitFilters, filtersName);

                    dropProcedure += Environment.NewLine + $"DROP PROCEDURE {commandNameWithFilter};";

                }
            }
            return dropProcedure;
        }
    }
}
