﻿using Dotmim.Sync.Builders;
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
        private readonly DmTable tableDescription;
        private readonly SqlObjectNames sqlObjectNames;
        private readonly SqlDbMetadata sqlDbMetadata;

        public ICollection<FilterClause> Filters { get; set; }

        public SqlBuilderProcedure(DmTable tableDescription, DbConnection connection, DbTransaction transaction = null)
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
            foreach (DmColumn pkColumn in this.tableDescription.PrimaryKey.Columns)
                sqlCommand.Parameters.Add(GetSqlParameter(pkColumn));
        }
        private void AddColumnParametersToCommand(SqlCommand sqlCommand)
        {
            foreach (DmColumn column in this.tableDescription.Columns.Where(c => !c.IsReadOnly))
                sqlCommand.Parameters.Add(GetSqlParameter(column));
        }

        private SqlParameter GetSqlParameter(DmColumn column)
        {
            var sqlParameter = new SqlParameter
            {
                ParameterName = $"@{ParserName.Parse(column).Unquoted().Normalized().ToString()}"
            };

            // Get the good SqlDbType (even if we are not from Sql Server def)
            var sqlDbType = (SqlDbType)this.sqlDbMetadata.TryGetOwnerDbType(column.OriginalDbType, column.DbType, false, false, column.MaxLength, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);

            sqlParameter.SqlDbType = sqlDbType;
            sqlParameter.IsNullable = column.AllowDBNull;

            var (p, s) = this.sqlDbMetadata.TryGetOwnerPrecisionAndScale(column.OriginalDbType, column.DbType, false, false, column.MaxLength, column.Precision, column.Scale, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);

            if (p > 0)
            {
                sqlParameter.Precision = p;
                if (s > 0)
                    sqlParameter.Scale = s;
            }

            var m = this.sqlDbMetadata.TryGetOwnerMaxLength(column.OriginalDbType, column.DbType, false, false, column.MaxLength, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);

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
            stringBuilder.AppendLine("--Select all ids not deleted for conflict");
            stringBuilder.Append("SELECT ");
            for (int i = 0; i < this.tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = ParserName.Parse(this.tableDescription.PrimaryKey.Columns[i])
                                   .Quoted().ToString();
                
                stringBuilder.Append($"{cc}");
                if (i < this.tableDescription.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
                else
                    stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine($"FROM @changeTable t");
            stringBuilder.AppendLine("WHERE NOT EXISTS (");
            stringBuilder.Append("\t SELECT ");
            for (int i = 0; i < this.tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = ParserName.Parse(this.tableDescription.PrimaryKey.Columns[i])
                                   .Quoted().ToString();

                stringBuilder.Append($"{cc}");
                if (i < this.tableDescription.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
                else
                    stringBuilder.AppendLine();
            }
            stringBuilder.AppendLine("\t FROM @changed i");
            stringBuilder.Append("\t WHERE ");
            for (int i = 0; i < this.tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = ParserName.Parse(this.tableDescription.PrimaryKey.Columns[i])
                                .Quoted().ToString();
                stringBuilder.Append($"t.{cc} = i.{cc}");
                if (i < this.tableDescription.PrimaryKey.Columns.Length - 1)
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
                TypeName = this.sqlObjectNames.GetCommandName(DbCommandType.BulkTableType).name
            };
            sqlCommand.Parameters.Add(sqlParameter2);


            string str4 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "p", "t");
            string str5 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "changes", "base");
            string str6 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "changes", "side");
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("-- use a temp table to store the list of PKs that successfully got updated/inserted");
            stringBuilder.Append("declare @changed TABLE (");
            foreach (var c in this.tableDescription.PrimaryKey.Columns)
            {
                // Get the good SqlDbType (even if we are not from Sql Server def)
                var sqlDbTypeString = this.sqlDbMetadata.TryGetOwnerDbTypeString(c.OriginalDbType, c.DbType, false, false, c.MaxLength, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
                var quotedColumnType = ParserName.Parse(sqlDbTypeString).Quoted().ToString();

                quotedColumnType += this.sqlDbMetadata.TryGetOwnerDbTypePrecision(c.OriginalDbType, c.DbType, false, false, c.MaxLength, c.Precision, c.Scale, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);

                stringBuilder.Append($"{ParserName.Parse(c).Quoted().ToString()} {quotedColumnType}, ");
            }
            stringBuilder.Append(" PRIMARY KEY (");
            for (int i = 0; i < this.tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = ParserName.Parse(this.tableDescription.PrimaryKey.Columns[i]).Quoted().ToString();
                stringBuilder.Append($"{cc}");

                if (i < this.tableDescription.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
            }
            stringBuilder.AppendLine("));");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"-- delete all items where timestamp <= @sync_min_timestamp or update_scope_id = @sync_scope_id");

            stringBuilder.AppendLine($"DELETE {tableName.Schema().Quoted().ToString()}");
            stringBuilder.Append($"OUTPUT ");
            for (int i = 0; i < this.tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = ParserName.Parse(this.tableDescription.PrimaryKey.Columns[i]).Quoted().ToString();
                stringBuilder.Append($"DELETED.{cc}");
                if (i < this.tableDescription.PrimaryKey.Columns.Length - 1)
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
            stringBuilder.AppendLine("\t, [p].[create_timestamp], [p].[update_timestamp]");
            stringBuilder.AppendLine("\t, [t].[update_scope_id], [t].[timestamp]");

            stringBuilder.AppendLine($"\tFROM @changeTable p ");


            //stringBuilder.AppendLine($"\tSELECT p.*, t.update_scope_id, t.timestamp");
            //stringBuilder.AppendLine($"\tFROM @changeTable p ");
            stringBuilder.AppendLine($"\tJOIN {trackingName.Schema().Quoted().ToString()} t ON ");
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
            stringBuilder.AppendLine($"FROM {trackingName.Schema().Quoted().ToString()} side");
            stringBuilder.AppendLine("JOIN (");
            stringBuilder.Append("\tSELECT ");
            for (int i = 0; i < this.tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = ParserName.Parse(this.tableDescription.PrimaryKey.Columns[i]).Quoted().ToString();
                stringBuilder.Append($"p.{cc}, ");
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
                TypeName = this.sqlObjectNames.GetCommandName(DbCommandType.BulkTableType).name
            };
            sqlCommand.Parameters.Add(sqlParameter2);


            string str4 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "[p]", "[t]");
            string str5 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "[changes]", "[base]");
            string str6 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "[changes]", "[side]");

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("-- use a temp table to store the list of PKs that successfully got updated/inserted");
            stringBuilder.Append("declare @changed TABLE (");
            foreach (var c in this.tableDescription.PrimaryKey.Columns)
            {
                var cc = ParserName.Parse(c).Quoted().ToString();

                // Get the good SqlDbType (even if we are not from Sql Server def)
                var sqlDbTypeString = this.sqlDbMetadata.TryGetOwnerDbTypeString(c.OriginalDbType, c.DbType, false, false, c.MaxLength, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
                var quotedColumnType = ParserName.Parse(sqlDbTypeString).Quoted().ToString();
                quotedColumnType += this.sqlDbMetadata.TryGetOwnerDbTypePrecision(c.OriginalDbType, c.DbType, false, false, c.MaxLength, c.Precision, c.Scale, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);

                stringBuilder.Append($"{cc} {quotedColumnType}, ");
            }
            stringBuilder.Append(" PRIMARY KEY (");
            for (int i = 0; i < this.tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = ParserName.Parse(this.tableDescription.PrimaryKey.Columns[i]).Quoted().ToString();
                stringBuilder.Append($"{cc}");
                if (i < this.tableDescription.PrimaryKey.Columns.Length - 1)
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

            stringBuilder.AppendLine("-- update/insert into the base table");
            stringBuilder.AppendLine($"MERGE {tableName.Schema().Quoted().ToString()} AS base USING");
            stringBuilder.AppendLine("\t-- join done here against the side table to get the local timestamp for concurrency check\n");


            //   stringBuilder.AppendLine("\t(SELECT p.*, t.timestamp FROM @changeTable p ");

            stringBuilder.AppendLine($"\t(SELECT ");
            string str = "";
            stringBuilder.Append("\t");
            foreach (var c in this.tableDescription.Columns.Where(col => !col.IsReadOnly))
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();
                stringBuilder.Append($"{str}[p].{columnName}");
                str = ", ";
            }
            stringBuilder.AppendLine("\t, [p].[create_timestamp], [p].[update_timestamp]");
            stringBuilder.AppendLine("\t, [t].[update_scope_id], [t].[timestamp]");

            stringBuilder.AppendLine($"\tFROM @changeTable p ");


            stringBuilder.Append($"\tLEFT JOIN {trackingName.Schema().Quoted().ToString()} t ON ");
            stringBuilder.AppendLine($"\t{str4}");
            stringBuilder.AppendLine($"\t) AS changes ON {str5}");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("-- Si la ligne n'existe pas en local et qu'elle a été créé avant le timestamp de référence");
            stringBuilder.Append("WHEN NOT MATCHED BY TARGET AND (changes.[timestamp] <= @sync_min_timestamp OR changes.[timestamp] IS NULL) THEN");

            StringBuilder stringBuilderArguments = new StringBuilder();
            StringBuilder stringBuilderParameters = new StringBuilder();

            string empty = string.Empty;
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
            for (int i = 0; i < this.tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var columnName = ParserName.Parse(this.tableDescription.PrimaryKey.Columns[i]).Quoted().ToString();

                stringBuilder.Append($"INSERTED.{columnName}");
                if (i < this.tableDescription.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
                else
                    stringBuilder.AppendLine();
            }
            stringBuilder.AppendLine($"\tINTO @changed; -- populates the temp table with successful PKs");
            stringBuilder.AppendLine();

            if (this.tableDescription.HasAutoIncrementColumns)
            {
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"SET IDENTITY_INSERT {tableName.Schema().Quoted().ToString()} OFF;");
                stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine("-- Since the insert trigger is passed, we update the tracking table to reflect the real scope inserter");
            stringBuilder.AppendLine("UPDATE side SET");
            stringBuilder.AppendLine("\tupdate_scope_id = @sync_scope_id,");
            stringBuilder.AppendLine("\tcreate_scope_id = @sync_scope_id,");
            stringBuilder.AppendLine("\tupdate_timestamp = changes.update_timestamp,");
            stringBuilder.AppendLine("\tcreate_timestamp = changes.create_timestamp");
            stringBuilder.AppendLine($"FROM {trackingName.Schema().Quoted().ToString()} side");
            stringBuilder.AppendLine("JOIN (");
            stringBuilder.Append("\tSELECT ");
            for (int i = 0; i < this.tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var columnName = ParserName.Parse(this.tableDescription.PrimaryKey.Columns[i]).Quoted().ToString();
                stringBuilder.Append($"p.{columnName}, ");
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
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.BulkInsertRows).name;
            CreateProcedureCommand(BuildBulkInsertCommand, commandName);
        }
        public string CreateBulkInsertScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.BulkInsertRows).name;
            return CreateProcedureCommandScriptText(BuildBulkInsertCommand, commandName);
        }
        public void DropBulkInsert()
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

                    var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.BulkInsertRows).name;

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
        public string DropBulkInsertScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.BulkInsertRows).name;

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
                TypeName = this.sqlObjectNames.GetCommandName(DbCommandType.BulkTableType).name
            };
            sqlCommand.Parameters.Add(sqlParameter2);


            string str4 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "[p]", "[t]");
            string str5 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "[changes]", "[base]");
            string str6 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "[changes]", "[side]");

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("-- use a temp table to store the list of PKs that successfully got updated/inserted");
            stringBuilder.Append("declare @changed TABLE (");
            foreach (var c in this.tableDescription.PrimaryKey.Columns)
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();

                // Get the good SqlDbType (even if we are not from Sql Server def)
                var sqlDbTypeString = this.sqlDbMetadata.TryGetOwnerDbTypeString(c.OriginalDbType, c.DbType, false, false, c.MaxLength, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
                var quotedColumnType = ParserName.Parse(sqlDbTypeString).Quoted().ToString();
                quotedColumnType += this.sqlDbMetadata.TryGetOwnerDbTypePrecision(c.OriginalDbType, c.DbType, false, false, c.MaxLength, c.Precision, c.Scale, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);

                stringBuilder.Append($"{columnName} {quotedColumnType}, ");
            }
            stringBuilder.Append(" PRIMARY KEY (");
            for (int i = 0; i < this.tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var columnName = ParserName.Parse(this.tableDescription.PrimaryKey.Columns[i]).Quoted().ToString();
                stringBuilder.Append($"{columnName}");
                if (i < this.tableDescription.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
            }
            stringBuilder.AppendLine("));");
            stringBuilder.AppendLine();


            stringBuilder.AppendLine("-- update the base table");
            stringBuilder.AppendLine($"MERGE {tableName.Schema().Quoted().ToString()} AS base USING");
            stringBuilder.AppendLine("\t-- join done here against the side table to get the local timestamp for concurrency check\n");

            //stringBuilder.AppendLine("\t(SELECT p.*, t.update_scope_id, t.timestamp FROM @changeTable p ");

            stringBuilder.AppendLine($"\t(SELECT ");
            string str = "";
            stringBuilder.Append("\t");
            foreach (var c in this.tableDescription.Columns.Where(col => !col.IsReadOnly))
            {
                var columnName = ParserName.Parse(c).Quoted().ToString();
                stringBuilder.Append($"{str}[p].{columnName}");
                str = ", ";
            }
            stringBuilder.AppendLine("\t, [p].[create_timestamp], [p].[update_timestamp]");
            stringBuilder.AppendLine("\t, [t].[update_scope_id], [t].[timestamp]");

            stringBuilder.AppendLine($"\tFROM @changeTable p ");
            stringBuilder.Append($"\tLEFT JOIN {trackingName.Schema().Quoted().ToString()} t ON ");
            stringBuilder.AppendLine($" {str4}");
            stringBuilder.AppendLine($"\t) AS changes ON {str5}");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine("WHEN MATCHED AND ([changes].[update_scope_id] = @sync_scope_id OR [changes].[timestamp] <= @sync_min_timestamp) THEN");

            StringBuilder stringBuilderArguments = new StringBuilder();
            StringBuilder stringBuilderParameters = new StringBuilder();

            string empty = string.Empty;
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
            foreach (DmColumn mutableColumn in this.tableDescription.MutableColumnsAndNotAutoInc)
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                stringBuilder.AppendLine($"\t{strSeparator}{columnName} = [changes].{columnName}");
                strSeparator = ", ";
            }
            stringBuilder.Append($"\tOUTPUT ");
            for (int i = 0; i < this.tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var columnName = ParserName.Parse(this.tableDescription.PrimaryKey.Columns[i]).Quoted().ToString();
                stringBuilder.Append($"INSERTED.{columnName}");
                if (i < this.tableDescription.PrimaryKey.Columns.Length - 1)
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
            stringBuilder.AppendLine($"FROM {trackingName.Schema().Quoted().ToString()} side");
            stringBuilder.AppendLine("JOIN (");
            stringBuilder.Append("\tSELECT ");
            for (int i = 0; i < this.tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var columnName = ParserName.Parse(this.tableDescription.PrimaryKey.Columns[i]).Quoted().ToString();
                stringBuilder.Append($"p.{columnName}, ");
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
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.BulkUpdateRows).name;
            CreateProcedureCommand(BuildBulkUpdateCommand, commandName);
        }
        public string CreateBulkUpdateScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.BulkUpdateRows).name;
            return CreateProcedureCommandScriptText(BuildBulkUpdateCommand, commandName);
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

            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "[base]", "[side]"));

            stringBuilder.AppendLine("WHERE ([side].[timestamp] <= @sync_min_timestamp  OR @sync_force_write = 1)");
            stringBuilder.Append("AND ");
            stringBuilder.AppendLine(string.Concat("(", SqlManagementUtils.ColumnsAndParameters(this.tableDescription.PrimaryKey.Columns, "[base]"), ");"));
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
            stringBuilder.AppendLine(SqlManagementUtils.ColumnsAndParameters(this.tableDescription.PrimaryKey.Columns, ""));
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
        // Insert command
        //------------------------------------------------------------------
        private SqlCommand BuildInsertCommand()
        {
            SqlCommand sqlCommand = new SqlCommand();
            StringBuilder stringBuilder = new StringBuilder();
            StringBuilder stringBuilderArguments = new StringBuilder();
            StringBuilder stringBuilderParameters = new StringBuilder();

            this.AddColumnParametersToCommand(sqlCommand);
            SqlParameter sqlParameter = new SqlParameter("@sync_row_count", SqlDbType.Int)
            {
                Direction = ParameterDirection.Output
            };
            sqlCommand.Parameters.Add(sqlParameter);

            stringBuilder.AppendLine($"SET {sqlParameter.ParameterName} = 0;");

            //stringBuilder.Append(string.Concat("IF NOT EXISTS (SELECT * FROM ", trackingName.Schema().Quoted().ToString(), " WHERE "));
            //stringBuilder.Append(SqlManagementUtils.ColumnsAndParameters(this.tableDescription.PrimaryKey.Columns, string.Empty));
            //stringBuilder.AppendLine(") ");
            //stringBuilder.AppendLine("BEGIN ");

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
            stringBuilder.AppendLine($"\tVALUES ({stringBuilderParameters.ToString()});");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine(string.Concat("\tSET ", sqlParameter.ParameterName, " = @@rowcount; "));

            if (this.tableDescription.HasAutoIncrementColumns)
            {
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"\tSET IDENTITY_INSERT {tableName.Quoted().ToString()} OFF;");
            }

            //stringBuilder.AppendLine("END ");
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
        public void DropInsert()
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

                    var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertRow).name;

                    command.CommandText = $"DROP PROCEDURE {commandName};";
                    command.Connection = this.connection;
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
                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();

            }

        }
        public string DropInsertScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertRow).name;

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
            this.AddPkColumnParametersToCommand(sqlCommand);
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

            stringBuilder.AppendLine($"UPDATE {trackingName.Schema().Quoted().ToString()} SET ");
            stringBuilder.AppendLine("\t[create_scope_id] = @sync_scope_id, ");
            stringBuilder.AppendLine("\t[create_timestamp] = @create_timestamp, ");
            stringBuilder.AppendLine("\t[update_scope_id] = @sync_scope_id, ");
            stringBuilder.AppendLine("\t[update_timestamp] = @update_timestamp, ");
            stringBuilder.AppendLine("\t[sync_row_is_tombstone] = @sync_row_is_tombstone ");
            stringBuilder.AppendLine($"WHERE ({SqlManagementUtils.ColumnsAndParameters(this.tableDescription.PrimaryKey.Columns, "")})");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"SET {sqlParameter8.ParameterName} = @@rowcount; ");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"IF ({sqlParameter8.ParameterName} = 0)");
            stringBuilder.AppendLine("BEGIN ");
            stringBuilder.AppendLine($"\tINSERT INTO {trackingName.Schema().Quoted().ToString()}");

            string empty = string.Empty;
            foreach (var pkColumn in this.tableDescription.PrimaryKey.Columns)
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                var parameterName = ParserName.Parse(pkColumn).Unquoted().Normalized().ToString();

                stringBuilderArguments.Append(string.Concat(empty, columnName));
                stringBuilderParameters.Append(string.Concat(empty, $"@{parameterName}"));
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
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertMetadata).name;
            CreateProcedureCommand(BuildInsertMetadataCommand, commandName);
        }
        public string CreateInsertMetadataScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertMetadata).name;
            return CreateProcedureCommandScriptText(BuildInsertMetadataCommand, commandName);
        }
        public void DropInsertMetadata()
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

                    var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertMetadata).name;

                    command.CommandText = $"DROP PROCEDURE {commandName};";
                    command.Connection = this.connection;
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
                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();

            }

        }
        public string DropInsertMetadataScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertMetadata).name;

            return $"DROP PROCEDURE {commandName};";
        }


        //------------------------------------------------------------------
        // Select Row command
        //------------------------------------------------------------------
        private SqlCommand BuildSelectRowCommand()
        {
            SqlCommand sqlCommand = new SqlCommand();
            this.AddPkColumnParametersToCommand(sqlCommand);
            SqlParameter sqlParameter = new SqlParameter("@sync_scope_id", SqlDbType.UniqueIdentifier);
            sqlCommand.Parameters.Add(sqlParameter);

            StringBuilder stringBuilder = new StringBuilder("SELECT ");
            stringBuilder.AppendLine();
            StringBuilder stringBuilder1 = new StringBuilder();
            string empty = string.Empty;
            foreach (var pkColumn in this.tableDescription.PrimaryKey.Columns)
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                var parameterName = ParserName.Parse(pkColumn).Unquoted().Normalized().ToString();

                stringBuilder.AppendLine($"\t[side].{columnName}, ");
                stringBuilder1.Append($"{empty}[side].{columnName} = @{parameterName}");
                empty = " AND ";
            }
            foreach (DmColumn mutableColumn in this.tableDescription.MutableColumns)
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                stringBuilder.AppendLine($"\t[base].{columnName}, ");
            }
            stringBuilder.AppendLine("\t[side].[sync_row_is_tombstone],");
            stringBuilder.AppendLine("\t[side].[create_scope_id],");
            stringBuilder.AppendLine("\t[side].[create_timestamp],");
            stringBuilder.AppendLine("\t[side].[update_scope_id],");
            stringBuilder.AppendLine("\t[side].[update_timestamp]");

            stringBuilder.AppendLine($"FROM {tableName.Schema().Quoted().ToString()} [base]");
            stringBuilder.AppendLine($"RIGHT JOIN {trackingName.Schema().Quoted().ToString()} [side] ON");

            string str = string.Empty;
            foreach (var pkColumn in this.tableDescription.PrimaryKey.Columns)
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
                var isPrimaryKey = this.tableDescription.PrimaryKey.Columns.Any(cc => this.tableDescription.IsEqual(cc.ColumnName, c.ColumnName));
                var columnName = ParserName.Parse(c).Quoted().ToString();
                var nullString = isPrimaryKey ? "NOT NULL" : "NULL";

                // Get the good SqlDbType (even if we are not from Sql Server def)
                var sqlDbTypeString = this.sqlDbMetadata.TryGetOwnerDbTypeString(c.OriginalDbType, c.DbType, false, false, c.MaxLength, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
                var quotedColumnType = ParserName.Parse(sqlDbTypeString).Quoted().ToString();
                quotedColumnType += this.sqlDbMetadata.TryGetOwnerDbTypePrecision(c.OriginalDbType, c.DbType, false, false, c.MaxLength, c.Precision, c.Scale, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);

                stringBuilder.AppendLine($"{str}{columnName} {quotedColumnType} {nullString}");
                str = ", ";
            }
            stringBuilder.AppendLine(", [create_scope_id] [uniqueidentifier] NULL");
            stringBuilder.AppendLine(", [create_timestamp] [bigint] NULL");
            stringBuilder.AppendLine(", [update_scope_id] [uniqueidentifier] NULL");
            stringBuilder.AppendLine(", [update_timestamp] [bigint] NULL");
            stringBuilder.Append(string.Concat(str, "PRIMARY KEY ("));
            str = "";
            foreach (var c in this.tableDescription.PrimaryKey.Columns)
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
        private SqlCommand BuildUpdateCommand()
        {
            SqlCommand sqlCommand = new SqlCommand();

            StringBuilder stringBuilder = new StringBuilder();
            this.AddColumnParametersToCommand(sqlCommand);

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


            stringBuilder.AppendLine($"UPDATE {tableName.Schema().Quoted().ToString()}");
            stringBuilder.Append($"SET {SqlManagementUtils.CommaSeparatedUpdateFromParameters(this.tableDescription)}");
            stringBuilder.AppendLine($"FROM {tableName.Schema().Quoted().ToString()} [base]");
            stringBuilder.AppendLine($"JOIN {trackingName.Schema().Quoted().ToString()} [side]");
            stringBuilder.Append($"ON ");
            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "[base]", "[side]"));
            stringBuilder.AppendLine("WHERE ([side].[timestamp] <= @sync_min_timestamp OR @sync_force_write = 1)");
            stringBuilder.Append("AND (");
            stringBuilder.Append(SqlManagementUtils.ColumnsAndParameters(this.tableDescription.PrimaryKey.Columns, "[base]"));
            stringBuilder.AppendLine(");");
            stringBuilder.AppendLine();


            stringBuilder.Append(string.Concat("SET ", sqlParameter2.ParameterName, " = @@ROWCOUNT;"));
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
        private SqlCommand BuildUpdateMetadataCommand()
        {
            SqlCommand sqlCommand = new SqlCommand();
            StringBuilder stringBuilder = new StringBuilder();
            this.AddPkColumnParametersToCommand(sqlCommand);
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

            string str1 = SqlManagementUtils.ColumnsAndParameters(this.tableDescription.PrimaryKey.Columns, "");

            stringBuilder.AppendLine($"SET {sqlParameter8.ParameterName} = 0;");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine("DECLARE @was_tombstone int; ");

            stringBuilder.AppendLine($"SELECT @was_tombstone = [sync_row_is_tombstone]");
            stringBuilder.AppendLine($"FROM {trackingName.Schema().Quoted().ToString()}");
            stringBuilder.AppendLine($"WHERE ({str1})");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("IF (@was_tombstone IS NOT NULL AND @was_tombstone = 1 AND @sync_row_is_tombstone = 0)");
            stringBuilder.AppendLine("BEGIN ");
            stringBuilder.AppendLine($"UPDATE {trackingName.Schema().Quoted().ToString()} SET ");
            stringBuilder.AppendLine("\t [create_scope_id] = @sync_scope_id, ");
            stringBuilder.AppendLine("\t [update_scope_id] = @sync_scope_id, ");
            stringBuilder.AppendLine("\t [create_timestamp] = @create_timestamp, ");
            stringBuilder.AppendLine("\t [update_timestamp] = @update_timestamp, ");
            stringBuilder.AppendLine("\t [sync_row_is_tombstone] = @sync_row_is_tombstone ");
            stringBuilder.AppendLine($"WHERE ({str1})");
            stringBuilder.AppendLine("END");
            stringBuilder.AppendLine("ELSE");
            stringBuilder.AppendLine("BEGIN ");
            stringBuilder.AppendLine($"UPDATE {trackingName.Schema().Quoted().ToString()} SET ");
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
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateMetadata).name;
            CreateProcedureCommand(BuildUpdateMetadataCommand, commandName);
        }
        public string CreateUpdateMetadataScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateMetadata).name;
            return CreateProcedureCommandScriptText(BuildUpdateMetadataCommand, commandName);
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
            SqlCommand sqlCommand = new SqlCommand();
            SqlParameter pTimestamp = new SqlParameter("@sync_min_timestamp", SqlDbType.BigInt);
            SqlParameter pScopeId = new SqlParameter("@sync_scope_id", SqlDbType.UniqueIdentifier);
            SqlParameter pScopeNew = new SqlParameter("@sync_scope_is_new", SqlDbType.Bit);
            SqlParameter pReinit = new SqlParameter("@sync_scope_is_reinit", SqlDbType.Bit);
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

                        SqlDbType sqlDbType = (SqlDbType)this.sqlDbMetadata.TryGetOwnerDbType(columnFilter.OriginalDbType, columnFilter.DbType, false, false, columnFilter.MaxLength, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
                        SqlParameter sqlParamFilter = new SqlParameter($"@{columnName}", sqlDbType);
                        sqlCommand.Parameters.Add(sqlParamFilter);
                    }
                    else
                    {
                        SqlDbType sqlDbType = (SqlDbType)this.sqlDbMetadata.TryGetOwnerDbType(null, c.ColumnType.Value, false, false, 0, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
                        var columnFilterName = ParserName.Parse(c.ColumnName).Unquoted().Normalized().ToString();
                        SqlParameter sqlParamFilter = new SqlParameter($"@{columnFilterName}", sqlDbType);
                        sqlCommand.Parameters.Add(sqlParamFilter);
                    }
                }
            }

            StringBuilder stringBuilder = new StringBuilder("SELECT ");
            foreach (var pkColumn in this.tableDescription.PrimaryKey.Columns)
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                stringBuilder.AppendLine($"\t[side].{columnName}, ");
            }
            foreach (var mutableColumn in this.tableDescription.MutableColumns)
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();
                stringBuilder.AppendLine($"\t[base].{columnName}, ");
            }
            stringBuilder.AppendLine($"\t[side].[sync_row_is_tombstone], ");
            stringBuilder.AppendLine($"\t[side].[create_scope_id], ");
            stringBuilder.AppendLine($"\t[side].[create_timestamp], ");
            stringBuilder.AppendLine($"\t[side].[update_scope_id], ");
            stringBuilder.AppendLine($"\t[side].[update_timestamp] ");
            stringBuilder.AppendLine($"FROM {tableName.Schema().Quoted().ToString()} [base]");
            stringBuilder.AppendLine($"RIGHT JOIN {trackingName.Schema().Quoted().ToString()} [side]");
            stringBuilder.Append($"ON ");

            string empty = "";
            foreach (var pkColumn in this.tableDescription.PrimaryKey.Columns)
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                stringBuilder.Append($"{empty}[base].{columnName} = [side].{columnName}");
                empty = " AND ";
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("WHERE (");
            string str = string.Empty;

            var columnFilters = this.Filters.GetColumnFilters();
            if (withFilter && columnFilters.Count != 0)
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

            empty = " AND ";
            foreach (var pkColumn in this.tableDescription.PrimaryKey.Columns)
            {
                var pkColumnName = ParserName.Parse(pkColumn).Quoted().ToString();
                stringBuilder.Append($"{empty}[base].{pkColumnName} is not null");
            }
            stringBuilder.AppendLine("\t)");
            stringBuilder.AppendLine(")");

            sqlCommand.CommandText = stringBuilder.ToString();

            //if (this._filterParameters != null)
            //{
            //    foreach (SqlParameter _filterParameter in this._filterParameters)
            //    {
            //        sqlCommand.Parameters.Add(((ICloneable)_filterParameter).Clone());
            //    }
            //}
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
