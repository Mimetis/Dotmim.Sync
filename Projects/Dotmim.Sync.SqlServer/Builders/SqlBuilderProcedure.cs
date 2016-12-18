using Dotmim.Sync.Core.Builders;
using System;
using System.Collections.Generic;
using System.Text;
using Dotmim.Sync.Data;
using System.Data.Common;
using Dotmim.Sync.Core.Common;
using System.Data.SqlClient;
using System.Data;
using System.Globalization;

namespace Dotmim.Sync.SqlServer.Builders
{
    public class SqlBuilderProcedure : IDbBuilderProcedureHelper
    {
        private DmTable table;
        private ObjectNameParser originalTableName;
        private ObjectNameParser trackingTableName;

        private string _selectChangesProcName;
        private string _selectRowProcName;
        private string _insertProcName;
        private string _updateProcName;
        private string _deleteProcName;
        private string _insertMetadataProcName;
        private string _updateMetadataProcName;
        private string _deleteMetadataProcName;
        private string _bulkTableTypeName;
        private string _bulkInsertProcName;
        private string _bulkUpdateProcName;
        private string _bulkDeleteProcName;

        public SqlBuilderProcedure(DmTable tableDescription)
        {
            this.table = tableDescription;
            string tableAndPrefixName = String.IsNullOrWhiteSpace(this.table.Prefix) ? this.table.TableName : $"{this.table.Prefix}.{this.table.TableName}";
            this.originalTableName = new ObjectNameParser(tableAndPrefixName, "[", "]");
            this.trackingTableName = new ObjectNameParser($"{tableAndPrefixName}_tracking", "[", "]");

            this.FilterColumns = new List<DmColumn>();
            this.FilterParameters = new List<DmColumn>();
            this.SetDefaultProcNames();

        }
        public List<DmColumn> FilterColumns { get; set; }
        public List<DmColumn> FilterParameters { get; set; }

        private void AddPkColumnParametersToCommand(SqlCommand sqlCommand)
        {
            foreach (DmColumn pkColumn in this.table.PrimaryKey.Columns)
                sqlCommand.Parameters.Add(pkColumn.GetSqlParameter());
        }

        private void AddColumnParametersToCommand(SqlCommand sqlCommand)
        {
            foreach (DmColumn pkColumn in this.table.Columns)
                sqlCommand.Parameters.Add(pkColumn.GetSqlParameter());
        }

        private (SqlConnection, SqlTransaction) GetTypedConnection(DbTransaction transaction)
        {
            SqlTransaction sqlTransaction = transaction as SqlTransaction;

            if (sqlTransaction == null)
                throw new Exception("Transaction is not a SqlTransaction. Wrong provider");

            SqlConnection sqlConnection = sqlTransaction.Connection;

            return (sqlConnection, sqlTransaction);

        }

        /// <summary>
        /// From a SqlParameter, create the declaration
        /// </summary>
        internal static string CreateParameterDeclaration(SqlParameter param)
        {
            StringBuilder stringBuilder3 = new StringBuilder();
            SqlDbType sqlDbType = param.SqlDbType;

            string empty = string.Empty;
            switch (sqlDbType)
            {
                case SqlDbType.Binary:
                case SqlDbType.Char:
                case SqlDbType.NChar:
                case SqlDbType.NVarChar:
                case SqlDbType.VarBinary:
                case SqlDbType.VarChar:
                    {
                        if (param.Size != -1)
                            empty = string.Concat("(", param.Size, ")");
                        else
                            empty = "(max)";
                        break;
                    }
                case SqlDbType.Decimal:
                    {
                        empty = string.Concat("(", param.Precision, ",", param.Scale, ")");
                        break;
                    }

            }

            if (param.SqlDbType != SqlDbType.Structured)
            {
                stringBuilder3.Append(string.Concat(param.ParameterName, " ", param.SqlDbType.ToString(), empty));

                if (param.Direction == ParameterDirection.Output || param.Direction == ParameterDirection.InputOutput)
                    stringBuilder3.Append(" OUTPUT");
            }
            else
            {
                stringBuilder3.Append(string.Concat(param.ParameterName, " ", param.TypeName, " READONLY"));
            }

            return stringBuilder3.ToString();

        }

        /// <summary>
        /// From a SqlCommand, create a stored procedure string
        /// </summary>
        private static string CreateProcedureCommandText(SqlCommand cmd, string procName)
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
        private void CreateProcedureCommand(DbTransaction transaction, Func<SqlCommand> BuildCommand, string procName, DbBuilderOption builderOption)
        {
            (var connection, var trans) = GetTypedConnection(transaction);

            if (NeedToCreateProc(connection, trans, procName, builderOption))
            {
                var str = CreateProcedureCommandText(BuildCommand(), procName);
                using (var command = new SqlCommand(str, connection, trans))
                {
                    command.ExecuteNonQuery();
                }
            }
        }

        private string CreateProcedureCommandScriptText(DbTransaction transaction, Func<SqlCommand> BuildCommand, string procName, DbBuilderOption builderOption)
        {
            (var connection, var trans) = GetTypedConnection(transaction);

            if (!NeedToCreateProc(connection, trans, procName, builderOption))
                return string.Empty;

            var str1 = $"Command {procName} for table {originalTableName.QuotedString}";
            var str = CreateProcedureCommandText(BuildCommand(), procName);
            return SqlBuilder.WrapScriptTextWithComments(str, str1);
        }

        /// <summary>
        /// Check if we need to create the stored procedure
        /// </summary>
        private static bool NeedToCreateProc(SqlConnection connection, SqlTransaction transaction, string objectName, DbBuilderOption option)
        {
            switch (option)
            {
                case DbBuilderOption.Create:
                    return true;
                case DbBuilderOption.Skip:
                    return false;
                case DbBuilderOption.CreateOrUseExisting:
                    return !SqlManagementUtils.ProcedureExists(connection, transaction, objectName);
            }
            return false;
        }

        /// <summary>
        /// Check if we need to create the TVP Type
        /// </summary>
        private static bool NeedToCreateType(SqlConnection connection, SqlTransaction transaction, string objectName, DbBuilderOption option)
        {
            switch (option)
            {
                case DbBuilderOption.Create:
                    return true;
                case DbBuilderOption.Skip:
                    return false;
                case DbBuilderOption.CreateOrUseExisting:
                    return !SqlManagementUtils.TypeExists(connection, transaction, objectName);
            }
            return false;
        }


        private string BulkSelectUnsuccessfulRows()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("--Select all ids not deleted for conflict");
            stringBuilder.Append("SELECT ");
            for (int i = 0; i < this.table.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(this.table.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"{cc.QuotedString}");
                if (i < this.table.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
                else
                    stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine($"FROM @changeTable t");
            stringBuilder.AppendLine("WHERE NOT EXISTS (");
            stringBuilder.Append("\t SELECT ");
            for (int i = 0; i < this.table.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(this.table.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"{cc.QuotedString}");
                if (i < this.table.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
                else
                    stringBuilder.AppendLine();
            }
            stringBuilder.AppendLine("\t FROM @changed i");
            stringBuilder.Append("\t WHERE ");
            for (int i = 0; i < this.table.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(this.table.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"t.{cc.QuotedString} = i.{cc.QuotedString}");
                if (i < this.table.PrimaryKey.Columns.Length - 1)
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
            SqlParameter sqlParameter1 = new SqlParameter("@sync_scope_name", SqlDbType.NVarChar, 100);
            sqlCommand.Parameters.Add(sqlParameter1);
            SqlParameter sqlParameter2 = new SqlParameter("@changeTable", SqlDbType.Structured);
            sqlParameter2.TypeName = this._bulkTableTypeName;
            sqlCommand.Parameters.Add(sqlParameter2);


            string str4 = SqlManagementUtils.JoinTwoTablesOnClause(this.table.PrimaryKey.Columns, "p", "t");
            string str5 = SqlManagementUtils.JoinTwoTablesOnClause(this.table.PrimaryKey.Columns, "changes", "base");
            string str6 = SqlManagementUtils.JoinTwoTablesOnClause(this.table.PrimaryKey.Columns, "changes", "side");
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("-- use a temp table to store the list of PKs that successfully got updated/inserted");
            stringBuilder.Append("declare @changed TABLE (");
            foreach (var c in this.table.PrimaryKey.Columns)
            {
                var cc = new ObjectNameParser(c.ColumnName);
                var quotedColumnType = new ObjectNameParser(c.GetSqlDbTypeString(), "[", "]").QuotedString;
                quotedColumnType += c.GetSqlTypePrecisionString();

                stringBuilder.Append($"{cc.QuotedString} {quotedColumnType}, ");
            }
            stringBuilder.Append(" PRIMARY KEY (");
            for (int i = 0; i < this.table.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(this.table.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"{cc.QuotedString}");
                if (i < this.table.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
            }
            stringBuilder.AppendLine("));");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"-- delete all items where timestamp <= @sync_min_timestamp or update_scope_name = @sync_scope_name");

            stringBuilder.AppendLine($"DELETE {this.originalTableName.QuotedString}");
            stringBuilder.Append($"OUTPUT ");
            for (int i = 0; i < this.table.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(this.table.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"DELETED.{cc.QuotedString}");
                if (i < this.table.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
                else
                    stringBuilder.AppendLine();
            }
            stringBuilder.AppendLine($"INTO @changed ");
            stringBuilder.AppendLine($"FROM {this.originalTableName.QuotedString} [base]");
            stringBuilder.AppendLine("JOIN (");
            stringBuilder.AppendLine($"\tSELECT p.*, t.update_scope_name, t.timestamp");
            stringBuilder.AppendLine($"\tFROM @changeTable p ");
            stringBuilder.AppendLine($"\tJOIN {this.trackingTableName.QuotedString} t ON ");
            stringBuilder.AppendLine($"\t{str4}");
            stringBuilder.AppendLine("\t)");
            stringBuilder.AppendLine("\tAS changes ON ");
            stringBuilder.AppendLine(str5);
            stringBuilder.AppendLine("WHERE");
            stringBuilder.AppendLine("-- Last chanegs was from the current scope, so we can delete it since we are sure no one else edit it ");
            stringBuilder.AppendLine("changes.update_scope_name = @sync_scope_name");
            stringBuilder.AppendLine("-- no change since the last time the current scope has sync (so no one has update the row)");
            stringBuilder.AppendLine("OR [changes].[timestamp] <= @sync_min_timestamp;");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("-- Update the tacking table");
            stringBuilder.AppendLine("UPDATE [side] SET");
            stringBuilder.AppendLine("\tsync_row_is_tombstone = 1, ");
            stringBuilder.AppendLine("\tupdate_scope_name = @sync_scope_name,");
            stringBuilder.AppendLine("\tupdate_timestamp = [changes].update_peer_timestamp");
            stringBuilder.AppendLine($"FROM {this.trackingTableName.QuotedString} side");
            stringBuilder.AppendLine("JOIN (");
            stringBuilder.Append("\tSELECT ");
            for (int i = 0; i < this.table.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(this.table.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"p.{cc.QuotedString}, ");
            }
            stringBuilder.AppendLine(" p.update_peer_timestamp, p.create_peer_timestamp ");
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
        public void CreateBulkDelete(DbTransaction transaction, DbBuilderOption builderOption)
        {
            CreateProcedureCommand(transaction, this.BuildBulkDeleteCommand, this._bulkDeleteProcName, builderOption);
        }
        public string CreateBulkDeleteScriptText(DbTransaction transaction, DbBuilderOption builderOption)
        {
            return CreateProcedureCommandScriptText(transaction, this.BuildBulkDeleteCommand, this._bulkDeleteProcName, builderOption);
        }

        //------------------------------------------------------------------
        // Bulk Insert command
        //------------------------------------------------------------------
        private SqlCommand BuildBulkInsertCommand()
        {
            SqlCommand sqlCommand = new SqlCommand();

            SqlParameter sqlParameter = new SqlParameter("@sync_min_timestamp", SqlDbType.BigInt);
            sqlCommand.Parameters.Add(sqlParameter);
            SqlParameter sqlParameter1 = new SqlParameter("@sync_scope_name", SqlDbType.NVarChar, 100);
            sqlCommand.Parameters.Add(sqlParameter1);
            SqlParameter sqlParameter2 = new SqlParameter("@changeTable", SqlDbType.Structured);
            sqlParameter2.TypeName = this._bulkTableTypeName;
            sqlCommand.Parameters.Add(sqlParameter2);


            string str4 = SqlManagementUtils.JoinTwoTablesOnClause(this.table.PrimaryKey.Columns, "[p]", "[t]");
            string str5 = SqlManagementUtils.JoinTwoTablesOnClause(this.table.PrimaryKey.Columns, "[changes]", "[base]");
            string str6 = SqlManagementUtils.JoinTwoTablesOnClause(this.table.PrimaryKey.Columns, "[changes]", "[side]");

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("-- use a temp table to store the list of PKs that successfully got updated/inserted");
            stringBuilder.Append("declare @changed TABLE (");
            foreach (var c in this.table.PrimaryKey.Columns)
            {
                var cc = new ObjectNameParser(c.ColumnName);
                var quotedColumnType = new ObjectNameParser(c.GetSqlDbTypeString(), "[", "]").QuotedString;
                quotedColumnType += c.GetSqlTypePrecisionString();

                stringBuilder.Append($"{cc.QuotedString} {quotedColumnType}, ");
            }
            stringBuilder.Append(" PRIMARY KEY (");
            for (int i = 0; i < this.table.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(this.table.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"{cc.QuotedString}");
                if (i < this.table.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
            }
            stringBuilder.AppendLine("));");
            stringBuilder.AppendLine();

            // Check if we have auto inc column
            bool flag = false;
            foreach (var mutableColumn in this.table.Columns)
            {
                if (!mutableColumn.AutoIncrement)
                    continue;
                flag = true;
            }

            if (flag)
            {
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"SET IDENTITY_INSERT {this.originalTableName.QuotedString} ON;");
                stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine("-- update/insert into the base table");
            stringBuilder.AppendLine($"MERGE {this.originalTableName.QuotedString} AS base USING");
            stringBuilder.AppendLine("\t-- join done here against the side table to get the local timestamp for concurrency check\n");
            stringBuilder.AppendLine("\t(SELECT p.*, t.timestamp FROM @changeTable p ");
            stringBuilder.Append($"\tLEFT JOIN {this.trackingTableName.QuotedString} t ON ");
            stringBuilder.AppendLine($"\t{str4}");
            stringBuilder.AppendLine($"\t) AS changes ON {str5}");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("-- Si la ligne n'existe pas en local et qu'elle a été créé avant le timestamp de référence");
            stringBuilder.Append("WHEN NOT MATCHED BY TARGET AND changes.[timestamp] <= @sync_min_timestamp OR changes.[timestamp] IS NULL THEN");

            StringBuilder stringBuilderArguments = new StringBuilder();
            StringBuilder stringBuilderParameters = new StringBuilder();

            string empty = string.Empty;
            foreach (var mutableColumn in this.table.Columns)
            {
                ObjectNameParser columnName = new ObjectNameParser(mutableColumn.ColumnName);
                stringBuilderArguments.Append(string.Concat(empty, columnName.QuotedString));
                stringBuilderParameters.Append(string.Concat(empty, $"changes.{columnName.UnquotedString}"));
                empty = ", ";
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"\tINSERT");
            stringBuilder.AppendLine($"\t({stringBuilderArguments.ToString()})");
            stringBuilder.AppendLine($"\tVALUES ({stringBuilderParameters.ToString()})");
            stringBuilder.Append($"\tOUTPUT ");
            for (int i = 0; i < this.table.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(this.table.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"INSERTED.{cc.QuotedString}");
                if (i < this.table.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
                else
                    stringBuilder.AppendLine();
            }
            stringBuilder.AppendLine($"\tINTO @changed; -- populates the temp table with successful PKs");
            stringBuilder.AppendLine();

            if (flag)
            {
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"SET IDENTITY_INSERT {this.originalTableName.QuotedString} OFF;");
                stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine("UPDATE side SET");
            stringBuilder.AppendLine("\tupdate_scope_name = @sync_scope_name,");
            stringBuilder.AppendLine("\tcreate_scope_name = @sync_scope_name,");
            stringBuilder.AppendLine("\tupdate_timestamp = changes.update_peer_timestamp,");
            stringBuilder.AppendLine("\tcreate_timestamp = changes.create_peer_timestamp");
            stringBuilder.AppendLine($"FROM {this.trackingTableName.QuotedString} side");
            stringBuilder.AppendLine("JOIN (");
            stringBuilder.Append("\tSELECT ");
            for (int i = 0; i < this.table.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(this.table.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"p.{cc.QuotedString}, ");
            }

            stringBuilder.AppendLine(" p.update_peer_timestamp, p.create_peer_timestamp ");
            stringBuilder.AppendLine("\tFROM @changed t");
            stringBuilder.AppendLine("\tJOIN @changeTable p ON ");
            stringBuilder.Append(str4);
            stringBuilder.AppendLine(") AS [changes] ON ");
            stringBuilder.AppendLine(str6);
            stringBuilder.Append(BulkSelectUnsuccessfulRows());


            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
        public void CreateBulkInsert(DbTransaction transaction, DbBuilderOption builderOption)
        {
            CreateProcedureCommand(transaction, this.BuildBulkInsertCommand, this._bulkInsertProcName, builderOption);
        }
        public string CreateBulkInsertScriptText(DbTransaction transaction, DbBuilderOption builderOption)
        {
            return CreateProcedureCommandScriptText(transaction, this.BuildBulkInsertCommand, this._bulkInsertProcName, builderOption);
        }

        //------------------------------------------------------------------
        // Bulk Update command
        //------------------------------------------------------------------
        private SqlCommand BuildBulkUpdateCommand()
        {
            SqlCommand sqlCommand = new SqlCommand();

            SqlParameter sqlParameter = new SqlParameter("@sync_min_timestamp", SqlDbType.BigInt);
            sqlCommand.Parameters.Add(sqlParameter);
            SqlParameter sqlParameter1 = new SqlParameter("@sync_scope_name", SqlDbType.NVarChar, 100);
            sqlCommand.Parameters.Add(sqlParameter1);
            SqlParameter sqlParameter2 = new SqlParameter("@changeTable", SqlDbType.Structured);
            sqlParameter2.TypeName = this._bulkTableTypeName;
            sqlCommand.Parameters.Add(sqlParameter2);


            string str4 = SqlManagementUtils.JoinTwoTablesOnClause(this.table.PrimaryKey.Columns, "[p]", "[t]");
            string str5 = SqlManagementUtils.JoinTwoTablesOnClause(this.table.PrimaryKey.Columns, "[changes]", "[base]");
            string str6 = SqlManagementUtils.JoinTwoTablesOnClause(this.table.PrimaryKey.Columns, "[changes]", "[side]");

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("-- use a temp table to store the list of PKs that successfully got updated/inserted");
            stringBuilder.Append("declare @changed TABLE (");
            foreach (var c in this.table.PrimaryKey.Columns)
            {
                var cc = new ObjectNameParser(c.ColumnName);
                var quotedColumnType = new ObjectNameParser(c.GetSqlDbTypeString(), "[", "]").QuotedString;
                quotedColumnType += c.GetSqlTypePrecisionString();

                stringBuilder.Append($"{cc.QuotedString} {quotedColumnType}, ");
            }
            stringBuilder.Append(" PRIMARY KEY (");
            for (int i = 0; i < this.table.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(this.table.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"{cc.QuotedString}");
                if (i < this.table.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
            }
            stringBuilder.AppendLine("));");
            stringBuilder.AppendLine();

            // Check if we have auto inc column
            bool flag = false;
            foreach (var mutableColumn in this.table.Columns)
            {
                if (!mutableColumn.AutoIncrement)
                    continue;
                flag = true;
            }

            if (flag)
            {
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"SET IDENTITY_INSERT {this.originalTableName.QuotedString} ON;");
                stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine("-- update the base table");
            stringBuilder.AppendLine($"MERGE {this.originalTableName.QuotedString} AS base USING");
            stringBuilder.AppendLine("\t-- join done here against the side table to get the local timestamp for concurrency check\n");
            stringBuilder.AppendLine("\t(SELECT p.*, t.update_scope_name, t.timestamp FROM @changeTable p ");
            stringBuilder.AppendLine($"\tLEFT JOIN {this.trackingTableName.QuotedString} t ON ");
            stringBuilder.AppendLine($"\t{str4}");
            stringBuilder.AppendLine($"\t) AS changes ON {str5}");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("WHEN MATCHED AND [changes].[update_scope_name] = @sync_scope_name OR [changes].[timestamp] <= @sync_min_timestamp THEN");

            StringBuilder stringBuilderArguments = new StringBuilder();
            StringBuilder stringBuilderParameters = new StringBuilder();

            string empty = string.Empty;
            foreach (var mutableColumn in this.table.Columns)
            {
                ObjectNameParser columnName = new ObjectNameParser(mutableColumn.ColumnName);
                stringBuilderArguments.Append(string.Concat(empty, columnName.QuotedString));
                stringBuilderParameters.Append(string.Concat(empty, $"changes.{columnName.UnquotedString}"));
                empty = ", ";
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"\tUPDATE SET");

            string strSeparator = "";
            foreach (DmColumn column in table.NonPkColumns)
            {
                ObjectNameParser quotedColumn = new ObjectNameParser(column.ColumnName);
                stringBuilder.AppendLine($"\t{strSeparator}{quotedColumn.QuotedString} = [changes].{quotedColumn.UnquotedString}");
                strSeparator = ", ";
            }
            stringBuilder.Append($"\tOUTPUT ");
            for (int i = 0; i < this.table.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(this.table.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"INSERTED.{cc.QuotedString}");
                if (i < this.table.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
                else
                    stringBuilder.AppendLine();
            }
            stringBuilder.AppendLine($"\tINTO @changed; -- populates the temp table with successful PKs");
            stringBuilder.AppendLine();

            if (flag)
            {
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"SET IDENTITY_INSERT {this.originalTableName.QuotedString} OFF;");
                stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine("UPDATE side SET");
            stringBuilder.AppendLine("\tupdate_scope_name = @sync_scope_name,");
            stringBuilder.AppendLine("\tupdate_timestamp = changes.update_peer_timestamp");
            stringBuilder.AppendLine($"FROM {this.trackingTableName.QuotedString} side");
            stringBuilder.AppendLine("JOIN (");
            stringBuilder.Append("\tSELECT ");
            for (int i = 0; i < this.table.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(this.table.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"p.{cc.QuotedString}, ");
            }

            stringBuilder.AppendLine(" p.update_peer_timestamp, p.create_peer_timestamp ");
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
        public void CreateBulkUpdate(DbTransaction transaction, DbBuilderOption builderOption)
        {
            CreateProcedureCommand(transaction, this.BuildBulkUpdateCommand, this._bulkUpdateProcName, builderOption);
        }
        public string CreateBulkUpdateScriptText(DbTransaction transaction, DbBuilderOption builderOption)
        {
            return CreateProcedureCommandScriptText(transaction, this.BuildBulkUpdateCommand, this._bulkUpdateProcName, builderOption);
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
            SqlParameter sqlParameter2 = new SqlParameter("@sync_row_count", SqlDbType.Int);
            sqlParameter2.Direction = ParameterDirection.Output;
            sqlCommand.Parameters.Add(sqlParameter2);

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"SET {sqlParameter2.ParameterName} = 0;");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"DELETE {this.originalTableName.QuotedString}");
            stringBuilder.AppendLine($"FROM {this.originalTableName.QuotedString} [base]");
            stringBuilder.AppendLine($"JOIN {this.trackingTableName.QuotedString} [side] ON ");

            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.table.PrimaryKey.Columns, "[base]", "[side]"));

            stringBuilder.AppendLine("WHERE ([side].[timestamp] <= @sync_min_timestamp  OR @sync_force_write = 1)");
            stringBuilder.Append("AND ");
            stringBuilder.AppendLine(string.Concat("(", SqlManagementUtils.WhereColumnAndParameters(this.table.PrimaryKey.Columns, "[base]"), ");"));
            stringBuilder.AppendLine();
            stringBuilder.AppendLine(string.Concat("SET ", sqlParameter2.ParameterName, " = @@ROWCOUNT;"));
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
        public void CreateDelete(DbTransaction transaction, DbBuilderOption builderOption)
        {
            CreateProcedureCommand(transaction, this.BuildDeleteCommand, this._deleteProcName, builderOption);
        }
        public string CreateDeleteScriptText(DbTransaction transaction, DbBuilderOption builderOption)
        {
            return CreateProcedureCommandScriptText(transaction, this.BuildDeleteCommand, this._deleteProcName, builderOption);
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
            SqlParameter sqlParameter2 = new SqlParameter("@sync_row_count", SqlDbType.Int);
            sqlParameter2.Direction = ParameterDirection.Output;
            sqlCommand.Parameters.Add(sqlParameter2);
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"SET {sqlParameter2.ParameterName} = 0;");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"DELETE [side] FROM {this.trackingTableName.QuotedString} [side]");
            stringBuilder.Append($"WHERE ");
            stringBuilder.AppendLine(SqlManagementUtils.WhereColumnAndParameters(this.table.PrimaryKey.Columns, ""));
            stringBuilder.AppendLine();
            stringBuilder.AppendLine(string.Concat("SET ", sqlParameter2.ParameterName, " = @@ROWCOUNT;"));
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
        public void CreateDeleteMetadata(DbTransaction transaction, DbBuilderOption builderOption)
        {
            CreateProcedureCommand(transaction, this.BuildDeleteMetadataCommand, this._deleteMetadataProcName, builderOption);
        }
        public string CreateDeleteMetadataScriptText(DbTransaction transaction, DbBuilderOption builderOption)
        {
            return CreateProcedureCommandScriptText(transaction, this.BuildDeleteMetadataCommand, this._deleteMetadataProcName, builderOption);
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
            SqlParameter sqlParameter = new SqlParameter("@sync_row_count", SqlDbType.Int);
            sqlParameter.Direction = ParameterDirection.Output;
            sqlCommand.Parameters.Add(sqlParameter);

            //Check auto increment column
            bool hasAutoIncColumn = false;
            foreach (var column in this.table.Columns)
            {
                if (!column.AutoIncrement)
                    continue;
                hasAutoIncColumn = true;
                break;
            }

            stringBuilder.AppendLine($"SET {sqlParameter.ParameterName} = 0;");

            stringBuilder.Append(string.Concat("IF NOT EXISTS (SELECT * FROM ", this.trackingTableName.QuotedString, " WHERE "));
            stringBuilder.Append(SqlManagementUtils.WhereColumnAndParameters(this.table.PrimaryKey.Columns, string.Empty));
            stringBuilder.AppendLine(") ");
            stringBuilder.AppendLine("BEGIN ");
            if (hasAutoIncColumn)
            {
                stringBuilder.AppendLine($"\tSET IDENTITY_INSERT {this.originalTableName.QuotedString} ON;");
                stringBuilder.AppendLine();
            }

            string empty = string.Empty;
            foreach (var mutableColumn in this.table.Columns)
            {
                ObjectNameParser columnName = new ObjectNameParser(mutableColumn.ColumnName);
                stringBuilderArguments.Append(string.Concat(empty, columnName.QuotedString));
                stringBuilderParameters.Append(string.Concat(empty, $"@{columnName.UnquotedString}"));
                empty = ", ";
            }
            stringBuilder.AppendLine($"\tINSERT INTO {this.originalTableName.QuotedString}");
            stringBuilder.AppendLine($"\t({stringBuilderArguments.ToString()})");
            stringBuilder.AppendLine($"\tVALUES ({stringBuilderParameters.ToString()});");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine(string.Concat("\tSET ", sqlParameter.ParameterName, " = @@rowcount; "));

            if (hasAutoIncColumn)
            {
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"\tSET IDENTITY_INSERT {this.originalTableName.QuotedString} OFF;");
            }

            stringBuilder.AppendLine("END ");
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
        public void CreateInsert(DbTransaction transaction, DbBuilderOption builderOption)
        {
            CreateProcedureCommand(transaction, this.BuildInsertCommand, this._insertProcName, builderOption);
        }
        public string CreateInsertScriptText(DbTransaction transaction, DbBuilderOption builderOption)
        {
            return CreateProcedureCommandScriptText(transaction, this.BuildInsertCommand, this._insertProcName, builderOption);
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
            SqlParameter sqlParameter = new SqlParameter("@sync_scope_name", SqlDbType.NVarChar, 100);
            sqlCommand.Parameters.Add(sqlParameter);
            SqlParameter sqlParameter1 = new SqlParameter("@sync_row_is_tombstone", SqlDbType.Int);
            sqlCommand.Parameters.Add(sqlParameter1);
            SqlParameter sqlParameter3 = new SqlParameter("@create_timestamp", SqlDbType.BigInt);
            sqlCommand.Parameters.Add(sqlParameter3);
            SqlParameter sqlParameter4 = new SqlParameter("@update_timestamp", SqlDbType.BigInt);
            sqlCommand.Parameters.Add(sqlParameter4);
            SqlParameter sqlParameter8 = new SqlParameter("@sync_row_count", SqlDbType.Int);
            sqlParameter8.Direction = ParameterDirection.Output;
            sqlCommand.Parameters.Add(sqlParameter8);

            stringBuilder.AppendLine($"SET {sqlParameter.ParameterName} = 0;");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"UPDATE {this.trackingTableName.QuotedString} SET ");
            stringBuilder.AppendLine("\t[create_scope_name] = @sync_scope_name, ");
            stringBuilder.AppendLine("\t[create_timestamp] = @create_timestamp, ");
            stringBuilder.AppendLine("\t[update_scope_name] = @sync_scope_name, ");
            stringBuilder.AppendLine("\t[update_timestamp] = @update_timestamp, ");
            stringBuilder.AppendLine("\t[sync_row_is_tombstone] = @sync_row_is_tombstone ");
            stringBuilder.AppendLine($"WHERE ({SqlManagementUtils.WhereColumnAndParameters(this.table.PrimaryKey.Columns, "")})");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"SET {sqlParameter8.ParameterName} = @@rowcount; ");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"IF ({sqlParameter8.ParameterName} = 0)");
            stringBuilder.AppendLine("BEGIN ");
            stringBuilder.AppendLine($"\tINSERT INTO {this.trackingTableName.QuotedString}");

            string empty = string.Empty;
            foreach (var pkColumn in this.table.PrimaryKey.Columns)
            {
                ObjectNameParser columnName = new ObjectNameParser(pkColumn.ColumnName);
                stringBuilderArguments.Append(string.Concat(empty, columnName.QuotedString));
                stringBuilderParameters.Append(string.Concat(empty, $"@{columnName.UnquotedString}"));
                empty = ", ";
            }
            stringBuilder.AppendLine($"\t({stringBuilderArguments.ToString()}, ");
            stringBuilder.AppendLine($"\t[create_scope_name], [create_timestamp], [update_scope_name], [update_timestamp],");
            stringBuilder.AppendLine($"\t[sync_row_is_tombstone], [last_change_datetime])");
            stringBuilder.AppendLine($"\tVALUES ({stringBuilderParameters.ToString()}, ");
            stringBuilder.AppendLine($"\t@sync_scope_name, @create_timestamp, @sync_scope_name, @update_timestamp, ");
            stringBuilder.AppendLine($"\t@sync_row_is_tombstone, GetDate());");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"\tSET {sqlParameter8.ParameterName} = @@rowcount; ");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("END");

            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
        public void CreateInsertMetadata(DbTransaction transaction, DbBuilderOption builderOption)
        {
            CreateProcedureCommand(transaction, this.BuildInsertMetadataCommand, this._insertMetadataProcName, builderOption);
        }
        public string CreateInsertMetadataScriptText(DbTransaction transaction, DbBuilderOption builderOption)
        {
            return CreateProcedureCommandScriptText(transaction, this.BuildInsertMetadataCommand, this._insertMetadataProcName, builderOption);
        }


        //------------------------------------------------------------------
        // Select Row command
        //------------------------------------------------------------------
        private SqlCommand BuildSelectRowCommand()
        {
            SqlCommand sqlCommand = new SqlCommand();
            this.AddPkColumnParametersToCommand(sqlCommand);
            SqlParameter sqlParameter = new SqlParameter("@sync_scope_name", SqlDbType.NVarChar, 100);
            sqlCommand.Parameters.Add(sqlParameter);

            StringBuilder stringBuilder = new StringBuilder("SELECT ");
            stringBuilder.AppendLine();
            StringBuilder stringBuilder1 = new StringBuilder();
            string empty = string.Empty;
            foreach (var pkColumn in this.table.PrimaryKey.Columns)
            {
                ObjectNameParser pkColumnName = new ObjectNameParser(pkColumn.ColumnName);
                stringBuilder.AppendLine($"\t[side].{pkColumnName.QuotedString}, ");
                stringBuilder1.Append($"{empty}[side].{pkColumnName.QuotedString} = @{pkColumnName.UnquotedString}");
                empty = " AND ";
            }
            foreach (DmColumn nonPkMutableColumn in this.table.NonPkColumns)
            {
                ObjectNameParser nonPkColumnName = new ObjectNameParser(nonPkMutableColumn.ColumnName);
                stringBuilder.AppendLine($"\t[base].{nonPkColumnName.QuotedString}, ");
            }
            stringBuilder.AppendLine("\t[side].[sync_row_is_tombstone],");
            stringBuilder.AppendLine("\t[side].[update_timestamp],");
            stringBuilder.AppendLine("\t[side].[create_timestamp]");

            stringBuilder.AppendLine($"FROM {this.originalTableName.QuotedString} [base]");
            stringBuilder.AppendLine($"RIGHT JOIN {this.trackingTableName.QuotedString} [side] ON");

            string str = string.Empty;
            foreach (var pkColumn in this.table.PrimaryKey.Columns)
            {
                ObjectNameParser pkColumnName = new ObjectNameParser(pkColumn.ColumnName);
                stringBuilder.Append($"{str}[base].{pkColumnName.QuotedString} = [side].{pkColumnName.QuotedString}");
                str = " AND ";
            }
            stringBuilder.AppendLine();
            stringBuilder.Append(string.Concat("WHERE ", stringBuilder1.ToString()));
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
        public void CreateSelectRow(DbTransaction transaction, DbBuilderOption builderOption)
        {
            CreateProcedureCommand(transaction, this.BuildSelectRowCommand, this._selectRowProcName, builderOption);
        }
        public string CreateSelectRowScriptText(DbTransaction transaction, DbBuilderOption builderOption)
        {
            return CreateProcedureCommandScriptText(transaction, this.BuildSelectRowCommand, this._selectRowProcName, builderOption);
        }

        //------------------------------------------------------------------
        // Create TVP command
        //------------------------------------------------------------------
        private string CreateTVPTypeCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"CREATE TYPE {this._bulkTableTypeName} AS TABLE (");
            string str = "";
            foreach (var c in this.table.Columns)
            {
                var columnName = new ObjectNameParser(c.ColumnName);
                var nullString = c.AllowDBNull ? "NULL" : "NOT NULL";
                var quotedColumnType = new ObjectNameParser(c.GetSqlDbTypeString(), "[", "]").QuotedString;
                quotedColumnType += c.GetSqlTypePrecisionString();

                stringBuilder.AppendLine($"{str}{columnName.QuotedString} {quotedColumnType} {nullString}");
                str = ", ";
            }
            stringBuilder.AppendLine(", [update_peer_timestamp] [bigint] NULL");
            stringBuilder.AppendLine(", [create_peer_timestamp] [bigint] NULL");
            stringBuilder.Append(string.Concat(str, "PRIMARY KEY ("));
            str = "";
            foreach (var c in this.table.PrimaryKey.Columns)
            {
                var columnName = new ObjectNameParser(c.ColumnName);
                stringBuilder.Append($"{str}{columnName.QuotedString} ASC");
                str = ", ";
            }

            stringBuilder.Append("))");
            return stringBuilder.ToString();
        }
        public void CreateTVPType(DbTransaction transaction, DbBuilderOption builderOption)
        {
            (var connection, var trans) = GetTypedConnection(transaction);
            if (!NeedToCreateType(trans.Connection, trans, this._bulkTableTypeName, builderOption))
                return;

            using (SqlCommand sqlCommand = new SqlCommand(this.CreateTVPTypeCommandText(), trans.Connection, trans))
            {
                sqlCommand.ExecuteNonQuery();
            }

        }
        public string CreateTVPTypeScriptText(DbTransaction transaction, DbBuilderOption builderOption)
        {
            (var connection, var trans) = GetTypedConnection(transaction);

            if (!NeedToCreateType(connection, trans, this._bulkTableTypeName, builderOption))
                return string.Empty;

            string str = string.Concat("Create TVP Type on table ", this.originalTableName.QuotedString);
            return SqlBuilder.WrapScriptTextWithComments(this.CreateTVPTypeCommandText(), str);
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
            SqlParameter sqlParameter2 = new SqlParameter("@sync_row_count", SqlDbType.Int);
            sqlParameter2.Direction = ParameterDirection.Output;
            sqlCommand.Parameters.Add(sqlParameter2);

            stringBuilder.AppendLine($"SET {sqlParameter2.ParameterName} = 0;");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"UPDATE {this.originalTableName.QuotedString}");
            stringBuilder.Append($"SET {SqlManagementUtils.CommaSeparatedUpdateFromParameters(this.table)}");
            stringBuilder.AppendLine($"FROM {this.originalTableName.QuotedString} [base]");
            stringBuilder.AppendLine($"JOIN {this.trackingTableName.QuotedString} [side]");
            stringBuilder.Append($"ON ");
            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.table.PrimaryKey.Columns, "[base]", "[side]"));
            stringBuilder.AppendLine("WHERE ([side].[timestamp] <= @sync_min_timestamp OR @sync_force_write = 1)");
            stringBuilder.Append("AND (");
            stringBuilder.Append(SqlManagementUtils.WhereColumnAndParameters(this.table.PrimaryKey.Columns, "[base]"));
            stringBuilder.AppendLine(");");
            stringBuilder.AppendLine();
            stringBuilder.Append(string.Concat("SET ", sqlParameter2.ParameterName, " = @@ROWCOUNT;"));
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
        public void CreateUpdate(DbTransaction transaction, DbBuilderOption builderOption)
        {
            CreateProcedureCommand(transaction, this.BuildUpdateCommand, this._updateProcName, builderOption);
        }
        public string CreateUpdateScriptText(DbTransaction transaction, DbBuilderOption builderOption)
        {
            return CreateProcedureCommandScriptText(transaction, this.BuildUpdateCommand, this._updateProcName, builderOption);
        }

        //------------------------------------------------------------------
        // Update Metadata command
        //------------------------------------------------------------------
        private SqlCommand BuildUpdateMetadataCommand()
        {
            SqlCommand sqlCommand = new SqlCommand();
            StringBuilder stringBuilder = new StringBuilder();
            this.AddPkColumnParametersToCommand(sqlCommand);
            SqlParameter sqlParameter = new SqlParameter("@sync_scope_name", SqlDbType.NVarChar, 100);
            sqlCommand.Parameters.Add(sqlParameter);
            SqlParameter sqlParameter1 = new SqlParameter("@sync_row_is_tombstone", SqlDbType.Int);
            sqlCommand.Parameters.Add(sqlParameter1);
            SqlParameter sqlParameter3 = new SqlParameter("@create_timestamp", SqlDbType.BigInt);
            sqlCommand.Parameters.Add(sqlParameter3);
            SqlParameter sqlParameter5 = new SqlParameter("@update_timestamp", SqlDbType.BigInt);
            sqlCommand.Parameters.Add(sqlParameter5);
            SqlParameter sqlParameter8 = new SqlParameter("@sync_row_count", SqlDbType.Int);
            sqlParameter8.Direction = ParameterDirection.Output;
            sqlCommand.Parameters.Add(sqlParameter8);

            string str1 = SqlManagementUtils.WhereColumnAndParameters(this.table.PrimaryKey.Columns, "");

            stringBuilder.AppendLine($"SET {sqlParameter8.ParameterName} = 0;");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine("DECLARE @was_tombstone int; ");

            stringBuilder.AppendLine($"SELECT @was_tombstone = [sync_row_is_tombstone]");
            stringBuilder.AppendLine($"FROM {this.trackingTableName.QuotedString}");
            stringBuilder.AppendLine($"WHERE ({str1})");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("IF (@was_tombstone IS NOT NULL AND @was_tombstone = 1 AND @sync_row_is_tombstone = 0)");
            stringBuilder.AppendLine("BEGIN ");
            stringBuilder.AppendLine($"UPDATE {this.trackingTableName.QuotedString} SET ");
            stringBuilder.AppendLine("\t [create_scope_name] = @sync_scope_name, ");
            stringBuilder.AppendLine("\t [update_scope_name] = @sync_scope_name, ");
            stringBuilder.AppendLine("\t [create_timestamp] = @create_timestamp, ");
            stringBuilder.AppendLine("\t [update_timestamp] = @update_timestamp, ");
            stringBuilder.AppendLine("\t [sync_row_is_tombstone] = @sync_row_is_tombstone ");
            stringBuilder.AppendLine($"WHERE ({str1})");
            stringBuilder.AppendLine("END");
            stringBuilder.AppendLine("ELSE");
            stringBuilder.AppendLine("BEGIN ");
            stringBuilder.AppendLine($"UPDATE {this.trackingTableName.QuotedString} SET ");
            stringBuilder.AppendLine("\t [update_scope_name] = @sync_scope_name, ");
            stringBuilder.AppendLine("\t [update_timestamp] = @update_timestamp, ");
            stringBuilder.AppendLine("\t [sync_row_is_tombstone] = @sync_row_is_tombstone ");
            stringBuilder.AppendLine($"WHERE ({str1})");
            stringBuilder.AppendLine("END;");
            stringBuilder.AppendLine();
            stringBuilder.Append($"SET {sqlParameter8.ParameterName} = @@ROWCOUNT;");
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
        public void CreateUpdateMetadata(DbTransaction transaction, DbBuilderOption builderOption)
        {
            CreateProcedureCommand(transaction, this.BuildUpdateMetadataCommand, this._updateMetadataProcName, builderOption);
        }
        public string CreateUpdateMetadataScriptText(DbTransaction transaction, DbBuilderOption builderOption)
        {
            return CreateProcedureCommandScriptText(transaction, this.BuildUpdateMetadataCommand, this._updateMetadataProcName, builderOption);
        }


        //------------------------------------------------------------------
        // Select changes command
        //------------------------------------------------------------------
        private SqlCommand BuildSelectIncrementalChangesCommand()
        {
            SqlCommand sqlCommand = new SqlCommand();
            SqlParameter sqlParameter1 = new SqlParameter("@sync_min_timestamp", SqlDbType.BigInt);
            SqlParameter sqlParameter3 = new SqlParameter("@sync_scope_name", SqlDbType.NVarChar, 100);
            sqlCommand.Parameters.Add(sqlParameter1);
            sqlCommand.Parameters.Add(sqlParameter3);

            StringBuilder stringBuilder = new StringBuilder("SELECT ");
            foreach (var pkColumn in this.table.PrimaryKey.Columns)
            {
                var pkColumnName = new ObjectNameParser(pkColumn.ColumnName);
                stringBuilder.AppendLine($"\t[side].{pkColumnName.QuotedString}, ");
            }
            foreach (var column in this.table.NonPkColumns)
            {
                var columnName = new ObjectNameParser(column.ColumnName);
                stringBuilder.AppendLine($"\t[base].{columnName.QuotedString}, ");
            }
            stringBuilder.AppendLine($"\t[side].[sync_row_is_tombstone], ");
            stringBuilder.AppendLine($"\t[side].[update_timestamp], ");
            stringBuilder.AppendLine($"\t[side].[create_timestamp] ");
            stringBuilder.AppendLine($"FROM {this.originalTableName.QuotedString} [base]");
            stringBuilder.AppendLine($"RIGHT JOIN {this.trackingTableName.QuotedString} [side]");
            stringBuilder.Append($"ON ");

            string empty = "";
            foreach (var pkColumn in this.table.PrimaryKey.Columns)
            {
                var pkColumnName = new ObjectNameParser(pkColumn.ColumnName);
                stringBuilder.Append($"{empty}[base].{pkColumnName.QuotedString} = [side].{pkColumnName.QuotedString}");
                empty = " AND ";
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("WHERE (");
            string str = string.Empty;
            //if (!SqlManagementUtils.IsStringNullOrWhitespace(this._filterClause))
            //{
            //    StringBuilder stringBuilder1 = new StringBuilder();
            //    stringBuilder1.Append("((").Append(this._filterClause).Append(") OR (");
            //    stringBuilder1.Append(SqlSyncProcedureHelper.TrackingTableAlias).Append(".").Append(this._trackingColNames.SyncRowIsTombstone).Append(" = 1 AND ");
            //    stringBuilder1.Append("(");
            //    stringBuilder1.Append(SqlSyncProcedureHelper.TrackingTableAlias).Append(".").Append(this._trackingColNames.UpdateScopeLocalId).Append(" = ").Append(sqlParameter.ParameterName);
            //    stringBuilder1.Append(" OR ");
            //    stringBuilder1.Append(SqlSyncProcedureHelper.TrackingTableAlias).Append(".").Append(this._trackingColNames.UpdateScopeLocalId).Append(" IS NULL");
            //    stringBuilder1.Append(") AND ");
            //    string empty1 = string.Empty;
            //    foreach (DbSyncColumnDescription _filterColumn in this._filterColumns)
            //    {
            //        stringBuilder1.Append(empty1).Append(SqlSyncProcedureHelper.TrackingTableAlias).Append(".").Append(_filterColumn.QuotedName).Append(" IS NULL");
            //        empty1 = " AND ";
            //    }
            //    stringBuilder1.Append("))");
            //    stringBuilder.Append(stringBuilder1.ToString());
            //    str = " AND ";
            //}

            stringBuilder.AppendLine("\t-- Update Machine");
            stringBuilder.AppendLine("\t[side].[update_scope_name] IS NULL");
            stringBuilder.AppendLine("\t-- Or Update different from remote");
            stringBuilder.AppendLine("\tOR [side].[update_scope_name] <> @sync_scope_name");
            stringBuilder.AppendLine("    )");
            stringBuilder.AppendLine("-- And Timestamp is > from remote timestamp");
            stringBuilder.AppendLine("AND [side].[timestamp] > @sync_min_timestamp");

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
        public void CreateSelectIncrementalChanges(DbTransaction transaction, DbBuilderOption builderOption)
        {
            CreateProcedureCommand(transaction, this.BuildSelectIncrementalChangesCommand, this._selectChangesProcName, builderOption);
        }
        public string CreateSelectIncrementalChangesScriptText(DbTransaction transaction, DbBuilderOption builderOption)
        {
            return CreateProcedureCommandScriptText(transaction, this.BuildSelectIncrementalChangesCommand, this._selectChangesProcName, builderOption);
        }



        internal void SetDefaultProcNames()
        {
            string str = "[";
            string prefix = "";
            string schema = "";
            if (!string.IsNullOrEmpty(this.originalTableName.SchemaName))
                str = string.Concat(this.originalTableName.QuotedSchemaName, ".[");

            if (this.originalTableName.ObjectName.StartsWith("sys", StringComparison.OrdinalIgnoreCase) && string.IsNullOrEmpty(prefix))
                prefix = "sync";



            this._selectChangesProcName = SqlManagementUtils.GetQuotedPrefixedName(prefix, string.Concat(str, originalTableName.ObjectName, "_selectchanges]"), schema);
            this._selectRowProcName = SqlManagementUtils.GetQuotedPrefixedName(prefix, string.Concat(str, originalTableName.ObjectName, "_selectrow]"), schema);
            this._insertProcName = SqlManagementUtils.GetQuotedPrefixedName(prefix, string.Concat(str, originalTableName.ObjectName, "_insert]"), schema);
            this._updateProcName = SqlManagementUtils.GetQuotedPrefixedName(prefix, string.Concat(str, originalTableName.ObjectName, "_update]"), schema);
            this._deleteProcName = SqlManagementUtils.GetQuotedPrefixedName(prefix, string.Concat(str, originalTableName.ObjectName, "_delete]"), schema);
            this._insertMetadataProcName = SqlManagementUtils.GetQuotedPrefixedName(prefix, string.Concat(str, originalTableName.ObjectName, "_insertmetadata]"), schema);
            this._updateMetadataProcName = SqlManagementUtils.GetQuotedPrefixedName(prefix, string.Concat(str, originalTableName.ObjectName, "_updatemetadata]"), schema);
            this._deleteMetadataProcName = SqlManagementUtils.GetQuotedPrefixedName(prefix, string.Concat(str, originalTableName.ObjectName, "_deletemetadata]"), schema);

            string tvpTypeName = string.Concat(originalTableName.ObjectName.Replace(".", "_").Replace(" ", "_"), "_BulkType");
            this._bulkTableTypeName = SqlManagementUtils.GetQuotedPrefixedName(prefix, string.Concat(str, tvpTypeName, "]"), schema);

            this._bulkInsertProcName = SqlManagementUtils.GetQuotedPrefixedName(prefix, string.Concat(str, originalTableName.ObjectName, "_bulkinsert]"), schema);
            this._bulkUpdateProcName = SqlManagementUtils.GetQuotedPrefixedName(prefix, string.Concat(str, originalTableName.ObjectName, "_bulkupdate]"), schema);
            this._bulkDeleteProcName = SqlManagementUtils.GetQuotedPrefixedName(prefix, string.Concat(str, originalTableName.ObjectName, "_bulkdelete]"), schema);
        }
    }
}
