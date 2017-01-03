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
using Dotmim.Sync.Core.Log;

namespace Dotmim.Sync.SqlServer.Builders
{
    public class SqlBuilderProcedure : IDbBuilderProcedureHelper
    {
        private ObjectNameParser tableName;
        private ObjectNameParser trackingName;
        private SqlConnection connection;
        private SqlTransaction transaction;
        private string bulkTableTypeName;
        private DmTable tableDescription;

        public DmTable TableDescription
        {
            get
            {
                return this.tableDescription;
            }
            set
            {
                this.tableDescription = value;
                (this.tableName, this.trackingName) = SqlBuilder.GetParsers(TableDescription);

            }
        }
        public DbObjectNames ObjectNames { get; set; }
        public List<DmColumn> FilterColumns { get; set; }
        public List<DmColumn> FilterParameters { get; set; }

        public SqlBuilderProcedure(DbConnection connection, DbTransaction transaction = null)
        {
            this.connection = connection as SqlConnection;
            this.transaction = transaction as SqlTransaction;
        }

        private void AddPkColumnParametersToCommand(SqlCommand sqlCommand)
        {
            foreach (DmColumn pkColumn in TableDescription.PrimaryKey.Columns)
                sqlCommand.Parameters.Add(pkColumn.GetSqlParameter());
        }
        private void AddColumnParametersToCommand(SqlCommand sqlCommand)
        {
            foreach (DmColumn column in TableDescription.Columns)
                sqlCommand.Parameters.Add(column.GetSqlParameter());
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
                Logger.Current.Error($"Error during CreateProcedureCommand : {ex}");
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

                var str1 = $"Command {procName} for table {tableName.QuotedString}";
                var str = CreateProcedureCommandText(BuildCommand(), procName);
                return SqlBuilder.WrapScriptTextWithComments(str, str1);


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
        public bool NeedToCreateProcedure(string objectName, DbBuilderOption option)
        {

            if (connection.State != ConnectionState.Open)
                throw new ArgumentException("Here, we need an opened connection please");

           if (option.HasFlag(DbBuilderOption.CreateOrUseExistingSchema))
                return !SqlManagementUtils.ProcedureExists(connection, transaction, objectName);

            return false;
        }

        /// <summary>
        /// Check if we need to create the TVP Type
        /// </summary>
        public bool NeedToCreateType(string objectName, DbBuilderOption option)
        {

            if (connection.State != ConnectionState.Open)
                throw new ArgumentException("Here, we need an opened connection please");

            if (option.HasFlag(DbBuilderOption.CreateOrUseExistingSchema))
                    return !SqlManagementUtils.TypeExists(connection, transaction, objectName);

            return false;
        }


        private string BulkSelectUnsuccessfulRows()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("--Select all ids not deleted for conflict");
            stringBuilder.Append("SELECT ");
            for (int i = 0; i < TableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(TableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"{cc.QuotedString}");
                if (i < TableDescription.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
                else
                    stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine($"FROM @changeTable t");
            stringBuilder.AppendLine("WHERE NOT EXISTS (");
            stringBuilder.Append("\t SELECT ");
            for (int i = 0; i < TableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(TableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"{cc.QuotedString}");
                if (i < TableDescription.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
                else
                    stringBuilder.AppendLine();
            }
            stringBuilder.AppendLine("\t FROM @changed i");
            stringBuilder.Append("\t WHERE ");
            for (int i = 0; i < TableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(TableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"t.{cc.QuotedString} = i.{cc.QuotedString}");
                if (i < TableDescription.PrimaryKey.Columns.Length - 1)
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
            sqlParameter2.TypeName = bulkTableTypeName;
            sqlCommand.Parameters.Add(sqlParameter2);


            string str4 = SqlManagementUtils.JoinTwoTablesOnClause(TableDescription.PrimaryKey.Columns, "p", "t");
            string str5 = SqlManagementUtils.JoinTwoTablesOnClause(TableDescription.PrimaryKey.Columns, "changes", "base");
            string str6 = SqlManagementUtils.JoinTwoTablesOnClause(TableDescription.PrimaryKey.Columns, "changes", "side");
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("-- use a temp table to store the list of PKs that successfully got updated/inserted");
            stringBuilder.Append("declare @changed TABLE (");
            foreach (var c in TableDescription.PrimaryKey.Columns)
            {
                var cc = new ObjectNameParser(c.ColumnName);
                var quotedColumnType = new ObjectNameParser(c.GetSqlDbTypeString(), "[", "]").QuotedString;
                quotedColumnType += c.GetSqlTypePrecisionString();

                stringBuilder.Append($"{cc.QuotedString} {quotedColumnType}, ");
            }
            stringBuilder.Append(" PRIMARY KEY (");
            for (int i = 0; i < TableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(TableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"{cc.QuotedString}");
                if (i < TableDescription.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
            }
            stringBuilder.AppendLine("));");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"-- delete all items where timestamp <= @sync_min_timestamp or update_scope_name = @sync_scope_name");

            stringBuilder.AppendLine($"DELETE {tableName.QuotedString}");
            stringBuilder.Append($"OUTPUT ");
            for (int i = 0; i < TableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(TableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"DELETED.{cc.QuotedString}");
                if (i < TableDescription.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
                else
                    stringBuilder.AppendLine();
            }
            stringBuilder.AppendLine($"INTO @changed ");
            stringBuilder.AppendLine($"FROM {tableName.QuotedString} [base]");
            stringBuilder.AppendLine("JOIN (");


            stringBuilder.AppendLine($"\tSELECT ");
            string str = "";
            stringBuilder.Append("\t");
            foreach (var c in TableDescription.Columns)
            {
                var columnName = new ObjectNameParser(c.ColumnName);
                stringBuilder.Append($"{str}[p].{columnName.QuotedString}");
                str = ", ";
            }
            stringBuilder.AppendLine("\t, [p].[create_timestamp], [p].[update_timestamp]");
            stringBuilder.AppendLine("\t, [t].[update_scope_name], [t].[timestamp]");

            stringBuilder.AppendLine($"\tFROM @changeTable p ");


            //stringBuilder.AppendLine($"\tSELECT p.*, t.update_scope_name, t.timestamp");
            //stringBuilder.AppendLine($"\tFROM @changeTable p ");
            stringBuilder.AppendLine($"\tJOIN {trackingName.QuotedString} t ON ");
            stringBuilder.AppendLine($"\t{str4}");
            stringBuilder.AppendLine("\t)");
            stringBuilder.AppendLine("\tAS changes ON ");
            stringBuilder.AppendLine(str5);
            stringBuilder.AppendLine("WHERE");
            stringBuilder.AppendLine("-- Last changes was from the current scope, so we can delete it since we are sure no one else edit it ");
            stringBuilder.AppendLine("changes.update_scope_name = @sync_scope_name");
            stringBuilder.AppendLine("-- no change since the last time the current scope has sync (so no one has update the row)");
            stringBuilder.AppendLine("OR [changes].[timestamp] <= @sync_min_timestamp;");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("-- Since the delete trigger is passed, we update the tracking table to reflect the real scope deleter");
            stringBuilder.AppendLine("UPDATE [side] SET");
            stringBuilder.AppendLine("\tsync_row_is_tombstone = 1, ");
            stringBuilder.AppendLine("\tupdate_scope_name = @sync_scope_name,");
            stringBuilder.AppendLine("\tupdate_timestamp = [changes].update_timestamp");
            stringBuilder.AppendLine($"FROM {trackingName.QuotedString} side");
            stringBuilder.AppendLine("JOIN (");
            stringBuilder.Append("\tSELECT ");
            for (int i = 0; i < TableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(TableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"p.{cc.QuotedString}, ");
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
        public void CreateBulkDelete(string bulkDeleteProcName)
        {
            CreateProcedureCommand(this.BuildBulkDeleteCommand, bulkDeleteProcName);
        }
        public string CreateBulkDeleteScriptText(string bulkDeleteProcName)
        {
            return CreateProcedureCommandScriptText(this.BuildBulkDeleteCommand, bulkDeleteProcName);
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
            sqlParameter2.TypeName = bulkTableTypeName;
            sqlCommand.Parameters.Add(sqlParameter2);


            string str4 = SqlManagementUtils.JoinTwoTablesOnClause(TableDescription.PrimaryKey.Columns, "[p]", "[t]");
            string str5 = SqlManagementUtils.JoinTwoTablesOnClause(TableDescription.PrimaryKey.Columns, "[changes]", "[base]");
            string str6 = SqlManagementUtils.JoinTwoTablesOnClause(TableDescription.PrimaryKey.Columns, "[changes]", "[side]");

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("-- use a temp table to store the list of PKs that successfully got updated/inserted");
            stringBuilder.Append("declare @changed TABLE (");
            foreach (var c in TableDescription.PrimaryKey.Columns)
            {
                var cc = new ObjectNameParser(c.ColumnName);
                var quotedColumnType = new ObjectNameParser(c.GetSqlDbTypeString(), "[", "]").QuotedString;
                quotedColumnType += c.GetSqlTypePrecisionString();

                stringBuilder.Append($"{cc.QuotedString} {quotedColumnType}, ");
            }
            stringBuilder.Append(" PRIMARY KEY (");
            for (int i = 0; i < TableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(TableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"{cc.QuotedString}");
                if (i < TableDescription.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
            }
            stringBuilder.AppendLine("));");
            stringBuilder.AppendLine();

            // Check if we have auto inc column
            bool flag = false;
            foreach (var mutableColumn in TableDescription.Columns)
            {
                if (!mutableColumn.AutoIncrement)
                    continue;
                flag = true;
            }

            if (flag)
            {
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"SET IDENTITY_INSERT {tableName.QuotedString} ON;");
                stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine("-- update/insert into the base table");
            stringBuilder.AppendLine($"MERGE {tableName.QuotedString} AS base USING");
            stringBuilder.AppendLine("\t-- join done here against the side table to get the local timestamp for concurrency check\n");


         //   stringBuilder.AppendLine("\t(SELECT p.*, t.timestamp FROM @changeTable p ");

            stringBuilder.AppendLine($"\t(SELECT ");
            string str = "";
            stringBuilder.Append("\t");
            foreach (var c in TableDescription.Columns)
            {
                var columnName = new ObjectNameParser(c.ColumnName);
                stringBuilder.Append($"{str}[p].{columnName.QuotedString}");
                str = ", ";
            }
            stringBuilder.AppendLine("\t, [p].[create_timestamp], [p].[update_timestamp]");
            stringBuilder.AppendLine("\t, [t].[update_scope_name], [t].[timestamp]");

            stringBuilder.AppendLine($"\tFROM @changeTable p ");


            stringBuilder.Append($"\tLEFT JOIN {trackingName.QuotedString} t ON ");
            stringBuilder.AppendLine($"\t{str4}");
            stringBuilder.AppendLine($"\t) AS changes ON {str5}");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("-- Si la ligne n'existe pas en local et qu'elle a été créé avant le timestamp de référence");
            stringBuilder.Append("WHEN NOT MATCHED BY TARGET AND changes.[timestamp] <= @sync_min_timestamp OR changes.[timestamp] IS NULL THEN");

            StringBuilder stringBuilderArguments = new StringBuilder();
            StringBuilder stringBuilderParameters = new StringBuilder();

            string empty = string.Empty;
            foreach (var mutableColumn in TableDescription.Columns)
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
            for (int i = 0; i < TableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(TableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"INSERTED.{cc.QuotedString}");
                if (i < TableDescription.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
                else
                    stringBuilder.AppendLine();
            }
            stringBuilder.AppendLine($"\tINTO @changed; -- populates the temp table with successful PKs");
            stringBuilder.AppendLine();

            if (flag)
            {
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"SET IDENTITY_INSERT {tableName.QuotedString} OFF;");
                stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine("-- Since the insert trigger is passed, we update the tracking table to reflect the real scope inserter");
            stringBuilder.AppendLine("UPDATE side SET");
            stringBuilder.AppendLine("\tupdate_scope_name = @sync_scope_name,");
            stringBuilder.AppendLine("\tcreate_scope_name = @sync_scope_name,");
            stringBuilder.AppendLine("\tupdate_timestamp = changes.update_timestamp,");
            stringBuilder.AppendLine("\tcreate_timestamp = changes.create_timestamp");
            stringBuilder.AppendLine($"FROM {trackingName.QuotedString} side");
            stringBuilder.AppendLine("JOIN (");
            stringBuilder.Append("\tSELECT ");
            for (int i = 0; i < TableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(TableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"p.{cc.QuotedString}, ");
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
        public void CreateBulkInsert(string bulkInsertProcName)
        {
            CreateProcedureCommand(BuildBulkInsertCommand, bulkInsertProcName);
        }
        public string CreateBulkInsertScriptText(string bulkInsertProcName)
        {
            return CreateProcedureCommandScriptText(BuildBulkInsertCommand, bulkInsertProcName);
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
            sqlParameter2.TypeName = bulkTableTypeName;
            sqlCommand.Parameters.Add(sqlParameter2);


            string str4 = SqlManagementUtils.JoinTwoTablesOnClause(TableDescription.PrimaryKey.Columns, "[p]", "[t]");
            string str5 = SqlManagementUtils.JoinTwoTablesOnClause(TableDescription.PrimaryKey.Columns, "[changes]", "[base]");
            string str6 = SqlManagementUtils.JoinTwoTablesOnClause(TableDescription.PrimaryKey.Columns, "[changes]", "[side]");

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("-- use a temp table to store the list of PKs that successfully got updated/inserted");
            stringBuilder.Append("declare @changed TABLE (");
            foreach (var c in TableDescription.PrimaryKey.Columns)
            {
                var cc = new ObjectNameParser(c.ColumnName);
                var quotedColumnType = new ObjectNameParser(c.GetSqlDbTypeString(), "[", "]").QuotedString;
                quotedColumnType += c.GetSqlTypePrecisionString();

                stringBuilder.Append($"{cc.QuotedString} {quotedColumnType}, ");
            }
            stringBuilder.Append(" PRIMARY KEY (");
            for (int i = 0; i < TableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(TableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"{cc.QuotedString}");
                if (i < TableDescription.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
            }
            stringBuilder.AppendLine("));");
            stringBuilder.AppendLine();

            // Check if we have auto inc column
            bool flag = false;
            foreach (var mutableColumn in TableDescription.Columns)
            {
                if (!mutableColumn.AutoIncrement)
                    continue;
                flag = true;
            }

            if (flag)
            {
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"SET IDENTITY_INSERT {tableName.QuotedString} ON;");
                stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine("-- update the base table");
            stringBuilder.AppendLine($"MERGE {tableName.QuotedString} AS base USING");
            stringBuilder.AppendLine("\t-- join done here against the side table to get the local timestamp for concurrency check\n");

            //stringBuilder.AppendLine("\t(SELECT p.*, t.update_scope_name, t.timestamp FROM @changeTable p ");

            stringBuilder.AppendLine($"\t(SELECT ");
            string str = "";
            stringBuilder.Append("\t");
            foreach (var c in TableDescription.Columns)
            {
                var columnName = new ObjectNameParser(c.ColumnName);
                stringBuilder.Append($"{str}[p].{columnName.QuotedString}");
                str = ", ";
            }
            stringBuilder.AppendLine("\t, [p].[create_timestamp], [p].[update_timestamp]");
            stringBuilder.AppendLine("\t, [t].[update_scope_name], [t].[timestamp]");

            stringBuilder.AppendLine($"\tFROM @changeTable p ");
            stringBuilder.Append($"\tLEFT JOIN {trackingName.QuotedString} t ON ");
            stringBuilder.AppendLine($" {str4}");
            stringBuilder.AppendLine($"\t) AS changes ON {str5}");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine("WHEN MATCHED AND [changes].[update_scope_name] = @sync_scope_name OR [changes].[timestamp] <= @sync_min_timestamp THEN");

            StringBuilder stringBuilderArguments = new StringBuilder();
            StringBuilder stringBuilderParameters = new StringBuilder();

            string empty = string.Empty;
            foreach (var mutableColumn in TableDescription.Columns)
            {
                ObjectNameParser columnName = new ObjectNameParser(mutableColumn.ColumnName);
                stringBuilderArguments.Append(string.Concat(empty, columnName.QuotedString));
                stringBuilderParameters.Append(string.Concat(empty, $"changes.{columnName.UnquotedString}"));
                empty = ", ";
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"\tUPDATE SET");

            string strSeparator = "";
            foreach (DmColumn column in TableDescription.NonPkColumns)
            {
                ObjectNameParser quotedColumn = new ObjectNameParser(column.ColumnName);
                stringBuilder.AppendLine($"\t{strSeparator}{quotedColumn.QuotedString} = [changes].{quotedColumn.UnquotedString}");
                strSeparator = ", ";
            }
            stringBuilder.Append($"\tOUTPUT ");
            for (int i = 0; i < TableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(TableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"INSERTED.{cc.QuotedString}");
                if (i < TableDescription.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
                else
                    stringBuilder.AppendLine();
            }
            stringBuilder.AppendLine($"\tINTO @changed; -- populates the temp table with successful PKs");
            stringBuilder.AppendLine();

            if (flag)
            {
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"SET IDENTITY_INSERT {tableName.QuotedString} OFF;");
                stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine("-- Since the update trigger is passed, we update the tracking table to reflect the real scope updater");
            stringBuilder.AppendLine("UPDATE side SET");
            stringBuilder.AppendLine("\tupdate_scope_name = @sync_scope_name,");
            stringBuilder.AppendLine("\tupdate_timestamp = changes.update_timestamp");
            stringBuilder.AppendLine($"FROM {trackingName.QuotedString} side");
            stringBuilder.AppendLine("JOIN (");
            stringBuilder.Append("\tSELECT ");
            for (int i = 0; i < TableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(TableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"p.{cc.QuotedString}, ");
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
        public void CreateBulkUpdate(string bulkUpdateProcName)
        {
            CreateProcedureCommand(BuildBulkUpdateCommand, bulkUpdateProcName);
        }
        public string CreateBulkUpdateScriptText(string bulkUpdateProcName)
        {
            return CreateProcedureCommandScriptText(BuildBulkUpdateCommand, bulkUpdateProcName);
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
            stringBuilder.AppendLine($"DELETE {tableName.QuotedString}");
            stringBuilder.AppendLine($"FROM {tableName.QuotedString} [base]");
            stringBuilder.AppendLine($"JOIN {trackingName.QuotedString} [side] ON ");

            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(TableDescription.PrimaryKey.Columns, "[base]", "[side]"));

            stringBuilder.AppendLine("WHERE ([side].[timestamp] <= @sync_min_timestamp  OR @sync_force_write = 1)");
            stringBuilder.Append("AND ");
            stringBuilder.AppendLine(string.Concat("(", SqlManagementUtils.WhereColumnAndParameters(TableDescription.PrimaryKey.Columns, "[base]"), ");"));
            stringBuilder.AppendLine();
            stringBuilder.AppendLine(string.Concat("SET ", sqlParameter2.ParameterName, " = @@ROWCOUNT;"));
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
        public void CreateDelete(string deleteProcName)
        {
            CreateProcedureCommand(BuildDeleteCommand, deleteProcName);
        }
        public string CreateDeleteScriptText(string deleteProcName)
        {
            return CreateProcedureCommandScriptText(BuildDeleteCommand, deleteProcName);
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
            stringBuilder.AppendLine($"DELETE [side] FROM {trackingName.QuotedString} [side]");
            stringBuilder.Append($"WHERE ");
            stringBuilder.AppendLine(SqlManagementUtils.WhereColumnAndParameters(TableDescription.PrimaryKey.Columns, ""));
            stringBuilder.AppendLine();
            stringBuilder.AppendLine(string.Concat("SET ", sqlParameter2.ParameterName, " = @@ROWCOUNT;"));
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
        public void CreateDeleteMetadata(string deleteMetadataProcName)
        {
            CreateProcedureCommand(BuildDeleteMetadataCommand, deleteMetadataProcName);
        }
        public string CreateDeleteMetadataScriptText(string deleteMetadataProcName)
        {
            return CreateProcedureCommandScriptText(BuildDeleteMetadataCommand, deleteMetadataProcName);
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
            foreach (var column in TableDescription.Columns)
            {
                if (!column.AutoIncrement)
                    continue;
                hasAutoIncColumn = true;
                break;
            }

            stringBuilder.AppendLine($"SET {sqlParameter.ParameterName} = 0;");

            stringBuilder.Append(string.Concat("IF NOT EXISTS (SELECT * FROM ", trackingName.QuotedString, " WHERE "));
            stringBuilder.Append(SqlManagementUtils.WhereColumnAndParameters(TableDescription.PrimaryKey.Columns, string.Empty));
            stringBuilder.AppendLine(") ");
            stringBuilder.AppendLine("BEGIN ");
            if (hasAutoIncColumn)
            {
                stringBuilder.AppendLine($"\tSET IDENTITY_INSERT {tableName.QuotedString} ON;");
                stringBuilder.AppendLine();
            }

            string empty = string.Empty;
            foreach (var mutableColumn in TableDescription.Columns)
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
            stringBuilder.AppendLine(string.Concat("\tSET ", sqlParameter.ParameterName, " = @@rowcount; "));

            if (hasAutoIncColumn)
            {
                stringBuilder.AppendLine();
                stringBuilder.AppendLine($"\tSET IDENTITY_INSERT {tableName.QuotedString} OFF;");
            }

            stringBuilder.AppendLine("END ");
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
        public void CreateInsert(string insertProcName)
        {
            CreateProcedureCommand(BuildInsertCommand, insertProcName);
        }
        public string CreateInsertScriptText(string insertProcName)
        {
            return CreateProcedureCommandScriptText(BuildInsertCommand, insertProcName);
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

            stringBuilder.AppendLine($"UPDATE {trackingName.QuotedString} SET ");
            stringBuilder.AppendLine("\t[create_scope_name] = @sync_scope_name, ");
            stringBuilder.AppendLine("\t[create_timestamp] = @create_timestamp, ");
            stringBuilder.AppendLine("\t[update_scope_name] = @sync_scope_name, ");
            stringBuilder.AppendLine("\t[update_timestamp] = @update_timestamp, ");
            stringBuilder.AppendLine("\t[sync_row_is_tombstone] = @sync_row_is_tombstone ");
            stringBuilder.AppendLine($"WHERE ({SqlManagementUtils.WhereColumnAndParameters(TableDescription.PrimaryKey.Columns, "")})");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"SET {sqlParameter8.ParameterName} = @@rowcount; ");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"IF ({sqlParameter8.ParameterName} = 0)");
            stringBuilder.AppendLine("BEGIN ");
            stringBuilder.AppendLine($"\tINSERT INTO {trackingName.QuotedString}");

            string empty = string.Empty;
            foreach (var pkColumn in TableDescription.PrimaryKey.Columns)
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
        public void CreateInsertMetadata(string insertMetadataProcName)
        {
            CreateProcedureCommand(BuildInsertMetadataCommand, insertMetadataProcName);
        }
        public string CreateInsertMetadataScriptText(string insertMetadataProcName)
        {
            return CreateProcedureCommandScriptText(BuildInsertMetadataCommand, insertMetadataProcName);
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
            foreach (var pkColumn in TableDescription.PrimaryKey.Columns)
            {
                ObjectNameParser pkColumnName = new ObjectNameParser(pkColumn.ColumnName);
                stringBuilder.AppendLine($"\t[side].{pkColumnName.QuotedString}, ");
                stringBuilder1.Append($"{empty}[side].{pkColumnName.QuotedString} = @{pkColumnName.UnquotedString}");
                empty = " AND ";
            }
            foreach (DmColumn nonPkMutableColumn in TableDescription.NonPkColumns)
            {
                ObjectNameParser nonPkColumnName = new ObjectNameParser(nonPkMutableColumn.ColumnName);
                stringBuilder.AppendLine($"\t[base].{nonPkColumnName.QuotedString}, ");
            }
            stringBuilder.AppendLine("\t[side].[sync_row_is_tombstone],");
            stringBuilder.AppendLine("\t[side].[create_scope_name],");
            stringBuilder.AppendLine("\t[side].[create_timestamp],");
            stringBuilder.AppendLine("\t[side].[update_scope_name],");
            stringBuilder.AppendLine("\t[side].[update_timestamp]");

            stringBuilder.AppendLine($"FROM {tableName.QuotedString} [base]");
            stringBuilder.AppendLine($"RIGHT JOIN {trackingName.QuotedString} [side] ON");

            string str = string.Empty;
            foreach (var pkColumn in TableDescription.PrimaryKey.Columns)
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
        public void CreateSelectRow(string selectRowProcName)
        {
            CreateProcedureCommand(BuildSelectRowCommand, selectRowProcName);
        }
        public string CreateSelectRowScriptText(string selectRowProcName)
        {
            return CreateProcedureCommandScriptText(BuildSelectRowCommand, selectRowProcName);
        }

        //------------------------------------------------------------------
        // Create TVP command
        //------------------------------------------------------------------
        private string CreateTVPTypeCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.AppendLine($"CREATE TYPE {bulkTableTypeName} AS TABLE (");
            string str = "";
            foreach (var c in TableDescription.Columns)
            {
                var columnName = new ObjectNameParser(c.ColumnName);
                var nullString = c.AllowDBNull ? "NULL" : "NOT NULL";
                var quotedColumnType = new ObjectNameParser(c.GetSqlDbTypeString(), "[", "]").QuotedString;
                quotedColumnType += c.GetSqlTypePrecisionString();

                stringBuilder.AppendLine($"{str}{columnName.QuotedString} {quotedColumnType} {nullString}");
                str = ", ";
            }
            stringBuilder.AppendLine(", [create_scope_name] [nvarchar] (100) NULL");
            stringBuilder.AppendLine(", [create_timestamp] [bigint] NULL");
            stringBuilder.AppendLine(", [update_scope_name] [nvarchar] (100) NULL");
            stringBuilder.AppendLine(", [update_timestamp] [bigint] NULL");
            stringBuilder.Append(string.Concat(str, "PRIMARY KEY ("));
            str = "";
            foreach (var c in TableDescription.PrimaryKey.Columns)
            {
                var columnName = new ObjectNameParser(c.ColumnName);
                stringBuilder.Append($"{str}{columnName.QuotedString} ASC");
                str = ", ";
            }

            stringBuilder.Append("))");
            return stringBuilder.ToString();
        }
        public void CreateTVPType(string bulkTableTypeName)
        {
            this.bulkTableTypeName = bulkTableTypeName;
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
                Logger.Current.Error($"Error during CreateTVPType : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

            }

        }
        public string CreateTVPTypeScriptText(string bulkTableTypeName)
        {
            this.bulkTableTypeName = bulkTableTypeName;
            string str = string.Concat("Create TVP Type on table ", tableName.QuotedString);
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
            stringBuilder.AppendLine($"UPDATE {tableName.QuotedString}");
            stringBuilder.Append($"SET {SqlManagementUtils.CommaSeparatedUpdateFromParameters(TableDescription)}");
            stringBuilder.AppendLine($"FROM {tableName.QuotedString} [base]");
            stringBuilder.AppendLine($"JOIN {trackingName.QuotedString} [side]");
            stringBuilder.Append($"ON ");
            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(TableDescription.PrimaryKey.Columns, "[base]", "[side]"));
            stringBuilder.AppendLine("WHERE ([side].[timestamp] <= @sync_min_timestamp OR @sync_force_write = 1)");
            stringBuilder.Append("AND (");
            stringBuilder.Append(SqlManagementUtils.WhereColumnAndParameters(TableDescription.PrimaryKey.Columns, "[base]"));
            stringBuilder.AppendLine(");");
            stringBuilder.AppendLine();
            stringBuilder.Append(string.Concat("SET ", sqlParameter2.ParameterName, " = @@ROWCOUNT;"));
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }
        public void CreateUpdate(string updateProcName)
        {
            CreateProcedureCommand(BuildUpdateCommand, updateProcName);
        }
        public string CreateUpdateScriptText(string updateProcName)
        {
            return CreateProcedureCommandScriptText(BuildUpdateCommand, updateProcName);
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

            string str1 = SqlManagementUtils.WhereColumnAndParameters(TableDescription.PrimaryKey.Columns, "");

            stringBuilder.AppendLine($"SET {sqlParameter8.ParameterName} = 0;");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine("DECLARE @was_tombstone int; ");

            stringBuilder.AppendLine($"SELECT @was_tombstone = [sync_row_is_tombstone]");
            stringBuilder.AppendLine($"FROM {trackingName.QuotedString}");
            stringBuilder.AppendLine($"WHERE ({str1})");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("IF (@was_tombstone IS NOT NULL AND @was_tombstone = 1 AND @sync_row_is_tombstone = 0)");
            stringBuilder.AppendLine("BEGIN ");
            stringBuilder.AppendLine($"UPDATE {trackingName.QuotedString} SET ");
            stringBuilder.AppendLine("\t [create_scope_name] = @sync_scope_name, ");
            stringBuilder.AppendLine("\t [update_scope_name] = @sync_scope_name, ");
            stringBuilder.AppendLine("\t [create_timestamp] = @create_timestamp, ");
            stringBuilder.AppendLine("\t [update_timestamp] = @update_timestamp, ");
            stringBuilder.AppendLine("\t [sync_row_is_tombstone] = @sync_row_is_tombstone ");
            stringBuilder.AppendLine($"WHERE ({str1})");
            stringBuilder.AppendLine("END");
            stringBuilder.AppendLine("ELSE");
            stringBuilder.AppendLine("BEGIN ");
            stringBuilder.AppendLine($"UPDATE {trackingName.QuotedString} SET ");
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
        public void CreateUpdateMetadata(string updateMetadataProcName)
        {
            CreateProcedureCommand(BuildUpdateMetadataCommand, updateMetadataProcName);
        }
        public string CreateUpdateMetadataScriptText(string updateMetadataProcName)
        {
            return CreateProcedureCommandScriptText(BuildUpdateMetadataCommand, updateMetadataProcName);
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
            foreach (var pkColumn in TableDescription.PrimaryKey.Columns)
            {
                var pkColumnName = new ObjectNameParser(pkColumn.ColumnName);
                stringBuilder.AppendLine($"\t[side].{pkColumnName.QuotedString}, ");
            }
            foreach (var column in TableDescription.NonPkColumns)
            {
                var columnName = new ObjectNameParser(column.ColumnName);
                stringBuilder.AppendLine($"\t[base].{columnName.QuotedString}, ");
            }
            stringBuilder.AppendLine($"\t[side].[sync_row_is_tombstone], ");
            stringBuilder.AppendLine($"\t[side].[create_scope_name], ");
            stringBuilder.AppendLine($"\t[side].[create_timestamp], ");
            stringBuilder.AppendLine($"\t[side].[update_scope_name], ");
            stringBuilder.AppendLine($"\t[side].[update_timestamp] ");
            stringBuilder.AppendLine($"FROM {tableName.QuotedString} [base]");
            stringBuilder.AppendLine($"RIGHT JOIN {trackingName.QuotedString} [side]");
            stringBuilder.Append($"ON ");

            string empty = "";
            foreach (var pkColumn in TableDescription.PrimaryKey.Columns)
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
        public void CreateSelectIncrementalChanges(string selectChangesProcName)
        {
            CreateProcedureCommand(BuildSelectIncrementalChangesCommand, selectChangesProcName);
        }
        public string CreateSelectIncrementalChangesScriptText(string selectChangesProcName)
        {
            return CreateProcedureCommandScriptText(BuildSelectIncrementalChangesCommand, selectChangesProcName);
        }



    }
}
