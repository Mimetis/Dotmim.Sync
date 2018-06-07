using System;
using System.Data;
using System.Data.Common;
using System.Data.OracleClient;
using System.Text;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using Dotmim.Sync.Filter;
using Dotmim.Sync.Oracle.Manager;

namespace Dotmim.Sync.Oracle.Builder
{
    internal class OracleBuilderProcedure : IDbBuilderProcedureHelper
    {
        private DmTable tableDescription;
        private OracleConnection connection;
        private OracleTransaction transaction;

        private ObjectNameParser tableName;
        private ObjectNameParser trackingName;
        private OracleObjectNames oracleObjectNames;
        private OracleDbMetadata oracleDbMetadata;

        public OracleBuilderProcedure(DmTable tableDescription, DbConnection connection, DbTransaction transaction)
        {
            this.tableDescription = tableDescription;
            this.connection = connection as OracleConnection;
            this.transaction = transaction as OracleTransaction;

            (this.tableName, this.trackingName) = OracleBuilder.GetParsers(tableDescription);
            this.oracleObjectNames = new OracleObjectNames(this.tableDescription);
            this.oracleDbMetadata = new OracleDbMetadata();
        }

        public FilterClauseCollection Filters { get; set; }

        #region Private Methods

        private void CreateProcedureCommand(Func<OracleCommand> BuildCommand, string procName)
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

        private string CreateProcedureCommandScriptText(Func<OracleCommand> BuildCommand, string procName)
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
                Debug.WriteLine($"Error during CreateProcedureCommand : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();
            }
        }

        private string CreateProcedureCommandText(OracleCommand cmd, string procName)
        {
            StringBuilder stringBuilder = new StringBuilder(string.Concat("CREATE PROCEDURE ", procName));
            string str = "\n\t";
            foreach (OracleParameter parameter in cmd.Parameters)
            {
                stringBuilder.Append(string.Concat(str, CreateParameterDeclaration(parameter)));
                str = ",\n\t";
            }
            stringBuilder.Append("\nAS\nBEGIN\n");
            stringBuilder.Append(cmd.CommandText);
            stringBuilder.Append("\nEND");
            return stringBuilder.ToString();
        }

        internal string CreateParameterDeclaration(OracleParameter param)
        {
            StringBuilder stringBuilder3 = new StringBuilder();
            SqlDbType sqlDbType = param.SqlDbType;

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

        #endregion

        #region Private Method Build

        //------------------------------------------------------------------
        // Bulk Insert command
        //------------------------------------------------------------------
        private OracleCommand BuildBulkInsertCommand()
        {
            OracleCommand sqlCommand = new OracleCommand();

            OracleParameter sqlParameter = new OracleParameter("@sync_min_timestamp", OracleType.BigInt);
            sqlCommand.Parameters.Add(sqlParameter);
            OracleParameter sqlParameter1 = new OracleParameter("@sync_scope_id", OracleType.UniqueIdentifier);
            sqlCommand.Parameters.Add(sqlParameter1);
            OracleParameter sqlParameter2 = new OracleParameter("@changeTable", OracleType.Structured);
            sqlParameter2.TypeName = this.oracleObjectNames.GetCommandName(DbCommandType.BulkTableType);
            sqlCommand.Parameters.Add(sqlParameter2);


            string str4 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "[p]", "[t]");
            string str5 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "[changes]", "[base]");
            string str6 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "[changes]", "[side]");

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("-- use a temp table to store the list of PKs that successfully got updated/inserted");
            stringBuilder.Append("declare @changed TABLE (");
            foreach (var c in this.tableDescription.PrimaryKey.Columns)
            {
                var cc = new ObjectNameParser(c.ColumnName);

                // Get the good SqlDbType (even if we are not from Sql Server def)
                var sqlDbTypeString = this.sqlDbMetadata.TryGetOwnerDbTypeString(c.OriginalDbType, c.DbType, false, false, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
                var quotedColumnType = new ObjectNameParser(sqlDbTypeString, "[", "]").QuotedString;
                quotedColumnType += this.sqlDbMetadata.TryGetOwnerDbTypePrecision(c.OriginalDbType, c.DbType, false, false, c.MaxLength, c.Precision, c.Scale, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);

                stringBuilder.Append($"{cc.QuotedString} {quotedColumnType}, ");
            }
            stringBuilder.Append(" PRIMARY KEY (");
            for (int i = 0; i < this.tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(this.tableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"{cc.QuotedString}");
                if (i < this.tableDescription.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
            }
            stringBuilder.AppendLine("));");
            stringBuilder.AppendLine();

            // Check if we have auto inc column
            bool flag = false;
            foreach (var mutableColumn in this.tableDescription.Columns.Where(c => !c.ReadOnly))
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
            foreach (var c in this.tableDescription.Columns.Where(col => !col.ReadOnly))
            {
                var columnName = new ObjectNameParser(c.ColumnName);
                stringBuilder.Append($"{str}[p].{columnName.QuotedString}");
                str = ", ";
            }
            stringBuilder.AppendLine("\t, [p].[create_timestamp], [p].[update_timestamp]");
            stringBuilder.AppendLine("\t, [t].[update_scope_id], [t].[timestamp]");

            stringBuilder.AppendLine($"\tFROM @changeTable p ");


            stringBuilder.Append($"\tLEFT JOIN {trackingName.QuotedString} t ON ");
            stringBuilder.AppendLine($"\t{str4}");
            stringBuilder.AppendLine($"\t) AS changes ON {str5}");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("-- Si la ligne n'existe pas en local et qu'elle a été créé avant le timestamp de référence");
            stringBuilder.Append("WHEN NOT MATCHED BY TARGET AND (changes.[timestamp] <= @sync_min_timestamp OR changes.[timestamp] IS NULL) THEN");

            StringBuilder stringBuilderArguments = new StringBuilder();
            StringBuilder stringBuilderParameters = new StringBuilder();

            string empty = string.Empty;
            foreach (var mutableColumn in this.tableDescription.Columns.Where(c => !c.ReadOnly))
            {
                ObjectNameParser columnName = new ObjectNameParser(mutableColumn.ColumnName);
                stringBuilderArguments.Append(string.Concat(empty, columnName.QuotedString));
                stringBuilderParameters.Append(string.Concat(empty, $"changes.{columnName.QuotedString}"));
                empty = ", ";
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"\tINSERT");
            stringBuilder.AppendLine($"\t({stringBuilderArguments.ToString()})");
            stringBuilder.AppendLine($"\tVALUES ({stringBuilderParameters.ToString()})");
            stringBuilder.Append($"\tOUTPUT ");
            for (int i = 0; i < this.tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(this.tableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"INSERTED.{cc.QuotedString}");
                if (i < this.tableDescription.PrimaryKey.Columns.Length - 1)
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
            stringBuilder.AppendLine("\tupdate_scope_id = @sync_scope_id,");
            stringBuilder.AppendLine("\tcreate_scope_id = @sync_scope_id,");
            stringBuilder.AppendLine("\tupdate_timestamp = changes.update_timestamp,");
            stringBuilder.AppendLine("\tcreate_timestamp = changes.create_timestamp");
            stringBuilder.AppendLine($"FROM {trackingName.QuotedString} side");
            stringBuilder.AppendLine("JOIN (");
            stringBuilder.Append("\tSELECT ");
            for (int i = 0; i < this.tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(this.tableDescription.PrimaryKey.Columns[i].ColumnName);
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

        //------------------------------------------------------------------
        // Bulk Delete command
        //------------------------------------------------------------------
        private OracleCommand BuildBulkDeleteCommand()
        {
            SqlCommand sqlCommand = new SqlCommand();

            SqlParameter sqlParameter = new SqlParameter("@sync_min_timestamp", SqlDbType.BigInt);
            sqlCommand.Parameters.Add(sqlParameter);
            SqlParameter sqlParameter1 = new SqlParameter("@sync_scope_id", SqlDbType.UniqueIdentifier);
            sqlCommand.Parameters.Add(sqlParameter1);
            SqlParameter sqlParameter2 = new SqlParameter("@changeTable", SqlDbType.Structured);
            sqlParameter2.TypeName = this.sqlObjectNames.GetCommandName(DbCommandType.BulkTableType);
            sqlCommand.Parameters.Add(sqlParameter2);


            string str4 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "p", "t");
            string str5 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "changes", "base");
            string str6 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "changes", "side");
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("-- use a temp table to store the list of PKs that successfully got updated/inserted");
            stringBuilder.Append("declare @changed TABLE (");
            foreach (var c in this.tableDescription.PrimaryKey.Columns)
            {
                var cc = new ObjectNameParser(c.ColumnName);

                // Get the good SqlDbType (even if we are not from Sql Server def)
                var sqlDbTypeString = this.sqlDbMetadata.TryGetOwnerDbTypeString(c.OriginalDbType, c.DbType, false, false, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
                var quotedColumnType = new ObjectNameParser(sqlDbTypeString, "[", "]").QuotedString;
                quotedColumnType += this.sqlDbMetadata.TryGetOwnerDbTypePrecision(c.OriginalDbType, c.DbType, false, false, c.MaxLength, c.Precision, c.Scale, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);

                stringBuilder.Append($"{cc.QuotedString} {quotedColumnType}, ");
            }
            stringBuilder.Append(" PRIMARY KEY (");
            for (int i = 0; i < this.tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(this.tableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"{cc.QuotedString}");
                if (i < this.tableDescription.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
            }
            stringBuilder.AppendLine("));");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"-- delete all items where timestamp <= @sync_min_timestamp or update_scope_id = @sync_scope_id");

            stringBuilder.AppendLine($"DELETE {tableName.QuotedString}");
            stringBuilder.Append($"OUTPUT ");
            for (int i = 0; i < this.tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(this.tableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"DELETED.{cc.QuotedString}");
                if (i < this.tableDescription.PrimaryKey.Columns.Length - 1)
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
            foreach (var c in this.tableDescription.Columns.Where(col => !col.ReadOnly))
            {
                var columnName = new ObjectNameParser(c.ColumnName);
                stringBuilder.Append($"{str}[p].{columnName.QuotedString}");
                str = ", ";
            }
            stringBuilder.AppendLine("\t, [p].[create_timestamp], [p].[update_timestamp]");
            stringBuilder.AppendLine("\t, [t].[update_scope_id], [t].[timestamp]");

            stringBuilder.AppendLine($"\tFROM @changeTable p ");


            //stringBuilder.AppendLine($"\tSELECT p.*, t.update_scope_id, t.timestamp");
            //stringBuilder.AppendLine($"\tFROM @changeTable p ");
            stringBuilder.AppendLine($"\tJOIN {trackingName.QuotedString} t ON ");
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
            stringBuilder.AppendLine($"FROM {trackingName.QuotedString} side");
            stringBuilder.AppendLine("JOIN (");
            stringBuilder.Append("\tSELECT ");
            for (int i = 0; i < this.tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(this.tableDescription.PrimaryKey.Columns[i].ColumnName);
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

        //------------------------------------------------------------------
        // Bulk Update command
        //------------------------------------------------------------------
        private OracleCommand BuildBulkUpdateCommand()
        {
            SqlCommand sqlCommand = new SqlCommand();

            SqlParameter sqlParameter = new SqlParameter("@sync_min_timestamp", SqlDbType.BigInt);
            sqlCommand.Parameters.Add(sqlParameter);
            SqlParameter sqlParameter1 = new SqlParameter("@sync_scope_id", SqlDbType.UniqueIdentifier);
            sqlCommand.Parameters.Add(sqlParameter1);
            SqlParameter sqlParameter2 = new SqlParameter("@changeTable", SqlDbType.Structured);
            sqlParameter2.TypeName = this.sqlObjectNames.GetCommandName(DbCommandType.BulkTableType);
            sqlCommand.Parameters.Add(sqlParameter2);


            string str4 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "[p]", "[t]");
            string str5 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "[changes]", "[base]");
            string str6 = SqlManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "[changes]", "[side]");

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("-- use a temp table to store the list of PKs that successfully got updated/inserted");
            stringBuilder.Append("declare @changed TABLE (");
            foreach (var c in this.tableDescription.PrimaryKey.Columns)
            {
                var cc = new ObjectNameParser(c.ColumnName);

                // Get the good SqlDbType (even if we are not from Sql Server def)
                var sqlDbTypeString = this.sqlDbMetadata.TryGetOwnerDbTypeString(c.OriginalDbType, c.DbType, false, false, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
                var quotedColumnType = new ObjectNameParser(sqlDbTypeString, "[", "]").QuotedString;
                quotedColumnType += this.sqlDbMetadata.TryGetOwnerDbTypePrecision(c.OriginalDbType, c.DbType, false, false, c.MaxLength, c.Precision, c.Scale, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);

                stringBuilder.Append($"{cc.QuotedString} {quotedColumnType}, ");
            }
            stringBuilder.Append(" PRIMARY KEY (");
            for (int i = 0; i < this.tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(this.tableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"{cc.QuotedString}");
                if (i < this.tableDescription.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
            }
            stringBuilder.AppendLine("));");
            stringBuilder.AppendLine();

            // Check if we have auto inc column
            bool flag = false;
            foreach (var mutableColumn in this.tableDescription.Columns.Where(c => !c.ReadOnly))
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

            //stringBuilder.AppendLine("\t(SELECT p.*, t.update_scope_id, t.timestamp FROM @changeTable p ");

            stringBuilder.AppendLine($"\t(SELECT ");
            string str = "";
            stringBuilder.Append("\t");
            foreach (var c in this.tableDescription.Columns.Where(col => !col.ReadOnly))
            {
                var columnName = new ObjectNameParser(c.ColumnName);
                stringBuilder.Append($"{str}[p].{columnName.QuotedString}");
                str = ", ";
            }
            stringBuilder.AppendLine("\t, [p].[create_timestamp], [p].[update_timestamp]");
            stringBuilder.AppendLine("\t, [t].[update_scope_id], [t].[timestamp]");

            stringBuilder.AppendLine($"\tFROM @changeTable p ");
            stringBuilder.Append($"\tLEFT JOIN {trackingName.QuotedString} t ON ");
            stringBuilder.AppendLine($" {str4}");
            stringBuilder.AppendLine($"\t) AS changes ON {str5}");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine("WHEN MATCHED AND ([changes].[update_scope_id] = @sync_scope_id OR [changes].[timestamp] <= @sync_min_timestamp) THEN");

            StringBuilder stringBuilderArguments = new StringBuilder();
            StringBuilder stringBuilderParameters = new StringBuilder();

            string empty = string.Empty;
            foreach (var mutableColumn in this.tableDescription.Columns.Where(c => !c.ReadOnly))
            {
                ObjectNameParser columnName = new ObjectNameParser(mutableColumn.ColumnName);
                stringBuilderArguments.Append(string.Concat(empty, columnName.QuotedString));
                stringBuilderParameters.Append(string.Concat(empty, $"changes.{columnName.QuotedString}"));
                empty = ", ";
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"\tUPDATE SET");

            string strSeparator = "";
            foreach (DmColumn column in this.tableDescription.NonPkColumns.Where(col => !col.ReadOnly))
            {
                ObjectNameParser quotedColumn = new ObjectNameParser(column.ColumnName);
                stringBuilder.AppendLine($"\t{strSeparator}{quotedColumn.QuotedString} = [changes].{quotedColumn.QuotedString}");
                strSeparator = ", ";
            }
            stringBuilder.Append($"\tOUTPUT ");
            for (int i = 0; i < this.tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(this.tableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"INSERTED.{cc.QuotedString}");
                if (i < this.tableDescription.PrimaryKey.Columns.Length - 1)
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
            stringBuilder.AppendLine("\tupdate_scope_id = @sync_scope_id,");
            stringBuilder.AppendLine("\tupdate_timestamp = changes.update_timestamp");
            stringBuilder.AppendLine($"FROM {trackingName.QuotedString} side");
            stringBuilder.AppendLine("JOIN (");
            stringBuilder.Append("\tSELECT ");
            for (int i = 0; i < this.tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(this.tableDescription.PrimaryKey.Columns[i].ColumnName);
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

        #endregion

        #region Implements IDbBuilderProcedureHelper

        public void CreateBulkDelete()
        {
            var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.BulkDeleteRows);
            CreateProcedureCommand(BuildBulkDeleteCommand, commandName);
        }

        public string CreateBulkDeleteScriptText()
        {
            var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.BulkDeleteRows);
            return CreateProcedureCommandScriptText(this.BuildBulkDeleteCommand, commandName);
        }

        public void CreateBulkInsert()
        {
            var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.BulkInsertRows);
            CreateProcedureCommand(BuildBulkInsertCommand, commandName);
        }

        public string CreateBulkInsertScriptText()
        {
            var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.BulkInsertRows);
            return CreateProcedureCommandScriptText(BuildBulkInsertCommand, commandName);
        }

        public void CreateBulkUpdate()
        {
            var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.BulkInsertRows);
            CreateProcedureCommand(BuildBulkUpdateCommand, commandName);
        }

        public string CreateBulkUpdateScriptText()
        {
            var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.BulkUpdateRows);
            return CreateProcedureCommandScriptText(BuildBulkUpdateCommand, commandName);
        }

        public void CreateDelete()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteRow);
            CreateProcedureCommand(BuildDeleteCommand, commandName);
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

        public string CreateDeleteScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteRow);
            return CreateProcedureCommandScriptText(BuildDeleteCommand, commandName);
        }

        public void CreateInsert()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertRow);
            CreateProcedureCommand(BuildInsertCommand, commandName);
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

        public string CreateInsertScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertRow);
            return CreateProcedureCommandScriptText(BuildInsertCommand, commandName);
        }

        public void CreateReset()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.Reset);
            CreateProcedureCommand(BuildResetCommand, commandName);
        }

        public string CreateResetScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.Reset);
            return CreateProcedureCommandScriptText(BuildResetCommand, commandName);
        }

        public void CreateSelectIncrementalChanges()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.SelectChanges);
            Func<SqlCommand> cmdWithoutFilter = () => BuildSelectIncrementalChangesCommand(false);
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
                Func<SqlCommand> cmdWithFilter = () => BuildSelectIncrementalChangesCommand(true);
                CreateProcedureCommand(cmdWithFilter, commandName);

            }
        }

        public string CreateSelectIncrementalChangesScriptText()
        {
            StringBuilder sbSelecteChanges = new StringBuilder();

            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.SelectChanges);
            Func<SqlCommand> cmdWithoutFilter = () => BuildSelectIncrementalChangesCommand(false);
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
                Func<SqlCommand> cmdWithFilter = () => BuildSelectIncrementalChangesCommand(true);
                sbSelecteChanges.AppendLine(CreateProcedureCommandScriptText(cmdWithFilter, commandName));

            }
            return sbSelecteChanges.ToString();
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
            string str = string.Concat("Create TVP Type on table ", tableName.QuotedString);
            return SqlBuilder.WrapScriptTextWithComments(this.CreateTVPTypeCommandText(), str);
        }

        public void CreateUpdate()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateRow);
            this.CreateProcedureCommand(BuildUpdateCommand, commandName);
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

        public string CreateUpdateScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateRow);
            return CreateProcedureCommandScriptText(BuildUpdateCommand, commandName);
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

                    var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.BulkDeleteRows);

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
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.BulkDeleteRows);

            return $"DROP PROCEDURE {commandName};";
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

                    var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.BulkInsertRows);

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
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.BulkInsertRows);

            return $"DROP PROCEDURE {commandName};";
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

                    var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.BulkUpdateRows);

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
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.BulkUpdateRows);

            return $"DROP PROCEDURE {commandName};";
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

                    var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteRow);

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

                    var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteMetadata);

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
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteMetadata);

            return $"DROP PROCEDURE {commandName};";
        }

        public string DropDeleteScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.DeleteRow);

            return $"DROP PROCEDURE {commandName};";
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

                    var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertRow);

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

                    var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertMetadata);

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
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertMetadata);

            return $"DROP PROCEDURE {commandName};";
        }

        public string DropInsertScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.InsertRow);

            return $"DROP PROCEDURE {commandName};";
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

                    var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.Reset);

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
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.Reset);

            return $"DROP PROCEDURE {commandName};";
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

                    var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.SelectChanges);

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

                        foreach (var c in this.Filters)
                        {
                            var columnFilter = this.tableDescription.Columns[c.ColumnName];

                            if (columnFilter == null)
                                throw new InvalidExpressionException($"Column {c.ColumnName} does not exist in Table {this.tableDescription.TableName}");
                        }

                        var filtersName = this.Filters.Select(f => f.ColumnName);
                        var commandNameWithFilter = this.sqlObjectNames.GetCommandName(DbCommandType.SelectChangesWitFilters, filtersName);

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
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.SelectChanges);

            string dropProcedure = $"DROP PROCEDURE {commandName};";

            if (this.Filters != null && this.Filters.Count > 0)
            {

                using (var command = new SqlCommand())
                {

                    foreach (var c in this.Filters)
                    {
                        var columnFilter = this.tableDescription.Columns[c.ColumnName];

                        if (columnFilter == null)
                            throw new InvalidExpressionException($"Column {c.ColumnName} does not exist in Table {this.tableDescription.TableName}");
                    }

                    var filtersName = this.Filters.Select(f => f.ColumnName);
                    var commandNameWithFilter = this.sqlObjectNames.GetCommandName(DbCommandType.SelectChangesWitFilters, filtersName);

                    dropProcedure += Environment.NewLine + $"DROP PROCEDURE {commandNameWithFilter};";

                }
            }
            return dropProcedure;
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

                    var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.SelectRow);

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
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.SelectRow);

            return $"DROP PROCEDURE {commandName};";
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

                    var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.BulkTableType);

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
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.BulkTableType);

            return $"DROP TYPE {commandName};";
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

                    var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateRow);

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

                    var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateMetadata);

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
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateMetadata);

            return $"DROP PROCEDURE {commandName};";
        }

        public string DropUpdateScriptText()
        {
            var commandName = this.sqlObjectNames.GetCommandName(DbCommandType.UpdateRow);

            return $"DROP PROCEDURE {commandName};";
        }

        public bool NeedToCreateProcedure(DbCommandType commandName)
        {
            if (connection.State != ConnectionState.Open)
                throw new ArgumentException("Here, we need an opened connection please");

            var commandName = this.sqlObjectNames.GetCommandName(commandType);

            return !SqlManagementUtils.ProcedureExists(connection, transaction, commandName);
        }

        public bool NeedToCreateType(DbCommandType typeName)
        {
            if (connection.State != ConnectionState.Open)
                throw new ArgumentException("Here, we need an opened connection please");

            var commandName = this.sqlObjectNames.GetCommandName(commandType);

            return !SqlManagementUtils.TypeExists(connection, transaction, commandName);
        }

        #endregion
    }
}