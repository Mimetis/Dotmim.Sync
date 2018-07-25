using System;
using System.Data;
using System.Data.Common;
using System.Data.OracleClient;
using System.Diagnostics;
using System.Text;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using Dotmim.Sync.Filter;
using Dotmim.Sync.Oracle.Manager;
using System.Linq;
using System.Collections.Generic;

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

        private void AddPkColumnParametersToCommand(OracleCommand sqlCommand)
        {
            foreach (DmColumn pkColumn in this.tableDescription.PrimaryKey.Columns)
                sqlCommand.Parameters.Add(GetSqlParameter(pkColumn));
        }

        private void AddColumnParametersToCommand(OracleCommand sqlCommand)
        {
            foreach (DmColumn column in this.tableDescription.Columns.Where(c => !c.ReadOnly))
                sqlCommand.Parameters.Add(GetSqlParameter(column));
        }

        private OracleParameter GetSqlParameter(DmColumn column)
        {
            OracleParameter sqlParameter = new OracleParameter();
            sqlParameter.ParameterName = $"{column.ColumnName}0";

            // Get the good SqlDbType (even if we are not from Sql Server def)
            OracleType sqlDbType = (OracleType)this.oracleDbMetadata.TryGetOwnerDbType(column.OriginalDbType, column.DbType, false, false, this.tableDescription.OriginalProvider, OracleSyncProvider.ProviderType);

            sqlParameter.OracleType = sqlDbType;
            sqlParameter.IsNullable = column.AllowDBNull;

            var (p, s) = this.oracleDbMetadata.TryGetOwnerPrecisionAndScale(column.OriginalDbType, column.DbType, false, false, column.Precision, column.Scale, this.tableDescription.OriginalProvider, OracleSyncProvider.ProviderType);

            if (p > 0)
            {
                sqlParameter.Precision = p;
                if (s > 0)
                    sqlParameter.Scale = s;
            }

            var m = this.oracleDbMetadata.TryGetOwnerMaxLength(column.OriginalDbType, column.DbType, false, false, column.MaxLength, this.tableDescription.OriginalProvider, OracleSyncProvider.ProviderType);

            if (m > 0)
                sqlParameter.Size = m;

            return sqlParameter;
        }

        #region Private Methods

        private void CreateProcedureCommand(Func<OracleCommand> BuildCommand, string procName, List<string> parameters = null)
        {

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                var str = CreateProcedureCommandText(BuildCommand(), procName, parameters);
                using (var command = new OracleCommand(str, connection))
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

        private string CreateProcedureCommandScriptText(Func<OracleCommand> BuildCommand, string procName, List<string> parameters = null)
        {

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                var str1 = $"Command {procName} for table {tableName.UnquotedString}";
                var str = CreateProcedureCommandText(BuildCommand(), procName, parameters);
                return OracleBuilder.WrapScriptTextWithComments(str, str1);
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

        private string CreateProcedureCommandText(OracleCommand cmd, string procName, List<string> parameters = null)
        { 
            StringBuilder stringBuilder = new StringBuilder();

            if (cmd.Parameters.Count > 0)
                stringBuilder.AppendLine(string.Concat("CREATE OR REPLACE PROCEDURE ", procName, " ("));
            else
                stringBuilder.AppendLine(string.Concat("CREATE OR REPLACE PROCEDURE ", procName));

            string str = "\n\t";
            foreach (OracleParameter parameter in cmd.Parameters)
            {
                stringBuilder.Append(string.Concat(str, CreateParameterDeclaration(parameter)));
                str = ",\n\t";
            }

            if (cmd.Parameters.Count > 0)
                stringBuilder.AppendLine(")");

            stringBuilder.AppendLine("AS");
            if(parameters != null)
                foreach(string parameter in parameters)
                    stringBuilder.AppendLine(parameter);
            stringBuilder.AppendLine("BEGIN");
            stringBuilder.Append(cmd.CommandText);
            stringBuilder.AppendLine("\nEND;");
            return stringBuilder.ToString();
        }

        internal string CreateParameterDeclaration(OracleParameter param)
        {
            StringBuilder stringBuilder3 = new StringBuilder();
            OracleType sqlDbType = param.OracleType;

            string empty = this.oracleDbMetadata.GetPrecisionStringFromOwnerDbType(sqlDbType, param.Size, param.Precision, param.Scale);

            var sqlDbTypeString = this.oracleDbMetadata.GetStringFromOwnerDbType(sqlDbType);
            string direction = "";

            if (param.Direction == ParameterDirection.Output)
                direction = "OUT";
            else if (param.Direction == ParameterDirection.InputOutput)
                direction = "IN OUT";
            else
                direction = "IN";

            if(sqlDbTypeString.Contains("varchar"))
                stringBuilder3.Append($"{param.ParameterName} {direction} NVARCHAR2");
            else
                stringBuilder3.Append($"{param.ParameterName} {direction} {sqlDbTypeString}{empty}");
            return stringBuilder3.ToString();
        }

        #endregion

        #region Private Method Build

        #region Bulk Not Used in Oracle for Moment

        //------------------------------------------------------------------
        // Create TVP command : NOT USED => FOR BULK
        //------------------------------------------------------------------
        private string CreateTVPTypeCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.BulkTableType);
            stringBuilder.AppendLine($"CREATE TYPE {commandName} AS TABLE (");
            string str = "";
            foreach (var c in this.tableDescription.Columns.Where(col => !col.ReadOnly))
            {
                var isPrimaryKey = this.tableDescription.PrimaryKey.Columns.Any(cc => this.tableDescription.IsEqual(cc.ColumnName, c.ColumnName));
                var columnName = new ObjectNameParser(c.ColumnName);
                var nullString = isPrimaryKey ? "NOT NULL" : "NULL";

                // Get the good SqlDbType (even if we are not from Sql Server def)
                var sqlDbTypeString = this.oracleDbMetadata.TryGetOwnerDbTypeString(c.OriginalDbType, c.DbType, false, false, this.tableDescription.OriginalProvider, OracleSyncProvider.ProviderType);
                var quotedColumnType = new ObjectNameParser(sqlDbTypeString, "", "").UnquotedString;
                quotedColumnType += this.oracleDbMetadata.TryGetOwnerDbTypePrecision(c.OriginalDbType, c.DbType, false, false, c.MaxLength, c.Precision, c.Scale, this.tableDescription.OriginalProvider, OracleSyncProvider.ProviderType);

                stringBuilder.AppendLine($"{str}{columnName.UnquotedString} {quotedColumnType} {nullString}");
                str = ", ";
            }
            stringBuilder.AppendLine(", create_scope_id uniqueidentifier NULL");
            stringBuilder.AppendLine(", create_timestamp bigint NULL");
            stringBuilder.AppendLine(", update_scope_id uniqueidentifier NULL");
            stringBuilder.AppendLine(", update_timestamp bigint NULL");
            stringBuilder.Append(string.Concat(str, "PRIMARY KEY ("));
            str = "";
            foreach (var c in this.tableDescription.PrimaryKey.Columns)
            {
                var columnName = new ObjectNameParser(c.ColumnName);
                stringBuilder.Append($"{str}{columnName.UnquotedString} ASC");
                str = ", ";
            }

            stringBuilder.Append("))");
            return stringBuilder.ToString();
        }

        //------------------------------------------------------------------
        // Bulk Insert command
        //------------------------------------------------------------------
        private OracleCommand BuildBulkInsertCommand()
        {
            OracleCommand sqlCommand = new OracleCommand();

            OracleParameter sqlParameter = new OracleParameter("sync_min_timestamp", OracleType.UInt32);
            sqlCommand.Parameters.Add(sqlParameter);
            OracleParameter sqlParameter1 = new OracleParameter("sync_scope_id", OracleType.LongVarChar);
            sqlCommand.Parameters.Add(sqlParameter1);
            OracleParameter sqlParameter2 = new OracleParameter("changeTable", OracleType.NVarChar);
            sqlParameter2.Value = this.oracleObjectNames.GetCommandName(DbCommandType.BulkTableType);
            sqlCommand.Parameters.Add(sqlParameter2);


            string str4 = OracleManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "p", "t");
            string str5 = OracleManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "changes", "base");
            string str6 = OracleManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "changes", "side");

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("-- use a temp table to store the list of PKs that successfully got updated/inserted");
            stringBuilder.Append("declare @changed TABLE (");
            foreach (var c in this.tableDescription.PrimaryKey.Columns)
            {
                var cc = new ObjectNameParser(c.ColumnName);

                // Get the good SqlDbType (even if we are not from Sql Server def)
                var sqlDbTypeString = this.oracleDbMetadata.TryGetOwnerDbTypeString(c.OriginalDbType, c.DbType, false, false, this.tableDescription.OriginalProvider, OracleSyncProvider.ProviderType);
                var quotedColumnType = new ObjectNameParser(sqlDbTypeString, "", "").UnquotedString;
                quotedColumnType += this.oracleDbMetadata.TryGetOwnerDbTypePrecision(c.OriginalDbType, c.DbType, false, false, c.MaxLength, c.Precision, c.Scale, this.tableDescription.OriginalProvider, OracleSyncProvider.ProviderType);

                stringBuilder.Append($"{cc.UnquotedString} {quotedColumnType}, ");
            }
            stringBuilder.Append(" PRIMARY KEY (");
            for (int i = 0; i < this.tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(this.tableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"{cc.UnquotedString}");
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
                stringBuilder.AppendLine($"SET IDENTITY_INSERT {tableName.UnquotedString} ON;");
                stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine("-- update/insert into the base table");
            stringBuilder.AppendLine($"MERGE {tableName.UnquotedString} AS base USING");
            stringBuilder.AppendLine("\t-- join done here against the side table to get the local timestamp for concurrency check\n");


            //   stringBuilder.AppendLine("\t(SELECT p.*, t.timestamp FROM @changeTable p ");

            stringBuilder.AppendLine($"\t(SELECT ");
            string str = "";
            stringBuilder.Append("\t");
            foreach (var c in this.tableDescription.Columns.Where(col => !col.ReadOnly))
            {
                var columnName = new ObjectNameParser(c.ColumnName);
                stringBuilder.Append($"{str}p.{columnName.UnquotedString}");
                str = ", ";
            }
            stringBuilder.AppendLine("\t, p.create_timestamp, p.update_timestamp");
            stringBuilder.AppendLine("\t, t.update_scope_id, t.timestamp");

            stringBuilder.AppendLine($"\tFROM @changeTable p ");


            stringBuilder.Append($"\tLEFT JOIN {trackingName.UnquotedString} t ON ");
            stringBuilder.AppendLine($"\t{str4}");
            stringBuilder.AppendLine($"\t) AS changes ON {str5}");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("-- Si la ligne n'existe pas en local et qu'elle a été créé avant le timestamp de référence");
            stringBuilder.Append("WHEN NOT MATCHED BY TARGET AND (changes.timestamp <= @sync_min_timestamp OR changes.timestamp IS NULL) THEN");

            StringBuilder stringBuilderArguments = new StringBuilder();
            StringBuilder stringBuilderParameters = new StringBuilder();

            string empty = string.Empty;
            foreach (var mutableColumn in this.tableDescription.Columns.Where(c => !c.ReadOnly))
            {
                ObjectNameParser columnName = new ObjectNameParser(mutableColumn.ColumnName);
                stringBuilderArguments.Append(string.Concat(empty, columnName.UnquotedString));
                stringBuilderParameters.Append(string.Concat(empty, $"changes.{columnName.UnquotedString}"));
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
                stringBuilder.Append($"INSERTED.{cc.UnquotedString}");
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
                stringBuilder.AppendLine($"SET IDENTITY_INSERT {tableName.UnquotedString} OFF;");
                stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine("-- Since the insert trigger is passed, we update the tracking table to reflect the real scope inserter");
            stringBuilder.AppendLine("UPDATE side SET");
            stringBuilder.AppendLine("\tupdate_scope_id = @sync_scope_id,");
            stringBuilder.AppendLine("\tcreate_scope_id = @sync_scope_id,");
            stringBuilder.AppendLine("\tupdate_timestamp = changes.update_timestamp,");
            stringBuilder.AppendLine("\tcreate_timestamp = changes.create_timestamp");
            stringBuilder.AppendLine($"FROM {trackingName.UnquotedString} side");
            stringBuilder.AppendLine("JOIN (");
            stringBuilder.Append("\tSELECT ");
            for (int i = 0; i < this.tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(this.tableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"p.{cc.UnquotedString}, ");
            }

            stringBuilder.AppendLine(" p.update_timestamp, p.create_timestamp ");
            stringBuilder.AppendLine("\tFROM @changed t");
            stringBuilder.AppendLine("\tJOIN @changeTable p ON ");
            stringBuilder.Append(str4);
            stringBuilder.AppendLine(") AS changes ON ");
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
            OracleCommand sqlCommand = new OracleCommand();

            OracleParameter sqlParameter = new OracleParameter("@sync_min_timestamp", SqlDbType.BigInt);
            sqlCommand.Parameters.Add(sqlParameter);
            OracleParameter sqlParameter1 = new OracleParameter("@sync_scope_id", SqlDbType.UniqueIdentifier);
            sqlCommand.Parameters.Add(sqlParameter1);
            OracleParameter sqlParameter2 = new OracleParameter("@changeTable", SqlDbType.Structured);
            sqlParameter2.Value = this.oracleObjectNames.GetCommandName(DbCommandType.BulkTableType);
            sqlCommand.Parameters.Add(sqlParameter2);


            string str4 = OracleManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "p", "t");
            string str5 = OracleManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "changes", "base");
            string str6 = OracleManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "changes", "side");
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("-- use a temp table to store the list of PKs that successfully got updated/inserted");
            stringBuilder.Append("declare @changed TABLE (");
            foreach (var c in this.tableDescription.PrimaryKey.Columns)
            {
                var cc = new ObjectNameParser(c.ColumnName);

                // Get the good SqlDbType (even if we are not from Sql Server def)
                var sqlDbTypeString = this.oracleDbMetadata.TryGetOwnerDbTypeString(c.OriginalDbType, c.DbType, false, false, this.tableDescription.OriginalProvider, OracleSyncProvider.ProviderType);
                var quotedColumnType = new ObjectNameParser(sqlDbTypeString, "", "").UnquotedString;
                quotedColumnType += this.oracleDbMetadata.TryGetOwnerDbTypePrecision(c.OriginalDbType, c.DbType, false, false, c.MaxLength, c.Precision, c.Scale, this.tableDescription.OriginalProvider, OracleSyncProvider.ProviderType);

                stringBuilder.Append($"{cc.UnquotedString} {quotedColumnType}, ");
            }
            stringBuilder.Append(" PRIMARY KEY (");
            for (int i = 0; i < this.tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(this.tableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"{cc.UnquotedString}");
                if (i < this.tableDescription.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
            }
            stringBuilder.AppendLine("));");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"-- delete all items where timestamp <= @sync_min_timestamp or update_scope_id = @sync_scope_id");

            stringBuilder.AppendLine($"DELETE {tableName.UnquotedString}");
            stringBuilder.Append($"OUTPUT ");
            for (int i = 0; i < this.tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(this.tableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"DELETED.{cc.UnquotedString}");
                if (i < this.tableDescription.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
                else
                    stringBuilder.AppendLine();
            }
            stringBuilder.AppendLine($"INTO @changed ");
            stringBuilder.AppendLine($"FROM {tableName.UnquotedString} base");
            stringBuilder.AppendLine("JOIN (");


            stringBuilder.AppendLine($"\tSELECT ");
            string str = "";
            stringBuilder.Append("\t");
            foreach (var c in this.tableDescription.Columns.Where(col => !col.ReadOnly))
            {
                var columnName = new ObjectNameParser(c.ColumnName);
                stringBuilder.Append($"{str}p.{columnName.UnquotedString}");
                str = ", ";
            }
            stringBuilder.AppendLine("\t, p.create_timestamp, p.update_timestamp");
            stringBuilder.AppendLine("\t, t.update_scope_id, t.timestamp");

            stringBuilder.AppendLine($"\tFROM @changeTable p ");


            //stringBuilder.AppendLine($"\tSELECT p.*, t.update_scope_id, t.timestamp");
            //stringBuilder.AppendLine($"\tFROM @changeTable p ");
            stringBuilder.AppendLine($"\tJOIN {trackingName.UnquotedString} t ON ");
            stringBuilder.AppendLine($"\t{str4}");
            stringBuilder.AppendLine("\t)");
            stringBuilder.AppendLine("\tAS changes ON ");
            stringBuilder.AppendLine(str5);
            stringBuilder.AppendLine("WHERE");
            stringBuilder.AppendLine("-- Last changes was from the current scope, so we can delete it since we are sure no one else edit it ");
            stringBuilder.AppendLine("changes.update_scope_id = @sync_scope_id");
            stringBuilder.AppendLine("-- no change since the last time the current scope has sync (so no one has update the row)");
            stringBuilder.AppendLine("OR changes.timestamp <= @sync_min_timestamp;");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("-- Since the delete trigger is passed, we update the tracking table to reflect the real scope deleter");
            stringBuilder.AppendLine("UPDATE side SET");
            stringBuilder.AppendLine("\tsync_row_is_tombstone = 1, ");
            stringBuilder.AppendLine("\tupdate_scope_id = @sync_scope_id,");
            stringBuilder.AppendLine("\tupdate_timestamp = changes.update_timestamp");
            stringBuilder.AppendLine($"FROM {trackingName.UnquotedString} side");
            stringBuilder.AppendLine("JOIN (");
            stringBuilder.Append("\tSELECT ");
            for (int i = 0; i < this.tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(this.tableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"p.{cc.UnquotedString}, ");
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
            OracleCommand sqlCommand = new OracleCommand();

            OracleParameter sqlParameter = new OracleParameter("@sync_min_timestamp", SqlDbType.BigInt);
            sqlCommand.Parameters.Add(sqlParameter);
            OracleParameter sqlParameter1 = new OracleParameter("@sync_scope_id", SqlDbType.UniqueIdentifier);
            sqlCommand.Parameters.Add(sqlParameter1);
            OracleParameter sqlParameter2 = new OracleParameter("@changeTable", SqlDbType.Structured);
            sqlParameter2.Value = this.oracleObjectNames.GetCommandName(DbCommandType.BulkTableType);
            sqlCommand.Parameters.Add(sqlParameter2);


            string str4 = OracleManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "p", "t");
            string str5 = OracleManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "changes", "base");
            string str6 = OracleManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "changes", "side");

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("-- use a temp table to store the list of PKs that successfully got updated/inserted");
            stringBuilder.Append("declare @changed TABLE (");
            foreach (var c in this.tableDescription.PrimaryKey.Columns)
            {
                var cc = new ObjectNameParser(c.ColumnName);

                // Get the good SqlDbType (even if we are not from Sql Server def)
                var sqlDbTypeString = this.oracleDbMetadata.TryGetOwnerDbTypeString(c.OriginalDbType, c.DbType, false, false, this.tableDescription.OriginalProvider, OracleSyncProvider.ProviderType);
                var quotedColumnType = new ObjectNameParser(sqlDbTypeString, "", "").UnquotedString;
                quotedColumnType += this.oracleDbMetadata.TryGetOwnerDbTypePrecision(c.OriginalDbType, c.DbType, false, false, c.MaxLength, c.Precision, c.Scale, this.tableDescription.OriginalProvider, OracleSyncProvider.ProviderType);

                stringBuilder.Append($"{cc.UnquotedString} {quotedColumnType}, ");
            }
            stringBuilder.Append(" PRIMARY KEY (");
            for (int i = 0; i < this.tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(this.tableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"{cc.UnquotedString}");
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
                stringBuilder.AppendLine($"SET IDENTITY_INSERT {tableName.UnquotedString} ON;");
                stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine("-- update the base table");
            stringBuilder.AppendLine($"MERGE {tableName.UnquotedString} AS base USING");
            stringBuilder.AppendLine("\t-- join done here against the side table to get the local timestamp for concurrency check\n");

            //stringBuilder.AppendLine("\t(SELECT p.*, t.update_scope_id, t.timestamp FROM @changeTable p ");

            stringBuilder.AppendLine($"\t(SELECT ");
            string str = "";
            stringBuilder.Append("\t");
            foreach (var c in this.tableDescription.Columns.Where(col => !col.ReadOnly))
            {
                var columnName = new ObjectNameParser(c.ColumnName);
                stringBuilder.Append($"{str}p.{columnName.UnquotedString}");
                str = ", ";
            }
            stringBuilder.AppendLine("\t, p.create_timestamp, p.update_timestamp");
            stringBuilder.AppendLine("\t, t.update_scope_id, t.timestamp");

            stringBuilder.AppendLine($"\tFROM @changeTable p ");
            stringBuilder.Append($"\tLEFT JOIN {trackingName.UnquotedString} t ON ");
            stringBuilder.AppendLine($" {str4}");
            stringBuilder.AppendLine($"\t) AS changes ON {str5}");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine("WHEN MATCHED AND (changes.update_scope_id = @sync_scope_id OR changes.timestamp <= @sync_min_timestamp) THEN");

            StringBuilder stringBuilderArguments = new StringBuilder();
            StringBuilder stringBuilderParameters = new StringBuilder();

            string empty = string.Empty;
            foreach (var mutableColumn in this.tableDescription.Columns.Where(c => !c.ReadOnly))
            {
                ObjectNameParser columnName = new ObjectNameParser(mutableColumn.ColumnName);
                stringBuilderArguments.Append(string.Concat(empty, columnName.UnquotedString));
                stringBuilderParameters.Append(string.Concat(empty, $"changes.{columnName.UnquotedString}"));
                empty = ", ";
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"\tUPDATE SET");

            string strSeparator = "";
            foreach (DmColumn column in this.tableDescription.NonPkColumns.Where(col => !col.ReadOnly))
            {
                ObjectNameParser quotedColumn = new ObjectNameParser(column.ColumnName);
                stringBuilder.AppendLine($"\t{strSeparator}{quotedColumn.UnquotedString} = changes.{quotedColumn.UnquotedString}");
                strSeparator = ", ";
            }
            stringBuilder.Append($"\tOUTPUT ");
            for (int i = 0; i < this.tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(this.tableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"INSERTED.{cc.UnquotedString}");
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
                stringBuilder.AppendLine($"SET IDENTITY_INSERT {tableName.UnquotedString} OFF;");
                stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine("-- Since the update trigger is passed, we update the tracking table to reflect the real scope updater");
            stringBuilder.AppendLine("UPDATE side SET");
            stringBuilder.AppendLine("\tupdate_scope_id = @sync_scope_id,");
            stringBuilder.AppendLine("\tupdate_timestamp = changes.update_timestamp");
            stringBuilder.AppendLine($"FROM {trackingName.UnquotedString} side");
            stringBuilder.AppendLine("JOIN (");
            stringBuilder.Append("\tSELECT ");
            for (int i = 0; i < this.tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(this.tableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"p.{cc.UnquotedString}, ");
            }

            stringBuilder.AppendLine(" p.update_timestamp, p.create_timestamp ");
            stringBuilder.AppendLine("\tFROM @changed t");
            stringBuilder.AppendLine("\tJOIN @changeTable p ON ");
            stringBuilder.AppendLine($"\t{str4}");
            stringBuilder.AppendLine(") AS changes ON ");
            stringBuilder.AppendLine($"\t{str6}");
            stringBuilder.AppendLine();
            stringBuilder.Append(BulkSelectUnsuccessfulRows());

            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        #endregion

        //------------------------------------------------------------------
        // Update Metadata command
        //------------------------------------------------------------------
        private OracleCommand BuildUpdateMetadataCommand()
        {
            OracleCommand sqlCommand = new OracleCommand();
            StringBuilder stringBuilder = new StringBuilder();
            this.AddPkColumnParametersToCommand(sqlCommand);
            OracleParameter sqlParameter = new OracleParameter("sync_scope_id", OracleType.NVarChar);
            sqlCommand.Parameters.Add(sqlParameter);
            OracleParameter sqlParameter1 = new OracleParameter("sync_row_is_tombstone", OracleType.Number);
            sqlCommand.Parameters.Add(sqlParameter1);
            OracleParameter sqlParameter3 = new OracleParameter("create_timestamp", OracleType.Number);
            sqlCommand.Parameters.Add(sqlParameter3);
            OracleParameter sqlParameter5 = new OracleParameter("update_timestamp", OracleType.Number);
            sqlCommand.Parameters.Add(sqlParameter5);
            OracleParameter sqlParameter8 = new OracleParameter("sync_row_count", OracleType.Number);
            sqlParameter8.Direction = ParameterDirection.Output;
            sqlCommand.Parameters.Add(sqlParameter8);

            string str1 = OracleManagementUtils.ColumnsAndParametersStoredProcedure(this.tableDescription.PrimaryKey.Columns, "");

            stringBuilder.AppendLine($"{sqlParameter8.ParameterName} := 0;");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"SELECT sync_row_is_tombstone into was_tombstone");
            stringBuilder.AppendLine($"FROM {trackingName.UnquotedString}");
            stringBuilder.AppendLine($"WHERE ({str1}) ;");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("IF (was_tombstone IS NOT NULL AND was_tombstone = 1 AND sync_row_is_tombstone = 0) THEN");
            stringBuilder.AppendLine("BEGIN ");
            stringBuilder.AppendLine($"UPDATE {trackingName.UnquotedString} SET ");
            stringBuilder.AppendLine("\t create_scope_id = sync_scope_id, ");
            stringBuilder.AppendLine("\t update_scope_id = sync_scope_id, ");
            stringBuilder.AppendLine("\t create_timestamp = create_timestamp, ");
            stringBuilder.AppendLine("\t update_timestamp = update_timestamp, ");
            stringBuilder.AppendLine("\t sync_row_is_tombstone = sync_row_is_tombstone ");
            stringBuilder.AppendLine($"WHERE ({str1}) ;");
            stringBuilder.AppendLine("END;");
            stringBuilder.AppendLine("ELSE");
            stringBuilder.AppendLine("BEGIN ");
            stringBuilder.AppendLine($"UPDATE {trackingName.UnquotedString} SET ");
            stringBuilder.AppendLine("\t update_scope_id = sync_scope_id, ");
            stringBuilder.AppendLine("\t update_timestamp = update_timestamp, ");
            stringBuilder.AppendLine("\t sync_row_is_tombstone = sync_row_is_tombstone ");
            stringBuilder.AppendLine($"WHERE ({str1}) ;");
            stringBuilder.AppendLine("END;");
            stringBuilder.AppendLine("END IF;");
            stringBuilder.AppendLine();
            stringBuilder.Append($" {sqlParameter8.ParameterName} := SQL%ROWCOUNT;");
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        //------------------------------------------------------------------
        // Select changes command : OK
        //------------------------------------------------------------------
        private OracleCommand BuildSelectIncrementalChangesCommand(bool withFilter = false)
        {
            OracleCommand sqlCommand = new OracleCommand();
            OracleParameter sqlParameter1 = new OracleParameter("sync_min_timestamp", OracleType.Number);
            OracleParameter sqlParameter3 = new OracleParameter("sync_scope_id", OracleType.NVarChar);
            OracleParameter sqlParameter4 = new OracleParameter("sync_scope_is_new", OracleType.Number);
            OracleParameter sqlParameter5 = new OracleParameter("sync_scope_is_reinit", OracleType.Number);
            OracleParameter sqlParameter6 = new OracleParameter("cur", OracleType.Cursor);
            sqlParameter6.Direction = ParameterDirection.Output;
            sqlCommand.Parameters.Add(sqlParameter1);
            sqlCommand.Parameters.Add(sqlParameter3);
            sqlCommand.Parameters.Add(sqlParameter4);
            sqlCommand.Parameters.Add(sqlParameter5);
            sqlCommand.Parameters.Add(sqlParameter6);

            if (withFilter && this.Filters != null && this.Filters.Count > 0)
            {
                foreach (var c in this.Filters)
                {
                    var columnFilter = this.tableDescription.Columns[c.ColumnName];

                    if (columnFilter == null)
                        throw new InvalidExpressionException($"Column {c.ColumnName} does not exist in Table {this.tableDescription.TableName}");

                    var columnFilterName = new ObjectNameParser(columnFilter.ColumnName, "", "");

                    // Get the good SqlDbType (even if we are not from Sql Server def)

                    OracleType sqlDbType = (OracleType)this.oracleDbMetadata.TryGetOwnerDbType(columnFilter.OriginalDbType, columnFilter.DbType, false, false, this.tableDescription.OriginalProvider, OracleSyncProvider.ProviderType);
                    OracleParameter sqlParamFilter = new OracleParameter($"{columnFilterName.UnquotedString}", sqlDbType);
                    sqlCommand.Parameters.Add(sqlParamFilter);
                }
            }

            StringBuilder stringBuilder = new StringBuilder("open cur for SELECT ");
            foreach (var pkColumn in this.tableDescription.PrimaryKey.Columns)
            {
                var pkColumnName = new ObjectNameParser(pkColumn.ColumnName);
                stringBuilder.AppendLine($"\t side.{pkColumnName.UnquotedString}, ");
            }
            foreach (var column in this.tableDescription.NonPkColumns.Where(col => !col.ReadOnly))
            {
                var columnName = new ObjectNameParser(column.ColumnName);
                stringBuilder.AppendLine($"\t base.{columnName.UnquotedString}, ");
            }
            
            stringBuilder.AppendLine($"\t side.create_scope_id, ");
            stringBuilder.AppendLine($"\t side.create_timestamp, ");
            stringBuilder.AppendLine($"\t side.update_scope_id, ");
            stringBuilder.AppendLine($"\t side.update_timestamp, ");
            stringBuilder.AppendLine($"\t side.sync_row_is_tombstone ");
            stringBuilder.AppendLine($"FROM {tableName.UnquotedString} base");
            stringBuilder.AppendLine($"RIGHT JOIN {trackingName.UnquotedString} side");
            stringBuilder.Append($"ON ");

            string empty = "";
            foreach (var pkColumn in this.tableDescription.PrimaryKey.Columns)
            {
                var pkColumnName = new ObjectNameParser(pkColumn.ColumnName);
                stringBuilder.Append($"{empty} base.{pkColumnName.UnquotedString} = side.{pkColumnName.UnquotedString}");
                empty = " AND ";
            }
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("WHERE (");
            string str = string.Empty;

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

                    var columnFilterName = new ObjectNameParser(columnFilter.ColumnName, "", "");

                    builderFilter.Append($"side.{columnFilterName.QuotedObjectName} = {columnFilterName.UnquotedString}{filterSeparationString}");
                    filterSeparationString = " AND ";
                }
                builderFilter.AppendLine(")");
                builderFilter.Append("\tOR (");
                builderFilter.AppendLine("(side.update_scope_id = sync_scope_id or side.update_scope_id IS NULL)");
                builderFilter.Append("\t\tAND (");

                filterSeparationString = "";
                foreach (var c in this.Filters)
                {
                    var columnFilter = this.tableDescription.Columns[c.ColumnName];
                    var columnFilterName = new ObjectNameParser(columnFilter.ColumnName, "", "");

                    builderFilter.Append($"side.{columnFilterName.QuotedObjectName} IS NULL{filterSeparationString}");
                    filterSeparationString = " OR ";
                }

                builderFilter.AppendLine("))");
                builderFilter.AppendLine("\t)");
                builderFilter.AppendLine("AND (");
                stringBuilder.Append(builderFilter.ToString());
            }

            stringBuilder.AppendLine("\t-- Update made by the local instance");
            stringBuilder.AppendLine("\tside.update_scope_id IS NULL");
            stringBuilder.AppendLine("\t-- Or Update different from remote");
            stringBuilder.AppendLine("\tOR side.update_scope_id <> sync_scope_id");
            stringBuilder.AppendLine("\t-- Or we are in reinit mode so we take rows even thoses updated by the scope");
            stringBuilder.AppendLine("\tOR sync_scope_is_reinit = 1");
            stringBuilder.AppendLine("    )");
            stringBuilder.AppendLine("AND (");
            stringBuilder.AppendLine("\t-- And Timestamp is > from remote timestamp");
            stringBuilder.AppendLine("\tside.timestamp > sync_min_timestamp");
            stringBuilder.AppendLine("\tOR");
            stringBuilder.AppendLine("\t-- remote instance is new, so we don't take the last timestamp");
            stringBuilder.AppendLine("\tsync_scope_is_new = 1");
            stringBuilder.AppendLine("\t)");
            stringBuilder.AppendLine("AND (");
            stringBuilder.AppendLine("\tside.sync_row_is_tombstone = 1 ");
            stringBuilder.AppendLine("\tOR");
            stringBuilder.Append("\t(side.sync_row_is_tombstone = 0");

            empty = " AND ";
            foreach (var pkColumn in this.tableDescription.PrimaryKey.Columns)
            {
                stringBuilder.Append($"{empty}base.{pkColumn.ColumnName} is not null");
            }
            stringBuilder.AppendLine("\t)");
            stringBuilder.AppendLine(");");

            sqlCommand.CommandText = stringBuilder.ToString();

            //if (this._filterParameters != null)
            //{
            //    foreach (OracleParameter _filterParameter in this._filterParameters)
            //    {
            //        sqlCommand.Parameters.Add(((ICloneable)_filterParameter).Clone());
            //    }
            //}
            return sqlCommand;
        }

        //------------------------------------------------------------------
        // Update command : OK
        //------------------------------------------------------------------
        private OracleCommand BuildUpdateCommand()
        {
            OracleCommand sqlCommand = new OracleCommand();

            StringBuilder stringBuilder = new StringBuilder();
            this.AddColumnParametersToCommand(sqlCommand);

            OracleParameter sqlParameter = new OracleParameter("sync_force_write", OracleType.Number);
            sqlCommand.Parameters.Add(sqlParameter);
            OracleParameter sqlParameter1 = new OracleParameter("sync_min_timestamp", OracleType.Number);
            sqlCommand.Parameters.Add(sqlParameter1);
            OracleParameter sqlParameter2 = new OracleParameter("sync_row_count", OracleType.Number);
            sqlParameter2.Direction = ParameterDirection.Output;
            sqlCommand.Parameters.Add(sqlParameter2);

            stringBuilder.AppendLine($" {sqlParameter2.ParameterName} := 0;");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"UPDATE {tableName.UnquotedString} base");
            stringBuilder.Append($"SET {OracleManagementUtils.CommaSeparatedUpdateFromParametersStoredProcedure(this.tableDescription)}");
            stringBuilder.Append($"WHERE EXISTS ( SELECT 1 FROM {trackingName.UnquotedString} side WHERE ");
            stringBuilder.Append(OracleManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "base", "side"));
            stringBuilder.AppendLine(" AND (side.timestamp <= sync_min_timestamp OR sync_force_write = 1)");
            stringBuilder.Append("AND (");
            stringBuilder.Append(OracleManagementUtils.ColumnsAndParametersStoredProcedure(this.tableDescription.PrimaryKey.Columns, "base"));
            stringBuilder.AppendLine("));");
            stringBuilder.AppendLine();
            stringBuilder.Append(string.Concat("", sqlParameter2.ParameterName, " := SQL%ROWCOUNT;"));
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        //------------------------------------------------------------------
        // Select Row command : OK
        //------------------------------------------------------------------
        private OracleCommand BuildSelectRowCommand()
        {
            OracleCommand sqlCommand = new OracleCommand();
            this.AddPkColumnParametersToCommand(sqlCommand);
            OracleParameter sqlParameter = new OracleParameter("sync_scope_id", OracleType.NVarChar);
            OracleParameter sqlParameter1 = new OracleParameter("cur", OracleType.Cursor);
            sqlParameter1.Direction = ParameterDirection.Output;
            sqlCommand.Parameters.Add(sqlParameter);
            sqlCommand.Parameters.Add(sqlParameter1);

            StringBuilder stringBuilder = new StringBuilder("open cur for SELECT ");
            stringBuilder.AppendLine();
            StringBuilder stringBuilder1 = new StringBuilder();
            string empty = string.Empty;
            foreach (var pkColumn in this.tableDescription.PrimaryKey.Columns)
            {
                ObjectNameParser pkColumnName = new ObjectNameParser(pkColumn.ColumnName);
                stringBuilder.AppendLine($"\t side.{pkColumnName.UnquotedString}, ");
                stringBuilder1.Append($"{empty} side.{pkColumnName.UnquotedString} = {pkColumnName.UnquotedString}0");
                empty = " AND ";
            }
            foreach (DmColumn nonPkMutableColumn in this.tableDescription.NonPkColumns.Where(c => !c.ReadOnly))
            {
                ObjectNameParser nonPkColumnName = new ObjectNameParser(nonPkMutableColumn.ColumnName);
                stringBuilder.AppendLine($"\t base.{nonPkColumnName.UnquotedString}, ");
            }
            stringBuilder.AppendLine("\t side.sync_row_is_tombstone,");
            stringBuilder.AppendLine("\t side.create_scope_id,");
            stringBuilder.AppendLine("\t side.create_timestamp,");
            stringBuilder.AppendLine("\t side.update_scope_id,");
            stringBuilder.AppendLine("\t side.update_timestamp");

            stringBuilder.AppendLine($"FROM {tableName.UnquotedString} base");
            stringBuilder.AppendLine($"RIGHT JOIN {trackingName.UnquotedString} side ON");

            string str = string.Empty;
            foreach (var pkColumn in this.tableDescription.PrimaryKey.Columns)
            {
                ObjectNameParser pkColumnName = new ObjectNameParser(pkColumn.ColumnName);
                stringBuilder.Append($"{str} base.{pkColumnName.UnquotedString} = side.{pkColumnName.UnquotedString}");
                str = " AND ";
            }
            stringBuilder.AppendLine();
            stringBuilder.Append(string.Concat("WHERE ", stringBuilder1.ToString(), " ;"));
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        //------------------------------------------------------------------
        // Insert Metadata command : OK
        //------------------------------------------------------------------
        private OracleCommand BuildInsertMetadataCommand()
        {
            StringBuilder stringBuilderArguments = new StringBuilder();
            StringBuilder stringBuilderParameters = new StringBuilder();
            OracleCommand sqlCommand = new OracleCommand();

            StringBuilder stringBuilder = new StringBuilder();
            this.AddPkColumnParametersToCommand(sqlCommand);
            OracleParameter sqlParameter = new OracleParameter("sync_scope_id", OracleType.NVarChar);
            sqlCommand.Parameters.Add(sqlParameter);
            OracleParameter sqlParameter1 = new OracleParameter("sync_row_is_tombstone", OracleType.Number);
            sqlCommand.Parameters.Add(sqlParameter1);
            OracleParameter sqlParameter3 = new OracleParameter("create_timestamp", OracleType.Number);
            sqlCommand.Parameters.Add(sqlParameter3);
            OracleParameter sqlParameter4 = new OracleParameter("update_timestamp", OracleType.Number);
            sqlCommand.Parameters.Add(sqlParameter4);
            OracleParameter sqlParameter8 = new OracleParameter("sync_row_count", OracleType.Number);
            sqlParameter8.Direction = ParameterDirection.Output;
            sqlCommand.Parameters.Add(sqlParameter8);

            stringBuilder.AppendLine($" {sqlParameter8.ParameterName} := 0;");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"UPDATE {trackingName.UnquotedString} SET ");
            stringBuilder.AppendLine("\tcreate_scope_id = sync_scope_id, ");
            stringBuilder.AppendLine("\tcreate_timestamp = create_timestamp, ");
            stringBuilder.AppendLine("\tupdate_scope_id = sync_scope_id, ");
            stringBuilder.AppendLine("\tupdate_timestamp = update_timestamp, ");
            stringBuilder.AppendLine("\tsync_row_is_tombstone = sync_row_is_tombstone ");
            stringBuilder.AppendLine($"WHERE ({OracleManagementUtils.ColumnsAndParametersStoredProcedure(this.tableDescription.PrimaryKey.Columns, "")});");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($" {sqlParameter8.ParameterName} := SQL%ROWCOUNT; ");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"IF ({sqlParameter8.ParameterName} = 0) THEN");
            stringBuilder.AppendLine("BEGIN ");
            stringBuilder.AppendLine($"\tINSERT INTO {trackingName.UnquotedString}");

            string empty = string.Empty;
            foreach (var pkColumn in this.tableDescription.PrimaryKey.Columns)
            {
                ObjectNameParser columnName = new ObjectNameParser(pkColumn.ColumnName);
                stringBuilderArguments.Append(string.Concat(empty, columnName.UnquotedString));
                stringBuilderParameters.Append(string.Concat(empty, $"{columnName.UnquotedString}0"));
                empty = ", ";
            }
            stringBuilder.AppendLine($"\t({stringBuilderArguments.ToString()}, ");
            stringBuilder.AppendLine($"\tcreate_scope_id, create_timestamp, update_scope_id, update_timestamp,");
            stringBuilder.AppendLine($"\tsync_row_is_tombstone, last_change_datetime)");
            stringBuilder.AppendLine($"\tVALUES ({stringBuilderParameters.ToString()}, ");
            stringBuilder.AppendLine($"\tsync_scope_id, create_timestamp, sync_scope_id, update_timestamp, ");
            stringBuilder.AppendLine($"\tsync_row_is_tombstone, sysdate);");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"\t {sqlParameter8.ParameterName} := SQL%ROWCOUNT; ");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("END;");
            stringBuilder.AppendLine("END IF;");

            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        //------------------------------------------------------------------
        // Insert command : OK
        //------------------------------------------------------------------
        private OracleCommand BuildInsertCommand()
        {
            OracleCommand sqlCommand = new OracleCommand();
            StringBuilder stringBuilder = new StringBuilder();
            StringBuilder stringBuilderArguments = new StringBuilder();
            StringBuilder stringBuilderParameters = new StringBuilder();

            this.AddColumnParametersToCommand(sqlCommand);
            OracleParameter sqlParameter = new OracleParameter("sync_row_count", OracleType.Number);
            sqlParameter.Direction = ParameterDirection.Output;
            sqlCommand.Parameters.Add(sqlParameter);

            //Check auto increment column
            bool hasAutoIncColumn = false;
            foreach (var column in this.tableDescription.Columns)
            {
                if (!column.AutoIncrement)
                    continue;
                hasAutoIncColumn = true;
                break;
            }

            stringBuilder.AppendLine($"{sqlParameter.ParameterName} := 0;");

            stringBuilder.Append(string.Concat("SELECT COUNT(1) into l_count FROM ", trackingName.UnquotedString, " WHERE "));
            stringBuilder.Append(OracleManagementUtils.ColumnsAndParametersStoredProcedure(this.tableDescription.PrimaryKey.Columns, string.Empty));
            stringBuilder.Append(";");
            stringBuilder.AppendLine("IF l_count > 0 THEN ");
            if (hasAutoIncColumn)
            {
                // stringBuilder.AppendLine($"\tSET IDENTITY_INSERT {tableName.UnquotedString} ON;");
                stringBuilder.AppendLine();
            }

            string empty = string.Empty;
            foreach (var mutableColumn in this.tableDescription.Columns.Where(c => !c.ReadOnly))
            {
                ObjectNameParser columnName = new ObjectNameParser(mutableColumn.ColumnName);
                stringBuilderArguments.Append(string.Concat(empty, columnName.UnquotedString));
                stringBuilderParameters.Append(string.Concat(empty, $"{columnName.UnquotedString}0"));
                empty = ", ";
            }
            stringBuilder.AppendLine($"\tINSERT INTO {tableName.UnquotedString}");
            stringBuilder.AppendLine($"\t({stringBuilderArguments.ToString()})");
            stringBuilder.AppendLine($"\tVALUES ({stringBuilderParameters.ToString()});");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine(string.Concat("\t", sqlParameter.ParameterName, " := SQL%ROWCOUNT; "));

            if (hasAutoIncColumn)
            {
                stringBuilder.AppendLine();
                // stringBuilder.AppendLine($"\tSET IDENTITY_INSERT {tableName.UnquotedString} OFF;");
            }

            stringBuilder.AppendLine("END IF;");
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        //------------------------------------------------------------------
        // Delete Metadata command : OK
        //------------------------------------------------------------------
        private OracleCommand BuildDeleteMetadataCommand()
        {
            OracleCommand sqlCommand = new OracleCommand();
            this.AddPkColumnParametersToCommand(sqlCommand);
            OracleParameter sqlParameter = new OracleParameter("sync_check_concurrency", OracleType.Number);
            sqlCommand.Parameters.Add(sqlParameter);
            OracleParameter sqlParameter1 = new OracleParameter("sync_row_timestamp", OracleType.Number);
            sqlCommand.Parameters.Add(sqlParameter1);
            OracleParameter sqlParameter2 = new OracleParameter("sync_row_count", OracleType.Number);
            sqlParameter2.Direction = ParameterDirection.Output;
            sqlCommand.Parameters.Add(sqlParameter2);
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"{sqlParameter2.ParameterName} := 0;");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"DELETE FROM {trackingName.UnquotedString} ");
            stringBuilder.Append($"WHERE ");
            stringBuilder.AppendLine(OracleManagementUtils.ColumnsAndParametersStoredProcedure(this.tableDescription.PrimaryKey.Columns, "") + ";");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine(string.Concat("", sqlParameter2.ParameterName, " := SQL%ROWCOUNT;"));
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        //------------------------------------------------------------------
        // Delete command : OK
        //------------------------------------------------------------------
        private OracleCommand BuildDeleteCommand()
        {
            OracleCommand sqlCommand = new OracleCommand();
            this.AddPkColumnParametersToCommand(sqlCommand);
            OracleParameter sqlParameter = new OracleParameter("sync_force_write", OracleType.Number);
            sqlCommand.Parameters.Add(sqlParameter);
            OracleParameter sqlParameter1 = new OracleParameter("sync_min_timestamp", OracleType.Number);
            sqlCommand.Parameters.Add(sqlParameter1);
            OracleParameter sqlParameter2 = new OracleParameter("sync_row_count", OracleType.Number);
            sqlParameter2.Direction = ParameterDirection.Output;
            sqlCommand.Parameters.Add(sqlParameter2);

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"{sqlParameter2.ParameterName} := 0;");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"DELETE {tableName.UnquotedString} base");
            stringBuilder.Append($"WHERE EXISTS ( SELECT 1 FROM {trackingName.UnquotedString} side WHERE ");
            stringBuilder.AppendLine(OracleManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "base", "side"));
            stringBuilder.AppendLine("AND (side.timestamp <= sync_min_timestamp OR sync_force_write = 1)");
            stringBuilder.Append("AND ");
            stringBuilder.AppendLine(string.Concat("(", OracleManagementUtils.ColumnsAndParametersStoredProcedure(this.tableDescription.PrimaryKey.Columns, "base"), "));"));
            stringBuilder.AppendLine();
            stringBuilder.AppendLine(string.Concat(sqlParameter2.ParameterName, " := SQL%ROWCOUNT;"));
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        //------------------------------------------------------------------
        // Reset command : OK
        //------------------------------------------------------------------
        private OracleCommand BuildResetCommand()
        {
            var updTriggerName = this.oracleObjectNames.GetCommandName(DbCommandType.UpdateTrigger);
            var delTriggerName = this.oracleObjectNames.GetCommandName(DbCommandType.DeleteTrigger);
            var insTriggerName = this.oracleObjectNames.GetCommandName(DbCommandType.InsertTrigger);

            OracleCommand sqlCommand = new OracleCommand();
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"EXECUTE IMMEDIATE 'ALTER TRIGGER  {updTriggerName} DISABLE';");
            stringBuilder.AppendLine($"EXECUTE IMMEDIATE 'ALTER TRIGGER  {insTriggerName} DISABLE';");
            stringBuilder.AppendLine($"EXECUTE IMMEDIATE 'ALTER TRIGGER  {delTriggerName} DISABLE';");

            stringBuilder.AppendLine($"DELETE FROM {tableName.UnquotedString};");
            stringBuilder.AppendLine($"DELETE FROM {trackingName.UnquotedString};");

            stringBuilder.AppendLine($"EXECUTE IMMEDIATE 'ALTER TRIGGER  {updTriggerName} ENABLE';");
            stringBuilder.AppendLine($"EXECUTE IMMEDIATE 'ALTER TRIGGER  {insTriggerName} ENABLE';");
            stringBuilder.AppendLine($"EXECUTE IMMEDIATE 'ALTER TRIGGER  {delTriggerName} ENABLE';");

            stringBuilder.AppendLine();
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        // NOT USED : FOR BULK
        private string BulkSelectUnsuccessfulRows()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("--Select all ids not deleted for conflict");
            stringBuilder.Append("SELECT ");
            for (int i = 0; i < this.tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(this.tableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"{cc.UnquotedString}");
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
                var cc = new ObjectNameParser(this.tableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"{cc.UnquotedString}");
                if (i < this.tableDescription.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
                else
                    stringBuilder.AppendLine();
            }
            stringBuilder.AppendLine("\t FROM @changed i");
            stringBuilder.Append("\t WHERE ");
            for (int i = 0; i < this.tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(this.tableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"t.{cc.UnquotedString} = i.{cc.UnquotedString}");
                if (i < this.tableDescription.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append("AND ");
                else
                    stringBuilder.AppendLine();
            }
            stringBuilder.AppendLine("\t)");
            return stringBuilder.ToString();
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
            var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.DeleteRow);
            CreateProcedureCommand(BuildDeleteCommand, commandName);
        }

        public void CreateDeleteMetadata()
        {
            var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.DeleteMetadata);
            CreateProcedureCommand(BuildDeleteMetadataCommand, commandName);
        }

        public string CreateDeleteMetadataScriptText()
        {
            var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.DeleteMetadata);
            return CreateProcedureCommandScriptText(BuildDeleteMetadataCommand, commandName);
        }

        public string CreateDeleteScriptText()
        {
            var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.DeleteRow);
            return CreateProcedureCommandScriptText(BuildDeleteCommand, commandName);
        }

        public void CreateInsert()
        {
            var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.InsertRow);
            CreateProcedureCommand(BuildInsertCommand, commandName, new List<string> { "l_count number;" });
        }

        public void CreateInsertMetadata()
        {
            var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.InsertMetadata);
            CreateProcedureCommand(BuildInsertMetadataCommand, commandName);
        }

        public string CreateInsertMetadataScriptText()
        {
            var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.InsertMetadata);
            return CreateProcedureCommandScriptText(BuildInsertMetadataCommand, commandName);
        }

        public string CreateInsertScriptText()
        {
            var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.InsertRow);
            return CreateProcedureCommandScriptText(BuildInsertCommand, commandName);
        }

        public void CreateReset()
        {
            var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.Reset);
            CreateProcedureCommand(BuildResetCommand, commandName);
        }

        public string CreateResetScriptText()
        {
            var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.Reset);
            return CreateProcedureCommandScriptText(BuildResetCommand, commandName);
        }

        public void CreateSelectIncrementalChanges()
        {
            var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.SelectChanges);
            Func<OracleCommand> cmdWithoutFilter = () => BuildSelectIncrementalChangesCommand(false);
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
                commandName = this.oracleObjectNames.GetCommandName(DbCommandType.SelectChangesWitFilters, filtersName);
                Func<OracleCommand> cmdWithFilter = () => BuildSelectIncrementalChangesCommand(true);
                CreateProcedureCommand(cmdWithFilter, commandName);

            }
        }

        public string CreateSelectIncrementalChangesScriptText()
        {
            StringBuilder sbSelecteChanges = new StringBuilder();

            var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.SelectChanges);
            Func<OracleCommand> cmdWithoutFilter = () => BuildSelectIncrementalChangesCommand(false);
            sbSelecteChanges.AppendLine(CreateProcedureCommandScriptText(cmdWithoutFilter, commandName));


            if (this.Filters != null && this.Filters.Count > 0)
            {
                commandName = this.oracleObjectNames.GetCommandName(DbCommandType.SelectChangesWitFilters);
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
                Func<OracleCommand> cmdWithFilter = () => BuildSelectIncrementalChangesCommand(true);
                sbSelecteChanges.AppendLine(CreateProcedureCommandScriptText(cmdWithFilter, commandName));

            }
            return sbSelecteChanges.ToString();
        }

        public void CreateSelectRow()
        {
            var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.SelectRow);
            CreateProcedureCommand(BuildSelectRowCommand, commandName);
        }

        public string CreateSelectRowScriptText()
        {
            var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.SelectRow);
            return CreateProcedureCommandScriptText(BuildSelectRowCommand, commandName);
        }

        public void CreateTVPType()
        {
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                using (OracleCommand sqlCommand = new OracleCommand(this.CreateTVPTypeCommandText(),
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
            string str = string.Concat("Create TVP Type on table ", tableName.UnquotedString);
            return OracleBuilder.WrapScriptTextWithComments(this.CreateTVPTypeCommandText(), str);
        }

        public void CreateUpdate()
        {
            var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.UpdateRow);
            this.CreateProcedureCommand(BuildUpdateCommand, commandName);
        }

        public void CreateUpdateMetadata()
        {
            var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.UpdateMetadata);
            CreateProcedureCommand(BuildUpdateMetadataCommand, commandName, new List<string> { "was_tombstone number;" });
        }

        public string CreateUpdateMetadataScriptText()
        {
            var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.UpdateMetadata);
            return CreateProcedureCommandScriptText(BuildUpdateMetadataCommand, commandName, new List<string> { "was_tombstone number;" });
        }

        public string CreateUpdateScriptText()
        {
            var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.UpdateRow);
            return CreateProcedureCommandScriptText(BuildUpdateCommand, commandName);
        }

        public void DropBulkDelete()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new OracleCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.BulkDeleteRows);

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
            var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.BulkDeleteRows);

            return $"DROP PROCEDURE {commandName};";
        }

        public void DropBulkInsert()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new OracleCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.BulkInsertRows);

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
            var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.BulkInsertRows);

            return $"DROP PROCEDURE {commandName};";
        }

        public void DropBulkUpdate()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new OracleCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.BulkUpdateRows);

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
            var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.BulkUpdateRows);

            return $"DROP PROCEDURE {commandName};";
        }

        public void DropDelete()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new OracleCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.DeleteRow);

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
                using (var command = new OracleCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.DeleteMetadata);

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
            var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.DeleteMetadata);

            return $"DROP PROCEDURE {commandName};";
        }

        public string DropDeleteScriptText()
        {
            var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.DeleteRow);

            return $"DROP PROCEDURE {commandName};";
        }

        public void DropInsert()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new OracleCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.InsertRow);

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
                using (var command = new OracleCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.InsertMetadata);

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
            var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.InsertMetadata);

            return $"DROP PROCEDURE {commandName};";
        }

        public string DropInsertScriptText()
        {
            var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.InsertRow);

            return $"DROP PROCEDURE {commandName};";
        }

        public void DropReset()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new OracleCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.Reset);

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
            var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.Reset);

            return $"DROP PROCEDURE {commandName};";
        }

        public void DropSelectIncrementalChanges()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new OracleCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.SelectChanges);

                    command.CommandText = $"DROP PROCEDURE {commandName};";
                    command.Connection = this.connection;
                    command.ExecuteNonQuery();

                }

                if (this.Filters != null && this.Filters.Count > 0)
                {

                    using (var command = new OracleCommand())
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
                        var commandNameWithFilter = this.oracleObjectNames.GetCommandName(DbCommandType.SelectChangesWitFilters, filtersName);

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
            var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.SelectChanges);

            string dropProcedure = $"DROP PROCEDURE {commandName};";

            if (this.Filters != null && this.Filters.Count > 0)
            {

                using (var command = new OracleCommand())
                {

                    foreach (var c in this.Filters)
                    {
                        var columnFilter = this.tableDescription.Columns[c.ColumnName];

                        if (columnFilter == null)
                            throw new InvalidExpressionException($"Column {c.ColumnName} does not exist in Table {this.tableDescription.TableName}");
                    }

                    var filtersName = this.Filters.Select(f => f.ColumnName);
                    var commandNameWithFilter = this.oracleObjectNames.GetCommandName(DbCommandType.SelectChangesWitFilters, filtersName);

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
                using (var command = new OracleCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.SelectRow);

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
            var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.SelectRow);

            return $"DROP PROCEDURE {commandName};";
        }

        public void DropTVPType()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new OracleCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.BulkTableType);

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
            var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.BulkTableType);

            return $"DROP TYPE {commandName};";
        }

        public void DropUpdate()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new OracleCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.UpdateRow);

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
                using (var command = new OracleCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.UpdateMetadata);

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
            var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.UpdateMetadata);

            return $"DROP PROCEDURE {commandName};";
        }

        public string DropUpdateScriptText()
        {
            var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.UpdateRow);

            return $"DROP PROCEDURE {commandName};";
        }

        public bool NeedToCreateProcedure(DbCommandType commandType)
        {
            if (connection.State != ConnectionState.Open)
                throw new ArgumentException("Here, we need an opened connection please");

            var commandName = this.oracleObjectNames.GetCommandName(commandType);

            return !OracleManagementUtils.ProcedureExists(connection, transaction, commandName);
        }

        public bool NeedToCreateType(DbCommandType typeName)
        {
            if (connection.State != ConnectionState.Open)
                throw new ArgumentException("Here, we need an opened connection please");

            var commandName = this.oracleObjectNames.GetCommandName(typeName);

            return !OracleManagementUtils.TypeExists(connection, transaction, commandName);
        }

        #endregion
    }
}