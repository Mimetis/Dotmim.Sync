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

        #region Bulk Procedure

        private void CreateBulkTable()
        {
            var bulkTableName = oracleObjectNames.GetCommandName(DbCommandType.BulkTable);
            var bulkType = oracleObjectNames.GetCommandName(DbCommandType.BulkTableType);

            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                if (!OracleManagementUtils.TableExists(connection, transaction, bulkTableName))
                {
                    command.CommandText = $"CREATE TABLE {bulkTableName} OF {bulkType}";
                    command.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during CreateBulkTable : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

                if (command != null)
                    command.Dispose();
            }
        }

        private void CreateTemporyBulkTable()
        {
            var bulkTemporyTableName = oracleObjectNames.GetCommandName(DbCommandType.BulkTemporyTable);

            var command = connection.CreateCommand();
            if (transaction != null)
                command.Transaction = transaction;
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                command.CommandText = this.CreateTemporyBulkTableCommandText();
                command.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during PreExecuteBatchCommand : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

                if (command != null)
                    command.Dispose();
            }
        }

        private string CreateTemporyBulkTableCommandText()
        {
            var bulkTemporyTableName = oracleObjectNames.GetCommandName(DbCommandType.BulkTemporyTable);
            StringBuilder stringBuilder = new StringBuilder($"CREATE TABLE {bulkTemporyTableName} (");
            string empty = string.Empty;
            stringBuilder.AppendLine();
            foreach (var column in this.tableDescription.PrimaryKey.Columns)
            {
                var columnName = new ObjectNameParser(column.ColumnName);

                var columnTypeString = this.oracleDbMetadata.TryGetOwnerDbTypeString(column.OriginalDbType, column.DbType, false, false, this.tableDescription.OriginalProvider, OracleSyncProvider.ProviderType);
                var columnPrecisionString = this.oracleDbMetadata.TryGetOwnerDbTypePrecision(column.OriginalDbType, column.DbType, false, false, column.MaxLength, column.Precision, column.Scale, this.tableDescription.OriginalProvider, OracleSyncProvider.ProviderType);
                var columnType = $"{columnTypeString} {columnPrecisionString}";
                var identity = string.Empty;
                var nullString = column.AllowDBNull ? "NULL" : "NOT NULL";

                stringBuilder.AppendLine($"\t{empty}{columnName.UnquotedString} {columnType} {identity} {nullString}");
                empty = ",";
            }
            stringBuilder.Append(")");

            return stringBuilder.ToString();
        }

        //------------------------------------------------------------------
        // Create Select Rows : OK
        //------------------------------------------------------------------
        private string BulkSelectUnsuccessfulRows()
        {
            var bulkTemporyTableName = oracleObjectNames.GetCommandName(DbCommandType.BulkTemporyTable);
            var bulkTableName = oracleObjectNames.GetCommandName(DbCommandType.BulkTable);

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("--Select all ids not deleted for conflict");
            stringBuilder.Append("open cur for SELECT ");
            for (int i = 0; i < this.tableDescription.PrimaryKey.Columns.Length; i++)
            {
                var cc = new ObjectNameParser(this.tableDescription.PrimaryKey.Columns[i].ColumnName);
                stringBuilder.Append($"{cc.UnquotedString}");
                if (i < this.tableDescription.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
                else
                    stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine($"FROM {bulkTableName} t");
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
            stringBuilder.AppendLine($"\t FROM {bulkTemporyTableName} i");
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
            stringBuilder.AppendLine("\t);");
            return stringBuilder.ToString();
        }

        //------------------------------------------------------------------
        // Create TVP command  : OK
        //------------------------------------------------------------------
        private string CreateTVPTypeCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.BulkTableType);
            stringBuilder.AppendLine($"CREATE or REPLACE TYPE {commandName} AS OBJECT (");
            string str = "";
            foreach (var c in this.tableDescription.Columns.Where(col => !col.ReadOnly))
            {
                var isPrimaryKey = this.tableDescription.PrimaryKey.Columns.Any(cc => this.tableDescription.IsEqual(cc.ColumnName, c.ColumnName));
                var columnName = new ObjectNameParser(c.ColumnName);
                
                // Get the good SqlDbType (even if we are not from Sql Server def)
                var sqlDbTypeString = this.oracleDbMetadata.TryGetOwnerDbTypeString(c.OriginalDbType, c.DbType, false, false, this.tableDescription.OriginalProvider, OracleSyncProvider.ProviderType);
                var quotedColumnType = new ObjectNameParser(sqlDbTypeString).UnquotedString;
                quotedColumnType += this.oracleDbMetadata.TryGetOwnerDbTypePrecision(c.OriginalDbType, c.DbType, false, false, c.MaxLength, c.Precision, c.Scale, this.tableDescription.OriginalProvider, OracleSyncProvider.ProviderType);

                stringBuilder.AppendLine($"{str}{columnName.UnquotedString} {quotedColumnType}");
                str = ", ";
            }
            stringBuilder.AppendLine(", create_scope_id VARCHAR2(200) NULL");
            stringBuilder.AppendLine(", create_timestamp NUMBER(20) NULL");
            stringBuilder.AppendLine(", update_scope_id VARCHAR2(200) NULL");
            stringBuilder.AppendLine(", update_timestamp NUMBER(20) NULL");

            stringBuilder.Append(")");
            return stringBuilder.ToString();
        }

        //------------------------------------------------------------------
        // Bulk Insert command
        //------------------------------------------------------------------
        private OracleCommand BuildBulkInsertCommand()
        {
            var bulkTemporyTableName = oracleObjectNames.GetCommandName(DbCommandType.BulkTemporyTable);
            var bulkTableName = oracleObjectNames.GetCommandName(DbCommandType.BulkTable);
            OracleCommand sqlCommand = new OracleCommand();

            OracleParameter sqlParameter = new OracleParameter("sync_min_timestamp", OracleType.Number);
            sqlCommand.Parameters.Add(sqlParameter);
            OracleParameter sqlParameter1 = new OracleParameter("sync_scope_id", OracleType.NVarChar);
            sqlCommand.Parameters.Add(sqlParameter1);
            OracleParameter sqlParameter2 = new OracleParameter("cur", OracleType.Cursor);
            sqlParameter2.Direction = ParameterDirection.Output;
            sqlCommand.Parameters.Add(sqlParameter2);

            string str4 = OracleManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "p", "t");
            string str5 = OracleManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "changes", "base");
            string str6 = OracleManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "changes", "side");
            string str7 = OracleManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "bk", "tt");
            string str8 = OracleManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "bk", "side");

            StringBuilder stringBuilder = new StringBuilder();
            StringBuilder stringBuilderArguments = new StringBuilder();
            StringBuilder stringBuilderParameters = new StringBuilder();

            string empty = string.Empty, str = "";
            foreach (var mutableColumn in this.tableDescription.Columns.Where(c => !c.ReadOnly))
            {
                ObjectNameParser columnName = new ObjectNameParser(mutableColumn.ColumnName);
                stringBuilderArguments.Append(string.Concat(empty, columnName.UnquotedString));
                stringBuilderParameters.Append(string.Concat(empty, $"changes.{columnName.UnquotedString}"));
                empty = ", ";
            }

            stringBuilder.AppendLine("-- truncate tempory table");
            stringBuilder.Append($"execute immediate 'TRUNCATE TABLE {bulkTemporyTableName}';");
            stringBuilder.AppendLine();


            string BuilderPKString(string baseString, string st = ", ")
            {
                string s = "";
                StringBuilder pk = new StringBuilder();
                foreach (var c in this.tableDescription.PrimaryKey.Columns)
                {
                    var columnName = new ObjectNameParser(c.ColumnName);
                    if (baseString.Equals(string.Empty))
                        pk.Append($"{s}{columnName.UnquotedString}");
                    else
                        pk.Append($"{s}{baseString}.{columnName.UnquotedString}");
                    s = st;
                }
                return pk.ToString();
            }

            stringBuilder.AppendLine();
            stringBuilder.AppendLine($"INSERT INTO {bulkTemporyTableName} ({BuilderPKString("")})");
            stringBuilder.AppendLine($"SELECT {BuilderPKString("changes")} FROM {tableName.UnquotedString} base");
            stringBuilder.AppendLine($"RIGHT JOIN (SELECT");
            stringBuilder.Append($"{ BuilderPKString("p")}");
            stringBuilder.AppendLine($" FROM {bulkTableName} p");
            stringBuilder.AppendLine($"LEFT JOIN {trackingName.UnquotedString} t ON {str4}");
            stringBuilder.AppendLine($"    ) changes");
            stringBuilder.AppendLine($"ON {str5}");
            stringBuilder.AppendLine($"WHERE ");
            str = "";
            foreach (var c in this.tableDescription.PrimaryKey.Columns)
            {
                var columnName = new ObjectNameParser(c.ColumnName);
                stringBuilder.Append($" {str}base.{columnName.UnquotedString} is null ");
                str = " AND ";
            }

            stringBuilder.AppendLine(";");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine("-- update/insert into the base table");
            stringBuilder.AppendLine($"MERGE INTO {tableName.UnquotedString} base USING");
            stringBuilder.AppendLine("\t-- join done here against the side table to get the local timestamp for concurrency check\n");

            stringBuilder.AppendLine($"\t(SELECT ");
            str = "";
            stringBuilder.Append("\t");
            foreach (var c in this.tableDescription.Columns.Where(col => !col.ReadOnly))
            {
                var columnName = new ObjectNameParser(c.ColumnName);
                stringBuilder.Append($"{str}p.{columnName.UnquotedString}");
                str = ", ";
            }
            stringBuilder.AppendLine("\t, p.create_timestamp, p.update_timestamp");
            stringBuilder.AppendLine("\t, t.update_scope_id, t.timestamp");

            stringBuilder.AppendLine($"\tFROM {bulkTableName} p ");
            stringBuilder.Append($"\tLEFT JOIN {trackingName.UnquotedString} t ON ");
            stringBuilder.AppendLine($"\t{str4}");
            stringBuilder.AppendLine($"\t) changes ON ({str5})");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("-- Si la ligne n'existe pas en local et qu'elle a été créé avant le timestamp de référence");
            stringBuilder.Append("WHEN NOT MATCHED THEN");
            stringBuilder.AppendLine($"\tINSERT");
            stringBuilder.AppendLine($"\t({stringBuilderArguments.ToString()})");
            stringBuilder.AppendLine($"\tVALUES ({stringBuilderParameters.ToString()}) ");
            stringBuilder.AppendLine("WHERE (changes.timestamp <= sync_min_timestamp OR changes.timestamp IS NULL);");

            stringBuilder.AppendLine();

            stringBuilder.AppendLine("-- Since the insert trigger is passed, we update the tracking table to reflect the real scope inserter");
            stringBuilder.AppendLine($"UPDATE {trackingName.UnquotedString} side SET");
            stringBuilder.AppendLine("\tupdate_scope_id = sync_scope_id,");
            stringBuilder.AppendLine("\tcreate_scope_id = sync_scope_id,");
            stringBuilder.AppendLine($"\tupdate_timestamp = (select update_timestamp from {bulkTableName} bk INNER JOIN {bulkTemporyTableName}  tt ON {str7} AND {str8}),");
            stringBuilder.AppendLine($"\tcreate_timestamp = (select create_timestamp from {bulkTableName} bk INNER JOIN {bulkTemporyTableName}  tt ON {str7} AND {str8})");
            stringBuilder.AppendLine($"WHERE EXISTS (SELECT {BuilderPKString("changes")} FROM ");
            stringBuilder.AppendLine($"(SELECT {BuilderPKString("p")}");
            stringBuilder.AppendLine($"FROM {bulkTemporyTableName} t ");
            stringBuilder.AppendLine($"JOIN {bulkTableName} p ON");
            stringBuilder.AppendLine($"{str4} ) changes ");
            stringBuilder.AppendLine($"WHERE {str6} );");

            stringBuilder.AppendLine(BulkSelectUnsuccessfulRows());

            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        //------------------------------------------------------------------
        // Bulk Delete command
        //------------------------------------------------------------------
        private OracleCommand BuildBulkDeleteCommand()
        {
            var bulkTemporyTableName = oracleObjectNames.GetCommandName(DbCommandType.BulkTemporyTable);
            var bulkTableName = oracleObjectNames.GetCommandName(DbCommandType.BulkTable);
            OracleCommand sqlCommand = new OracleCommand();

            string BuilderPKString(string baseString, string st = ", ")
            {
                string s = "";
                StringBuilder pk = new StringBuilder();
                foreach (var c in this.tableDescription.PrimaryKey.Columns)
                {
                    var columnName = new ObjectNameParser(c.ColumnName);
                    if (baseString.Equals(string.Empty))
                        pk.Append($"{s}{columnName.UnquotedString}");
                    else
                        pk.Append($"{s}{baseString}.{columnName.UnquotedString}");
                    s = st;
                }
                return pk.ToString();
            }

            OracleParameter sqlParameter = new OracleParameter("sync_min_timestamp", OracleType.Number);
            sqlCommand.Parameters.Add(sqlParameter);
            OracleParameter sqlParameter1 = new OracleParameter("sync_scope_id", OracleType.VarChar);
            sqlCommand.Parameters.Add(sqlParameter1);
            OracleParameter sqlParameter2 = new OracleParameter("cur", OracleType.Cursor);
            sqlParameter2.Direction = ParameterDirection.Output;
            sqlCommand.Parameters.Add(sqlParameter2);


            string str4 = OracleManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "p", "t");
            string str5 = OracleManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "changes", "base");
            string str6 = OracleManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "changes", "side");
            string str7 = OracleManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "bk", "tt");
            string str8 = OracleManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "bk", "side");
            string str9 = OracleManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "p", "base");

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("-- truncate tempory table");
            stringBuilder.Append($"execute immediate 'TRUNCATE TABLE {bulkTemporyTableName}';");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"INSERT INTO {bulkTemporyTableName} ({BuilderPKString("")})");
            stringBuilder.AppendLine($"SELECT {BuilderPKString("base")} FROM {tableName.UnquotedString} base");
            stringBuilder.AppendLine($"RIGHT JOIN (SELECT");
            stringBuilder.Append($"{ BuilderPKString("p")}");
            stringBuilder.AppendLine($" FROM {bulkTableName} p");
            stringBuilder.AppendLine($"LEFT JOIN {trackingName.UnquotedString} t ON {str4}");
            stringBuilder.AppendLine($"    ) changes");
            stringBuilder.AppendLine($"ON {str5}");
            stringBuilder.AppendLine($"WHERE ");
            string str = "";
            foreach (var c in this.tableDescription.PrimaryKey.Columns)
            {
                var columnName = new ObjectNameParser(c.ColumnName);
                stringBuilder.Append($" {str}base.{columnName.UnquotedString} is not null ");
                str = " AND ";
            }

            stringBuilder.AppendLine(";");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"-- delete all items where timestamp <= sync_min_timestamp or update_scope_id = sync_scope_id");

            stringBuilder.AppendLine($"DELETE");
            stringBuilder.AppendLine($"FROM {tableName.UnquotedString} base");
            stringBuilder.AppendLine($"WHERE EXISTS ( SELECT ");
            stringBuilder.AppendLine($"{BuilderPKString("p")}, p.create_timestamp, p.update_timestamp");
            stringBuilder.AppendLine(", t.update_scope_id, t.timestamp");
            stringBuilder.AppendLine($"FROM {bulkTableName} p");
            stringBuilder.AppendLine($"INNER JOIN {trackingName.UnquotedString} t ON");
            stringBuilder.AppendLine($"{str4}");
            stringBuilder.AppendLine($"WHERE ");
            stringBuilder.AppendLine($"{str9}");
            stringBuilder.AppendLine($" AND (t.update_scope_id = sync_scope_id OR t.timestamp <= sync_min_timestamp ));");

            stringBuilder.AppendLine();

            stringBuilder.AppendLine("-- Since the delete trigger is passed, we update the tracking table to reflect the real scope deleter");
            stringBuilder.AppendLine($"UPDATE {trackingName.UnquotedString} side SET");
            stringBuilder.AppendLine("\tsync_row_is_tombstone = 1, ");
            stringBuilder.AppendLine("\tupdate_scope_id = sync_scope_id,");
            stringBuilder.AppendLine($"\tupdate_timestamp = (select update_timestamp from {bulkTableName} bk INNER JOIN {bulkTemporyTableName} tt ON {str7} AND {str8})");
            stringBuilder.AppendLine("WHERE EXISTS (");
            stringBuilder.AppendLine($"\tSELECT {BuilderPKString("changes")} FROM (");
            stringBuilder.AppendLine($"\tSELECT {BuilderPKString("p")} FROM {bulkTemporyTableName} t");
            stringBuilder.AppendLine($"\tINNER JOIN {bulkTableName} p ON ");
            stringBuilder.AppendLine($"\t{str4}");
            stringBuilder.AppendLine($"\t ) changes");
            stringBuilder.AppendLine($"\t WHERE {str6} );");

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
            var bulkTemporyTableName = oracleObjectNames.GetCommandName(DbCommandType.BulkTemporyTable);
            var bulkTableName = oracleObjectNames.GetCommandName(DbCommandType.BulkTable);
            OracleCommand sqlCommand = new OracleCommand();

            OracleParameter sqlParameter = new OracleParameter("sync_min_timestamp", OracleType.Number);
            sqlCommand.Parameters.Add(sqlParameter);
            OracleParameter sqlParameter1 = new OracleParameter("sync_scope_id", OracleType.VarChar);
            sqlCommand.Parameters.Add(sqlParameter1);
            OracleParameter sqlParameter2 = new OracleParameter("cur", OracleType.Cursor);
            sqlParameter2.Direction = ParameterDirection.Output;
            sqlCommand.Parameters.Add(sqlParameter2);


            string str4 = OracleManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "p", "t");
            string str5 = OracleManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "changes", "base");
            string str6 = OracleManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "changes", "side");
            string str7 = OracleManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "bk", "tt");
            string str8 = OracleManagementUtils.JoinTwoTablesOnClause(this.tableDescription.PrimaryKey.Columns, "bk", "side");

            string BuilderPKString(string baseString, string st = ", ")
            {
                string s = "";
                StringBuilder pk = new StringBuilder();
                foreach (var c in this.tableDescription.PrimaryKey.Columns)
                {
                    var columnName = new ObjectNameParser(c.ColumnName);
                    if (baseString.Equals(string.Empty))
                        pk.Append($"{s}{columnName.UnquotedString}");
                    else
                        pk.Append($"{s}{baseString}.{columnName.UnquotedString}");
                    s = st;
                }
                return pk.ToString();
            }

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine("is_update := 0;");
            stringBuilder.AppendLine("-- truncate tempory table");
            stringBuilder.Append($"execute immediate 'TRUNCATE TABLE {bulkTemporyTableName}';");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"INSERT INTO {bulkTemporyTableName} ({BuilderPKString("")})");
            stringBuilder.AppendLine($"SELECT {BuilderPKString("changes")} FROM {tableName.UnquotedString} base");
            stringBuilder.AppendLine($"RIGHT JOIN (SELECT {BuilderPKString("p")}");
            stringBuilder.AppendLine($"FROM {bulkTableName} p");
            stringBuilder.AppendLine($"LEFT JOIN {trackingName.UnquotedString} t ON {str4}");
            stringBuilder.AppendLine($"    ) changes");
            stringBuilder.AppendLine($"ON {str5}");
            stringBuilder.AppendLine($"WHERE ");
            string str = "";
            foreach (var c in this.tableDescription.PrimaryKey.Columns)
            {
                var columnName = new ObjectNameParser(c.ColumnName);
                stringBuilder.Append($" {str}base.{columnName.UnquotedString} is null ");
                str = " AND ";
            }
            stringBuilder.AppendLine(";");

            stringBuilder.AppendLine();

            stringBuilder.AppendLine("-- update the base table");
            stringBuilder.AppendLine($"MERGE INTO {tableName.UnquotedString} base USING");

            stringBuilder.AppendLine($"\t(SELECT ");
            str = "";
            stringBuilder.Append("\t");
            foreach (var c in this.tableDescription.Columns.Where(col => !col.ReadOnly))
            {
                var columnName = new ObjectNameParser(c.ColumnName);
                stringBuilder.Append($"{str}p.{columnName.UnquotedString}");
                str = ", ";
            }
            stringBuilder.AppendLine("\t, p.create_timestamp, p.update_timestamp");
            stringBuilder.AppendLine("\t, t.update_scope_id, t.timestamp");

            stringBuilder.AppendLine($"\tFROM {bulkTableName} p ");
            stringBuilder.Append($"\tLEFT JOIN {trackingName.UnquotedString} t ON ");
            stringBuilder.AppendLine($" {str4}");
            stringBuilder.AppendLine($"\t) changes ON ({str5})");
            stringBuilder.AppendLine();

            stringBuilder.AppendLine("WHEN MATCHED THEN");

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
            stringBuilder.AppendLine("WHERE changes.update_scope_id = sync_scope_id OR changes.timestamp <= sync_min_timestamp; ");

            stringBuilder.AppendLine("is_update := SQL%ROWCOUNT;");

            stringBuilder.AppendLine("-- Since the update trigger is passed, we update the tracking table to reflect the real scope updater");
            stringBuilder.AppendLine($"UPDATE {trackingName.UnquotedString} side SET");
            stringBuilder.AppendLine("\tupdate_scope_id = sync_scope_id,");
            stringBuilder.AppendLine($"\tupdate_timestamp = (select update_timestamp from {bulkTableName} bk INNER JOIN {bulkTemporyTableName}  tt ON {str7} AND {str8})");
            stringBuilder.AppendLine($"WHERE EXISTS (SELECT {BuilderPKString("changes")} FROM ");
            stringBuilder.AppendLine($"\t (SELECT {BuilderPKString("p")}");
            stringBuilder.AppendLine($"\tFROM {bulkTemporyTableName} t");
            stringBuilder.AppendLine($"\tJOIN {bulkTableName} p ON ");
            stringBuilder.AppendLine($"\t{str4} ) changes");
            stringBuilder.AppendLine($"WHERE {str6}); ");
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("IF is_update > 0 THEN");
            stringBuilder.Append(BulkSelectUnsuccessfulRows());
            stringBuilder.AppendLine("END IF;");

            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        #endregion

        #region Unitary Procedure

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

                    builderFilter.Append($"side.{columnFilterName.UnquotedString} = {columnFilterName.UnquotedString}{filterSeparationString}");
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

                    builderFilter.Append($"side.{columnFilterName.UnquotedString} IS NULL{filterSeparationString}");
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
            stringBuilder.AppendLine("IF l_count = 0 THEN ");
            if (hasAutoIncColumn)
            {
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

        #endregion

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
            var commandName = this.oracleObjectNames.GetCommandName(DbCommandType.BulkUpdateRows);
            CreateProcedureCommand(BuildBulkUpdateCommand, commandName, new List<string> { "is_update number;" });
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

                CreateBulkTable();
                CreateTemporyBulkTable();
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