using System;
using System.Data;
using System.Data.Common;
using System.Data.OracleClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using Dotmim.Sync.Filter;
using Dotmim.Sync.Oracle.Manager;

namespace Dotmim.Sync.Oracle.Builder
{
    internal class OracleBuilderTrackingTable : IDbBuilderTrackingTableHelper
    {
        private DmTable tableDescription;
        private OracleConnection connection;
        private OracleTransaction transaction;

        private OracleDbMetadata oracleDbMetadata;
        private ObjectNameParser tableName;
        private ObjectNameParser trackingName;

        public OracleBuilderTrackingTable(DmTable tableDescription, DbConnection connection, DbTransaction transaction)
        {
            this.tableDescription = tableDescription;
            this.connection = connection as OracleConnection;
            this.transaction = transaction as OracleTransaction;

            (this.tableName, this.trackingName) = OracleBuilder.GetParsers(this.tableDescription);
            this.oracleDbMetadata = new OracleDbMetadata();
        }

        public FilterClauseCollection Filters { get; set; }

        #region Private Methods

        private string CreateIndexCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"CREATE UNIQUE INDEX {trackingName.UnquotedStringWithUnderScore}_index ON {trackingName.UnquotedString} (");
            stringBuilder.AppendLine($"\tupdate_timestamp");
            stringBuilder.AppendLine($"\t,update_scope_id");
            stringBuilder.AppendLine($"\t,sync_row_is_tombstone");
            // Filter columns
            if (this.Filters != null && this.Filters.Count > 0)
            {
                for (int i = 0; i < this.Filters.Count; i++)
                {
                    var filterColumn = this.Filters[i];

                    if (this.tableDescription.Columns.Any(c => c.ColumnName == filterColumn.ColumnName))
                        continue;

                    ObjectNameParser columnName = new ObjectNameParser(filterColumn.ColumnName);
                    stringBuilder.AppendLine($"\t,{columnName.QuotedString}");
                }
            }

            foreach (var pkColumn in this.tableDescription.PrimaryKey.Columns)
            {
                stringBuilder.AppendLine($"\t,{pkColumn.ColumnName}");
            }
            stringBuilder.Append(")");
            return stringBuilder.ToString();
        }

        private string CreatePkCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.Append($"ALTER TABLE {trackingName.UnquotedString} ADD CONSTRAINT PK_{trackingName.UnquotedStringWithUnderScore} PRIMARY KEY (");

            for (int i = 0; i < this.tableDescription.PrimaryKey.Columns.Length; i++)
            {
                DmColumn pkColumn = this.tableDescription.PrimaryKey.Columns[i];
                var quotedColumnName = pkColumn.ColumnName;

                stringBuilder.Append(quotedColumnName);

                if (i < this.tableDescription.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
            }
            stringBuilder.Append(")");

            return stringBuilder.ToString();
        }

        private string CreateDropTableCommandText()
        {
            return $"DROP TABLE {trackingName.QuotedString};";
        }

        private string CreateTableCommandText()
        {
            bool isFilter = false;
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"CREATE TABLE {trackingName.ObjectName} (");

            // Adding the primary key
            foreach (DmColumn pkColumn in this.tableDescription.PrimaryKey.Columns)
            {
                var quotedColumnName = pkColumn.ColumnName;

                var columnTypeString = this.oracleDbMetadata.TryGetOwnerDbTypeString(pkColumn.OriginalDbType, pkColumn.DbType, false, false, this.tableDescription.OriginalProvider, OracleSyncProvider.ProviderType);
                var quotedColumnType = columnTypeString;
                var columnPrecisionString = this.oracleDbMetadata.TryGetOwnerDbTypePrecision(pkColumn.OriginalDbType, pkColumn.DbType, false, false, pkColumn.MaxLength, pkColumn.Precision, pkColumn.Scale, this.tableDescription.OriginalProvider, OracleSyncProvider.ProviderType);
                var columnType = $"{quotedColumnType} {columnPrecisionString}";

                var nullableColumn = pkColumn.AllowDBNull ? "NULL" : "NOT NULL";
                stringBuilder.AppendLine($"{quotedColumnName} {columnType} {nullableColumn}, ");
            }

            // adding the tracking columns
            stringBuilder.AppendLine($"create_scope_id VARCHAR2(200) NULL, ");
            stringBuilder.AppendLine($"update_scope_id VARCHAR2(200) NULL, ");
            stringBuilder.AppendLine($"create_timestamp NUMBER(20) NULL, ");
            stringBuilder.AppendLine($"update_timestamp NUMBER(20) NULL, ");
            stringBuilder.AppendLine($"timestamp NUMBER(20) NULL, ");
            stringBuilder.AppendLine($"sync_row_is_tombstone number(1) default(0) NOT NULL , ");
            stringBuilder.Append($"last_change_datetime date NULL");

            // adding the filter columns
            if (this.Filters != null && this.Filters.Count > 0)
            {
                isFilter = true;
                stringBuilder.AppendLine(", ");
                foreach (var filter in this.Filters)
                {
                    var columnFilter = this.tableDescription.Columns[filter.ColumnName];

                    if (columnFilter == null)
                        throw new InvalidExpressionException($"Column {filter.ColumnName} does not exist in Table {this.tableDescription.TableName}");

                    var isPk = this.tableDescription.PrimaryKey.Columns.Any(dm => this.tableDescription.IsEqual(dm.ColumnName, filter.ColumnName));
                    if (isPk)
                        continue;


                    var quotedColumnName = new ObjectNameParser(columnFilter.ColumnName, "", "").QuotedString;

                    var columnTypeString = this.oracleDbMetadata.TryGetOwnerDbTypeString(columnFilter.OriginalDbType, columnFilter.DbType, false, false, this.tableDescription.OriginalProvider, OracleSyncProvider.ProviderType);
                    var quotedColumnType = new ObjectNameParser(columnTypeString, "", "").QuotedString;
                    var columnPrecisionString = this.oracleDbMetadata.TryGetOwnerDbTypePrecision(columnFilter.OriginalDbType, columnFilter.DbType, false, false, columnFilter.MaxLength, columnFilter.Precision, columnFilter.Scale, this.tableDescription.OriginalProvider, OracleSyncProvider.ProviderType);
                    var columnType = $"{quotedColumnType} {columnPrecisionString}";

                    var nullableColumn = columnFilter.AllowDBNull ? "NULL" : "NOT NULL";

                    stringBuilder.Append($"{quotedColumnName} {columnType} {nullableColumn} ");
                }
            }
            stringBuilder.Append(")");
            return stringBuilder.ToString();
        }

        private string CreatePopulateFromBaseTableCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine(string.Concat("INSERT INTO ", trackingName.UnquotedString, " ("));
            StringBuilder stringBuilder1 = new StringBuilder();
            StringBuilder stringBuilder2 = new StringBuilder();
            string empty = string.Empty;
            StringBuilder stringBuilderOnClause = new StringBuilder("ON ");
            StringBuilder stringBuilderWhereClause = new StringBuilder("WHERE ");
            string str = string.Empty;
            string baseTable = "base";
            string sideTable = "side";
            foreach (var pkColumn in this.tableDescription.PrimaryKey.Columns)
            {
                var quotedColumnName = pkColumn.ColumnName;

                stringBuilder1.Append(string.Concat(empty, quotedColumnName));

                stringBuilder2.Append(string.Concat(empty, baseTable, ".", quotedColumnName));

                string[] quotedName = new string[] { str, baseTable, ".", quotedColumnName, " = ", sideTable, ".", quotedColumnName };
                stringBuilderOnClause.Append(string.Concat(quotedName));
                string[] strArrays = new string[] { str, sideTable, ".", quotedColumnName, " IS NULL" };
                stringBuilderWhereClause.Append(string.Concat(strArrays));
                empty = ", ";
                str = " AND ";
            }
            StringBuilder stringBuilder5 = new StringBuilder();
            StringBuilder stringBuilder6 = new StringBuilder();

            if (Filters != null && Filters.Count > 0)
                foreach (var filter in this.Filters)
                {
                    var columnFilter = this.tableDescription.Columns[filter.ColumnName];

                    if (columnFilter == null)
                        throw new InvalidExpressionException($"Column {filter.ColumnName} does not exist in Table {this.tableDescription.TableName}");

                    var isPk = this.tableDescription.PrimaryKey.Columns.Any(dm => this.tableDescription.IsEqual(dm.ColumnName, columnFilter.ColumnName));
                    if (isPk)
                        continue;

                    var quotedColumnName = columnFilter.ColumnName;

                    stringBuilder6.Append(string.Concat(empty, quotedColumnName));
                    stringBuilder5.Append(string.Concat(empty, baseTable, ".", quotedColumnName));
                }

            // (list of pkeys)
            stringBuilder.Append(string.Concat(stringBuilder1.ToString(), ", "));

            stringBuilder.Append("create_scope_id, ");
            stringBuilder.Append("update_scope_id, ");
            stringBuilder.Append("create_timestamp, ");
            stringBuilder.Append("update_timestamp, ");
            //stringBuilder.Append("[timestamp], "); // timestamp is not a column we update, it's auto
            stringBuilder.Append("sync_row_is_tombstone ");
            stringBuilder.AppendLine(string.Concat(stringBuilder6.ToString(), ") "));
            stringBuilder.Append(string.Concat("SELECT ", stringBuilder2.ToString(), ", "));
            stringBuilder.Append("NULL, ");
            stringBuilder.Append("NULL, ");
            stringBuilder.Append("to_number(to_char(systimestamp, 'YYYYMMDDHH24MISSFF3')), ");
            stringBuilder.Append("0, ");
            //stringBuilder.Append("@@DBTS+1, "); // timestamp is not a column we update, it's auto
            stringBuilder.Append("0");
            stringBuilder.AppendLine(string.Concat(stringBuilder5.ToString(), " "));
            string[] localName = new string[] { "FROM ", tableName.UnquotedString, " ", baseTable, " LEFT OUTER JOIN ", trackingName.UnquotedString, " ", sideTable, " " };
            stringBuilder.AppendLine(string.Concat(localName));
            stringBuilder.AppendLine(string.Concat(stringBuilderOnClause.ToString(), " "));
            stringBuilder.AppendLine(string.Concat(stringBuilderWhereClause.ToString(), " \n"));
            return stringBuilder.ToString();
        }

        private string AddFilterColumnCommandText(DmColumn col)
        {
            var quotedColumnName = new ObjectNameParser(col.ColumnName, "", "").QuotedString;
            var quotedColumnType = new ObjectNameParser(col.OriginalDbType, "", "").QuotedString;

            var columnTypeString = this.oracleDbMetadata.TryGetOwnerDbTypeString(col.OriginalDbType, col.DbType, false, false, this.tableDescription.OriginalProvider, OracleSyncProvider.ProviderType);
            var columnPrecisionString = this.oracleDbMetadata.TryGetOwnerDbTypePrecision(col.OriginalDbType, col.DbType, false, false, col.MaxLength, col.Precision, col.Scale, this.tableDescription.OriginalProvider, OracleSyncProvider.ProviderType);
            var columnType = $"{columnTypeString} {columnPrecisionString}";

            return string.Concat("ALTER TABLE ", quotedColumnName, " ADD ", columnType);
        }

        #endregion

        public void CreateIndex()
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

                    command.CommandText = this.CreateIndexCommandText();
                    command.Connection = this.connection;
                    command.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during CreateIndex : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();

            }

        }

        public string CreateIndexScriptText()
        {
            string str = string.Concat("Create index on Tracking Table ", trackingName.QuotedString);
            return OracleBuilder.WrapScriptTextWithComments(this.CreateIndexCommandText(), str);
        }

        public void CreatePk()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new OracleCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (transaction != null)
                        command.Transaction = transaction;

                    command.CommandText = this.CreatePkCommandText();
                    command.Connection = this.connection;
                    command.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during CreateIndex : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();
            }
        }

        public string CreatePkScriptText()
        {
            string str = string.Concat("Create Primary Key on Tracking Table ", trackingName.QuotedString);
            return OracleBuilder.WrapScriptTextWithComments(this.CreatePkCommandText(), str);
        }

        public void CreateTable()
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

                    command.CommandText = this.CreateTableCommandText();
                    command.Connection = this.connection;
                    command.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during CreateIndex : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();

            }


        }

        public void DropTable()
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

                    command.CommandText = this.CreateDropTableCommandText();
                    command.Connection = this.connection;
                    command.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during DropTable : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();

            }


        }

        public string CreateTableScriptText()
        {
            string str = string.Concat("Create Tracking Table ", trackingName.QuotedString);
            return OracleBuilder.WrapScriptTextWithComments(this.CreateTableCommandText(), str);
        }

        public string DropTableScriptText()
        {
            string str = string.Concat("Droping Tracking Table ", trackingName.QuotedString);
            return OracleBuilder.WrapScriptTextWithComments(this.CreateTableCommandText(), str);
        }

        public bool NeedToCreateTrackingTable()
        {
            return !OracleManagementUtils.TableExists(connection, transaction, trackingName.QuotedString);
        }

        public void PopulateFromBaseTable()
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

                    command.CommandText = this.CreatePopulateFromBaseTableCommandText();
                    command.Connection = this.connection;
                    command.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during CreateIndex : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();

            }

        }

        public string CreatePopulateFromBaseTableScriptText()
        {
            string str = string.Concat("Populate tracking table ", trackingName.QuotedString, " for existing data in table ", tableName.QuotedString);
            return OracleBuilder.WrapScriptTextWithComments(this.CreatePopulateFromBaseTableCommandText(), str);
        }

        public void PopulateNewFilterColumnFromBaseTable(DmColumn filterColumn)
        {
            throw new NotImplementedException();
        }

        public string ScriptPopulateNewFilterColumnFromBaseTable(DmColumn filterColumn)
        {
            throw new NotImplementedException();
        }

        public void AddFilterColumn(DmColumn filterColumn)
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

                    command.CommandText = this.AddFilterColumnCommandText(filterColumn);
                    command.Connection = this.connection;
                    command.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during CreateIndex : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();

            }

        }

        public string ScriptAddFilterColumn(DmColumn filterColumn)
        {
            var quotedColumnName = new ObjectNameParser(filterColumn.ColumnName, "[", "]");

            string str = string.Concat("Add new filter column, ", quotedColumnName.UnquotedString, ", to Tracking Table ", trackingName.QuotedString);
            return OracleBuilder.WrapScriptTextWithComments(this.AddFilterColumnCommandText(filterColumn), str);
        }
    }
}
