using Dotmim.Sync.Builders;
using System;
using System.Text;
using Dotmim.Sync.Data;
using System.Data.Common;
using Dotmim.Sync.Log;
using System.Data;
using MySql.Data.MySqlClient;
using Dotmim.Sync.Filter;
using System.Linq;
using Dotmim.Sync.MySql.Builders;
using System.Diagnostics;

namespace Dotmim.Sync.MySql
{
    public class MySqlBuilderTrackingTable : IDbBuilderTrackingTableHelper
    {
        private ObjectNameParser tableName;
        private ObjectNameParser trackingName;
        private DmTable tableDescription;
        private MySqlConnection connection;
        private MySqlTransaction transaction;
        public FilterClauseCollection Filters { get; set; }
        private MySqlDbMetadata mySqlDbMetadata;


        public MySqlBuilderTrackingTable(DmTable tableDescription, DbConnection connection, DbTransaction transaction = null)
        {
            this.connection = connection as MySqlConnection;
            this.transaction = transaction as MySqlTransaction;
            this.tableDescription = tableDescription;
            (this.tableName, this.trackingName) = MySqlBuilder.GetParsers(this.tableDescription);
            this.mySqlDbMetadata = new MySqlDbMetadata();
        }


        public void CreateIndex()
        {


        }

        private string CreateIndexCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder();

            return stringBuilder.ToString();
        }

        public string CreateIndexScriptText()
        {
            string str = string.Concat("Create index on Tracking Table ", trackingName.QuotedString);
            return "";
        }

        public void CreatePk()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;
            try
            {
                using (var command = new MySqlCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (transaction != null)
                        command.Transaction = transaction;

                    command.CommandText = this.CreatePkCommandText();
                    command.Connection = this.connection;

                    // Sometimes we could have an empty string if pk is created during table creation
                    if (!string.IsNullOrEmpty(command.CommandText))
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
            string str = string.Concat("No need to Create Primary Key on Tracking Table since it's done during table creation ", trackingName.QuotedString);
            return "";
        }

        public string CreatePkCommandText()
        {
            return "";
        }

        public void CreateTable()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new MySqlCommand())
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

        public string CreateTableScriptText()
        {
            string str = string.Concat("Create Tracking Table ", trackingName.QuotedString);
            return MySqlBuilder.WrapScriptTextWithComments(this.CreateTableCommandText(), str);
        }

        public string CreateTableCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"CREATE TABLE {trackingName.QuotedString} (");

            // Adding the primary key
            foreach (DmColumn pkColumn in this.tableDescription.PrimaryKey.Columns)
            {
                var quotedColumnName = new ObjectNameParser(pkColumn.ColumnName.ToLowerInvariant(), "`", "`").QuotedString;

                var columnTypeString = this.mySqlDbMetadata.TryGetOwnerDbTypeString(pkColumn.OriginalDbType, pkColumn.DbType, false, false, this.tableDescription.OriginalProvider, MySqlSyncProvider.ProviderType);
                var unQuotedColumnType = new ObjectNameParser(columnTypeString, "`", "`").UnquotedString;
                var columnPrecisionString = this.mySqlDbMetadata.TryGetOwnerDbTypePrecision(pkColumn.OriginalDbType, pkColumn.DbType, false, false, pkColumn.MaxLength, pkColumn.Precision, pkColumn.Scale, this.tableDescription.OriginalProvider, MySqlSyncProvider.ProviderType);
                var columnType = $"{unQuotedColumnType} {columnPrecisionString}";

                stringBuilder.AppendLine($"{quotedColumnName} {columnType} NOT NULL, ");
            }

            // adding the tracking columns
            stringBuilder.AppendLine($"`create_scope_id` VARCHAR(36) NULL, ");
            stringBuilder.AppendLine($"`update_scope_id` VARCHAR(36) NULL, ");
            stringBuilder.AppendLine($"`create_timestamp` BIGINT NULL, ");
            stringBuilder.AppendLine($"`update_timestamp` BIGINT NULL, ");
            stringBuilder.AppendLine($"`timestamp` BIGINT NULL, ");
            stringBuilder.AppendLine($"`sync_row_is_tombstone` BIT NOT NULL default 0 , ");
            stringBuilder.AppendLine($"`last_change_datetime` DATETIME NULL, ");

            if (this.Filters != null && this.Filters.Count > 0)
                foreach (var filter in this.Filters)
                {
                    var columnFilter = this.tableDescription.Columns[filter.ColumnName.ToLowerInvariant()];

                    if (columnFilter == null)
                        throw new InvalidExpressionException($"Column {filter.ColumnName.ToLowerInvariant()} does not exist in Table {this.tableDescription.TableName.ToLowerInvariant()}");

                    var isPk = this.tableDescription.PrimaryKey.Columns.Any(dm => this.tableDescription.IsEqual(dm.ColumnName.ToLowerInvariant(), filter.ColumnName.ToLowerInvariant()));
                    if (isPk)
                        continue;


                    var quotedColumnName = new ObjectNameParser(columnFilter.ColumnName.ToLowerInvariant(), "`", "`").QuotedString;

                    var columnTypeString = this.mySqlDbMetadata.TryGetOwnerDbTypeString(columnFilter.OriginalDbType, columnFilter.DbType, false, false, this.tableDescription.OriginalProvider, MySqlSyncProvider.ProviderType);
                    var unQuotedColumnType = new ObjectNameParser(columnTypeString, "`", "`").UnquotedString;
                    var columnPrecisionString = this.mySqlDbMetadata.TryGetOwnerDbTypePrecision(columnFilter.OriginalDbType, columnFilter.DbType, false, false, columnFilter.MaxLength, columnFilter.Precision, columnFilter.Scale, this.tableDescription.OriginalProvider, MySqlSyncProvider.ProviderType);
                    var columnType = $"{unQuotedColumnType} {columnPrecisionString}";

                    var nullableColumn = columnFilter.AllowDBNull ? "NULL" : "NOT NULL";

                    stringBuilder.AppendLine($"{quotedColumnName} {columnType} {nullableColumn}, ");
                }

            stringBuilder.Append(" PRIMARY KEY (");
            for (int i = 0; i < this.tableDescription.PrimaryKey.Columns.Length; i++)
            {
                DmColumn pkColumn = this.tableDescription.PrimaryKey.Columns[i];
                var quotedColumnName = new ObjectNameParser(pkColumn.ColumnName.ToLowerInvariant(), "`", "`").QuotedObjectName;

                stringBuilder.Append(quotedColumnName);

                if (i < this.tableDescription.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
            }
            stringBuilder.Append("))");

            return stringBuilder.ToString();
        }

        public bool NeedToCreateTrackingTable()
        {
            return !MySqlManagementUtils.TableExists(connection, transaction, trackingName.UnquotedString);

        }

        public void PopulateFromBaseTable()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new MySqlCommand())
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

        private string CreatePopulateFromBaseTableCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine(string.Concat("INSERT INTO ", trackingName.QuotedString, " ("));
            StringBuilder stringBuilder1 = new StringBuilder();
            StringBuilder stringBuilder2 = new StringBuilder();
            string empty = string.Empty;
            StringBuilder stringBuilderOnClause = new StringBuilder("ON ");
            StringBuilder stringBuilderWhereClause = new StringBuilder("WHERE ");
            string str = string.Empty;
            string baseTable = "`base`";
            string sideTable = "`side`";
            foreach (var pkColumn in this.tableDescription.PrimaryKey.Columns)
            {
                var quotedColumnName = new ObjectNameParser(pkColumn.ColumnName.ToLowerInvariant(), "`", "`").QuotedString;

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

            if (Filters != null)
                foreach (var filterColumn in this.Filters)
                {
                    var isPk = this.tableDescription.PrimaryKey.Columns.Any(dm => this.tableDescription.IsEqual(dm.ColumnName.ToLowerInvariant(), filterColumn.ColumnName.ToLowerInvariant()));
                    if (isPk)
                        continue;

                    var quotedColumnName = new ObjectNameParser(filterColumn.ColumnName.ToLowerInvariant(), "`", "`").QuotedString;

                    stringBuilder6.Append(string.Concat(empty, quotedColumnName));
                    stringBuilder5.Append(string.Concat(empty, baseTable, ".", quotedColumnName));
                }

            // (list of pkeys)
            stringBuilder.Append(string.Concat(stringBuilder1.ToString(), ", "));

            stringBuilder.Append("`create_scope_id`, ");
            stringBuilder.Append("`update_scope_id`, ");
            stringBuilder.Append("`create_timestamp`, ");
            stringBuilder.Append("`update_timestamp`, ");
            stringBuilder.Append("`timestamp`, "); // timestamp is not a column we update, it's auto
            stringBuilder.Append("`sync_row_is_tombstone` ");
            stringBuilder.AppendLine(string.Concat(stringBuilder6.ToString(), ") "));
            stringBuilder.Append(string.Concat("SELECT ", stringBuilder2.ToString(), ", "));
            stringBuilder.Append("NULL, ");
            stringBuilder.Append("NULL, ");
            stringBuilder.Append($"{MySqlObjectNames.TimestampValue}, ");
            stringBuilder.Append("0, ");
            stringBuilder.Append($"{MySqlObjectNames.TimestampValue}, ");
            stringBuilder.Append("0");
            stringBuilder.AppendLine(string.Concat(stringBuilder5.ToString(), " "));
            string[] localName = new string[] { "FROM ", tableName.QuotedString, " ", baseTable, " LEFT OUTER JOIN ", trackingName.QuotedString, " ", sideTable, " " };
            stringBuilder.AppendLine(string.Concat(localName));
            stringBuilder.AppendLine(string.Concat(stringBuilderOnClause.ToString(), " "));
            stringBuilder.AppendLine(string.Concat(stringBuilderWhereClause.ToString(), "; \n"));
            return stringBuilder.ToString();
        }

        public string CreatePopulateFromBaseTableScriptText()
        {
            string str = string.Concat("Populate tracking table ", trackingName.QuotedString, " for existing data in table ", tableName.QuotedString);
            return MySqlBuilder.WrapScriptTextWithComments(this.CreatePopulateFromBaseTableCommandText(), str);
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
                using (var command = new MySqlCommand())
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

        private string AddFilterColumnCommandText(DmColumn col)
        {
            var quotedColumnName = new ObjectNameParser(col.ColumnName.ToLowerInvariant(), "`", "`").QuotedString;

            var columnTypeString = this.mySqlDbMetadata.TryGetOwnerDbTypeString(col.OriginalDbType, col.DbType, false, false, this.tableDescription.OriginalProvider, MySqlSyncProvider.ProviderType);
            var columnPrecisionString = this.mySqlDbMetadata.TryGetOwnerDbTypePrecision(col.OriginalDbType, col.DbType, false, false, col.MaxLength, col.Precision, col.Scale, this.tableDescription.OriginalProvider, MySqlSyncProvider.ProviderType);
            var columnType = $"{columnTypeString} {columnPrecisionString}";

            return string.Concat("ALTER TABLE ", quotedColumnName, " ADD ", columnType);
        }
        public string ScriptAddFilterColumn(DmColumn filterColumn)
        {
            var quotedColumnName = new ObjectNameParser(filterColumn.ColumnName.ToLowerInvariant(), "`", "`");

            string str = string.Concat("Add new filter column, ", quotedColumnName.UnquotedString, ", to Tracking Table ", trackingName.QuotedString);
            return MySqlBuilder.WrapScriptTextWithComments(this.AddFilterColumnCommandText(filterColumn), str);
        }


    }
}
