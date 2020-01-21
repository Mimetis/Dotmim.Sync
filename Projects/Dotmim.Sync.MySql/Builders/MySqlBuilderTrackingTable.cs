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
using System.Collections.Generic;

namespace Dotmim.Sync.MySql
{
    public class MySqlBuilderTrackingTable : IDbBuilderTrackingTableHelper
    {
        private ParserName tableName;
        private ParserName trackingName;
        private SyncTable tableDescription;
        private MySqlConnection connection;
        private MySqlTransaction transaction;
        public IEnumerable<SyncFilter> Filters { get; set; }
        private MySqlDbMetadata mySqlDbMetadata;


        public MySqlBuilderTrackingTable(SyncTable tableDescription, DbConnection connection, DbTransaction transaction = null)
        {
            this.connection = connection as MySqlConnection;
            this.transaction = transaction as MySqlTransaction;
            this.tableDescription = tableDescription;
            (this.tableName, this.trackingName) = MyTableSqlBuilder.GetParsers(this.tableDescription);
            this.mySqlDbMetadata = new MySqlDbMetadata();
        }


        public void CreateIndex()
        {


        }

        private string CreateIndexCommandText()
        {
            return string.Empty;
        }

        public string CreateIndexScriptText()
        {
            return string.Empty;
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
            string str = string.Concat("No need to Create Primary Key on Tracking Table since it's done during table creation ", trackingName.Quoted().ToString());
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
            string str = string.Concat("Create Tracking Table ", trackingName.Quoted().ToString());
            return MyTableSqlBuilder.WrapScriptTextWithComments(this.CreateTableCommandText(), str);
        }

        public string CreateTableCommandText()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"CREATE TABLE {trackingName.Quoted().ToString()} (");

            // Adding the primary key
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn, "`").Quoted().ToString();
                var columnTypeString = this.mySqlDbMetadata.TryGetOwnerDbTypeString(pkColumn.OriginalDbType, pkColumn.GetDbType(), false, false, pkColumn.MaxLength, this.tableDescription.OriginalProvider, MySqlSyncProvider.ProviderType);
                var unQuotedColumnType = ParserName.Parse(columnTypeString, "`").Unquoted().Normalized().ToString();

                var columnPrecisionString = this.mySqlDbMetadata.TryGetOwnerDbTypePrecision(pkColumn.OriginalDbType, pkColumn.GetDbType(), false, false, pkColumn.MaxLength, pkColumn.Precision, pkColumn.Scale, this.tableDescription.OriginalProvider, MySqlSyncProvider.ProviderType);
                var columnType = $"{unQuotedColumnType} {columnPrecisionString}";

                stringBuilder.AppendLine($"{columnName} {columnType} NOT NULL, ");
            }

            // adding the tracking columns
            stringBuilder.AppendLine($"`update_scope_id` VARCHAR(36) NULL, ");
            stringBuilder.AppendLine($"`timestamp` BIGINT NULL, ");
            stringBuilder.AppendLine($"`sync_row_is_tombstone` BIT NOT NULL default 0, ");
            stringBuilder.AppendLine($"`last_change_datetime` DATETIME NULL, ");

            if (this.Filters != null && this.Filters.Count() > 0)
                foreach (var filter in this.Filters)
                {
                    var columnFilter = this.tableDescription.Columns[filter.ColumnName];

                    if (columnFilter == null)
                        throw new InvalidExpressionException($"Column {filter.ColumnName} does not exist in Table {this.tableDescription.TableName}");

                    var isPk = this.tableDescription.GetPrimaryKeysColumns().Any(pk => pk.ColumnName.Equals(filter.ColumnName, SyncGlobalization.DataSourceStringComparison));
                    if (isPk)
                        continue;


                    var quotedColumnName = ParserName.Parse(columnFilter, "`").Quoted().ToString();
                    var columnTypeString = this.mySqlDbMetadata.TryGetOwnerDbTypeString(columnFilter.OriginalDbType, columnFilter.GetDbType(), false, false, columnFilter.MaxLength, this.tableDescription.OriginalProvider, MySqlSyncProvider.ProviderType);
                    var unQuotedColumnType = ParserName.Parse(columnTypeString, "`").Unquoted().Normalized().ToString();

                    var columnPrecisionString = this.mySqlDbMetadata.TryGetOwnerDbTypePrecision(columnFilter.OriginalDbType, columnFilter.GetDbType(), false, false, columnFilter.MaxLength, columnFilter.Precision, columnFilter.Scale, this.tableDescription.OriginalProvider, MySqlSyncProvider.ProviderType);
                    var columnType = $"{unQuotedColumnType} {columnPrecisionString}";

                    var nullableColumn = columnFilter.AllowDBNull ? "NULL" : "NOT NULL";

                    stringBuilder.AppendLine($"{quotedColumnName} {columnType} {nullableColumn}, ");
                }

            stringBuilder.Append(" PRIMARY KEY (");
            for (int i = 0; i < this.tableDescription.PrimaryKeys.Count; i++)
            {
                var pkColumn = this.tableDescription.PrimaryKeys[i];
                var quotedColumnName = ParserName.Parse(pkColumn, "`").Quoted().ToString();

                stringBuilder.Append(quotedColumnName);

                if (i < this.tableDescription.PrimaryKeys.Count - 1)
                    stringBuilder.Append(", ");
            }
            stringBuilder.Append("))");

            return stringBuilder.ToString();
        }

        public bool NeedToCreateTrackingTable()
        {
            return !MySqlManagementUtils.TableExists(connection, transaction, trackingName);

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
            stringBuilder.AppendLine(string.Concat("INSERT INTO ", trackingName.Quoted().ToString(), " ("));
            StringBuilder stringBuilder1 = new StringBuilder();
            StringBuilder stringBuilder2 = new StringBuilder();
            string empty = string.Empty;
            StringBuilder stringBuilderOnClause = new StringBuilder("ON ");
            StringBuilder stringBuilderWhereClause = new StringBuilder("WHERE ");
            string str = string.Empty;
            string baseTable = "`base`";
            string sideTable = "`side`";
            foreach (var pkColumn in this.tableDescription.PrimaryKeys)
            {
                var quotedColumnName = ParserName.Parse(pkColumn, "`").Quoted().ToString();

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
                    var isPk = this.tableDescription.PrimaryKeys.Any(pk => pk.Equals(filterColumn.ColumnName, SyncGlobalization.DataSourceStringComparison));
                    if (isPk)
                        continue;

                    var quotedColumnName = ParserName.Parse(filterColumn.ColumnName, "`").Quoted().ToString();

                    stringBuilder6.Append(string.Concat(empty, quotedColumnName));
                    stringBuilder5.Append(string.Concat(empty, baseTable, ".", quotedColumnName));
                }

            // (list of pkeys)
            stringBuilder.Append(string.Concat(stringBuilder1.ToString(), ", "));

            stringBuilder.Append("`update_scope_id`, ");
            stringBuilder.Append("`timestamp`, "); 
            stringBuilder.Append("`sync_row_is_tombstone` ");
            stringBuilder.AppendLine(string.Concat(stringBuilder6.ToString(), ") "));
            stringBuilder.Append(string.Concat("SELECT ", stringBuilder2.ToString(), ", "));
            stringBuilder.Append("NULL, ");
            stringBuilder.Append($"{MySqlObjectNames.TimestampValue}, ");
            stringBuilder.Append("0 ");
            stringBuilder.AppendLine(string.Concat(stringBuilder5.ToString(), " "));
            string[] localName = new string[] { "FROM ", tableName.Quoted().ToString(), " ", baseTable, " LEFT OUTER JOIN ", trackingName.Quoted().ToString(), " ", sideTable, " " };
            stringBuilder.AppendLine(string.Concat(localName));
            stringBuilder.AppendLine(string.Concat(stringBuilderOnClause.ToString(), " "));
            stringBuilder.AppendLine(string.Concat(stringBuilderWhereClause.ToString(), "; \n"));
            return stringBuilder.ToString();
        }

        public string CreatePopulateFromBaseTableScriptText()
        {
            string str = string.Concat("Populate tracking table ", trackingName.Quoted().ToString(), " for existing data in table ", tableName.Quoted().ToString());
            return MyTableSqlBuilder.WrapScriptTextWithComments(this.CreatePopulateFromBaseTableCommandText(), str);
        }

        public void PopulateNewFilterColumnFromBaseTable(SyncColumn filterColumn)
        {
            throw new NotImplementedException();
        }

        public string ScriptPopulateNewFilterColumnFromBaseTable(SyncColumn filterColumn)
        {
            throw new NotImplementedException();
        }

        public void AddFilterColumn(SyncColumn filterColumn)
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

        private string AddFilterColumnCommandText(SyncColumn col)
        {
            var quotedColumnName = ParserName.Parse(col, "`").Quoted().ToString();

            var columnTypeString = this.mySqlDbMetadata.TryGetOwnerDbTypeString(col.OriginalDbType, col.GetDbType(), false, false, col.MaxLength, this.tableDescription.OriginalProvider, MySqlSyncProvider.ProviderType);
            var columnPrecisionString = this.mySqlDbMetadata.TryGetOwnerDbTypePrecision(col.OriginalDbType, col.GetDbType(), false, false, col.MaxLength, col.Precision, col.Scale, this.tableDescription.OriginalProvider, MySqlSyncProvider.ProviderType);
            var columnType = $"{columnTypeString} {columnPrecisionString}";

            return string.Concat("ALTER TABLE ", quotedColumnName, " ADD ", columnType);
        }
        public string ScriptAddFilterColumn(SyncColumn filterColumn)
        {
            var quotedColumnName = ParserName.Parse(filterColumn, "`").Quoted().ToString();

            string str = string.Concat("Add new filter column, ", quotedColumnName, ", to Tracking Table ", trackingName.Quoted().ToString());
            return MyTableSqlBuilder.WrapScriptTextWithComments(this.AddFilterColumnCommandText(filterColumn), str);
        }

        public void DropTable()
        {
            var commandText = $"drop table if exists {trackingName.Quoted().ToString()}";

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                using (var command = new MySqlCommand(commandText, connection))
                {
                    if (transaction != null)
                        command.Transaction = transaction;

                    command.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during DropTableCommand : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

            }

        }

        public string DropTableScriptText()
        {
            var commandText = $"drop table if exists {trackingName.Quoted().ToString()}";

            var str1 = $"Drop table {trackingName.Quoted().ToString()}";
            return MyTableSqlBuilder.WrapScriptTextWithComments(commandText, str1);
        }

    }
}
