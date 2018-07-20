using Dotmim.Sync.Builders;
using System;
using System.Text;
using Dotmim.Sync.Data;
using System.Data.Common;
using Dotmim.Sync.Log;
using System.Data;
using Microsoft.Data.Sqlite;
using Dotmim.Sync.Filter;
using System.Diagnostics;
using System.Collections.Generic;

namespace Dotmim.Sync.Sqlite
{
    public class SqliteBuilderTrackingTable : IDbBuilderTrackingTableHelper
    {
        private ObjectNameParser tableName;
        private ObjectNameParser trackingName;
        private DmTable tableDescription;
        private SqliteConnection connection;
        private SqliteTransaction transaction;
        private SqliteDbMetadata sqliteDbMetadata;

        public ICollection<FilterClause> Filters { get; set; }


        public SqliteBuilderTrackingTable(DmTable tableDescription, DbConnection connection, DbTransaction transaction = null)
        {
            this.connection = connection as SqliteConnection;
            this.transaction = transaction as SqliteTransaction;
            this.tableDescription = tableDescription;
            (this.tableName, this.trackingName) = SqliteBuilder.GetParsers(this.tableDescription);
            this.sqliteDbMetadata = new SqliteDbMetadata();
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
            string str = string.Concat("Create index on Tracking Table ", trackingName.FullQuotedString);
            return "";
        }

        public void CreatePk()
        {
            return;

            //bool alreadyOpened = this.connection.State == ConnectionState.Open;
            //try
            //{
            //    using (var command = new SqliteCommand())
            //    {
            //        if (!alreadyOpened)
            //            this.connection.Open();

            //        if (transaction != null)
            //            command.Transaction = transaction;

            //        command.CommandText = this.CreatePkCommandText();
            //        command.Connection = this.connection;
            //        command.ExecuteNonQuery();
            //    }
            //}
            //catch (Exception ex)
            //{
            //    Debug.WriteLine($"Error during CreateIndex : {ex}");
            //    throw;
            //}
            //finally
            //{
            //    if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
            //        this.connection.Close();
            //}
        }
        public string CreatePkScriptText()
        {
            string str = string.Concat("No need to Create Primary Key on Tracking Table since it's done during table creation ", trackingName.FullQuotedString);
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
                using (var command = new SqliteCommand())
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
            string str = string.Concat("Create Tracking Table ", trackingName.FullQuotedString);
            return SqliteBuilder.WrapScriptTextWithComments(this.CreateTableCommandText(), str);
        }

        public string CreateTableCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"CREATE TABLE {trackingName.FullQuotedString} (");

            // Adding the primary key
            foreach (DmColumn pkColumn in this.tableDescription.PrimaryKey.Columns)
            {
                var quotedColumnName = new ObjectNameParser(pkColumn.ColumnName, "[", "]").FullQuotedString;

                var columnTypeString = this.sqliteDbMetadata.TryGetOwnerDbTypeString(pkColumn.OriginalDbType, pkColumn.DbType, false, false, this.tableDescription.OriginalProvider, SqliteSyncProvider.ProviderType);
                var columnPrecisionString = this.sqliteDbMetadata.TryGetOwnerDbTypePrecision(pkColumn.OriginalDbType, pkColumn.DbType, false, false, pkColumn.MaxLength, pkColumn.Precision, pkColumn.Scale, this.tableDescription.OriginalProvider, SqliteSyncProvider.ProviderType);
                var quotedColumnType = new ObjectNameParser(columnTypeString, "[", "]").FullQuotedString;
                quotedColumnType += columnPrecisionString;

                stringBuilder.AppendLine($"{quotedColumnName} {quotedColumnType} NOT NULL COLLATE NOCASE, ");
            }

            // adding the tracking columns
            stringBuilder.AppendLine($"[create_scope_id] [blob] NULL COLLATE NOCASE, ");
            stringBuilder.AppendLine($"[update_scope_id] [blob] NULL COLLATE NOCASE, ");
            stringBuilder.AppendLine($"[create_timestamp] [integer] NULL, ");
            stringBuilder.AppendLine($"[update_timestamp] [integer] NULL, ");
            stringBuilder.AppendLine($"[timestamp] [integer] NULL, ");
            stringBuilder.AppendLine($"[sync_row_is_tombstone] [integer] NOT NULL default(0), ");
            stringBuilder.AppendLine($"[last_change_datetime] [datetime] NULL, ");

            // adding the filter columns
            // --------------------------------------------------------------------------------
            // SQLITE doesnot support (yet) filtering columns, since it's only a client provider
            // --------------------------------------------------------------------------------
            //if (this.FilterColumns != null)
            //    foreach (DmColumn filterColumn in this.FilterColumns)
            //    {
            //        var isPk = this.tableDescription.PrimaryKey.Columns.Any(dm => this.tableDescription.IsEqual(dm.ColumnName, filterColumn.ColumnName));
            //        if (isPk)
            //            continue;

            //        var quotedColumnName = new ObjectNameParser(filterColumn.ColumnName, "[", "]").QuotedString;
            //        var quotedColumnType = new ObjectNameParser(filterColumn.GetSqliteDbTypeString(), "[", "]").QuotedString;
            //        quotedColumnType += filterColumn.GetSqliteTypePrecisionString();
            //        var nullableColumn = filterColumn.AllowDBNull ? "NULL" : "NOT NULL";

            //        stringBuilder.AppendLine($"{quotedColumnName} {quotedColumnType} {nullableColumn}, ");
            //    }

            stringBuilder.Append(" PRIMARY KEY (");
            for (int i = 0; i < this.tableDescription.PrimaryKey.Columns.Length; i++)
            {
                DmColumn pkColumn = this.tableDescription.PrimaryKey.Columns[i];
                var quotedColumnName = new ObjectNameParser(pkColumn.ColumnName).ObjectName;

                stringBuilder.Append(quotedColumnName);

                if (i < this.tableDescription.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
            }
            stringBuilder.Append(")");


            stringBuilder.Append(")");

            return stringBuilder.ToString();
        }

        public bool NeedToCreateTrackingTable()
        {
            return !SqliteManagementUtils.TableExists(connection, transaction, trackingName.FullQuotedString);
        }

        public void PopulateFromBaseTable()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqliteCommand())
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
            stringBuilder.AppendLine(string.Concat("INSERT INTO ", trackingName.FullQuotedString, " ("));
            StringBuilder stringBuilder1 = new StringBuilder();
            StringBuilder stringBuilder2 = new StringBuilder();
            string empty = string.Empty;
            StringBuilder stringBuilderOnClause = new StringBuilder("ON ");
            StringBuilder stringBuilderWhereClause = new StringBuilder("WHERE ");
            string str = string.Empty;
            string baseTable = "[base]";
            string sideTable = "[side]";
            foreach (var pkColumn in this.tableDescription.PrimaryKey.Columns)
            {
                var quotedColumnName = new ObjectNameParser(pkColumn.ColumnName, "[", "]").FullQuotedString;

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

            // --------------------------------------------------------------------------------
            // SQLITE doesnot support (yet) filtering columns, since it's only a client provider
            // --------------------------------------------------------------------------------
            //if (FilterColumns != null)
            //    foreach (var filterColumn in this.FilterColumns)
            //    {
            //        var isPk = this.tableDescription.PrimaryKey.Columns.Any(dm => this.tableDescription.IsEqual(dm.ColumnName, filterColumn.ColumnName));
            //        if (isPk)
            //            continue;

            //        var quotedColumnName = new ObjectNameParser(filterColumn.ColumnName, "[", "]").QuotedString;

            //        stringBuilder6.Append(string.Concat(empty, quotedColumnName));
            //        stringBuilder5.Append(string.Concat(empty, baseTable, ".", quotedColumnName));
            //    }

            // (list of pkeys)
            stringBuilder.Append(string.Concat(stringBuilder1.ToString(), ", "));

            stringBuilder.Append("[create_scope_id], ");
            stringBuilder.Append("[update_scope_id], ");
            stringBuilder.Append("[create_timestamp], ");
            stringBuilder.Append("[update_timestamp], ");
            stringBuilder.Append("[timestamp], "); // timestamp is not a column we update, it's auto
            stringBuilder.Append("[sync_row_is_tombstone] ");
            stringBuilder.AppendLine(string.Concat(stringBuilder6.ToString(), ") "));
            stringBuilder.Append(string.Concat("SELECT ", stringBuilder2.ToString(), ", "));
            stringBuilder.Append("NULL, ");
            stringBuilder.Append("NULL, ");
            stringBuilder.Append($"{SqliteObjectNames.TimestampValue}, ");
            stringBuilder.Append("0, ");
            stringBuilder.Append($"{SqliteObjectNames.TimestampValue}, ");
            stringBuilder.Append("0");
            stringBuilder.AppendLine(string.Concat(stringBuilder5.ToString(), " "));
            string[] localName = new string[] { "FROM ", tableName.FullQuotedString, " ", baseTable, " LEFT OUTER JOIN ", trackingName.FullQuotedString, " ", sideTable, " " };
            stringBuilder.AppendLine(string.Concat(localName));
            stringBuilder.AppendLine(string.Concat(stringBuilderOnClause.ToString(), " "));
            stringBuilder.AppendLine(string.Concat(stringBuilderWhereClause.ToString(), "; \n"));
            return stringBuilder.ToString();
        }

        public string CreatePopulateFromBaseTableScriptText()
        {
            string str = string.Concat("Populate tracking table ", trackingName.FullQuotedString, " for existing data in table ", tableName.FullQuotedString);
            return SqliteBuilder.WrapScriptTextWithComments(this.CreatePopulateFromBaseTableCommandText(), str);
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
                using (var command = new SqliteCommand())
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
            var quotedColumnName = new ObjectNameParser(col.ColumnName, "[", "]").FullQuotedString;
            var columnTypeString = this.sqliteDbMetadata.TryGetOwnerDbTypeString(col.OriginalDbType, col.DbType, false, false, this.tableDescription.OriginalProvider, SqliteSyncProvider.ProviderType);
            var columnPrecisionString = this.sqliteDbMetadata.TryGetOwnerDbTypePrecision(col.OriginalDbType, col.DbType, false, false, col.MaxLength, col.Precision, col.Scale, this.tableDescription.OriginalProvider, SqliteSyncProvider.ProviderType);
            var quotedColumnType = new ObjectNameParser(columnTypeString, "[", "]").FullQuotedString;
            quotedColumnType += columnPrecisionString;

            return string.Concat("ALTER TABLE ", quotedColumnName, " ADD ", quotedColumnType);
        }
        public string ScriptAddFilterColumn(DmColumn filterColumn)
        {
            var quotedColumnName = new ObjectNameParser(filterColumn.ColumnName, "[", "]");

            string str = string.Concat("Add new filter column, ", quotedColumnName.FullUnquotedString, ", to Tracking Table ", trackingName.FullQuotedString);
            return SqliteBuilder.WrapScriptTextWithComments(this.AddFilterColumnCommandText(filterColumn), str);
        }

        public void DropTable()
        {
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqliteCommand($"DROP TABLE IF EXISTS {tableName.FullQuotedString}", connection))
                {
                    if (!alreadyOpened)
                        connection.Open();

                    if (transaction != null)
                        command.Transaction = transaction;

                    command.Connection = connection;
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
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

            }

        }

        public string DropTableScriptText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            var tableNameScript = $"Drop Table {tableName.FullQuotedString}";
            var tableScript = $"DROP TABLE IF EXISTS {tableName.FullQuotedString}";
            stringBuilder.Append(SqliteBuilder.WrapScriptTextWithComments(tableScript, tableNameScript));
            stringBuilder.AppendLine();
            return stringBuilder.ToString();
        }
    }
}
