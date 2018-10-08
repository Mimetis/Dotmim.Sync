using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using Dotmim.Sync.Filter;
using Dotmim.Sync.Log;
using Dotmim.Sync.SqlServer.Manager;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace Dotmim.Sync.SqlServer.Builders
{
    public class SqlBuilderTrackingTable : IDbBuilderTrackingTableHelper
    {
        private ObjectNameParser tableName;
        private ObjectNameParser trackingName;
        private readonly DmTable tableDescription;
        private readonly SqlConnection connection;
        private readonly SqlTransaction transaction;
        private readonly SqlDbMetadata sqlDbMetadata;

        public IList<FilterClause2> Filters { get; set; }

        public SqlBuilderTrackingTable(DmTable tableDescription, DbConnection connection, DbTransaction transaction = null)
        {
            this.connection = connection as SqlConnection;
            this.transaction = transaction as SqlTransaction;
            this.tableDescription = tableDescription;

            (this.tableName, this.trackingName) = SqlBuilder.GetParsers(this.tableDescription);
            this.sqlDbMetadata = new SqlDbMetadata();
        }


        public void CreateIndex()
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

        private string CreateIndexCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"CREATE NONCLUSTERED INDEX [{trackingName.ObjectNameNormalized}_timestamp_index] ON {trackingName.FullQuotedString} (");
            stringBuilder.AppendLine($"\t[update_timestamp] ASC");
            stringBuilder.AppendLine($"\t,[update_scope_id] ASC");
            stringBuilder.AppendLine($"\t,[sync_row_is_tombstone] ASC");
            //// Filter columns
            //if (this.Filters != null && this.Filters.Count > 0)
            //{
            //    foreach (var filterColumn in this.Filters)
            //    {
            //        // check if the filter column is already a primary key.
            //        // in this case, we will add it as an index in the next foreach
            //        if (this.tableDescription.PrimaryKey.Columns.Any(c => c.ColumnName.ToLowerInvariant() == filterColumn.ColumnName.ToLowerInvariant()))
            //            continue;

            //        if (!this.tableDescription.Columns.Any(c => c.ColumnName.ToLowerInvariant() == filterColumn.ColumnName.ToLowerInvariant()))
            //            continue;

            //        ObjectNameParser columnName = new ObjectNameParser(filterColumn.ColumnName);
            //        stringBuilder.AppendLine($"\t,{columnName.FullQuotedString} ASC");
            //    }
            //}

            foreach (var pkColumn in this.tableDescription.PrimaryKey.Columns)
            {
                ObjectNameParser columnName = new ObjectNameParser(pkColumn.ColumnName);
                stringBuilder.AppendLine($"\t,{columnName.FullQuotedString} ASC");
            }
            stringBuilder.Append(")");
            return stringBuilder.ToString();
        }

        public string CreateIndexScriptText()
        {
            string str = string.Concat("Create index on Tracking Table ", trackingName.FullQuotedString);
            return SqlBuilder.WrapScriptTextWithComments(this.CreateIndexCommandText(), str);
        }

        public void CreatePk()
        {
            return;
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqlCommand())
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
            string str = string.Concat("Create Primary Key on Tracking Table ", trackingName.FullQuotedString);
            return SqlBuilder.WrapScriptTextWithComments(this.CreatePkCommandText(), str);
        }

        /// <summary>
        /// The primary key will regroup primary keys columns + filtered columns
        /// </summary>
        public string CreatePkCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            //stringBuilder.Append($"ALTER TABLE {trackingName.FullQuotedString} ADD CONSTRAINT [PK_{trackingName.ObjectNameNormalized}] PRIMARY KEY (");


            //for (int i = 0; i < this.tableDescription.PrimaryKey.Columns.Length; i++)
            //{
            //    DmColumn pkColumn = this.tableDescription.PrimaryKey.Columns[i];
            //    var quotedColumnName = new ObjectNameParser(pkColumn.ColumnName, "[", "]").FullQuotedString;

            //    stringBuilder.Append(quotedColumnName);

            //    if (i < this.tableDescription.PrimaryKey.Columns.Length - 1)
            //        stringBuilder.Append(", ");
            //}


            //stringBuilder.Append(")");

            return stringBuilder.ToString();
        }

        public void CreateTable()
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
                using (var command = new SqlCommand())
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
            string str = string.Concat("Create Tracking Table ", trackingName.FullQuotedString);
            return SqlBuilder.WrapScriptTextWithComments(this.CreateTableCommandText(), str);
        }
        public string DropTableScriptText()
        {
            string str = string.Concat("Droping Tracking Table ", trackingName.FullQuotedString);
            return SqlBuilder.WrapScriptTextWithComments(this.CreateTableCommandText(), str);
        }

        private string CreateDropTableCommandText()
        {
            return $"DROP TABLE {trackingName.FullQuotedString};";
        }

        private string CreateTableCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"CREATE TABLE {trackingName.FullQuotedString} (");

            List<DmColumn> addedColumns = new List<DmColumn>();

            stringBuilder.AppendLine($"[id] [bigint] NOT NULL IDENTITY(1,1) PRIMARY KEY, ");


            // Adding the primary key
            foreach (DmColumn pkColumn in this.tableDescription.PrimaryKey.Columns)
            {
                var quotedColumnName = new ObjectNameParser(pkColumn.ColumnName, "[", "]").FullQuotedString;

                var columnTypeString = this.sqlDbMetadata.TryGetOwnerDbTypeString(pkColumn.OriginalDbType, pkColumn.DbType, false, false, pkColumn.MaxLength, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
                var quotedColumnType = new ObjectNameParser(columnTypeString, "[", "]").FullQuotedString;
                var columnPrecisionString = this.sqlDbMetadata.TryGetOwnerDbTypePrecision(pkColumn.OriginalDbType, pkColumn.DbType, false, false, pkColumn.MaxLength, pkColumn.Precision, pkColumn.Scale, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
                var columnType = $"{quotedColumnType} {columnPrecisionString}";

                var nullableColumn = pkColumn.AllowDBNull ? "NULL" : "NOT NULL";
                stringBuilder.AppendLine($"{quotedColumnName} {columnType} {nullableColumn}, ");

                addedColumns.Add(pkColumn);
            }

            // adding the tracking columns
            stringBuilder.AppendLine($"[create_scope_id] [uniqueidentifier] NULL, ");
            stringBuilder.AppendLine($"[update_scope_id] [uniqueidentifier] NULL, ");
            stringBuilder.AppendLine($"[create_timestamp] [bigint] NULL, ");
            stringBuilder.AppendLine($"[update_timestamp] [bigint] NULL, ");
            stringBuilder.AppendLine($"[timestamp] [timestamp] NULL, ");
            stringBuilder.AppendLine($"[sync_row_is_tombstone] [bit] NOT NULL default(0), ");
            stringBuilder.AppendLine($"[last_change_datetime] [datetime] NULL, ");

            //-----------------------------------------------------------
            // Adding the foreign keys
            //-----------------------------------------------------------
            foreach (var pr in this.tableDescription.ChildRelations)
            {
                // get the parent columns to have the correct name of the column (if we have mulitple columns who is bind to same child table)
                // ie : AddressBillId and AddressInvoiceId
                foreach (var c in pr.ParentColumns)
                {
                    // dont add doublons
                    if (addedColumns.Any(col => col.ColumnName.ToLowerInvariant() == c.ColumnName.ToLowerInvariant()))
                        continue;

                    var quotedColumnName = new ObjectNameParser(c.ColumnName, "[", "]").FullQuotedString;

                    var columnTypeString = this.sqlDbMetadata.TryGetOwnerDbTypeString(c.OriginalDbType, c.DbType, c.IsUnsigned, c.IsUnicode, c.MaxLength, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
                    var quotedColumnType = new ObjectNameParser(columnTypeString, "[", "]").FullQuotedString;
                    var columnPrecisionString = this.sqlDbMetadata.TryGetOwnerDbTypePrecision(c.OriginalDbType, c.DbType, c.IsUnsigned, c.IsUnicode, c.MaxLength, c.Precision, c.Scale, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
                    var columnType = $"{quotedColumnType} {columnPrecisionString}";
                    var nullableColumn = "NULL";

                    stringBuilder.AppendLine($"{quotedColumnName} {columnType} {nullableColumn}, ");

                    addedColumns.Add(c);

                }
            }

            // ---------------------------------------------------------------------
            // Add the filter columns if needed, and if not already added from Pkeys or Fkeys
            // ---------------------------------------------------------------------
            if (this.Filters != null && this.Filters.Count > 0)
            {
                foreach (var filter in this.Filters)
                {
                    // if column is null, we are in a table that need a relation before
                    if (string.IsNullOrEmpty(filter.FilterTable.ColumnName))
                        continue;

                    var columnFilter = this.tableDescription.Columns[filter.FilterTable.ColumnName];

                    if (columnFilter == null)
                        throw new InvalidExpressionException($"Column {filter.FilterTable.ColumnName} does not exist in Table {this.tableDescription.TableName}");

                    if (addedColumns.Any(ac => ac.ColumnName.ToLowerInvariant() == columnFilter.ColumnName.ToLowerInvariant()))
                        continue;

                    var quotedColumnName = new ObjectNameParser(columnFilter.ColumnName, "[", "]").FullQuotedString;

                    var columnTypeString = this.sqlDbMetadata.TryGetOwnerDbTypeString(columnFilter.OriginalDbType, columnFilter.DbType, columnFilter.IsUnsigned, columnFilter.IsUnicode, columnFilter.MaxLength, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
                    var quotedColumnType = new ObjectNameParser(columnTypeString, "[", "]").FullQuotedString;
                    var columnPrecisionString = this.sqlDbMetadata.TryGetOwnerDbTypePrecision(columnFilter.OriginalDbType, columnFilter.DbType, columnFilter.IsUnsigned, columnFilter.IsUnicode, columnFilter.MaxLength, columnFilter.Precision, columnFilter.Scale, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
                    var columnType = $"{quotedColumnType} {columnPrecisionString}";

                    var nullableColumn = "NULL";

                    stringBuilder.AppendLine($"{quotedColumnName} {columnType} {nullableColumn}, ");

                    addedColumns.Add(columnFilter);
                }
            }

            addedColumns.Clear();
            stringBuilder.Append(")");
            return stringBuilder.ToString();
        }

        public bool NeedToCreateTrackingTable()
        {
            return !SqlManagementUtils.TableExists(connection, transaction, trackingName.FullQuotedString);
        }

        public void PopulateFromBaseTable()
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
            var addedColumns = new List<DmColumn>();

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

                addedColumns.Add(pkColumn);
            }
            StringBuilder stringBuilder5 = new StringBuilder();
            StringBuilder stringBuilder6 = new StringBuilder();

            //-----------------------------------------------------------
            // Adding the foreign keys
            //-----------------------------------------------------------
            foreach (var pr in this.tableDescription.ChildRelations)
            {
                // get the parent columns to have the correct name of the column (if we have mulitple columns who is bind to same child table)
                // ie : AddressBillId and AddressInvoiceId
                foreach (var c in pr.ParentColumns)
                {
                    // dont add doublons
                    if (addedColumns.Any(col => col.ColumnName.ToLowerInvariant() == c.ColumnName.ToLowerInvariant()))
                        continue;

                    var quotedColumnName = new ObjectNameParser(c.ColumnName, "[", "]").FullQuotedString;

                    stringBuilder6.Append(string.Concat(empty, quotedColumnName));
                    stringBuilder5.Append(string.Concat(empty, baseTable, ".", quotedColumnName));

                    addedColumns.Add(c);

                }
            }

            // ---------------------------------------------------------------------
            // Add the filter columns if needed, and if not already added from Pkeys or Fkeys
            // ---------------------------------------------------------------------
            if (this.Filters != null && this.Filters.Count > 0)
            {
                foreach (var filter in this.Filters)
                {
                    // if column is null, we are in a table that need a relation before
                    if (string.IsNullOrEmpty(filter.FilterTable.ColumnName))
                        continue;

                    var columnFilter = this.tableDescription.Columns[filter.FilterTable.ColumnName];

                    if (columnFilter == null)
                        throw new InvalidExpressionException($"Column {filter.FilterTable.ColumnName} does not exist in Table {this.tableDescription.TableName}");

                    if (addedColumns.Any(ac => ac.ColumnName.ToLowerInvariant() == columnFilter.ColumnName.ToLowerInvariant()))
                        continue;

                    var quotedColumnName = new ObjectNameParser(columnFilter.ColumnName, "[", "]").FullQuotedString;

                    stringBuilder6.Append(string.Concat(empty, quotedColumnName));
                    stringBuilder5.Append(string.Concat(empty, baseTable, ".", quotedColumnName));

                    addedColumns.Add(columnFilter);
                }
            }

            //if (Filters != null && Filters.Count > 0)
            //    foreach (var filter in this.Filters)
            //    {
            //        var columnFilter = this.tableDescription.Columns[filter.ColumnName];

            //        if (columnFilter == null)
            //            throw new InvalidExpressionException($"Column {filter.ColumnName} does not exist in Table {this.tableDescription.TableName}");

            //        var isPk = this.tableDescription.PrimaryKey.Columns.Any(dm => this.tableDescription.IsEqual(dm.ColumnName, columnFilter.ColumnName));
            //        if (isPk)
            //            continue;

            //        var quotedColumnName = new ObjectNameParser(columnFilter.ColumnName, "[", "]").FullQuotedString;

            //        stringBuilder6.Append(string.Concat(empty, quotedColumnName));
            //        stringBuilder5.Append(string.Concat(empty, baseTable, ".", quotedColumnName));
            //    }

            // (list of pkeys)
            stringBuilder.Append(string.Concat(stringBuilder1.ToString(), ", "));

            stringBuilder.Append("[create_scope_id], ");
            stringBuilder.Append("[update_scope_id], ");
            stringBuilder.Append("[create_timestamp], ");
            stringBuilder.Append("[update_timestamp], ");
            //stringBuilder.Append("[timestamp], "); // timestamp is not a column we update, it's auto
            stringBuilder.Append("[sync_row_is_tombstone] ");
            stringBuilder.AppendLine(string.Concat(stringBuilder6.ToString(), ") "));
            stringBuilder.Append(string.Concat("SELECT ", stringBuilder2.ToString(), ", "));
            stringBuilder.Append("NULL, ");
            stringBuilder.Append("NULL, ");
            stringBuilder.Append("@@DBTS+1, ");
            stringBuilder.Append("0, ");
            //stringBuilder.Append("@@DBTS+1, "); // timestamp is not a column we update, it's auto
            stringBuilder.Append("0");
            stringBuilder.AppendLine(string.Concat(stringBuilder5.ToString(), " "));
            string[] localName = new string[] { "FROM ", tableName.FullQuotedString, " ", baseTable, " LEFT OUTER JOIN ", trackingName.FullQuotedString, " ", sideTable, " " };
            stringBuilder.AppendLine(string.Concat(localName));
            stringBuilder.AppendLine(string.Concat(stringBuilderOnClause.ToString(), " "));
            stringBuilder.AppendLine(string.Concat(stringBuilderWhereClause.ToString(), "; \n"));

            var scriptInsertTrackingTableData = stringBuilder.ToString();

            return scriptInsertTrackingTableData;
        }

        public string CreatePopulateFromBaseTableScriptText()
        {
            string str = string.Concat("Populate tracking table ", trackingName.FullQuotedString, " for existing data in table ", tableName.FullQuotedString);
            return SqlBuilder.WrapScriptTextWithComments(this.CreatePopulateFromBaseTableCommandText(), str);
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
                using (var command = new SqlCommand())
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
            var quotedColumnType = new ObjectNameParser(col.OriginalDbType, "[", "]").FullQuotedString;

            var columnTypeString = this.sqlDbMetadata.TryGetOwnerDbTypeString(col.OriginalDbType, col.DbType, false, false, col.MaxLength, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
            var columnPrecisionString = this.sqlDbMetadata.TryGetOwnerDbTypePrecision(col.OriginalDbType, col.DbType, false, false, col.MaxLength, col.Precision, col.Scale, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
            var columnType = $"{columnTypeString} {columnPrecisionString}";

            return string.Concat("ALTER TABLE ", quotedColumnName, " ADD ", columnType);
        }
        public string ScriptAddFilterColumn(DmColumn filterColumn)
        {
            var quotedColumnName = new ObjectNameParser(filterColumn.ColumnName, "[", "]");

            string str = string.Concat("Add new filter column, ", quotedColumnName.FullUnquotedString, ", to Tracking Table ", trackingName.FullQuotedString);
            return SqlBuilder.WrapScriptTextWithComments(this.AddFilterColumnCommandText(filterColumn), str);
        }


    }
}
