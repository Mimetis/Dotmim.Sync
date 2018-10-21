using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using Dotmim.Sync.Filter;
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
        private readonly ObjectNameParser tableName;
        private readonly ObjectNameParser trackingName;
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
            var alreadyOpened = this.connection.State == ConnectionState.Open;

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
            var addedColumns = new List<DmColumn>();
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"CREATE NONCLUSTERED INDEX [{this.trackingName.ObjectNameNormalized}_timestamp_index] ON {this.trackingName.FullQuotedString} (");

            var comma = "";
            foreach (var pkColumn in this.tableDescription.PrimaryKey.Columns)
            {
                var columnName = new ObjectNameParser(pkColumn.ColumnName);
                stringBuilder.AppendLine($"\t{comma}{columnName.FullQuotedString} ASC");
                comma = ",";
                addedColumns.Add(pkColumn);
            }

            // ---------------------------------------------------------------------
            // Add the filter columns if needed
            // ---------------------------------------------------------------------
            foreach (var filter in this.Filters)
            {
                // if column is null, we are in a table that need a relation before
                if (string.IsNullOrEmpty(filter.FilterTable.ColumnName))
                    continue;

                // get the column from the original filtered table (could be another table)
                var tableFilter = this.tableDescription.DmSet.Tables[filter.FilterTable.TableName.ObjectName];
                var columnFilter = tableFilter.Columns[filter.FilterTable.ColumnName];

                if (columnFilter == null)
                    throw new InvalidExpressionException($"Column {filter.FilterTable.ColumnName} does not exist in Table {this.tableDescription.TableName}");

                // get hierarchy from this tabledescription to filtertable
                var hierarchy = this.tableDescription.GetParentsTo(tableFilter);

                foreach (var relation in hierarchy)
                {
                    foreach (var column in relation.ParentColumns)
                    {
                        var quotedColumnName = new ObjectNameParser($"{column.Table.TableName}_{column.ColumnName}", "[", "]").FullQuotedString;

                        if (addedColumns.Any(ac => ac.ColumnName.ToLowerInvariant() == column.ColumnName.ToLowerInvariant() && ac.Table.TableName.ToLowerInvariant() == column.Table.TableName.ToLowerInvariant()))
                            continue;

                        stringBuilder.AppendLine($"\t,{quotedColumnName} ASC");

                        addedColumns.Add(column);
                    }
                }

            }


            stringBuilder.AppendLine($"\t,[update_timestamp] ASC");
            stringBuilder.AppendLine($"\t,[update_scope_id] ASC");
            stringBuilder.AppendLine($"\t,[sync_row_is_tombstone] ASC");

            stringBuilder.Append(")");
            return stringBuilder.ToString();
        }

        public string CreateIndexScriptText()
        {
            var str = string.Concat("Create index on Tracking Table ", this.trackingName.FullQuotedString);
            return SqlBuilder.WrapScriptTextWithComments(this.CreateIndexCommandText(), str);
        }

        public void CreatePk()
        {
            return;
            var alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqlCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

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
            var str = string.Concat("Create Primary Key on Tracking Table ", this.trackingName.FullQuotedString);
            return SqlBuilder.WrapScriptTextWithComments(this.CreatePkCommandText(), str);
        }

        /// <summary>
        /// The primary key will regroup primary keys columns + filtered columns
        /// </summary>
        public string CreatePkCommandText()
        {
            var stringBuilder = new StringBuilder();
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
            var alreadyOpened = this.connection.State == ConnectionState.Open;

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
            var alreadyOpened = this.connection.State == ConnectionState.Open;

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
            var str = string.Concat("Create Tracking Table ", this.trackingName.FullQuotedString);
            return SqlBuilder.WrapScriptTextWithComments(this.CreateTableCommandText(), str);
        }

        public string DropTableScriptText()
        {
            var str = string.Concat("Droping Tracking Table ", this.trackingName.FullQuotedString);
            return SqlBuilder.WrapScriptTextWithComments(this.CreateTableCommandText(), str);
        }

        private string CreateDropTableCommandText() => $"DROP TABLE {this.trackingName.FullQuotedString};";

        private string CreateTableCommandText()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"CREATE TABLE {this.trackingName.FullQuotedString} (");

            var addedColumns = new List<DmColumn>();

            stringBuilder.AppendLine($"[id] [bigint] NOT NULL IDENTITY(1,1) PRIMARY KEY, ");


            // Adding the primary key
            foreach (var pkColumn in this.tableDescription.PrimaryKey.Columns)
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

            // ---------------------------------------------------------------------
            // Add the filter columns if needed
            // ---------------------------------------------------------------------
            foreach (var filter in this.Filters)
            {
                // if column is null, we are in a table that need a relation before
                if (string.IsNullOrEmpty(filter.FilterTable.ColumnName))
                    continue;

                // get the column from the original filtered table (could be another table)
                var tableFilter = this.tableDescription.DmSet.Tables[filter.FilterTable.TableName.ObjectName];
                var columnFilter = tableFilter.Columns[filter.FilterTable.ColumnName];

                if (columnFilter == null)
                    throw new InvalidExpressionException($"Column {filter.FilterTable.ColumnName} does not exist in Table {this.tableDescription.TableName}");

                // get hierarchy from this tabledescription to filtertable
                var hierarchy = this.tableDescription.GetParentsTo(tableFilter);

                foreach (var relation in hierarchy)
                {
                    foreach (var column in relation.ParentColumns)
                    {
                        var quotedColumnName = new ObjectNameParser($"{column.Table.TableName}_{column.ColumnName}", "[", "]").FullQuotedString;
                        var columnTypeString = this.sqlDbMetadata.TryGetOwnerDbTypeString(column.OriginalDbType, column.DbType, column.IsUnsigned, column.IsUnicode, column.MaxLength, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
                        var quotedColumnType = new ObjectNameParser(columnTypeString, "[", "]").FullQuotedString;
                        var columnPrecisionString = this.sqlDbMetadata.TryGetOwnerDbTypePrecision(column.OriginalDbType, column.DbType, column.IsUnsigned, column.IsUnicode, column.MaxLength, column.Precision, column.Scale, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
                        var columnType = $"{quotedColumnType} {columnPrecisionString}";
                        var nullableColumn = "NULL";

                        if (addedColumns.Any(ac => ac.ColumnName.ToLowerInvariant() == column.ColumnName.ToLowerInvariant() && ac.Table.TableName.ToLowerInvariant() == column.Table.TableName.ToLowerInvariant()))
                            continue;

                        stringBuilder.AppendLine($"{quotedColumnName} {columnType} {nullableColumn}, ");

                        addedColumns.Add(column);
                    }
                }

            }

            addedColumns.Clear();
            stringBuilder.Append(")");
            return stringBuilder.ToString();
        }

        public bool NeedToCreateTrackingTable() => !SqlManagementUtils.TableExists(this.connection, this.transaction, this.trackingName.FullQuotedString);

        public void PopulateFromBaseTable()
        {
            var alreadyOpened = this.connection.State == ConnectionState.Open;

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

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine(string.Concat("INSERT INTO ", this.trackingName.FullQuotedString, " ("));

            var primaryKeysColumns = new StringBuilder();
            var selectColumns = new StringBuilder();

            var empty = string.Empty;
            var stringBuilderOnClause = new StringBuilder("ON ");
            var stringBuilderWhereClause = new StringBuilder("WHERE ");
            var str = string.Empty;
            var baseTable = "[base]";
            var sideTable = "[side]";
            var sideFilteredTable = "[base_{0}]";

            // iterate through primary keys columns
            foreach (var pkColumn in this.tableDescription.PrimaryKey.Columns)
            {
                var quotedColumnName = new ObjectNameParser(pkColumn.ColumnName, "[", "]").FullQuotedString;

                primaryKeysColumns.Append(string.Concat(empty, quotedColumnName));

                selectColumns.Append(string.Concat(empty, baseTable, ".", quotedColumnName));

                var quotedName = new string[] { str, baseTable, ".", quotedColumnName, " = ", sideTable, ".", quotedColumnName };
                stringBuilderOnClause.Append(string.Concat(quotedName));
                var strArrays = new string[] { str, sideTable, ".", quotedColumnName, " IS NULL" };
                stringBuilderWhereClause.Append(string.Concat(strArrays));
                empty = ", ";
                str = " AND ";

                addedColumns.Add(pkColumn);
            }
            var fileteredSelectedColumns = new StringBuilder();
            var filteredColumns = new StringBuilder();

            // ---------------------------------------------------------------------
            // Add the filter columns if needed, and if not already added from primary keys columns
            // ---------------------------------------------------------------------

            foreach (var filter in this.Filters)
            {
                // if column is null, we are in a table that need a relation before
                if (string.IsNullOrEmpty(filter.FilterTable.ColumnName))
                    continue;

                // get the column from the original filtered table (could be another table)
                var tableFilter = this.tableDescription.DmSet.Tables[filter.FilterTable.TableName.ObjectName];
                var columnFilter = tableFilter.Columns[filter.FilterTable.ColumnName];

                if (columnFilter == null)
                    throw new InvalidExpressionException($"Column {filter.FilterTable.ColumnName} does not exist in Table {this.tableDescription.TableName}");

                // get hierarchy from this tabledescription to filtertable
                var hierarchy = this.tableDescription.GetParentsTo(tableFilter);

                int cpt = 0;
                foreach (var relation in hierarchy)
                {
                    foreach (var column in relation.ParentColumns)
                    {
                        var quotedColumnName1 = new ObjectNameParser(column.ColumnName, "[", "]").FullQuotedString;
                        var quotedColumnName2 = new ObjectNameParser($"{column.Table.TableName}_{column.ColumnName}", "[", "]").FullQuotedString;

                        if (addedColumns.Any(ac => ac.ColumnName.ToLowerInvariant() == column.ColumnName.ToLowerInvariant() && ac.Table.TableName.ToLowerInvariant() == column.Table.TableName.ToLowerInvariant()))
                            continue;

                        var alias_plus = new ObjectNameParser(string.Format(sideFilteredTable, $"{cpt}")).FullQuotedString;

                        fileteredSelectedColumns.Append(string.Concat(empty, $"{alias_plus}.{quotedColumnName1}"));
                        filteredColumns.Append(string.Concat(empty, quotedColumnName2));

                        addedColumns.Add(column);
                        cpt++;
                    }
                }

            }


            // (list of pkeys)
            stringBuilder.Append(string.Concat(primaryKeysColumns.ToString(), ", "));

            stringBuilder.Append("[create_scope_id], ");
            stringBuilder.Append("[update_scope_id], ");
            stringBuilder.Append("[create_timestamp], ");
            stringBuilder.Append("[update_timestamp], ");
            //stringBuilder.Append("[timestamp], "); // timestamp is not a column we update, it's auto
            stringBuilder.Append("[sync_row_is_tombstone] ");
            stringBuilder.AppendLine(string.Concat(filteredColumns.ToString(), ") "));
            stringBuilder.Append(string.Concat("SELECT ", selectColumns.ToString(), ", "));
            stringBuilder.Append("NULL, ");
            stringBuilder.Append("NULL, ");
            stringBuilder.Append("@@DBTS+1, ");
            stringBuilder.Append("0, ");
            //stringBuilder.Append("@@DBTS+1, "); // timestamp is not a column we update, it's auto
            stringBuilder.Append("0");
            stringBuilder.AppendLine(string.Concat(fileteredSelectedColumns.ToString(), " "));
            var localName = new string[] { "FROM ", this.tableName.FullQuotedString, " ", baseTable, " LEFT OUTER JOIN ", this.trackingName.FullQuotedString, " ", sideTable, " " };
            stringBuilder.AppendLine(string.Concat(localName));
            stringBuilder.AppendLine(string.Concat(stringBuilderOnClause.ToString(), " "));


            for (var filterIndex = 0; filterIndex < this.Filters.Count; filterIndex++)
            {
                var filter = this.Filters[filterIndex];
                // if column is null, we are in a table that need a relation before
                if (string.IsNullOrEmpty(filter.FilterTable.ColumnName))
                    continue;

                var tableFilter = this.tableDescription.DmSet.Tables[filter.FilterTable.TableName.ObjectNameNormalized];

                var hierarchy = this.tableDescription.GetParentsTo(tableFilter);

                for (var index = 0; index < hierarchy.Count; index++)
                {

                    var alias_plus = new ObjectNameParser(string.Format(sideFilteredTable, $"{index}")).FullQuotedString;
                    var alias_minus = index == 0 ? "[base]" : new ObjectNameParser(string.Format(sideFilteredTable, $"{index - 1}")).FullQuotedString;

                    var dmRelation = hierarchy[index];
                    var columnFilter = dmRelation.ParentColumns[0];

                    var parentTableName = new ObjectNameParser(dmRelation.ParentTable.TableName).FullQuotedString;
                    var childTableName = new ObjectNameParser(dmRelation.ChildTable.TableName).FullQuotedString;

                    // todo : iterate through all relation columns
                    var parentTableColumnName = new ObjectNameParser(dmRelation.ParentColumns[0].ColumnName).FullQuotedString;
                    var childTableColumnName = new ObjectNameParser(dmRelation.ChildColumns[0].ColumnName).FullQuotedString;

                    stringBuilder.AppendLine($"LEFT OUTER JOIN {parentTableName} {alias_plus} ON {alias_plus}.{parentTableColumnName} =  {alias_minus}.{childTableColumnName} ");

                }

            }

            stringBuilder.AppendLine(string.Concat(stringBuilderWhereClause.ToString(), "; \n"));

            var scriptInsertTrackingTableData = stringBuilder.ToString();

            return scriptInsertTrackingTableData;
        }

        public string CreatePopulateFromBaseTableScriptText()
        {
            var str = string.Concat("Populate tracking table ", this.trackingName.FullQuotedString, " for existing data in table ", this.tableName.FullQuotedString);
            return SqlBuilder.WrapScriptTextWithComments(this.CreatePopulateFromBaseTableCommandText(), str);
        }

        public void PopulateNewFilterColumnFromBaseTable(DmColumn filterColumn) => throw new NotImplementedException();

        public string ScriptPopulateNewFilterColumnFromBaseTable(DmColumn filterColumn) => throw new NotImplementedException();

        public void AddFilterColumn(DmColumn filterColumn)
        {
            var alreadyOpened = this.connection.State == ConnectionState.Open;

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

            var str = string.Concat("Add new filter column, ", quotedColumnName.FullUnquotedString, ", to Tracking Table ", this.trackingName.FullQuotedString);
            return SqlBuilder.WrapScriptTextWithComments(this.AddFilterColumnCommandText(filterColumn), str);
        }


    }
}
