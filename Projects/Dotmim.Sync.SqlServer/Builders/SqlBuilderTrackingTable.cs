using Dotmim.Sync.Core.Builders;
using System;
using System.Collections.Generic;
using System.Text;
using Dotmim.Sync.Data;
using System.Data.Common;
using System.Data.SqlClient;
using Dotmim.Sync.Core.Common;
using System.Linq;

namespace Dotmim.Sync.SqlServer.Builders
{
    public class SqlBuilderTrackingTable : IDbBuilderTrackingTableHelper
    {

        private DmTable table;
        private ObjectNameParser originalTableName;
        private ObjectNameParser trackingTableName;


        public SqlBuilderTrackingTable(DmTable tableDescription)
        {
            this.table = tableDescription;
            string tableAndPrefixName = String.IsNullOrWhiteSpace(this.table.Prefix) ? this.table.TableName : $"{this.table.Prefix}.{this.table.TableName}";
            this.originalTableName = new ObjectNameParser(tableAndPrefixName, "[", "]");
            this.trackingTableName = new ObjectNameParser($"{tableAndPrefixName}_tracking", "[", "]");
        }

        private (SqlConnection, SqlTransaction) GetTypedConnection(DbTransaction transaction)
        {
            SqlTransaction sqlTransaction = transaction as SqlTransaction;

            if (sqlTransaction == null)
                throw new Exception("Transaction is not a SqlTransaction. Wrong provider");

            SqlConnection sqlConnection = sqlTransaction.Connection;

            return (sqlConnection, sqlTransaction);

        }
        public List<DmColumn> FilterColumns { get; set; } = new List<DmColumn>();


        public void CreateIndex(DbTransaction transaction)
        {
            (SqlConnection sqlConnection, SqlTransaction sqlTransaction) = GetTypedConnection(transaction);

            using (SqlCommand command = new SqlCommand())
            {
                command.Transaction = sqlTransaction;
                command.Connection = sqlConnection;
                command.CommandText = this.CreateIndexCommandText();
                command.ExecuteNonQuery();

            }
        }
        private string CreateIndexCommandText()
        {
            return null;
            //StringBuilder stringBuilder = new StringBuilder();
            //stringBuilder.Append($"CREATE NONCLUSTERED INDEX [local_update_peer_timestamp_index] ON {this.quotedTableName} (", this._trackingColNames.LocalUpdatePeerTimestamp));

            //string str = ", ";
            //if (this._filterColumns.Count > 0)
            //{
            //    stringBuilder.Append(string.Concat(str, this._trackingColNames.UpdateScopeLocalId));
            //    stringBuilder.Append(string.Concat(str, this._trackingColNames.SyncRowIsTombstone));
            //}
            //foreach (DbSyncColumnDescription _filterColumn in this._filterColumns)
            //{
            //    if (_filterColumn.IsPrimaryKey)
            //    {
            //        continue;
            //    }
            //    stringBuilder.Append(string.Concat(str, _filterColumn.QuotedName));
            //}
            //foreach (DbSyncColumnDescription pkColumn in this._tableDesc.PkColumns)
            //{
            //    stringBuilder.Append(string.Concat(str, pkColumn.QuotedName));
            //}
            //stringBuilder.Append(")");
            //return stringBuilder.ToString();
        }

        public string CreateIndexScriptText()
        {
            string str = string.Concat("Create index on Tracking Table ", this.trackingTableName.QuotedString);
            return SqlBuilder.WrapScriptTextWithComments(this.CreateIndexCommandText(), str);
        }

        public void CreatePk(DbTransaction transaction)
        {
            (SqlConnection sqlConnection, SqlTransaction sqlTransaction) = GetTypedConnection(transaction);

            using (SqlCommand command = new SqlCommand())
            {
                command.Transaction = sqlTransaction;
                command.Connection = sqlConnection;
                command.CommandText = this.CreatePkCommandText();
                command.ExecuteNonQuery();

            }
        }
        public string CreatePkScriptText()
        {
            string str = string.Concat("Create Primary Key on Tracking Table ", this.trackingTableName.QuotedString);
            return SqlBuilder.WrapScriptTextWithComments(this.CreatePkCommandText(), str);
        }

        public string CreatePkCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.Append($"ALTER TABLE {this.trackingTableName.QuotedString} ADD CONSTRAINT [PK_{this.trackingTableName.UnquotedStringWithUnderScore}] PRIMARY KEY (");

            for (int i = 0; i < this.table.PrimaryKey.Columns.Length; i++)
            {
                DmColumn pkColumn = this.table.PrimaryKey.Columns[i];
                var quotedColumnName = new ObjectNameParser(pkColumn.ColumnName, "[", "]").QuotedString;

                stringBuilder.Append(quotedColumnName);

                if (i < this.table.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
            }
            stringBuilder.Append(")");

            return stringBuilder.ToString();
        }

        public void CreateTable(DbTransaction transaction)
        {
            (SqlConnection sqlConnection, SqlTransaction sqlTransaction) = GetTypedConnection(transaction);

            using (SqlCommand command = new SqlCommand())
            {
                command.Transaction = sqlTransaction;
                command.Connection = sqlConnection;
                command.CommandText = this.CreateTableCommandText();
                command.ExecuteNonQuery();

            }
        }

        public string CreateTableScriptText()
        {
            string str = string.Concat("Create Tracking Table ", this.trackingTableName.QuotedString);
            return SqlBuilder.WrapScriptTextWithComments(this.CreateTableCommandText(), str);
        }

        public string CreateTableCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"CREATE TABLE {this.trackingTableName.QuotedString} (");

            // Adding the primary key
            foreach (DmColumn pkColumn in this.table.PrimaryKey.Columns)
            {
                var quotedColumnName = new ObjectNameParser(pkColumn.ColumnName, "[", "]").QuotedString;
                var quotedColumnType = new ObjectNameParser(pkColumn.GetSqlTypeInfo(), "[", "]").QuotedString;
                quotedColumnType += pkColumn.GetSqlTypePrecision(); 
                var nullableColumn = pkColumn.AllowDBNull ? "NULL" : "NOT NULL";

                stringBuilder.AppendLine($"{quotedColumnName} {quotedColumnType} {nullableColumn}, ");
            }

            // adding the tracking columns
            stringBuilder.AppendLine($"[create_scope_name] [nvarchar](100) NULL, ");
            stringBuilder.AppendLine($"[update_scope_name] [nvarchar](100) NULL, ");
            stringBuilder.AppendLine($"[create_timestamp] [bigint] NULL, ");
            stringBuilder.AppendLine($"[update_timestamp] [bigint] NULL, ");
            stringBuilder.AppendLine($"[timestamp] [timestamp] NULL, ");
            stringBuilder.AppendLine($"[sync_row_is_tombstone] [bit] NOT NULL default(0), ");
            stringBuilder.AppendLine($"[last_change_datetime] [datetime] NULL, ");

            // adding the filter columns
            foreach (DmColumn filterColumn in this.FilterColumns)
            {
                var isPk = this.table.PrimaryKey.Columns.Any(dm => this.table.IsEqual(dm.ColumnName, filterColumn.ColumnName));
                if (isPk)
                    continue;

                var quotedColumnName = new ObjectNameParser(filterColumn.ColumnName, "[", "]").QuotedString;
                var quotedColumnType = new ObjectNameParser(filterColumn.GetSqlTypeInfo(), "[", "]").QuotedString;
                quotedColumnType += filterColumn.GetSqlTypePrecision();
                var nullableColumn = filterColumn.AllowDBNull ? "NULL" : "NOT NULL";

                stringBuilder.AppendLine($"{quotedColumnName} {quotedColumnType} {nullableColumn}, ");
            }
            stringBuilder.Append(")");
            return stringBuilder.ToString();
        }

        public bool NeedToCreateTrackingTable(DbTransaction transaction, DmTable tableDescription, DbBuilderOption builderOption)
        {
            (SqlConnection sqlConnection, SqlTransaction sqlTransaction) = GetTypedConnection(transaction);

            switch (builderOption)
            {
                case DbBuilderOption.Create:
                        return true;
                case DbBuilderOption.Skip:
                        return false;
                case DbBuilderOption.CreateOrUseExisting:
                        return !SqlManagementUtils.TableExists(sqlConnection, sqlTransaction, this.trackingTableName.QuotedString);
            }
            return false;
        }

        public void PopulateFromBaseTable(DbTransaction transaction)
        {
            (SqlConnection sqlConnection, SqlTransaction sqlTransaction) = GetTypedConnection(transaction);

            using (SqlCommand command = new SqlCommand())
            {
                command.Transaction = sqlTransaction;
                command.Connection = sqlConnection;
                command.CommandText = this.CreatePopulateFromBaseTableCommandText();
                command.ExecuteNonQuery();

            }
        }

        private string CreatePopulateFromBaseTableCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine(string.Concat("INSERT INTO ", this.trackingTableName.QuotedString, " ("));
            StringBuilder stringBuilder1 = new StringBuilder();
            StringBuilder stringBuilder2 = new StringBuilder();
            string empty = string.Empty;
            StringBuilder stringBuilderOnClause = new StringBuilder("ON ");
            StringBuilder stringBuilderWhereClause = new StringBuilder("WHERE ");
            string str = string.Empty;
            string baseTable = "[base]";
            string sideTable = "[side]";
            foreach (var pkColumn in this.table.PrimaryKey.Columns)
            {
                var quotedColumnName = new ObjectNameParser(pkColumn.ColumnName, "[", "]").QuotedString;

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

            foreach (var filterColumn in this.FilterColumns)
            {
                var isPk = this.table.PrimaryKey.Columns.Any(dm => this.table.IsEqual(dm.ColumnName, filterColumn.ColumnName));
                if (isPk)
                    continue;

                var quotedColumnName = new ObjectNameParser(filterColumn.ColumnName, "[", "]").QuotedString;

                stringBuilder6.Append(string.Concat(empty, quotedColumnName));
                stringBuilder5.Append(string.Concat(empty, baseTable, ".", quotedColumnName));
            }

            // (list of pkeys)
            stringBuilder.Append(string.Concat(stringBuilder1.ToString(), ", "));

            stringBuilder.Append("[create_scope_name], ");
            stringBuilder.Append("[update_scope_name], ");
            stringBuilder.Append("[create_timestamp], ");
            stringBuilder.Append("[update_timestamp], ");
            //stringBuilder.Append("[timestamp], ");
            stringBuilder.Append("[sync_row_is_tombstone] ");
            stringBuilder.AppendLine(string.Concat(stringBuilder6.ToString(), ") "));
            stringBuilder.Append(string.Concat("SELECT ", stringBuilder2.ToString(), ", "));
            stringBuilder.Append("NULL, ");
            stringBuilder.Append("NULL, ");
            stringBuilder.Append("0, ");
            stringBuilder.Append("0, ");
            //stringBuilder.Append("@@DBTS+1, "); // timestamp is not a column we update, it's auto
            stringBuilder.Append("0");
            stringBuilder.AppendLine(string.Concat(stringBuilder5.ToString(), " "));
            string[] localName = new string[] { "FROM ", this.originalTableName.QuotedString, " ", baseTable, " LEFT OUTER JOIN ", this.trackingTableName.QuotedString, " ", sideTable, " " };
            stringBuilder.AppendLine(string.Concat(localName));
            stringBuilder.AppendLine(string.Concat(stringBuilderOnClause.ToString(), " "));
            stringBuilder.AppendLine(string.Concat(stringBuilderWhereClause.ToString(), "; \n"));
            return stringBuilder.ToString();
        }


        public string CreatePopulateFromBaseTableScriptText()
        {
            string str = string.Concat("Populate tracking table ", this.trackingTableName.QuotedString, " for existing data in table ", this.originalTableName.QuotedString);
            return SqlBuilder.WrapScriptTextWithComments(this.CreatePopulateFromBaseTableCommandText(), str);
        }

        public void PopulateNewFilterColumnFromBaseTable(DbTransaction transaction, DmColumn filterColumn)
        {
            throw new NotImplementedException();
        }


        public string ScriptPopulateNewFilterColumnFromBaseTable(DmColumn filterColumn)
        {
            throw new NotImplementedException();
        }

        public void AddFilterColumn(DbTransaction transaction, DmColumn filterColumn)
        {
            (SqlConnection sqlConnection, SqlTransaction sqlTransaction) = GetTypedConnection(transaction);

            using (SqlCommand command = new SqlCommand())
            {
                command.Transaction = sqlTransaction;
                command.Connection = sqlConnection;
                command.CommandText = this.AddFilterColumnCommandText(filterColumn);
                command.ExecuteNonQuery();
            }
        }

        private string AddFilterColumnCommandText(DmColumn col)
        {
            var quotedColumnName = new ObjectNameParser(col.ColumnName, "[", "]").QuotedString;
            var quotedColumnType = new ObjectNameParser(col.GetSqlTypeInfo(), "[", "]").QuotedString;
            quotedColumnType += col.GetSqlTypePrecision();

            return string.Concat("ALTER TABLE ", quotedColumnName, " ADD ", quotedColumnType);
        }
        public string ScriptAddFilterColumn(DmColumn filterColumn)
        {
            var quotedColumnName = new ObjectNameParser(filterColumn.ColumnName, "[", "]");

            string str = string.Concat("Add new filter column, ", quotedColumnName.UnquotedString, ", to Tracking Table ", this.trackingTableName.QuotedString);
            return SqlBuilder.WrapScriptTextWithComments(this.AddFilterColumnCommandText(filterColumn), str);
        }

    }
}
