using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using Dotmim.Sync.Filter;
using Dotmim.Sync.Log;
using Dotmim.Sync.SqlServer.Builders;
using Dotmim.Sync.SqlServer.Manager;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace Dotmim.Sync.SqlServer.ChangeTracking.Builders
{
    public class SqlChangeTrackingBuilderTrackingTable : IDbBuilderTrackingTableHelper
    {
        private ParserName tableName;
        private ParserName trackingName;
        private readonly SyncTable tableDescription;
        private readonly SqlConnection connection;
        private readonly SqlTransaction transaction;
        private readonly SqlDbMetadata sqlDbMetadata;

        public IEnumerable<SyncFilter> Filters { get; set; }

        public SqlChangeTrackingBuilderTrackingTable(SyncTable tableDescription, DbConnection connection, DbTransaction transaction = null)
        {
            this.connection = connection as SqlConnection;
            this.transaction = transaction as SqlTransaction;
            this.tableDescription = tableDescription;
            (this.tableName, this.trackingName) = SqlTableBuilder.GetParsers(this.tableDescription);
            this.sqlDbMetadata = new SqlDbMetadata();
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
                Debug.WriteLine($"Error during alter table for change tracking : {ex}");
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
                Debug.WriteLine($"Error during disabling change tracking : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();

            }
        }


        private string CreateDropTableCommandText()
        {
            return $"ALTER TABLE {tableName.Schema().Quoted().ToString()} DISABLE CHANGE_TRACKING;";
        }

        private string CreateTableCommandText()
        {
            return $"ALTER TABLE {tableName.Schema().Quoted().ToString()} ENABLE CHANGE_TRACKING WITH(TRACK_COLUMNS_UPDATED = OFF);";
        }

        public bool NeedToCreateTrackingTable()
        {
            var schemaName = this.tableName.SchemaName;
            var tableName = this.tableName.ObjectName;

            var dmTable = SqlChangeTrackingManagementUtils.ChangeTrackingTable(connection, transaction, tableName, schemaName);

            return dmTable.Rows.Count <= 0;
        }


        public void CreateIndex(){}
        private string CreateIndexCommandText() => string.Empty;
        public string CreateIndexScriptText() => string.Empty;
        public void CreatePk() { }
        public string CreatePkScriptText() => string.Empty;
        public string CreatePkCommandText() => string.Empty;
        public string CreateTableScriptText() => string.Empty;
        public string DropTableScriptText() => string.Empty;
        public void PopulateFromBaseTable() { }
        private string CreatePopulateFromBaseTableCommandText() => string.Empty;
        public string CreatePopulateFromBaseTableScriptText() => string.Empty;
        public void AddFilterColumn(SyncColumn filterColumn) { }
        private string AddFilterColumnCommandText(SyncColumn col) => string.Empty;
        public string ScriptAddFilterColumn(SyncColumn filterColumn) => string.Empty;
    }
}
