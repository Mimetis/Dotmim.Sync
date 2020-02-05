using Dotmim.Sync.Builders;
using System;
using System.Text;

using System.Data.Common;
using Dotmim.Sync.Log;
using System.Data;
using Microsoft.Data.Sqlite;

using System.Diagnostics;
using System.Collections.Generic;
using System.Linq;

namespace Dotmim.Sync.Sqlite
{
    public class SqliteBuilderTrackingTable : IDbBuilderTrackingTableHelper
    {
        private ParserName tableName;
        private ParserName trackingName;
        private SyncTable tableDescription;
        private SqliteConnection connection;
        private SqliteTransaction transaction;
        private SqliteDbMetadata sqliteDbMetadata;

        public SqliteBuilderTrackingTable(SyncTable tableDescription, DbConnection connection, DbTransaction transaction = null)
        {
            this.connection = connection as SqliteConnection;
            this.transaction = transaction as SqliteTransaction;
            this.tableDescription = tableDescription;
            (this.tableName, this.trackingName) = SqliteTableBuilder.GetParsers(this.tableDescription);
            this.sqliteDbMetadata = new SqliteDbMetadata();
        }


        public void CreateIndex() { }

        public void CreatePk() { return; }

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

        public string CreateTableCommandText()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"CREATE TABLE {trackingName.Quoted().ToString()} (");

            // Adding the primary key
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var quotedColumnName = ParserName.Parse(pkColumn).Quoted().ToString();

                var columnTypeString = this.sqliteDbMetadata.TryGetOwnerDbTypeString(pkColumn.OriginalDbType, pkColumn.GetDbType(), false, false, pkColumn.MaxLength, this.tableDescription.OriginalProvider, SqliteSyncProvider.ProviderType);
                var columnPrecisionString = this.sqliteDbMetadata.TryGetOwnerDbTypePrecision(pkColumn.OriginalDbType, pkColumn.GetDbType(), false, false, pkColumn.MaxLength, pkColumn.Precision, pkColumn.Scale, this.tableDescription.OriginalProvider, SqliteSyncProvider.ProviderType);
                var quotedColumnType = ParserName.Parse(columnTypeString).Quoted().ToString();
                quotedColumnType += columnPrecisionString;

                stringBuilder.AppendLine($"{quotedColumnName} {quotedColumnType} NOT NULL COLLATE NOCASE, ");
            }

            // adding the tracking columns
            stringBuilder.AppendLine($"[update_scope_id] [text] NULL COLLATE NOCASE, ");
            stringBuilder.AppendLine($"[timestamp] [integer] NULL, ");
            stringBuilder.AppendLine($"[sync_row_is_tombstone] [integer] NOT NULL default(0), ");
            stringBuilder.AppendLine($"[last_change_datetime] [datetime] NULL, ");

            stringBuilder.Append(" PRIMARY KEY (");
            for (int i = 0; i < this.tableDescription.PrimaryKeys.Count; i++)
            {
                var pkColumn = this.tableDescription.PrimaryKeys[i];
                var quotedColumnName = ParserName.Parse(pkColumn).Quoted().ToString();

                stringBuilder.Append(quotedColumnName);

                if (i < this.tableDescription.PrimaryKeys.Count- 1)
                    stringBuilder.Append(", ");
            }
            stringBuilder.Append(")");


            stringBuilder.Append(");");

            stringBuilder.AppendLine($"CREATE INDEX [{trackingName.Schema().Unquoted().Normalized().ToString()}_timestamp_index] ON {trackingName.Schema().Quoted().ToString()} (");
            stringBuilder.AppendLine($"\t [timestamp] ASC");
            stringBuilder.AppendLine($"\t,[update_scope_id] ASC");
            stringBuilder.AppendLine($"\t,[sync_row_is_tombstone] ASC");
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                stringBuilder.AppendLine($"\t,{columnName} ASC");
            }
            stringBuilder.Append(");");
            return stringBuilder.ToString();
        }

        public bool NeedToCreateTrackingTable() 
            => !SqliteManagementUtils.TableExists(connection, transaction, trackingName);

  
        public void DropTable()
        {
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqliteCommand($"DROP TABLE IF EXISTS {trackingName.Quoted().ToString()}", connection))
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

    
    }
}
