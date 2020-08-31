﻿using Dotmim.Sync.Builders;



using Dotmim.Sync.SqlServer.Manager;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.SqlServer.Builders
{
    public class SqlBuilderTrackingTable : IDbBuilderTrackingTableHelper
    {
        private ParserName tableName;
        private ParserName trackingName;
        private readonly SyncTable tableDescription;
        private readonly SyncSetup setup;
        private readonly SqlDbMetadata sqlDbMetadata;

        public SqlBuilderTrackingTable(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup)
        {
            this.tableDescription = tableDescription;
            this.setup = setup;
            this.tableName = tableName;
            this.trackingName = trackingName;
            this.sqlDbMetadata = new SqlDbMetadata();
        }

        public async Task CreateIndexAsync(DbConnection connection, DbTransaction transaction)
        {
            using (var command = new SqlCommand(this.CreateIndexCommandText(), (SqlConnection)connection, (SqlTransaction)transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }

        }

        private string CreateIndexCommandText()
        {
            var stringBuilder = new StringBuilder();
            var indexName = trackingName.Schema().Unquoted().Normalized().ToString();
            var tableName = trackingName.Schema().Quoted().ToString();

            stringBuilder.AppendLine($"CREATE NONCLUSTERED INDEX [{indexName}_timestamp_index] ON {tableName} (");
            stringBuilder.AppendLine($"\t  [timestamp_bigint] ASC");
            stringBuilder.AppendLine($"\t, [update_scope_id] ASC");
            stringBuilder.AppendLine($"\t, [sync_row_is_tombstone] ASC");
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                stringBuilder.AppendLine($"\t,{columnName} ASC");
            }
            stringBuilder.Append(")");
            return stringBuilder.ToString();
        }

        public async Task CreatePkAsync(DbConnection connection, DbTransaction transaction)
        {
            using (var command = new SqlCommand(this.CreatePkCommandText(), (SqlConnection)connection, (SqlTransaction)transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }

        public string CreatePkCommandText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.Append($"ALTER TABLE {trackingName.Schema().Quoted().ToString()} ADD CONSTRAINT [PK_{trackingName.Schema().Unquoted().Normalized().ToString()}] PRIMARY KEY (");

            var primaryKeysColumns = this.tableDescription.GetPrimaryKeysColumns().ToList();
            for (int i = 0; i < primaryKeysColumns.Count; i++)
            {
                var pkColumn = primaryKeysColumns[i];
                var quotedColumnName = ParserName.Parse(pkColumn).Quoted().ToString();
                stringBuilder.Append(quotedColumnName);

                if (i < primaryKeysColumns.Count - 1)
                    stringBuilder.Append(", ");
            }
            stringBuilder.Append(")");

            return stringBuilder.ToString();
        }

        public async Task CreateTableAsync(DbConnection connection, DbTransaction transaction)
        {
            using (var command = new SqlCommand(this.CreateTableCommandText(), (SqlConnection)connection, (SqlTransaction)transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }

        public async Task DropTableAsync(DbConnection connection, DbTransaction transaction)
        {
            using (var command = new SqlCommand(this.CreateDropTableCommandText(), (SqlConnection)connection, (SqlTransaction)transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }

        private string CreateDropTableCommandText()
            => $"DROP TABLE {trackingName.Schema().Quoted().ToString()};";

        private string CreateTableCommandText()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"CREATE TABLE {trackingName.Schema().Quoted().ToString()} (");

            // Adding the primary key
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var quotedColumnName = ParserName.Parse(pkColumn).Quoted().ToString();

                var columnTypeString = this.sqlDbMetadata.TryGetOwnerDbTypeString(pkColumn.OriginalDbType, pkColumn.GetDbType(), false, false, pkColumn.MaxLength, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
                var quotedColumnType = ParserName.Parse(columnTypeString).Quoted().ToString();
                var columnPrecisionString = this.sqlDbMetadata.TryGetOwnerDbTypePrecision(pkColumn.OriginalDbType, pkColumn.GetDbType(), false, false, pkColumn.MaxLength, pkColumn.Precision, pkColumn.Scale, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
                var columnType = $"{quotedColumnType} {columnPrecisionString}";

                var nullableColumn = pkColumn.AllowDBNull ? "NULL" : "NOT NULL";
                stringBuilder.AppendLine($"{quotedColumnName} {columnType} {nullableColumn}, ");
            }

            // adding the tracking columns
            stringBuilder.AppendLine($"[update_scope_id] [uniqueidentifier] NULL, ");
            stringBuilder.AppendLine($"[timestamp] [timestamp] NULL, ");
            stringBuilder.AppendLine($"[timestamp_bigint] AS (CONVERT([bigint],[timestamp])) PERSISTED, ");
            stringBuilder.AppendLine($"[sync_row_is_tombstone] [bit] NOT NULL default(0), ");
            stringBuilder.AppendLine($"[last_change_datetime] [datetime] NULL, ");

            stringBuilder.Append(")");
            return stringBuilder.ToString();
        }

        public async Task<bool> NeedToCreateTrackingTableAsync(DbConnection connection, DbTransaction transaction) =>
            !await SqlManagementUtils.TableExistsAsync((SqlConnection)connection, (SqlTransaction)transaction, trackingName.Schema().Quoted().ToString()).ConfigureAwait(false);


        public async Task RenameTableAsync(ParserName oldTableName, DbConnection connection, DbTransaction transaction)
        {
            using (var command = new SqlCommand(this.RenameTableCommandText(oldTableName), (SqlConnection)connection, (SqlTransaction)transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }

        public string RenameTableCommandText(ParserName oldTableName)
        {
            StringBuilder stringBuilder = new StringBuilder();

            var schemaName = this.trackingName.SchemaName;
            var tableName = this.trackingName.ObjectName;

            schemaName = string.IsNullOrEmpty(schemaName) ? "dbo" : schemaName;
            var oldSchemaNameString = string.IsNullOrEmpty(oldTableName.SchemaName) ? "dbo" : oldTableName.SchemaName;

            var oldFullName = $"{oldSchemaNameString}.{oldTableName}";

            // First of all, renaming the table   
            stringBuilder.Append($"EXEC sp_rename '{oldFullName}', '{tableName}'; ");

            // then if necessary, move to another schema
            if (!string.Equals(oldSchemaNameString, schemaName, SyncGlobalization.DataSourceStringComparison))
            {
                var tmpName = $"[{oldSchemaNameString}].[{tableName}]";
                stringBuilder.Append($"ALTER SCHEMA {schemaName} TRANSFER {tmpName};");
            }

            return stringBuilder.ToString();
        }


    }
}