using Dotmim.Sync.Builders;



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
    public class SqlBuilderTrackingTable
    {
        private ParserName trackingName;
        private readonly SyncTable tableDescription;
        private readonly SyncSetup setup;
        private readonly SqlDbMetadata sqlDbMetadata;

        public SqlBuilderTrackingTable(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup)
        {
            this.tableDescription = tableDescription;
            this.setup = setup;
            this.trackingName = trackingName;
            this.sqlDbMetadata = new SqlDbMetadata();
        }

        public Task<DbCommand> GetCreateTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var stringBuilder = new StringBuilder();
            var tbl = trackingName.ToString();
            var schema = SqlManagementUtils.GetUnquotedSqlSchemaName(trackingName);
            stringBuilder.Append("CREATE TABLE ").Append(trackingName.Schema().Quoted()).AppendLine(" (");

            // Adding the primary key
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var quotedColumnName = ParserName.Parse(pkColumn).Quoted().ToString();
                var columnType = this.sqlDbMetadata.GetCompatibleColumnTypeDeclarationString(pkColumn, this.tableDescription.OriginalProvider);

                var nullableColumn = pkColumn.AllowDBNull ? "NULL" : "NOT NULL";
                stringBuilder.Append(quotedColumnName).Append(' ').Append(columnType).Append(' ').Append(nullableColumn).AppendLine(", ");
            }

            // adding the tracking columns
            stringBuilder.AppendLine($"[update_scope_id] [uniqueidentifier] NULL, ");
            stringBuilder.AppendLine($"[timestamp] [timestamp] NULL, ");
            stringBuilder.AppendLine($"[timestamp_bigint] AS (CONVERT([bigint],[timestamp])) PERSISTED, ");
            stringBuilder.AppendLine($"[sync_row_is_tombstone] [bit] NOT NULL default(0), ");
            stringBuilder.AppendLine($"[last_change_datetime] [datetime] NULL, ");
            stringBuilder.AppendLine(");");

            // Primary Keys
            stringBuilder.Append("ALTER TABLE ").Append(trackingName.Schema().Quoted()).Append(" ADD CONSTRAINT [PK_").Append(trackingName.Schema().Unquoted().Normalized()).Append("] PRIMARY KEY (");

            var primaryKeysColumns = this.tableDescription.GetPrimaryKeysColumns().ToList();
            for (int i = 0; i < primaryKeysColumns.Count; i++)
            {
                var pkColumn = primaryKeysColumns[i];
                var quotedColumnName = ParserName.Parse(pkColumn).Quoted().ToString();
                stringBuilder.Append(quotedColumnName);

                if (i < primaryKeysColumns.Count - 1)
                    stringBuilder.Append(", ");
            }
            stringBuilder.AppendLine(");");


            // Index
            var indexName = trackingName.Schema().Unquoted().Normalized().ToString();

            stringBuilder.Append("CREATE NONCLUSTERED INDEX [").Append(indexName).Append("_timestamp_index] ON ").Append(trackingName.Schema().Quoted()).AppendLine(" (");
            stringBuilder.AppendLine($"\t  [timestamp_bigint] ASC");
            stringBuilder.AppendLine($"\t, [update_scope_id] ASC");
            stringBuilder.AppendLine($"\t, [sync_row_is_tombstone] ASC");
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                stringBuilder.Append("\t,").Append(columnName).AppendLine(" ASC");
            }
            stringBuilder.Append(");");

            var command = new SqlCommand(stringBuilder.ToString(), (SqlConnection)connection, (SqlTransaction)transaction);
            SqlParameter sqlParameter = new SqlParameter()
            {
                ParameterName = "@tableName",
                Value = tbl
            };
            command.Parameters.Add(sqlParameter);

            sqlParameter = new SqlParameter()
            {
                ParameterName = "@schemaName",
                Value = schema
            };
            command.Parameters.Add(sqlParameter);

            return Task.FromResult((DbCommand)command);
        }

        public Task<DbCommand> GetDropTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var tbl = trackingName.ToString();
            var schema = SqlManagementUtils.GetUnquotedSqlSchemaName(trackingName);

            var stringBuilder = new StringBuilder();
            stringBuilder.Append("ALTER TABLE ").Append(trackingName.Schema().Quoted().ToString()).Append(" NOCHECK CONSTRAINT ALL; DROP TABLE ").Append(trackingName.Schema().Quoted()).AppendLine(";");

            var command = new SqlCommand(stringBuilder.ToString(), (SqlConnection)connection, (SqlTransaction)transaction);

            SqlParameter sqlParameter = new SqlParameter()
            {
                ParameterName = "@tableName",
                Value = tbl
            };
            command.Parameters.Add(sqlParameter);

            sqlParameter = new SqlParameter()
            {
                ParameterName = "@schemaName",
                Value = schema
            };
            command.Parameters.Add(sqlParameter);

            return Task.FromResult((DbCommand)command);
        }
        public Task<DbCommand> GetRenameTrackingTableCommandAsync(ParserName oldTableName, DbConnection connection, DbTransaction transaction)
        {
            StringBuilder stringBuilder = new StringBuilder();

            var schemaName = this.trackingName.SchemaName;
            var tableName = this.trackingName.ObjectName;

            schemaName = string.IsNullOrEmpty(schemaName) ? "dbo" : schemaName;
            var oldSchemaNameString = string.IsNullOrEmpty(oldTableName.SchemaName) ? "dbo" : oldTableName.SchemaName;

            var oldFullName = $"{oldSchemaNameString}.{oldTableName}";

            // First of all, renaming the table   
            stringBuilder.Append("EXEC sp_rename '").Append(oldFullName).Append("', '").Append(tableName).Append("'; ");

            // then if necessary, move to another schema
            if (!string.Equals(oldSchemaNameString, schemaName, SyncGlobalization.DataSourceStringComparison))
            {
                var tmpName = $"[{oldSchemaNameString}].[{tableName}]";
                stringBuilder.Append("ALTER SCHEMA ").Append(schemaName).Append(" TRANSFER ").Append(tmpName).Append(';');
            }
            var command = new SqlCommand(stringBuilder.ToString(), (SqlConnection)connection, (SqlTransaction)transaction);

            return Task.FromResult((DbCommand)command);
        }
        public Task<DbCommand> GetExistsTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var tbl = trackingName.ToString();
            var schema = SqlManagementUtils.GetUnquotedSqlSchemaName(trackingName);

            var command = connection.CreateCommand();

            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = $"IF EXISTS (SELECT t.name FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE t.name = @tableName AND s.name = @schemaName) SELECT 1 ELSE SELECT 0;";

            var parameter = command.CreateParameter();
            parameter.ParameterName = "@tableName";
            parameter.Value = tbl;
            command.Parameters.Add(parameter);

            parameter = command.CreateParameter();
            parameter.ParameterName = "@schemaName";
            parameter.Value = schema;
            command.Parameters.Add(parameter);

            return Task.FromResult(command);
        }

    }
}