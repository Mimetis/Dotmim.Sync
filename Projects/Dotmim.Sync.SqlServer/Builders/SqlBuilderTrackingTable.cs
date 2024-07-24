using Dotmim.Sync.Builders;
using Dotmim.Sync.DatabaseStringParsers;
using Dotmim.Sync.SqlServer.Manager;
using Microsoft.Data.SqlClient;
using System;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.SqlServer.Builders
{
    /// <summary>
    /// Sql tracking table builder for Sql Server.
    /// </summary>
    public class SqlBuilderTrackingTable
    {
        private readonly SyncTable tableDescription;
        private readonly SqlDbMetadata sqlDbMetadata;

        private readonly string trackingTableName;
        private readonly string quotedTrackingTableName;
        private readonly string normalizedTrackingTableName;
        private readonly string trackingSchemaName;

        private readonly string trackingTableFullName;

        /// <inheritdoc cref="SqlBuilderTrackingTable" />
        public SqlBuilderTrackingTable(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup)
        {
            this.trackingTableFullName = trackingName.Quoted().Schema().ToString();

            this.tableDescription = tableDescription;
            var trackingTableParser = new TableParser(this.trackingTableFullName.AsSpan(), '[', ']');

            this.trackingTableName = trackingTableParser.TableName;
            this.normalizedTrackingTableName = trackingTableParser.NormalizedFullName;
            this.quotedTrackingTableName = trackingTableParser.QuotedFullName;
            this.trackingSchemaName = SqlManagementUtils.GetUnquotedSqlSchemaName(trackingTableParser);

            this.sqlDbMetadata = new SqlDbMetadata();
        }

        public Task<DbCommand> GetCreateTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var stringBuilder = new StringBuilder();

            stringBuilder.AppendLine($"CREATE TABLE {this.quotedTrackingTableName} (");

            // Adding the primary key
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var qColumnName = new ColumnParser(pkColumn.ColumnName.AsSpan(), '[', ']');
                var columnType = this.sqlDbMetadata.GetCompatibleColumnTypeDeclarationString(pkColumn, this.tableDescription.OriginalProvider);

                var nullableColumn = pkColumn.AllowDBNull ? "NULL" : "NOT NULL";
                stringBuilder.AppendLine($"{qColumnName.QuotedShortName} {columnType} {nullableColumn}, ");
            }

            // adding the tracking columns
            stringBuilder.AppendLine($"[update_scope_id] [uniqueidentifier] NULL, ");
            stringBuilder.AppendLine($"[timestamp] [timestamp] NULL, ");
            stringBuilder.AppendLine($"[timestamp_bigint] AS (CONVERT([bigint],[timestamp])) PERSISTED, ");
            stringBuilder.AppendLine($"[sync_row_is_tombstone] [bit] NOT NULL default(0), ");
            stringBuilder.AppendLine($"[last_change_datetime] [datetime] NULL, ");
            stringBuilder.AppendLine(");");

            // Primary Keys
            stringBuilder.Append($"ALTER TABLE {this.quotedTrackingTableName} ADD CONSTRAINT [PK_{this.normalizedTrackingTableName}] PRIMARY KEY (");

            var primaryKeysColumns = this.tableDescription.GetPrimaryKeysColumns().ToList();
            for (int i = 0; i < primaryKeysColumns.Count; i++)
            {
                var qColumnName = new ColumnParser(primaryKeysColumns[i].ColumnName.AsSpan(), '[', ']');

                stringBuilder.Append(qColumnName.QuotedShortName);

                if (i < primaryKeysColumns.Count - 1)
                    stringBuilder.Append(", ");
            }

            stringBuilder.AppendLine(");");

            stringBuilder.AppendLine($"CREATE NONCLUSTERED INDEX [{this.normalizedTrackingTableName}_timestamp_index] ON {this.quotedTrackingTableName} (");
            stringBuilder.AppendLine($"\t  [timestamp_bigint] ASC");
            stringBuilder.AppendLine($"\t, [update_scope_id] ASC");
            stringBuilder.AppendLine($"\t, [sync_row_is_tombstone] ASC");
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var qColumnName = new ColumnParser(pkColumn.ColumnName.AsSpan(), '[', ']');
                stringBuilder.AppendLine($"\t,{qColumnName.QuotedShortName} ASC");
            }

            stringBuilder.Append(");");

            var command = new SqlCommand(stringBuilder.ToString(), (SqlConnection)connection, (SqlTransaction)transaction);

            return Task.FromResult((DbCommand)command);
        }

        public Task<DbCommand> GetDropTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"ALTER TABLE {this.quotedTrackingTableName} NOCHECK CONSTRAINT ALL; DROP TABLE {this.quotedTrackingTableName};");

            var command = new SqlCommand(stringBuilder.ToString(), (SqlConnection)connection, (SqlTransaction)transaction);

            return Task.FromResult((DbCommand)command);
        }

        public Task<DbCommand> GetRenameTrackingTableCommandAsync(ParserName oldTableName, DbConnection connection, DbTransaction transaction)
        {
            StringBuilder stringBuilder = new StringBuilder();

            var oldSchemaNameString = string.IsNullOrEmpty(oldTableName.SchemaName) ? "dbo" : oldTableName.SchemaName;
            var oldFullName = $"{oldSchemaNameString}.{oldTableName}";

            // First of all, renaming the table
            stringBuilder.Append($"EXEC sp_rename '{oldFullName}', '{this.trackingTableName}'; ");

            // then if necessary, move to another schema
            if (!string.Equals(oldSchemaNameString, this.trackingSchemaName, SyncGlobalization.DataSourceStringComparison))
            {
                var tmpName = $"[{oldSchemaNameString}].[{this.trackingTableName}]";
                stringBuilder.Append($"ALTER SCHEMA {this.trackingSchemaName} TRANSFER {tmpName};");
            }

            var command = new SqlCommand(stringBuilder.ToString(), (SqlConnection)connection, (SqlTransaction)transaction);

            return Task.FromResult((DbCommand)command);
        }

        public Task<DbCommand> GetExistsTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();

            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = $"IF EXISTS (SELECT t.name FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE t.name = @tableName AND s.name = @schemaName) SELECT 1 ELSE SELECT 0;";

            var parameter = command.CreateParameter();
            parameter.ParameterName = "@tableName";
            parameter.Value = this.trackingTableName;
            command.Parameters.Add(parameter);

            parameter = command.CreateParameter();
            parameter.ParameterName = "@schemaName";
            parameter.Value = this.trackingSchemaName;
            command.Parameters.Add(parameter);

            return Task.FromResult(command);
        }
    }
}