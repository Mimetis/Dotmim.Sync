using Dotmim.Sync.DatabaseStringParsers;
using Dotmim.Sync.SqlServer.Manager;
using Microsoft.Data.SqlClient;
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
        /// <summary>
        /// Gets the table description.
        /// </summary>
        protected SyncTable TableDescription { get; }

        /// <summary>
        /// Gets the sql object names.
        /// </summary>
        protected SqlObjectNames SqlObjectNames { get; }

        /// <summary>
        /// Gets the sql database metadata.
        /// </summary>
        protected SqlDbMetadata SqlDbMetadata { get; }

        /// <inheritdoc cref="SqlBuilderTrackingTable" />
        public SqlBuilderTrackingTable(SyncTable tableDescription, SqlObjectNames sqlObjectNames, SqlDbMetadata sqlDbMetadata)
        {
            this.TableDescription = tableDescription;
            this.SqlObjectNames = sqlObjectNames;
            this.SqlDbMetadata = sqlDbMetadata;
        }

        /// <summary>
        /// Gets the command to create the tracking table.
        /// </summary>
        public Task<DbCommand> GetCreateTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var stringBuilder = new StringBuilder();

            stringBuilder.AppendLine($"CREATE TABLE {this.SqlObjectNames.TrackingTableQuotedFullName} (");

            // Adding the primary key
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var qColumnName = new ObjectParser(pkColumn.ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);
                var columnType = this.SqlDbMetadata.GetCompatibleColumnTypeDeclarationString(pkColumn, this.TableDescription.OriginalProvider);

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
            stringBuilder.Append($"ALTER TABLE {this.SqlObjectNames.TrackingTableQuotedFullName} ADD CONSTRAINT [PK_{this.SqlObjectNames.TrackingTableNormalizedFullName}] PRIMARY KEY (");

            var primaryKeysColumns = this.TableDescription.GetPrimaryKeysColumns().ToList();
            for (int i = 0; i < primaryKeysColumns.Count; i++)
            {
                var qColumnName = new ObjectParser(primaryKeysColumns[i].ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);

                stringBuilder.Append(qColumnName.QuotedShortName);

                if (i < primaryKeysColumns.Count - 1)
                    stringBuilder.Append(", ");
            }

            stringBuilder.AppendLine(");");

            stringBuilder.AppendLine($"CREATE NONCLUSTERED INDEX [{this.SqlObjectNames.TrackingTableNormalizedFullName}_timestamp_index] ON {this.SqlObjectNames.TrackingTableQuotedFullName} (");
            stringBuilder.AppendLine($"\t  [timestamp_bigint] ASC");
            stringBuilder.AppendLine($"\t, [update_scope_id] ASC");
            stringBuilder.AppendLine($"\t, [sync_row_is_tombstone] ASC");
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var qColumnName = new ObjectParser(pkColumn.ColumnName, SqlObjectNames.LeftQuote, SqlObjectNames.RightQuote);
                stringBuilder.AppendLine($"\t,{qColumnName.QuotedShortName} ASC");
            }

            stringBuilder.Append(");");

            var command = new SqlCommand(stringBuilder.ToString(), (SqlConnection)connection, (SqlTransaction)transaction);

            return Task.FromResult((DbCommand)command);
        }

        /// <summary>
        /// Get the command to drop the tracking table.
        /// </summary>
        public Task<DbCommand> GetDropTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"ALTER TABLE {this.SqlObjectNames.TrackingTableQuotedFullName} NOCHECK CONSTRAINT ALL; DROP TABLE {this.SqlObjectNames.TrackingTableQuotedFullName};");

            var command = new SqlCommand(stringBuilder.ToString(), (SqlConnection)connection, (SqlTransaction)transaction);

            return Task.FromResult((DbCommand)command);
        }

        /// <summary>
        /// Get the command to check if the tracking table exists.
        /// </summary>
        public Task<DbCommand> GetExistsTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();

            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = $"IF EXISTS (SELECT t.name FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id WHERE t.name = @tableName AND s.name = @schemaName) SELECT 1 ELSE SELECT 0;";

            var parameter = command.CreateParameter();
            parameter.ParameterName = "@tableName";
            parameter.Value = this.SqlObjectNames.TrackingTableName;
            command.Parameters.Add(parameter);

            parameter = command.CreateParameter();
            parameter.ParameterName = "@schemaName";
            parameter.Value = this.SqlObjectNames.TrackingTableSchemaName;
            command.Parameters.Add(parameter);

            return Task.FromResult(command);
        }
    }
}