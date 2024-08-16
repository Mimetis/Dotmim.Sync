using Dotmim.Sync.DatabaseStringParsers;
using Npgsql;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.PostgreSql.Builders
{

    /// <summary>
    /// Represents a tracking table builder for PostgreSql.
    /// </summary>
    public class NpgsqlBuilderTrackingTable
    {

        /// <summary>
        /// Gets the table description.
        /// </summary>
        protected SyncTable TableDescription { get; }

        /// <summary>
        /// Gets the ,npgsql object names.
        /// </summary>
        protected NpgsqlObjectNames NpgsqlObjectNames { get; }

        /// <summary>
        /// Gets the npgsql database metadata.
        /// </summary>
        protected NpgsqlDbMetadata NpgsqlDbMetadata { get; }

        /// <inheritdoc cref="NpgsqlBuilderTrackingTable"/>
        public NpgsqlBuilderTrackingTable(SyncTable tableDescription, NpgsqlObjectNames npgsqlObjectNames, NpgsqlDbMetadata npgsqlDbMetadata)
        {
            this.TableDescription = tableDescription;
            this.NpgsqlObjectNames = npgsqlObjectNames;
            this.NpgsqlDbMetadata = npgsqlDbMetadata;
        }

        /// <summary>
        /// Returns a command to create a tracking table.
        /// </summary>
        public Task<DbCommand> GetCreateTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"CREATE TABLE {this.NpgsqlObjectNames.TrackingTableQuotedFullName} (");

            // Adding the primary key
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(pkColumn.ColumnName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);

                var columnType = this.NpgsqlDbMetadata.GetCompatibleColumnTypeDeclarationString(pkColumn, this.TableDescription.OriginalProvider);

                var nullableColumn = pkColumn.AllowDBNull ? "NULL" : "NOT NULL";
                stringBuilder.AppendLine($"{columnParser.QuotedShortName} {columnType} {nullableColumn}, ");
            }

            // adding the tracking columns
            stringBuilder.AppendLine($"\"update_scope_id\" uuid NULL, ");
            stringBuilder.AppendLine($"\"timestamp\" bigint NULL, ");
            stringBuilder.AppendLine($"\"sync_row_is_tombstone\" smallint NOT NULL default 0, ");
            stringBuilder.AppendLine($"\"last_change_datetime\" timestamptz NULL ");
            stringBuilder.AppendLine(");");

            // Primary Keys
            stringBuilder.Append($"ALTER TABLE {this.NpgsqlObjectNames.TrackingTableQuotedFullName} ADD CONSTRAINT PK_{this.NpgsqlObjectNames.TrackingTableNormalizedFullName} PRIMARY KEY (");

            var primaryKeysColumns = this.TableDescription.GetPrimaryKeysColumns().ToList();
            for (int i = 0; i < primaryKeysColumns.Count; i++)
            {
                var pkColumn = primaryKeysColumns[i];
                var pkColumnParser = new ObjectParser(pkColumn.ColumnName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);
                stringBuilder.Append(pkColumnParser.QuotedShortName);

                if (i < primaryKeysColumns.Count - 1)
                    stringBuilder.Append(", ");
            }

            stringBuilder.AppendLine(");");

            stringBuilder.AppendLine($"CREATE INDEX {this.NpgsqlObjectNames.TrackingTableNormalizedFullName}_timestamp_index ON {this.NpgsqlObjectNames.TrackingTableQuotedFullName} (");
            stringBuilder.AppendLine($"\t  \"timestamp\" ASC");
            stringBuilder.AppendLine($"\t, \"update_scope_id\" ASC");
            stringBuilder.AppendLine($"\t, \"sync_row_is_tombstone\" ASC");
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var pkColumnParser = new ObjectParser(pkColumn.ColumnName, NpgsqlObjectNames.LeftQuote, NpgsqlObjectNames.RightQuote);
                stringBuilder.AppendLine($"\t,{pkColumnParser.QuotedShortName} ASC");
            }

            stringBuilder.Append(");");

            var command = new NpgsqlCommand(stringBuilder.ToString(), (NpgsqlConnection)connection, (NpgsqlTransaction)transaction);
            NpgsqlParameter sqlParameter = new NpgsqlParameter()
            {
                ParameterName = "@tableName",
                Value = this.NpgsqlObjectNames.TrackingTableName,
            };
            command.Parameters.Add(sqlParameter);

            sqlParameter = new NpgsqlParameter()
            {
                ParameterName = "@schemaName",
                Value = this.NpgsqlObjectNames.TrackingTableSchemaName,
            };
            command.Parameters.Add(sqlParameter);

            return Task.FromResult((DbCommand)command);
        }

        /// <summary>
        /// Returns a command to drop a tracking table.
        /// </summary>
        public Task<DbCommand> GetDropTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"DROP TABLE {this.NpgsqlObjectNames.TrackingTableQuotedFullName};");

            var command = new NpgsqlCommand(stringBuilder.ToString(), (NpgsqlConnection)connection, (NpgsqlTransaction)transaction);

            NpgsqlParameter sqlParameter = new NpgsqlParameter()
            {
                ParameterName = "@tableName",
                Value = this.NpgsqlObjectNames.TrackingTableName,
            };
            command.Parameters.Add(sqlParameter);

            sqlParameter = new NpgsqlParameter()
            {
                ParameterName = "@schemaName",
                Value = this.NpgsqlObjectNames.TrackingTableSchemaName,
            };
            command.Parameters.Add(sqlParameter);

            return Task.FromResult((DbCommand)command);
        }

        /// <summary>
        /// Returns a command to check if a tracking table exists.
        /// </summary>
        public Task<DbCommand> GetExistsTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();

            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = @"select exists (select from pg_tables where schemaname=@schemaname and tablename=@tablename)";

            var parameter = command.CreateParameter();
            parameter.ParameterName = "@tablename";
            parameter.Value = this.NpgsqlObjectNames.TrackingTableName;
            command.Parameters.Add(parameter);

            parameter = command.CreateParameter();
            parameter.ParameterName = "@schemaname";
            parameter.Value = this.NpgsqlObjectNames.TrackingTableSchemaName;
            command.Parameters.Add(parameter);

            return Task.FromResult(command);
        }
    }
}