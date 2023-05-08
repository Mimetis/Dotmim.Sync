using Dotmim.Sync.Builders;
using Dotmim.Sync.PostgreSql.Builders;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Input;
using static Npgsql.Replication.PgOutput.Messages.RelationMessage;

namespace Dotmim.Sync.PostgreSql.Builders
{
    public class NpgsqlBuilderTrackingTable
    {
        private readonly NpgsqlDbMetadata dbMetadata;
        private SyncSetup setup;
        private SyncTable tableDescription;
        private ParserName tableName;
        private ParserName trackingName;

        public NpgsqlBuilderTrackingTable(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup)
        {
            this.tableDescription = tableDescription;
            this.tableName = tableName;
            this.trackingName = trackingTableName;
            this.setup = setup;
            this.dbMetadata = new NpgsqlDbMetadata();
        }

        public Task<DbCommand> GetCreateTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var trackingTableQuoted = ParserName.Parse(trackingName.ToString(), "\"").Quoted().ToString();
            var trackingTableUnquoted = trackingName.Unquoted().ToString();
            var stringBuilder = new StringBuilder();
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(trackingName);
            stringBuilder.Append("CREATE TABLE \"").Append(schema).Append("\".").Append(trackingTableQuoted).AppendLine(" (");

            // Adding the primary key
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var quotedColumnName = ParserName.Parse(pkColumn, "\"").Quoted().ToString();

                var columnType = this.dbMetadata.GetCompatibleColumnTypeDeclarationString(pkColumn, this.tableDescription.OriginalProvider);

                var nullableColumn = pkColumn.AllowDBNull ? "NULL" : "NOT NULL";
                stringBuilder.Append(quotedColumnName).Append(' ').Append(columnType).Append(' ').Append(nullableColumn).AppendLine(", ");
            }

            // adding the tracking columns
            stringBuilder.AppendLine($"\"update_scope_id\" uuid NULL, ");
            stringBuilder.AppendLine($"\"timestamp\" bigint NULL, ");
            stringBuilder.AppendLine($"\"sync_row_is_tombstone\" smallint NOT NULL default 0, ");
            stringBuilder.AppendLine($"\"last_change_datetime\" timestamptz NULL ");
            stringBuilder.AppendLine(");");

            // Primary Keys
            stringBuilder.Append("ALTER TABLE \"").Append(schema).Append("\".").Append(trackingTableQuoted).Append(" ADD CONSTRAINT PK_").Append(trackingTableUnquoted).Append(" PRIMARY KEY (");

            var primaryKeysColumns = this.tableDescription.GetPrimaryKeysColumns().ToList();
            for (int i = 0; i < primaryKeysColumns.Count; i++)
            {
                var pkColumn = primaryKeysColumns[i];
                var quotedColumnName = ParserName.Parse(pkColumn, "\"").Quoted().ToString();
                stringBuilder.Append(quotedColumnName);

                if (i < primaryKeysColumns.Count - 1)
                    stringBuilder.Append(", ");
            }
            stringBuilder.AppendLine(");");


            // Index
            var indexName = trackingName.Schema().Quoted().Normalized().ToString();

            stringBuilder.Append("CREATE INDEX ").Append(trackingTableUnquoted).Append("_timestamp_index ON \"").Append(schema).Append("\".").Append(trackingTableQuoted).AppendLine(" (");
            stringBuilder.AppendLine($"\t  \"timestamp\" ASC");
            stringBuilder.AppendLine($"\t, \"update_scope_id\" ASC");
            stringBuilder.AppendLine($"\t, \"sync_row_is_tombstone\" ASC");
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn, "\"").Quoted().ToString();
                stringBuilder.Append("\t,").Append(columnName).AppendLine(" ASC");
            }
            stringBuilder.Append(");");

            var command = new NpgsqlCommand(stringBuilder.ToString(), (NpgsqlConnection)connection, (NpgsqlTransaction)transaction);
            NpgsqlParameter sqlParameter = new NpgsqlParameter()
            {
                ParameterName = "@tableName",
                Value = trackingTableUnquoted
            };
            command.Parameters.Add(sqlParameter);

            sqlParameter = new NpgsqlParameter()
            {
                ParameterName = "@schemaName",
                Value = schema
            };
            command.Parameters.Add(sqlParameter);

            var query = stringBuilder.ToString();
            return Task.FromResult((DbCommand)command);
        }

        public Task<DbCommand> GetDropTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var trackingTableQuoted = ParserName.Parse(trackingName.ToString(), "\"").Quoted().ToString();
            var trackingTableUnquoted = trackingName.Unquoted().ToString();
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(trackingName);

            var stringBuilder = new StringBuilder();
            stringBuilder.Append("DROP TABLE \"").Append(schema).Append("\".").Append(trackingTableQuoted).AppendLine(";");

            var command = new NpgsqlCommand(stringBuilder.ToString(), (NpgsqlConnection)connection, (NpgsqlTransaction)transaction);

            NpgsqlParameter sqlParameter = new NpgsqlParameter()
            {
                ParameterName = "@tableName",
                Value = trackingTableUnquoted
            };
            command.Parameters.Add(sqlParameter);

            sqlParameter = new NpgsqlParameter()
            {
                ParameterName = "@schemaName",
                Value = schema
            };
            command.Parameters.Add(sqlParameter);

            return Task.FromResult((DbCommand)command);
        }

        public Task<DbCommand> GetExistsTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var trackingTableQuoted = ParserName.Parse(trackingName.ToString(), "\"").Quoted().ToString();
            var trackingTableUnquoted = trackingName.Unquoted().ToString();
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(trackingName);

            var command = connection.CreateCommand();

            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = @"select exists (select from pg_tables where schemaname=@schemaname and tablename=@tablename)";

            var parameter = command.CreateParameter();
            parameter.ParameterName = "@tablename";
            parameter.Value = trackingTableUnquoted;
            command.Parameters.Add(parameter);

            parameter = command.CreateParameter();
            parameter.ParameterName = "@schemaname";
            parameter.Value = schema;
            command.Parameters.Add(parameter);

            return Task.FromResult(command);
        }

        public Task<DbCommand> GetRenameTrackingTableCommandAsync(ParserName oldTableName, DbConnection connection, DbTransaction transaction)
        {
            return Task.FromResult((DbCommand)null);
        }
    }
}