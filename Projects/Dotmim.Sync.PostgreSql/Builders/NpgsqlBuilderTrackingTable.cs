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
            stringBuilder.AppendLine($"CREATE TABLE \"{schema}\".{trackingTableQuoted} (");

            // Adding the primary key
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var quotedColumnName = ParserName.Parse(pkColumn, "\"").Quoted().ToString();

                var columnType = this.dbMetadata.GetCompatibleColumnTypeDeclarationString(pkColumn, this.tableDescription.OriginalProvider);

                var nullableColumn = pkColumn.AllowDBNull ? "NULL" : "NOT NULL";
                stringBuilder.AppendLine($"{quotedColumnName} {columnType} {nullableColumn}, ");
            }

            // adding the tracking columns
            stringBuilder.AppendLine($"\"update_scope_id\" uuid NULL, ");
            stringBuilder.AppendLine($"\"timestamp\" bigint NULL, ");
            stringBuilder.AppendLine($"\"sync_row_is_tombstone\" boolean NOT NULL default FALSE, ");
            stringBuilder.AppendLine($"\"last_change_datetime\" timestamptz NULL ");
            stringBuilder.AppendLine(");");

            // Primary Keys
            stringBuilder.Append($"ALTER TABLE \"{schema}\".{trackingTableQuoted} ADD CONSTRAINT PK_{trackingTableUnquoted} PRIMARY KEY (");

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

            stringBuilder.AppendLine($"CREATE INDEX {trackingTableUnquoted}_timestamp_index ON \"{schema}\".{trackingTableQuoted} (");
            stringBuilder.AppendLine($"\t  \"timestamp\" ASC");
            stringBuilder.AppendLine($"\t, \"update_scope_id\" ASC");
            stringBuilder.AppendLine($"\t, \"sync_row_is_tombstone\" ASC");
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn, "\"").Quoted().ToString();
                stringBuilder.AppendLine($"\t,{columnName} ASC");
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
            stringBuilder.AppendLine($"DROP TABLE \"{schema}\".{trackingTableQuoted};");

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