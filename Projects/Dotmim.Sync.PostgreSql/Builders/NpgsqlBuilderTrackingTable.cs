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
            var stringBuilder = new StringBuilder();
            var tbl = trackingName.ToString();
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(trackingName);
            stringBuilder.AppendLine($"CREATE TABLE {trackingName.Schema().Unquoted().ToString()} (");

            // Adding the primary key
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var quotedColumnName = ParserName.Parse(pkColumn).Unquoted().ToString();
                var columnType = this.dbMetadata.GetCompatibleColumnTypeDeclarationString(pkColumn, this.tableDescription.OriginalProvider);

                var nullableColumn = pkColumn.AllowDBNull ? "NULL" : "NOT NULL";
                stringBuilder.AppendLine($"{quotedColumnName} {columnType} {nullableColumn}, ");
            }

            // adding the tracking columns
            stringBuilder.AppendLine($"update_scope_id uuid NULL, ");
            stringBuilder.AppendLine($"timestamp bigint NULL, ");
            stringBuilder.AppendLine($"sync_row_is_tombstone boolean NOT NULL default FALSE, ");
            stringBuilder.AppendLine($"last_change_datetime timestamptz NULL ");
            stringBuilder.AppendLine(");");

            // Primary Keys
            stringBuilder.Append($"ALTER TABLE {trackingName.Schema().Unquoted().ToString()} ADD CONSTRAINT PK_{trackingName.Schema().Unquoted().Normalized().ToString()} PRIMARY KEY (");

            var primaryKeysColumns = this.tableDescription.GetPrimaryKeysColumns().ToList();
            for (int i = 0; i < primaryKeysColumns.Count; i++)
            {
                var pkColumn = primaryKeysColumns[i];
                var quotedColumnName = ParserName.Parse(pkColumn).Unquoted().ToString();
                stringBuilder.Append(quotedColumnName);

                if (i < primaryKeysColumns.Count - 1)
                    stringBuilder.Append(", ");
            }
            stringBuilder.AppendLine(");");


            // Index
            var indexName = trackingName.Schema().Unquoted().Normalized().ToString();

            stringBuilder.AppendLine($"CREATE INDEX {indexName}_timestamp_index ON {trackingName.Schema().Unquoted().ToString()} (");
            stringBuilder.AppendLine($"\t  timestamp ASC");
            stringBuilder.AppendLine($"\t, update_scope_id ASC");
            stringBuilder.AppendLine($"\t, sync_row_is_tombstone ASC");
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn).Unquoted().ToString();
                stringBuilder.AppendLine($"\t,{columnName} ASC");
            }
            stringBuilder.Append(");");

            var command = new NpgsqlCommand(stringBuilder.ToString(), (NpgsqlConnection)connection, (NpgsqlTransaction)transaction);
            NpgsqlParameter sqlParameter = new NpgsqlParameter()
            {
                ParameterName = "@tableName",
                Value = tbl
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

        public Task<DbCommand> GetDropTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var tbl = trackingName.ToString();
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(trackingName);

            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"DROP TABLE {trackingName.Schema().Unquoted().ToString()};");

            var command = new NpgsqlCommand(stringBuilder.ToString(), (NpgsqlConnection)connection, (NpgsqlTransaction)transaction);

            NpgsqlParameter sqlParameter = new NpgsqlParameter()
            {
                ParameterName = "@tableName",
                Value = tbl
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
            var tbl = trackingName.ToString();
            var schema = NpgsqlManagementUtils.GetUnquotedSqlSchemaName(trackingName);

            var command = connection.CreateCommand();

            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = @"SELECT EXISTS (SELECT FROM PG_TABLES WHERE SCHEMANAME=@SCHEMANAME AND TABLENAME=@TABLENAME)";
            
            var parameter = command.CreateParameter();
            parameter.ParameterName = "@TABLENAME";
            parameter.Value = tbl;
            command.Parameters.Add(parameter);

            parameter = command.CreateParameter();
            parameter.ParameterName = "@SCHEMANAME";
            parameter.Value = schema;
            command.Parameters.Add(parameter);

            return Task.FromResult(command);
        }

        public Task<DbCommand> GetRenameTrackingTableCommandAsync(ParserName oldTableName, DbConnection connection, DbTransaction transaction)
        {
            StringBuilder stringBuilder = new StringBuilder();

            var schemaName = this.trackingName.SchemaName;
            var tableName = this.trackingName.ObjectName;

            schemaName = string.IsNullOrEmpty(schemaName) ? "public" : schemaName;
            var oldSchemaNameString = string.IsNullOrEmpty(oldTableName.SchemaName) ? "public" : oldTableName.SchemaName;

            var oldFullName = $"{oldSchemaNameString}.{oldTableName}";

            // First of all, renaming the table   
            stringBuilder.Append($"EXEC sp_rename '{oldFullName}', '{tableName}'; ");

            // then if necessary, move to another schema
            if (!string.Equals(oldSchemaNameString, schemaName, SyncGlobalization.DataSourceStringComparison))
            {
                var tmpName = $"{oldSchemaNameString}.{tableName}";
                stringBuilder.Append($"ALTER SCHEMA {schemaName} TRANSFER {tmpName};");
            }
            var command = new NpgsqlCommand(stringBuilder.ToString(), (NpgsqlConnection)connection, (NpgsqlTransaction)transaction);

            return Task.FromResult((DbCommand)command);
        }
    }
}