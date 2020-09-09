using Dotmim.Sync.Builders;

using Dotmim.Sync.MySql.Builders;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.MySql
{
    public class MySqlBuilderTable : IDbBuilderTableHelper
    {
        private readonly ParserName tableName;
        private readonly ParserName trackingName;
        private readonly SyncTable tableDescription;
        private readonly SyncSetup setup;
        private readonly MySqlDbMetadata mySqlDbMetadata;

        private static Dictionary<string, string> createdRelationNames = new Dictionary<string, string>();

        private static string GetRandomString()
            => Path.GetRandomFileName().Replace(".", "").ToLowerInvariant();

        /// <summary>
        /// Ensure the relation name is correct to be created in MySql
        /// </summary>
        public static string NormalizeRelationName(string relation)
        {
            if (createdRelationNames.ContainsKey(relation))
                return createdRelationNames[relation];

            var name = relation;

            if (relation.Length > 65)
                name = $"{relation.Substring(0, 50)}_{GetRandomString()}";

            createdRelationNames.Add(relation, name);

            return name;
        }
        public MySqlBuilderTable(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup)
        {
            this.tableDescription = tableDescription;
            this.setup = setup;
            this.tableName = tableName;
            this.trackingName = trackingName;
            this.mySqlDbMetadata = new MySqlDbMetadata();
        }

        private MySqlCommand BuildTableCommand(DbConnection connection, DbTransaction transaction)
        {
            var stringBuilder = new StringBuilder();

            stringBuilder.AppendLine("SET FOREIGN_KEY_CHECKS=0;");
            stringBuilder.AppendLine($"CREATE TABLE IF NOT EXISTS {this.tableName.Quoted().ToString()} (");
            string empty = string.Empty;
            stringBuilder.AppendLine();
            foreach (var column in this.tableDescription.Columns)
            {
                var columnName = ParserName.Parse(column, "`").Quoted().ToString();
                var stringType = this.mySqlDbMetadata.TryGetOwnerDbTypeString(column.OriginalDbType, column.GetDbType(), false, false, column.MaxLength, this.tableDescription.OriginalProvider, MySqlSyncProvider.ProviderType);
                var stringPrecision = this.mySqlDbMetadata.TryGetOwnerDbTypePrecision(column.OriginalDbType, column.GetDbType(), false, false, column.MaxLength, column.Precision, column.Scale, this.tableDescription.OriginalProvider, MySqlSyncProvider.ProviderType);
                var columnType = $"{stringType} {stringPrecision}";

                var identity = string.Empty;

                if (column.IsAutoIncrement)
                {
                    var s = column.GetAutoIncrementSeedAndStep();
                    if (s.Seed > 1 || s.Step > 1)
                        throw new NotSupportedException("can't establish a seed / step in MySql autoinc value");

                    identity = $"AUTO_INCREMENT";
                }
                var nullString = column.AllowDBNull ? "NULL" : "NOT NULL";

                // if we have a readonly column, we may have a computed one, so we need to allow null
                if (column.IsReadOnly)
                    nullString = "NULL";

                stringBuilder.AppendLine($"\t{empty}{columnName} {columnType} {identity} {nullString}");
                empty = ",";
            }

            if (this.tableDescription.GetMutableColumns().Any(mc => mc.IsAutoIncrement))
                stringBuilder.Append("\t, KEY (");

            empty = string.Empty;
            foreach (var column in this.tableDescription.GetMutableColumns().Where(c => c.IsAutoIncrement))
            {
                var columnName = ParserName.Parse(column, "`").Quoted().ToString();
                stringBuilder.Append($"{empty} {columnName}");
                empty = ",";
            }

            if (this.tableDescription.GetMutableColumns().Any(mc => mc.IsAutoIncrement))
                stringBuilder.AppendLine(")");

            stringBuilder.Append("\t,PRIMARY KEY (");

            int i = 0;
            // It seems we need to specify the increment column in first place
            foreach (var pkColumn in this.tableDescription.GetPrimaryKeysColumns().OrderByDescending(pk => pk.IsAutoIncrement))
            {
                var columnName = ParserName.Parse(pkColumn, "`").Quoted().ToString();

                stringBuilder.Append(columnName);

                if (i < this.tableDescription.PrimaryKeys.Count - 1)
                    stringBuilder.Append(", ");
                i++;
            }

            stringBuilder.Append(")");
            stringBuilder.Append(");");


            // Foreign Keys
            foreach (var constraint in this.tableDescription.GetRelations())
            {
                var tableName = ParserName.Parse(constraint.GetTable(), "`").Quoted().ToString();
                var parentTableName = ParserName.Parse(constraint.GetParentTable(), "`").Quoted().ToString();
                var relationName = NormalizeRelationName(constraint.RelationName);
                var keyColumns = constraint.Keys;
                var referencesColumns = constraint.ParentKeys;

                stringBuilder.Append("ALTER TABLE ");
                stringBuilder.AppendLine(tableName);
                stringBuilder.Append("ADD CONSTRAINT ");

                stringBuilder.AppendLine($"`{relationName}`");
                stringBuilder.Append("FOREIGN KEY (");
                empty = string.Empty;
                foreach (var keyColumn in keyColumns)
                {
                    var foreignKeyColumnName = ParserName.Parse(keyColumn.ColumnName, "`").Quoted().ToString();
                    stringBuilder.Append($"{empty} {foreignKeyColumnName}");
                    empty = ", ";
                }
                stringBuilder.AppendLine(" )");
                stringBuilder.Append("REFERENCES ");
                stringBuilder.Append(parentTableName).Append(" (");
                empty = string.Empty;
                foreach (var referencesColumn in referencesColumns)
                {
                    var referencesColumnName = ParserName.Parse(referencesColumn.ColumnName, "`").Quoted().ToString();
                    stringBuilder.Append($"{empty} {referencesColumnName}");
                    empty = ", ";
                }
                stringBuilder.AppendLine(" );");
            }
            stringBuilder.AppendLine("SET FOREIGN_KEY_CHECKS=1;");


            return new MySqlCommand(stringBuilder.ToString(), (MySqlConnection)connection, (MySqlTransaction)transaction);
        }

        public async Task CreateTableAsync(DbConnection connection, DbTransaction transaction)
        {
            using (var command = this.BuildTableCommand(connection, transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }

        public Task CreateSchemaAsync(DbConnection connection, DbTransaction transaction) => Task.CompletedTask;

        public async Task DropTableAsync(DbConnection connection, DbTransaction transaction)
        {
            var commandText = $"drop table if exists {this.tableName.Quoted().ToString()}";

            using (var command = new MySqlCommand(commandText, (MySqlConnection)connection, (MySqlTransaction)transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }

        }

    }
}
