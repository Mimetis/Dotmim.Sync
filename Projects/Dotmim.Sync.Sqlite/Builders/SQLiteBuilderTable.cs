using Dotmim.Sync.Builders;
using System;
using System.Text;

using System.Data.Common;
using System.Linq;
using System.Data;
using Microsoft.Data.Sqlite;
using System.Diagnostics;
using System.Threading.Tasks;

namespace Dotmim.Sync.Sqlite
{
    public class SqliteBuilderTable : IDbBuilderTableHelper
    {
        private ParserName tableName;
        private ParserName trackingName;
        private SyncTable tableDescription;
        private SyncSetup setup;
        private SqliteDbMetadata sqliteDbMetadata;

        public SqliteBuilderTable(SyncTable tableDescription, ParserName tableName, ParserName trackingName, SyncSetup setup)
        {
            this.tableDescription = tableDescription;
            this.setup = setup;
            this.tableName = tableName;
            this.trackingName = trackingName;
            this.sqliteDbMetadata = new SqliteDbMetadata();
        }

        private string BuildTableCommandText()
        {
            var stringBuilder = new StringBuilder($"CREATE TABLE IF NOT EXISTS {tableName.Quoted().ToString()} (");
            string empty = string.Empty;
            stringBuilder.AppendLine();
            foreach (var column in this.tableDescription.Columns)
            {
                var columnName = ParserName.Parse(column).Quoted().ToString();

                var columnTypeString = this.sqliteDbMetadata.TryGetOwnerDbTypeString(column.OriginalDbType, column.GetDbType(), false, false, column.MaxLength, this.tableDescription.OriginalProvider, SqliteSyncProvider.ProviderType);
                var columnPrecisionString = this.sqliteDbMetadata.TryGetOwnerDbTypePrecision(column.OriginalDbType, column.GetDbType(), false, false, column.MaxLength, column.Precision, column.Scale, this.tableDescription.OriginalProvider, SqliteSyncProvider.ProviderType);
                var columnType = $"{columnTypeString} {columnPrecisionString}";

                // check case
                string casesensitive = "";
                if (this.sqliteDbMetadata.IsTextType(column.GetDbType()))
                {
                    casesensitive = SyncGlobalization.IsCaseSensitive() ? "" : "COLLATE NOCASE";

                    //check if it's a primary key, then, even if it's case sensitive, we turn on case insensitive
                    if (SyncGlobalization.IsCaseSensitive())
                    {
                        if (this.tableDescription.PrimaryKeys.Contains(column.ColumnName))
                            casesensitive = "COLLATE NOCASE";
                    }
                }

                var identity = string.Empty;

                if (column.IsAutoIncrement)
                {
                    var (step, seed) = column.GetAutoIncrementSeedAndStep();
                    if (seed > 1 || step > 1)
                        throw new NotSupportedException("can't establish a seed / step in Sqlite autoinc value");

                    //identity = $"AUTOINCREMENT";
                    // Actually no need to set AutoIncrement, if we insert a null value
                    identity = "";
                }
                var nullString = column.AllowDBNull ? "NULL" : "NOT NULL";

                // if auto inc, don't specify NOT NULL option, since we need to insert a null value to make it auto inc.
                if (column.IsAutoIncrement)
                    nullString = "";
                // if it's a readonly column, it could be a computed column, so we need to allow null
                else if (column.IsReadOnly)
                    nullString = "NULL";

                stringBuilder.AppendLine($"\t{empty}{columnName} {columnType} {identity} {nullString} {casesensitive}");
                empty = ",";
            }
            stringBuilder.Append("\t,PRIMARY KEY (");
            for (int i = 0; i < this.tableDescription.PrimaryKeys.Count; i++)
            {
                var pkColumn = this.tableDescription.PrimaryKeys[i];
                var quotedColumnName = ParserName.Parse(pkColumn).Quoted().ToString();

                stringBuilder.Append(quotedColumnName);

                if (i < this.tableDescription.PrimaryKeys.Count - 1)
                    stringBuilder.Append(", ");
            }
            stringBuilder.Append(")");

            // Constraints
            foreach (var constraint in this.tableDescription.GetRelations())
            {
                // Don't want foreign key on same table since it could be a problem on first 
                // sync. We are not sure that parent row will be inserted in first position
                if (constraint.GetParentTable().EqualsByName(constraint.GetTable()))
                    continue;

                var parentTable = constraint.GetParentTable();
                var parentTableName = ParserName.Parse(parentTable.TableName).Quoted().ToString();

                stringBuilder.AppendLine();
                stringBuilder.Append($"\tFOREIGN KEY (");
                empty = string.Empty;
                foreach (var column in constraint.Keys)
                {
                    var columnName = ParserName.Parse(column.ColumnName).Quoted().ToString();
                    stringBuilder.Append($"{empty} {columnName}");
                    empty = ", ";
                }
                stringBuilder.Append($") ");
                stringBuilder.Append($"REFERENCES {parentTableName}(");
                empty = string.Empty;
                foreach (var column in constraint.ParentKeys)
                {
                    var columnName = ParserName.Parse(column.ColumnName).Quoted().ToString();
                    stringBuilder.Append($"{empty} {columnName}");
                    empty = ", ";
                }
                stringBuilder.AppendLine(" )");
            }
            stringBuilder.Append(")");
            return stringBuilder.ToString();
        }

        public async Task CreateTableAsync(DbConnection connection, DbTransaction transaction)
        {
            using (var command = new SqliteCommand(BuildTableCommandText(), (SqliteConnection)connection, (SqliteTransaction)transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }

        public Task CreateSchemaAsync(DbConnection connection, DbTransaction transaction) => Task.CompletedTask;

        public async Task DropTableAsync(DbConnection connection, DbTransaction transaction)
        {
            using (var command = new SqliteCommand($"DROP TABLE IF EXISTS {tableName.Quoted().ToString()}", (SqliteConnection)connection, (SqliteTransaction)transaction))
            {
                await command.ExecuteNonQueryAsync().ConfigureAwait(false);
            }
        }
    }
}
