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
        private readonly MySqlConnection connection;
        private readonly MySqlTransaction transaction;
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
        public MySqlBuilderTable(SyncTable tableDescription, SyncSetup setup, DbConnection connection, DbTransaction transaction = null)
        {

            this.connection = connection as MySqlConnection;
            this.transaction = transaction as MySqlTransaction;
            this.tableDescription = tableDescription;
            this.setup = setup;
            (this.tableName, this.trackingName) = MyTableSqlBuilder.GetParsers(this.tableDescription, setup);
            this.mySqlDbMetadata = new MySqlDbMetadata();
        }


        private MySqlCommand BuildForeignKeyConstraintsCommand(SyncRelation constraint)
        {
            var sqlCommand = new MySqlCommand();

            var tableName = ParserName.Parse(constraint.GetTable(), "`").Quoted().ToString();
            var parentTableName = ParserName.Parse(constraint.GetParentTable(), "`").Quoted().ToString();

            var relationName = NormalizeRelationName(constraint.RelationName);

            var keyColumns = constraint.Keys;
            var referencesColumns = constraint.ParentKeys;

            var stringBuilder = new StringBuilder();
            stringBuilder.Append("ALTER TABLE ");
            stringBuilder.AppendLine(tableName);
            stringBuilder.Append("ADD CONSTRAINT ");

            stringBuilder.AppendLine($"`{relationName}`");
            stringBuilder.Append("FOREIGN KEY (");
            string empty = string.Empty;
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
            stringBuilder.Append(" ) ");
            sqlCommand.CommandText = stringBuilder.ToString();

            return sqlCommand;
        }

        public async Task<bool> NeedToCreateForeignKeyConstraintsAsync(SyncRelation relation)
        {
            string tableName = relation.GetTable().TableName;

            var relationName = NormalizeRelationName(relation.RelationName);

            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                var relations = await MySqlManagementUtils.GetRelationsForTableAsync(this.connection, this.transaction, tableName).ConfigureAwait(false);

                var foreignKeyExist = relations.Rows.Any(r =>
                   string.Equals(r["ForeignKey"].ToString(), relationName, SyncGlobalization.DataSourceStringComparison));

                return !foreignKeyExist;
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during checking foreign keys: {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();
            }
        }

        public async Task CreateForeignKeyConstraintsAsync(SyncRelation constraint)
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                using (var command = this.BuildForeignKeyConstraintsCommand(constraint))
                {
                    command.Connection = this.connection;

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                }

            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during CreateForeignKeyConstraints : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();

            }

        }



        public Task CreatePrimaryKeyAsync() => Task.CompletedTask;


        private MySqlCommand BuildTableCommand()
        {
            var command = new MySqlCommand();

            var stringBuilder = new StringBuilder($"CREATE TABLE IF NOT EXISTS {this.tableName.Quoted().ToString()} (");
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
            stringBuilder.Append(")");
            return new MySqlCommand(stringBuilder.ToString());
        }

        public async Task CreateTableAsync()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = this.BuildTableCommand())
                {
                    if (!alreadyOpened)
                        await connection.OpenAsync().ConfigureAwait(false);

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    command.Connection = this.connection;
                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);

                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during CreateTable : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();

            }

        }


        /// <summary>
        /// Check if we need to create the table in the current database
        /// </summary>
        public async Task<bool> NeedToCreateTableAsync()
            => !(await MySqlManagementUtils.TableExistsAsync(this.connection, this.transaction, this.tableName).ConfigureAwait(false));

        public Task<bool> NeedToCreateSchemaAsync() => Task.FromResult(false);

        public Task CreateSchemaAsync() => Task.CompletedTask;

        public async Task DropTableAsync()
        {
            var commandText = $"drop table if exists {this.tableName.Quoted().ToString()}";

            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    await connection.OpenAsync().ConfigureAwait(false);

                using (var command = new MySqlCommand(commandText, this.connection))
                {
                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    await command.ExecuteNonQueryAsync().ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during DropTableCommand : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();

            }

        }

    }
}
