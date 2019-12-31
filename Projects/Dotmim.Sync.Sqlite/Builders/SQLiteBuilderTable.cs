using Dotmim.Sync.Builders;
using System;
using System.Text;
using Dotmim.Sync.Data;
using System.Data.Common;
using System.Linq;
using System.Data;
using Microsoft.Data.Sqlite;
using System.Diagnostics;

namespace Dotmim.Sync.Sqlite
{
    public class SqliteBuilderTable : IDbBuilderTableHelper
    {
        private ParserName tableName;
        private ParserName trackingName;
        private SyncTable tableDescription;
        private SqliteConnection connection;
        private SqliteTransaction transaction;
        private SqliteDbMetadata sqliteDbMetadata;

        public SqliteBuilderTable(SyncTable tableDescription, DbConnection connection, DbTransaction transaction = null)
        {
            this.connection = connection as SqliteConnection;
            this.transaction = transaction as SqliteTransaction;
            this.tableDescription = tableDescription;
            (this.tableName, this.trackingName) = SqliteBuilder.GetParsers(this.tableDescription);
            this.sqliteDbMetadata = new SqliteDbMetadata();
        }
        private SqliteCommand BuildForeignKeyConstraintsCommand(SyncRelation foreignKey)
        {
            SqliteCommand sqlCommand = new SqliteCommand();

            var childTable = foreignKey.GetChildTable();
            var childTableName = ParserName.Parse(childTable.TableName).Quoted().ToString();

            var parentTable = foreignKey.GetParentTable();
            var parentTableName = ParserName.Parse(parentTable.TableName).Quoted().ToString();

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.Append("ALTER TABLE ");
            stringBuilder.AppendLine(childTableName);
            stringBuilder.Append("ADD CONSTRAINT ");
            stringBuilder.AppendLine(foreignKey.RelationName);
            stringBuilder.Append("FOREIGN KEY (");
            string empty = string.Empty;
            foreach (var childColumn in foreignKey.ChildKeys)
            {
                var childColumnName = ParserName.Parse(childColumn.ColumnName).Quoted().ToString();
                stringBuilder.Append($"{empty} {childColumnName}");
            }
            stringBuilder.AppendLine(" )");
            stringBuilder.Append("REFERENCES ");
            stringBuilder.Append(parentTableName).Append(" (");
            empty = string.Empty;
            foreach (var parentdColumn in foreignKey.ParentKeys)
            {
                var parentColumnName = ParserName.Parse(parentdColumn.ColumnName).Quoted().ToString();
                stringBuilder.Append($"{empty} {parentColumnName}");
                empty = ", ";
            }
            stringBuilder.Append(" ) ");
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        public void CreatePrimaryKey()
        {
            return;

        }
        public string CreatePrimaryKeyScriptText()
        {
            return string.Empty;
        }


        private SqliteCommand BuildTableCommand()
        {
            var command = new SqliteCommand();

            var stringBuilder = new StringBuilder($"CREATE TABLE IF NOT EXISTS {tableName.Quoted().ToString()} (");
            string empty = string.Empty;
            stringBuilder.AppendLine();
            foreach (var column in this.tableDescription.GetMutableColumns(false, true))
            {
                var columnName = ParserName.Parse(column).Quoted().ToString();

                var columnTypeString = this.sqliteDbMetadata.TryGetOwnerDbTypeString(column.OriginalDbType, column.GetDbType(), false, false, column.MaxLength, this.tableDescription.OriginalProvider, SqliteSyncProvider.ProviderType);
                var columnPrecisionString = this.sqliteDbMetadata.TryGetOwnerDbTypePrecision(column.OriginalDbType, column.GetDbType(), false, false, column.MaxLength, column.Precision, column.Scale, this.tableDescription.OriginalProvider, SqliteSyncProvider.ProviderType);
                var columnType = $"{columnTypeString} {columnPrecisionString}";

                // check case
                string casesensitive = "";
                if (this.sqliteDbMetadata.IsTextType(column.GetDbType()))
                {
                    casesensitive = this.tableDescription.Schema.CaseSensitive ? "" : "COLLATE NOCASE";

                    //check if it's a primary key, then, even if it's case sensitive, we turn on case insensitive
                    if (this.tableDescription.Schema.CaseSensitive)
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
            foreach (var constraint in this.tableDescription.GetParentRelations())
            {
                // Don't want foreign key on same table since it could be a problem on first 
                // sync. We are not sure that parent row will be inserted in first position
                if (string.Equals(constraint.GetParentTable().TableName, constraint.GetChildTable().TableName, StringComparison.CurrentCultureIgnoreCase))
                    continue;

                var parentTable = constraint.GetParentTable();
                var parentTableName = ParserName.Parse(parentTable.TableName).Quoted().ToString();

                stringBuilder.AppendLine();
                stringBuilder.Append($"\tFOREIGN KEY (");
                empty = string.Empty;
                foreach (var column in constraint.ChildKeys)
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
            return new SqliteCommand(stringBuilder.ToString());
        }

        public void CreateTable()
        {
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                using (var command = BuildTableCommand())
                {
                    if (!alreadyOpened)
                        connection.Open();

                    if (transaction != null)
                        command.Transaction = transaction;

                    command.Connection = connection;
                    command.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during CreateTable : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

            }

        }
        public string CreateTableScriptText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            var tableNameScript = $"Create Table {tableName.Quoted().ToString()}";
            var tableScript = BuildTableCommand().CommandText;
            stringBuilder.Append(SqliteBuilder.WrapScriptTextWithComments(tableScript, tableNameScript));
            stringBuilder.AppendLine();
            return stringBuilder.ToString();
        }


        /// <summary>
        /// For a foreign key, check if the Parent table exists
        /// </summary>
        private bool EnsureForeignKeysTableExist(SyncRelation foreignKey)
        {
            var childTable = foreignKey.GetChildTable();
            var parentTable = foreignKey.GetParentTable();

            // The foreignkey comes from the child table
            var ds = foreignKey.GetChildTable().Schema;

            if (ds == null)
                return false;

            // Check if the parent table is part of the sync configuration
            var exist = ds.Tables.Any(t => ds.StringEquals(t.TableName, parentTable.TableName));

            if (!exist)
                return false;

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                return SqliteManagementUtils.TableExists(connection, transaction, ParserName.Parse(parentTable));

            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during EnsureForeignKeysTableExist : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

            }


        }

        /// <summary>
        /// Check if we need to create the table in the current database
        /// </summary>
        public bool NeedToCreateTable()
        {
            return !SqliteManagementUtils.TableExists(connection, transaction, tableName);

        }

        public bool NeedToCreateSchema()
        {
            return false;
        }

        public void CreateSchema()
        {
            return;
        }

        public string CreateSchemaScriptText()
        {
            return string.Empty;
        }

        public bool NeedToCreateForeignKeyConstraints(SyncRelation constraint)
        {
            return false;
        }

        public void CreateForeignKeyConstraints(SyncRelation constraint)
        {
            return;
        }

        public string CreateForeignKeyConstraintsScriptText(SyncRelation constraint)
        {
            return string.Empty;
        }

        public void DropTable()
        {
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                using (var command = new SqliteCommand($"DROP TABLE IF EXISTS {tableName.Quoted().ToString()}", connection))
                {
                    if (!alreadyOpened)
                        connection.Open();

                    if (transaction != null)
                        command.Transaction = transaction;

                    command.Connection = connection;
                    command.ExecuteNonQuery();

                }
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during DropTable : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

            }

        }

        public string DropTableScriptText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            var tableNameScript = $"Drop Table {tableName.Quoted().ToString()}";
            var tableScript = $"DROP TABLE IF EXISTS {tableName.Quoted().ToString()}";
            stringBuilder.Append(SqliteBuilder.WrapScriptTextWithComments(tableScript, tableNameScript));
            stringBuilder.AppendLine();
            return stringBuilder.ToString();
        }
    }
}
