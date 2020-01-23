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

namespace Dotmim.Sync.MySql
{
    public class MySqlBuilderTable : IDbBuilderTableHelper
    {
        private readonly ParserName tableName;
        private readonly ParserName trackingName;
        private readonly SyncTable tableDescription;
        private readonly MySqlConnection connection;
        private readonly MySqlTransaction transaction;
        private readonly MySqlDbMetadata mySqlDbMetadata;

        private static Dictionary<string, string> createdRelationNames = new Dictionary<string, string>();

        private static string GetRandomString()
        {
            return Path.GetRandomFileName().Replace(".", "").ToLowerInvariant();
        }

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
        public MySqlBuilderTable(SyncTable tableDescription, DbConnection connection, DbTransaction transaction = null)
        {

            this.connection = connection as MySqlConnection;
            this.transaction = transaction as MySqlTransaction;
            this.tableDescription = tableDescription;
            (this.tableName, this.trackingName) = MyTableSqlBuilder.GetParsers(this.tableDescription);
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

        public bool NeedToCreateForeignKeyConstraints(SyncRelation relation)
        {
            // Don't want foreign key on same table since it could be a problem on first 
            // sync. We are not sure that parent row will be inserted in first position
            //if (relation.GetParentTable() == relation.GetTable())
            //    return false;

            string tableName = relation.GetTable().TableName;

            var relationName = NormalizeRelationName(relation.RelationName);

            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    this.connection.Open();

                var relations = MySqlManagementUtils.RelationsForTable(this.connection, this.transaction, tableName);

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

        public void CreateForeignKeyConstraints(SyncRelation constraint)
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    this.connection.Open();

                using (var command = this.BuildForeignKeyConstraintsCommand(constraint))
                {
                    command.Connection = this.connection;

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    command.ExecuteNonQuery();
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

        public string CreateForeignKeyConstraintsScriptText(SyncRelation constraint)
        {
            var stringBuilder = new StringBuilder();
            var relationName = NormalizeRelationName(constraint.RelationName);

            var constraintName = $"Create Constraint {constraint.RelationName} between parent {constraint.GetParentTable().TableName} and child {constraint.GetTable().TableName}";
            var constraintScript = this.BuildForeignKeyConstraintsCommand(constraint).CommandText;
            stringBuilder.Append(MyTableSqlBuilder.WrapScriptTextWithComments(constraintScript, constraintName));
            stringBuilder.AppendLine();

            return stringBuilder.ToString();
        }

        public void CreatePrimaryKey()
        {
            return;
        }
        public string CreatePrimaryKeyScriptText()
        {
            return string.Empty;
        }


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

        public void CreateTable()
        {
            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                using (var command = this.BuildTableCommand())
                {
                    if (!alreadyOpened)
                        this.connection.Open();

                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    command.Connection = this.connection;
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
                if (!alreadyOpened && this.connection.State != ConnectionState.Closed)
                    this.connection.Close();

            }

        }
        public string CreateTableScriptText()
        {
            var stringBuilder = new StringBuilder();
            var tableNameScript = $"Create Table {this.tableName.Quoted().ToString()}";
            var tableScript = this.BuildTableCommand().CommandText;
            stringBuilder.Append(MyTableSqlBuilder.WrapScriptTextWithComments(tableScript, tableNameScript));
            stringBuilder.AppendLine();
            return stringBuilder.ToString();
        }

        /// <summary>
        /// For a foreign key, check if the Parent table exists
        /// </summary>
        private bool EnsureForeignKeysTableExist(SyncRelation constraint)
        {
            var table = constraint.GetTable();
            var parentTable = constraint.GetParentTable();

            // The foreignkey comes from the child table
            var ds = constraint.GetTable().Schema;

            if (ds == null)
                return false;

            // Check if the parent table is part of the sync configuration
            var exist = ds.Tables.Any(t => t == parentTable);

            if (!exist)
                return false;

            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    this.connection.Open();

                return MySqlManagementUtils.TableExists(this.connection, this.transaction, ParserName.Parse(parentTable));

            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during EnsureForeignKeysTableExist : {ex}");
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
        public bool NeedToCreateTable()
        {
            return !MySqlManagementUtils.TableExists(this.connection, this.transaction, this.tableName);
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

        public void DropTable()
        {
            var commandText = $"drop table if exists {this.tableName.Quoted().ToString()}";

            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    this.connection.Open();

                using (var command = new MySqlCommand(commandText, this.connection))
                {
                    if (this.transaction != null)
                        command.Transaction = this.transaction;

                    command.ExecuteNonQuery();
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

        public string DropTableScriptText()
        {
            var commandText = $"drop table if exists {this.tableName.Quoted().ToString()}";

            var str1 = $"Drop table {this.tableName.Quoted().ToString()}";
            return MyTableSqlBuilder.WrapScriptTextWithComments(commandText, str1);
        }
    }
}
