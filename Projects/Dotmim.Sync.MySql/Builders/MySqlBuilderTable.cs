using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
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
        private readonly DmTable tableDescription;
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
        public MySqlBuilderTable(DmTable tableDescription, DbConnection connection, DbTransaction transaction = null)
        {

            this.connection = connection as MySqlConnection;
            this.transaction = transaction as MySqlTransaction;
            this.tableDescription = tableDescription;
            (this.tableName, this.trackingName) = MySqlBuilder.GetParsers(this.tableDescription);
            this.mySqlDbMetadata = new MySqlDbMetadata();
        }


        private MySqlCommand BuildForeignKeyConstraintsCommand(DmRelation foreignKey)
        {
            var sqlCommand = new MySqlCommand();

            var childTableName = ParserName.Parse(foreignKey.ChildTable, "`").Quoted().ToString();
            var parentTableName = ParserName.Parse(foreignKey.ParentTable, "`").Quoted().ToString();

            var relationName = NormalizeRelationName(foreignKey.RelationName);

            DmColumn[] foreignKeyColumns = foreignKey.ChildColumns;
            DmColumn[] referencesColumns = foreignKey.ParentColumns;

            var stringBuilder = new StringBuilder();
            stringBuilder.Append("ALTER TABLE ");
            stringBuilder.AppendLine(childTableName);
            stringBuilder.Append("ADD CONSTRAINT ");

            stringBuilder.AppendLine($"`{relationName}`");
            stringBuilder.Append("FOREIGN KEY (");
            string empty = string.Empty;
            foreach (var foreignKeyColumn in foreignKeyColumns)
            {
                var foreignKeyColumnName = ParserName.Parse(foreignKeyColumn, "`").Quoted().ToString();
                stringBuilder.Append($"{empty} {foreignKeyColumnName}");
                empty = ", ";
            }
            stringBuilder.AppendLine(" )");
            stringBuilder.Append("REFERENCES ");
            stringBuilder.Append(parentTableName).Append(" (");
            empty = string.Empty;
            foreach (var referencesColumn in referencesColumns)
            {
                var referencesColumnName = ParserName.Parse(referencesColumn, "`").Quoted().ToString();
                stringBuilder.Append($"{empty} {referencesColumnName}");
                empty = ", ";
            }
            stringBuilder.Append(" ) ");
            sqlCommand.CommandText = stringBuilder.ToString();

            return sqlCommand;
        }

        public bool NeedToCreateForeignKeyConstraints(DmRelation foreignKey)
        {
            string parentTable = foreignKey.ParentTable.TableName;
            string parentSchema = foreignKey.ParentTable.Schema;
            string parentFullName = string.IsNullOrEmpty(parentSchema) ? parentTable : $"{parentSchema}.{parentTable}";

            var relationName = NormalizeRelationName(foreignKey.RelationName);

            bool alreadyOpened = this.connection.State == ConnectionState.Open;

            // Don't want foreign key on same table since it could be a problem on first 
            // sync. We are not sure that parent row will be inserted in first position
            if (string.Equals(parentTable, foreignKey.ChildTable.TableName, StringComparison.CurrentCultureIgnoreCase))
                return false;

            try
            {
                if (!alreadyOpened)
                    this.connection.Open();

                var dmTable = MySqlManagementUtils.RelationsForTable(this.connection, this.transaction, parentFullName);

                var foreignKeyExist = dmTable.Rows.Any(r =>
                   dmTable.IsEqual(r["ForeignKey"].ToString(), relationName));

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

        public void CreateForeignKeyConstraints(DmRelation constraint)
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

        public string CreateForeignKeyConstraintsScriptText(DmRelation constraint)
        {
            var stringBuilder = new StringBuilder();
            var relationName = NormalizeRelationName(constraint.RelationName);

            var constraintName = $"Create Constraint {constraint.RelationName} between parent {constraint.ParentTable.TableName} and child {constraint.ChildTable.TableName}";
            var constraintScript = this.BuildForeignKeyConstraintsCommand(constraint).CommandText;
            stringBuilder.Append(MySqlBuilder.WrapScriptTextWithComments(constraintScript, constraintName));
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
                var stringType = this.mySqlDbMetadata.TryGetOwnerDbTypeString(column.OriginalDbType, column.DbType, false, false, column.MaxLength, this.tableDescription.OriginalProvider, MySqlSyncProvider.ProviderType);
                var stringPrecision = this.mySqlDbMetadata.TryGetOwnerDbTypePrecision(column.OriginalDbType, column.DbType, false, false, column.MaxLength, column.Precision, column.Scale, this.tableDescription.OriginalProvider, MySqlSyncProvider.ProviderType);
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

            if (this.tableDescription.MutableColumns.Any(mc => mc.IsAutoIncrement))
                stringBuilder.Append("\t, KEY (");

            empty = string.Empty;
            foreach (var column in this.tableDescription.MutableColumns.Where(c => c.IsAutoIncrement))
            {
                var columnName = ParserName.Parse(column, "`").Quoted().ToString();
                stringBuilder.Append($"{empty} {columnName}");
                empty = ",";
            }

            if (this.tableDescription.MutableColumns.Any(mc => mc.IsAutoIncrement))
                stringBuilder.AppendLine(")");

            stringBuilder.Append("\t,PRIMARY KEY (");

            int i = 0;
            // It seems we need to specify the increment column in first place
            foreach (var pkColumn in this.tableDescription.PrimaryKey.Columns.OrderByDescending(pk => pk.IsAutoIncrement))
            {
                var columnName = ParserName.Parse(pkColumn, "`").Quoted().ToString();

                stringBuilder.Append(columnName);

                if (i < this.tableDescription.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
                i++;
            }

            //for (int i = 0; i < this.tableDescription.PrimaryKey.Columns.Length; i++)
            //{
            //    DmColumn pkColumn = this.tableDescription.PrimaryKey.Columns[i];
            //    var quotedColumnName = new ObjectNameParser(pkColumn.ColumnName., "`", "`").QuotedObjectName;

            //    stringBuilder.Append(quotedColumnName);

            //    if (i < this.tableDescription.PrimaryKey.Columns.Length - 1)
            //        stringBuilder.Append(", ");
            //}
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
            stringBuilder.Append(MySqlBuilder.WrapScriptTextWithComments(tableScript, tableNameScript));
            stringBuilder.AppendLine();
            return stringBuilder.ToString();
        }

        /// <summary>
        /// For a foreign key, check if the Parent table exists
        /// </summary>
        private bool EnsureForeignKeysTableExist(DmRelation foreignKey)
        {
            var childTable = foreignKey.ChildTable;
            var parentTable = foreignKey.ParentTable;

            // The foreignkey comes from the child table
            var ds = foreignKey.ChildTable.DmSet;

            if (ds == null)
                return false;

            // Check if the parent table is part of the sync configuration
            var exist = ds.Tables.Any(t => ds.IsEqual(t.TableName.ToLowerInvariant(), parentTable.TableName.ToLowerInvariant()));

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
            return MySqlBuilder.WrapScriptTextWithComments(commandText, str1);
        }
    }
}
