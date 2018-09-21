using Dotmim.Sync.Builders;
using System;
using System.Text;
using Dotmim.Sync.Data;
using System.Data.Common;
using System.Linq;
using System.Data;
using MySql.Data.MySqlClient;
using Dotmim.Sync.MySql.Builders;
using System.Diagnostics;

namespace Dotmim.Sync.MySql
{
    public class MySqlBuilderTable : IDbBuilderTableHelper
    {
        private ObjectNameParser tableName;
        private ObjectNameParser trackingName;
        private DmTable tableDescription;
        private MySqlConnection connection;
        private MySqlTransaction transaction;
        private MySqlDbMetadata mySqlDbMetadata;

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
            MySqlCommand sqlCommand = new MySqlCommand();

            var childTable = foreignKey.ChildTable;
            var childTableName = new ObjectNameParser(childTable.TableName, "`", "`");
            var parentTable = foreignKey.ParentTable;
            var parentTableName = new ObjectNameParser(parentTable.TableName, "`", "`"); ;

            var relationName = foreignKey.RelationName;

            DmColumn[] foreignKeyColumns = foreignKey.ChildColumns;
            DmColumn[] referencesColumns = foreignKey.ParentColumns;

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.Append("ALTER TABLE ");
            stringBuilder.AppendLine(childTableName.FullQuotedString);
            stringBuilder.Append("ADD CONSTRAINT ");
            stringBuilder.AppendLine(relationName);
            stringBuilder.Append("FOREIGN KEY (");
            string empty = string.Empty;
            foreach (var foreignKeyColumn in foreignKeyColumns)
            {
                var foreignKeyColumnName = new ObjectNameParser(foreignKeyColumn.ColumnName.ToLowerInvariant(), "`", "`");
                stringBuilder.Append($"{empty} {foreignKeyColumnName.FullQuotedString}");
                empty = ", ";
            }
            stringBuilder.AppendLine(" )");
            stringBuilder.Append("REFERENCES ");
            stringBuilder.Append(parentTableName.FullQuotedString).Append(" (");
            empty = string.Empty;
            foreach (var referencesColumn in referencesColumns)
            {
                var referencesColumnName = new ObjectNameParser(referencesColumn.ColumnName.ToLowerInvariant(), "`", "`");
                stringBuilder.Append($"{empty} {referencesColumnName.FullQuotedString}");
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
            string parentFullName = String.IsNullOrEmpty(parentSchema) ? parentTable : $"{parentSchema}.{parentTable}";

            bool alreadyOpened = connection.State == ConnectionState.Open;

            // Don't want foreign key on same table since it could be a problem on first 
            // sync. We are not sure that parent row will be inserted in first position
            if (String.Equals(parentTable, foreignKey.ChildTable.TableName, StringComparison.CurrentCultureIgnoreCase))
                return false;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                var dmTable = MySqlManagementUtils.RelationsForTable(connection, transaction, parentFullName);

                var foreignKeyExist = dmTable.Rows.Any(r =>
                   dmTable.IsEqual(r["ForeignKey"].ToString(), foreignKey.RelationName));

                return !foreignKeyExist;
            }
            catch (Exception ex)
            {
                Debug.WriteLine($"Error during checking foreign keys: {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();
            }
        }


        public void CreateForeignKeyConstraints(DmRelation constraint)
        {
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                using (var command = BuildForeignKeyConstraintsCommand(constraint))
                {
                    command.Connection = connection;

                    if (transaction != null)
                        command.Transaction = transaction;

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
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

            }

        }

        public string CreateForeignKeyConstraintsScriptText(DmRelation constraint)
        {
            StringBuilder stringBuilder = new StringBuilder();

            var constraintName = $"Create Constraint {constraint.RelationName} between parent {constraint.ParentTable.TableName} and child {constraint.ChildTable.TableName}";
            var constraintScript = BuildForeignKeyConstraintsCommand(constraint).CommandText;
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
            MySqlCommand command = new MySqlCommand();

            StringBuilder stringBuilder = new StringBuilder($"CREATE TABLE IF NOT EXISTS {tableName.FullQuotedString} (");
            string empty = string.Empty;
            stringBuilder.AppendLine();
            foreach (var column in this.tableDescription.Columns)
            {
                var columnName = new ObjectNameParser(column.ColumnName, "`", "`");
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

                stringBuilder.AppendLine($"\t{empty}{columnName.FullQuotedString} {columnType} {identity} {nullString}");
                empty = ",";
            }

            if (this.tableDescription.MutableColumns.Any(mc => mc.IsAutoIncrement))
                stringBuilder.Append("\t, KEY (");

            empty = string.Empty;
            foreach (var column in this.tableDescription.MutableColumns.Where(c => c.IsAutoIncrement))
            {
                var columnName = new ObjectNameParser(column.ColumnName, "`", "`");
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
                var quotedColumnName = new ObjectNameParser(pkColumn.ColumnName, "`", "`").QuotedObjectName;

                stringBuilder.Append(quotedColumnName);

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
            var tableNameScript = $"Create Table {tableName.FullQuotedString}";
            var tableScript = BuildTableCommand().CommandText;
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

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                return MySqlManagementUtils.TableExists(connection, transaction, parentTable.TableName);

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
            return !MySqlManagementUtils.TableExists(connection, transaction, tableName.FullUnquotedString);

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
            var commandText = $"drop table if exists {tableName.FullQuotedString}";

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                using (var command = new MySqlCommand(commandText, connection))
                {
                    if (transaction != null)
                        command.Transaction = transaction;

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
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

            }

        }

        public string DropTableScriptText()
        {
            var commandText = $"drop table if exists {tableName.FullQuotedString}";

            var str1 = $"Drop table {tableName.FullQuotedString}";
            return MySqlBuilder.WrapScriptTextWithComments(commandText, str1);
        }
    }
}
