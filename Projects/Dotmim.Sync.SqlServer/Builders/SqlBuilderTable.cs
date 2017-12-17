using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using Dotmim.Sync.Log;
using Dotmim.Sync.SqlServer.Manager;
using System;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace Dotmim.Sync.SqlServer.Builders
{
    public class SqlBuilderTable : IDbBuilderTableHelper
    {
        private ObjectNameParser tableName;
        private ObjectNameParser trackingName;
        private DmTable tableDescription;
        private SqlConnection connection;
        private SqlTransaction transaction;
        private SqlDbMetadata sqlDbMetadata;


        public SqlBuilderTable(DmTable tableDescription, DbConnection connection, DbTransaction transaction = null)
        {
            this.connection = connection as SqlConnection;
            this.transaction = transaction as SqlTransaction;

            this.tableDescription = tableDescription;
            (this.tableName, this.trackingName) = SqlBuilder.GetParsers(this.tableDescription);
            this.sqlDbMetadata = new SqlDbMetadata();
        }

        public bool NeedToCreateForeignKeyConstraints(DmRelation foreignKey)
        {

            string parentTable = foreignKey.ParentTable.TableName;
            string parentSchema = foreignKey.ParentTable.Schema;
            string parentFullName = String.IsNullOrEmpty(parentSchema) ? parentTable : $"{parentSchema}.{parentTable}";

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                var dmTable = SqlManagementUtils.RelationsForTable(connection, transaction, parentFullName);

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
        private SqlCommand BuildForeignKeyConstraintsCommand(DmRelation foreignKey)
        {
            SqlCommand sqlCommand = new SqlCommand();

            string childTable = foreignKey.ChildTable.TableName;
            string childSchema = foreignKey.ChildTable.Schema;
            string childFullName = String.IsNullOrEmpty(childSchema) ? childTable : $"{childSchema}.{childTable}";

            var childTableName = new ObjectNameParser(childFullName);

            string parentTable = foreignKey.ParentTable.TableName;
            string parentSchema = foreignKey.ParentTable.Schema;
            string parentFullName = String.IsNullOrEmpty(parentSchema) ? parentTable : $"{parentSchema}.{parentTable}";
            var parentTableName = new ObjectNameParser(parentFullName);

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.Append("ALTER TABLE ");
            stringBuilder.AppendLine(parentTableName.QuotedString);
            stringBuilder.Append("ADD CONSTRAINT ");
            stringBuilder.AppendLine(foreignKey.RelationName);
            stringBuilder.Append("FOREIGN KEY (");
            string empty = string.Empty;
            foreach (var parentdColumn in foreignKey.ParentColumns)
            {
                var parentColumnName = new ObjectNameParser(parentdColumn.ColumnName);

                stringBuilder.Append($"{empty} {parentColumnName.QuotedString}");
                empty = ", ";
            }
            stringBuilder.AppendLine(" )");
            stringBuilder.Append("REFERENCES ");
            stringBuilder.Append(childTableName.QuotedString).Append(" (");
            empty = string.Empty;
            foreach (var childColumn in foreignKey.ChildColumns)
            {
                var childColumnName = new ObjectNameParser(childColumn.ColumnName);
                stringBuilder.Append($"{empty} {childColumnName.QuotedString}");
            }
            stringBuilder.Append(" ) ");
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
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

            var constraintName =
                $"Create Constraint {constraint.RelationName} " +
                $"between parent {constraint.ParentTable.TableName} " +
                $"and child {constraint.ChildTable.TableName}";

            var constraintScript = BuildForeignKeyConstraintsCommand(constraint).CommandText;
            stringBuilder.Append(SqlBuilder.WrapScriptTextWithComments(constraintScript, constraintName));
            stringBuilder.AppendLine();

            return stringBuilder.ToString();
        }

        private SqlCommand BuildPkCommand()
        {
            string[] localName = new string[] { };
            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.AppendLine($"ALTER TABLE {tableName.QuotedString} ADD CONSTRAINT [PK_{tableName.ObjectName}] PRIMARY KEY(");
            for (int i = 0; i < this.tableDescription.PrimaryKey.Columns.Length; i++)
            {
                DmColumn pkColumn = this.tableDescription.PrimaryKey.Columns[i];
                var quotedColumnName = new ObjectNameParser(pkColumn.ColumnName, "[", "]").QuotedString;

                stringBuilder.Append(quotedColumnName);

                if (i < this.tableDescription.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
            }
            stringBuilder.Append(")");

            return new SqlCommand(stringBuilder.ToString());
        }
        public void CreatePrimaryKey()
        {
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                using (var command = BuildPkCommand())
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
                Debug.WriteLine($"Error during Create Pk Command : {ex}");
                throw;
            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();
            }

        }
        public string CreatePrimaryKeyScriptText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            var pkName = $"Create primary keys for table {tableName.QuotedString}";
            var pkScript = BuildPkCommand().CommandText;
            stringBuilder.Append(SqlBuilder.WrapScriptTextWithComments(pkScript, pkName));
            stringBuilder.AppendLine();
            return stringBuilder.ToString();
        }

        private SqlCommand BuildTableCommand()
        {
            StringBuilder stringBuilder = new StringBuilder($"CREATE TABLE {tableName.QuotedString} (");
            string empty = string.Empty;
            stringBuilder.AppendLine();
            foreach (var column in this.tableDescription.Columns)
            {
                var columnName = new ObjectNameParser(column.ColumnName);

                var columnTypeString = this.sqlDbMetadata.TryGetOwnerDbTypeString(column.OriginalDbType, column.DbType, false, false, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
                var columnPrecisionString = this.sqlDbMetadata.TryGetOwnerDbTypePrecision(column.OriginalDbType, column.DbType, false, false, column.MaxLength, column.Precision, column.Scale, this.tableDescription.OriginalProvider, SqlSyncProvider.ProviderType);
                var columnType = $"{columnTypeString} {columnPrecisionString}";
                var identity = string.Empty;

                if (column.AutoIncrement)
                {
                    var s = column.GetAutoIncrementSeedAndStep();
                    identity = $"IDENTITY({s.Step},{s.Seed})";
                }
                var nullString = column.AllowDBNull ? "NULL" : "NOT NULL";

                // if we have a computed column, we should allow null
                if (column.ReadOnly)
                    nullString = "NULL";

                stringBuilder.AppendLine($"\t{empty}{columnName.QuotedString} {columnType} {identity} {nullString}");
                empty = ",";
            }
            stringBuilder.Append(")");
            return new SqlCommand(stringBuilder.ToString());
        }

        private SqlCommand BuildDeleteTableCommand()
        {
            return new SqlCommand($"DROP TABLE {tableName.QuotedString};");
        }

        public void CreateSchema()
        {
            if (string.IsNullOrEmpty(tableName.SchemaName) || tableName.SchemaName.ToLowerInvariant() == "dbo")
                return;

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                var schemaCommand = $"Create Schema {tableName.SchemaName}";

                using (var command = new SqlCommand(schemaCommand))
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

        public void DropTable()
        {
            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                using (var command = BuildDeleteTableCommand())
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
                Debug.WriteLine($"Error during DeleteTable : {ex}");
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
            var tableNameScript = $"Create Table {tableName.QuotedString}";
            var tableScript = BuildTableCommand().CommandText;
            stringBuilder.Append(SqlBuilder.WrapScriptTextWithComments(tableScript, tableNameScript));
            stringBuilder.AppendLine();
            return stringBuilder.ToString();
        }

        public string DropTableScriptText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            var tableNameScript = $"Drop Table {tableName.QuotedString}";
            var tableScript = BuildTableCommand().CommandText;
            stringBuilder.Append(SqlBuilder.WrapScriptTextWithComments(tableScript, tableNameScript));
            stringBuilder.AppendLine();
            return stringBuilder.ToString();
        }

        public string CreateSchemaScriptText()
        {
            if (String.IsNullOrEmpty(this.tableDescription.Schema) || this.tableDescription.Schema.ToLowerInvariant() == "dbo")
                return null;

            StringBuilder stringBuilder = new StringBuilder();
            var schemaNameScript = $"Create Schema {tableName.SchemaName}";
            var schemaScript = $"Create Schema {tableName.SchemaName}";
            stringBuilder.Append(SqlBuilder.WrapScriptTextWithComments(schemaScript, schemaNameScript));
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
            var exist = ds.Tables.Any(t => ds.IsEqual(t.TableName, parentTable.TableName));

            if (!exist)
                return false;

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                return SqlManagementUtils.TableExists(connection, transaction, parentTable.TableName);

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
            return !SqlManagementUtils.TableExists(connection, transaction, tableName.QuotedString);

        }

        /// <summary>
        /// Check if we need to create the table in the current database
        /// </summary>
        public bool NeedToCreateSchema()
        {
            if (string.IsNullOrEmpty(tableName.SchemaName) || tableName.SchemaName.ToLowerInvariant() == "dbo")
                return false;

            return !SqlManagementUtils.SchemaExists(connection, transaction, tableName.SchemaName);
        }


    }
}
