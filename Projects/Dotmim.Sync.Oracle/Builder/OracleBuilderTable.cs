using System;
using System.Data;
using System.Data.Common;
using System.Data.OracleClient;
using System.Diagnostics;
using System.Text;
using Dotmim.Sync.Builders;
using Dotmim.Sync.Data;
using Dotmim.Sync.Oracle.Manager;
using System.Linq;

namespace Dotmim.Sync.Oracle.Builder
{
    internal class OracleBuilderTable : IDbBuilderTableHelper
    {
        private ObjectNameParser tableName;
        private ObjectNameParser trackingName;
        private DmTable tableDescription;
        private OracleConnection connection;
        private OracleTransaction transaction;
        private OracleDbMetadata oracleDbMetadata;

        public OracleBuilderTable(DmTable tableDescription, DbConnection connection, DbTransaction transaction)
        {
            this.tableDescription = tableDescription;
            this.connection = connection as OracleConnection;
            this.transaction = transaction as OracleTransaction;
            this.oracleDbMetadata = new OracleDbMetadata();

            (this.tableName, this.trackingName) = OracleBuilder.GetParsers(this.tableDescription);
        }

        #region Private Methods

        private OracleCommand BuildPkCommand()
        {
            string[] localName = new string[] { };
            StringBuilder stringBuilder = new StringBuilder();

            stringBuilder.AppendLine($"ALTER TABLE {tableName.QuotedString} ADD CONSTRAINT PK_{tableName.ObjectName} PRIMARY KEY(");
            for (int i = 0; i < this.tableDescription.PrimaryKey.Columns.Length; i++)
            {
                DmColumn pkColumn = this.tableDescription.PrimaryKey.Columns[i];
                var quotedColumnName = new ObjectNameParser(pkColumn.ColumnName, string.Empty, string.Empty).QuotedString;

                stringBuilder.Append(quotedColumnName);

                if (i < this.tableDescription.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
            }
            stringBuilder.Append(")");

            return new OracleCommand(stringBuilder.ToString());
        }

        private OracleCommand BuildTableCommand()
        {
            StringBuilder stringBuilder = new StringBuilder($"CREATE TABLE {tableName.QuotedString} (");
            string empty = string.Empty;
            stringBuilder.AppendLine();
            foreach (var column in this.tableDescription.Columns)
            {
                var columnName = new ObjectNameParser(column.ColumnName);

                var columnTypeString = this.oracleDbMetadata.TryGetOwnerDbTypeString(column.OriginalDbType, column.DbType, false, false, this.tableDescription.OriginalProvider, OracleSyncProvider.ProviderType);
                var columnPrecisionString = this.oracleDbMetadata.TryGetOwnerDbTypePrecision(column.OriginalDbType, column.DbType, false, false, column.MaxLength, column.Precision, column.Scale, this.tableDescription.OriginalProvider, OracleSyncProvider.ProviderType);
                var columnType = $"{columnTypeString} {columnPrecisionString}";
                var identity = string.Empty;

                if (column.AutoIncrement)
                {
                    var s = column.GetAutoIncrementSeedAndStep();
                    //identity = $"GENERATED  ALWAYS as IDENTITY(START {s.Step},{s.Seed})";
                    identity = $"GENERATED ALWAYS as IDENTITY(START with 1 INCREMENT by 1)";
                }
                var nullString = column.AllowDBNull ? "NULL" : "NOT NULL";

                // if we have a computed column, we should allow null
                if (column.ReadOnly)
                    nullString = "NULL";

                stringBuilder.AppendLine($"\t{empty}{columnName.QuotedString} {columnType} {identity} {nullString}");
                empty = ",";
            }
            stringBuilder.Append(")");
            return new OracleCommand(stringBuilder.ToString());
        }

        private OracleCommand BuildDeleteTableCommand()
        {
            return new OracleCommand($"DROP TABLE {tableName.QuotedString};");
        }

        private OracleCommand BuildForeignKeyConstraintsCommand(DmRelation foreignKey)
        {
            OracleCommand sqlCommand = new OracleCommand();

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

        #endregion

        #region Implementation IDbBuilderTableHelper

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
            stringBuilder.Append(OracleBuilder.WrapScriptTextWithComments(constraintScript, constraintName));
            stringBuilder.AppendLine();

            return stringBuilder.ToString();
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
            stringBuilder.Append(OracleBuilder.WrapScriptTextWithComments(pkScript, pkName));
            stringBuilder.AppendLine();
            return stringBuilder.ToString();
        }

        public void CreateSchema()
        {
            if (string.IsNullOrEmpty(tableName.SchemaName))
                return;

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                var schemaCommand = $"CREATE USER {tableName.SchemaName} IDENTIFIED BY {tableName.SchemaName}";

                using (var command = new OracleCommand(schemaCommand))
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

        public string CreateSchemaScriptText()
        {
            if (String.IsNullOrEmpty(this.tableDescription.Schema))
                return null;

            StringBuilder stringBuilder = new StringBuilder();
            var schemaNameScript = $"Create Schema {tableName.SchemaName}";
            var schemaScript = $"Create Schema {tableName.SchemaName}";
            stringBuilder.Append(OracleBuilder.WrapScriptTextWithComments(schemaScript, schemaNameScript));
            stringBuilder.AppendLine();
            return stringBuilder.ToString();
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
            var tableNameScript = $"Create Table {tableName.QuotedString}";
            var tableScript = BuildTableCommand().CommandText;
            stringBuilder.Append(OracleBuilder.WrapScriptTextWithComments(tableScript, tableNameScript));
            stringBuilder.AppendLine();
            return stringBuilder.ToString();
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

        public string DropTableScriptText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            var tableNameScript = $"Drop Table {tableName.QuotedString}";
            var tableScript = BuildTableCommand().CommandText;
            stringBuilder.Append(OracleBuilder.WrapScriptTextWithComments(tableScript, tableNameScript));
            stringBuilder.AppendLine();
            return stringBuilder.ToString();
        }

        public bool NeedToCreateForeignKeyConstraints(DmRelation constraint)
        {
            string parentTable = constraint.ParentTable.TableName;
            string parentSchema = constraint.ParentTable.Schema;
            string parentFullName = String.IsNullOrEmpty(parentSchema) ? parentTable : $"{parentSchema}.{parentTable}";

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                var dmTable = OracleManagementUtils.RelationsForTable(connection, transaction, parentFullName);

                var foreignKeyExist = dmTable.Rows.Any(r =>
                   dmTable.IsEqual(r["ForeignKey"].ToString(), constraint.RelationName));

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

        public bool NeedToCreateSchema()
        {
            if (string.IsNullOrEmpty(tableName.SchemaName))
                return false;

            return !OracleManagementUtils.SchemaExists(connection, transaction, tableName.SchemaName);
        }

        public bool NeedToCreateTable()
        {
            return !OracleManagementUtils.TableExists(connection, transaction, tableName.QuotedString);
        }

        #endregion
    }
}