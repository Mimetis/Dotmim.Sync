using Dotmim.Sync.Core.Builders;
using System;
using System.Collections.Generic;
using System.Text;
using Dotmim.Sync.Data;
using System.Data.Common;
using Dotmim.Sync.Core.Common;
using System.Data.SqlClient;

namespace Dotmim.Sync.SqlServer.Builders
{
    public class SqlBuilderTable : IDbBuilderTableHelper
    {
        private DmTable table;
        private ObjectNameParser originalTableName;

        public SqlBuilderTable(DmTable tableDescription)
        {
            this.table = tableDescription;
            string tableAndPrefixName = String.IsNullOrWhiteSpace(this.table.Prefix) ? this.table.TableName : $"{this.table.Prefix}.{this.table.TableName}";
            this.originalTableName = new ObjectNameParser(tableAndPrefixName, "[", "]");
        }
        private (SqlConnection, SqlTransaction) GetTypedConnection(DbTransaction transaction)
        {
            SqlTransaction sqlTransaction = transaction as SqlTransaction;

            if (sqlTransaction == null)
                throw new Exception("Transaction is not a SqlTransaction. Wrong provider");

            SqlConnection sqlConnection = sqlTransaction.Connection;

            return (sqlConnection, sqlTransaction);

        }

        private SqlCommand BuildForeignKeyConstraintsCommand(DmRelation foreignKey)
        {
            SqlCommand sqlCommand = new SqlCommand();

            var childTable = foreignKey.ChildTable;
            var childTableName = new ObjectNameParser(childTable.TableName);
            var parentTable = foreignKey.ParentTable;
            var parentTableName = new ObjectNameParser(parentTable.TableName);

            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.Append("ALTER TABLE ");
            stringBuilder.AppendLine(childTableName.QuotedString);
            stringBuilder.Append("ADD CONSTRAINT ");
            stringBuilder.AppendLine(foreignKey.RelationName);
            stringBuilder.Append("FOREIGN KEY (");
            string empty = string.Empty;
            foreach (var childColumn in foreignKey.ChildColumns)
            {
                var childColumnName = new ObjectNameParser(childColumn.ColumnName);

                stringBuilder.Append($"{empty} {childColumnName.QuotedString}");
                empty = ", ";
            }
            stringBuilder.AppendLine(" )");
            stringBuilder.Append("REFERENCES ");
            stringBuilder.Append(parentTableName.QuotedString).Append(" (");
            empty = string.Empty;
            foreach (var parentColumn in foreignKey.ParentColumns)
            {
                var parentColumnName = new ObjectNameParser(parentColumn.ColumnName);
                stringBuilder.Append($"{empty} {parentColumnName.QuotedString}");
            }
            stringBuilder.Append(" ) ");
            sqlCommand.CommandText = stringBuilder.ToString();
            return sqlCommand;
        }

        public void CreateForeignKeyConstraints(DbTransaction transaction, DbBuilderOption builderOption)
        {
            (var connection, var trans) = GetTypedConnection(transaction);

            foreach (DmRelation constraint in this.table.ParentRelations)
            {
                using (var command = BuildForeignKeyConstraintsCommand(constraint))
                {
                    command.Connection = connection;
                    command.Transaction = trans;
                    command.ExecuteNonQuery();
                }
            }
        }

        public string CreateForeignKeyConstraintsScriptText(DbTransaction transaction, DbBuilderOption builderOption)
        {
            StringBuilder stringBuilder = new StringBuilder();
            foreach (DmRelation constraint in this.table.ParentRelations)
            {
                var constraintName = $"Create Constraint {constraint.RelationName} between parent {constraint.ParentTable.TableName} and child {constraint.ChildTable.TableName}";
                var constraintScript = BuildForeignKeyConstraintsCommand(constraint).CommandText;
                stringBuilder.Append(SqlBuilder.WrapScriptTextWithComments(constraintScript, constraintName));
                stringBuilder.AppendLine();
            }
            return stringBuilder.ToString();
        }


        private SqlCommand BuildPkCommand()
        {
            string[] localName = new string[] { };
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"ALTER TABLE {this.originalTableName.QuotedString} ADD CONSTRAINT [PK_{this.originalTableName.UnquotedStringWithUnderScore}] PRIMARY KEY(");
            for (int i = 0; i < this.table.PrimaryKey.Columns.Length; i++)
            {
                DmColumn pkColumn = this.table.PrimaryKey.Columns[i];
                var quotedColumnName = new ObjectNameParser(pkColumn.ColumnName, "[", "]").QuotedString;

                stringBuilder.Append(quotedColumnName);

                if (i < this.table.PrimaryKey.Columns.Length - 1)
                    stringBuilder.Append(", ");
            }
            stringBuilder.Append(")");

            return new SqlCommand(stringBuilder.ToString());
        }
        public void CreatePk(DbTransaction transaction, DbBuilderOption builderOption)
        {
            (var connection, var trans) = GetTypedConnection(transaction);

            using (var command = BuildPkCommand())
            {
                command.Connection = connection;
                command.Transaction = trans;
                command.ExecuteNonQuery();
            }
        }

        public string CreatePkScriptText(DbTransaction transaction, DbBuilderOption builderOption)
        {
            StringBuilder stringBuilder = new StringBuilder();
            var pkName = $"Create primary keys for table {this.originalTableName.QuotedString}";
            var pkScript = BuildPkCommand().CommandText;
            stringBuilder.Append(SqlBuilder.WrapScriptTextWithComments(pkScript, pkName));
            stringBuilder.AppendLine();
            return stringBuilder.ToString();
        }


        private SqlCommand BuildTableCommand()
        {
            SqlCommand command = new SqlCommand();

            StringBuilder stringBuilder = new StringBuilder($"CREATE TABLE {this.originalTableName.QuotedString} (");
            string empty = string.Empty;
            stringBuilder.AppendLine();
            foreach (var column in this.table.Columns)
            {
                var columnName = new ObjectNameParser(column.ColumnName);
                var columnType = $"{column.GetSqlDbTypeString()} {column.GetSqlTypePrecisionString()}";
                var nullString = column.AllowDBNull ? "NULL" : "NOT NULL";

                stringBuilder.AppendLine($"\t{empty}{columnName.QuotedString} {columnType} {nullString}");
                empty = ",";
            }
            stringBuilder.Append(")");
            return new SqlCommand(stringBuilder.ToString());
        }
        public void CreateTable(DbTransaction transaction, DbBuilderOption builderOption)
        {
            (var connection, var trans) = GetTypedConnection(transaction);

            using (var command = BuildTableCommand())
            {
                command.Connection = connection;
                command.Transaction = trans;
                command.ExecuteNonQuery();
            }

        }
        public string CreateTableScriptText(DbTransaction transaction, DbBuilderOption builderOption)
        {
            StringBuilder stringBuilder = new StringBuilder();
            var tableName = $"Create Table {this.originalTableName.QuotedString}";
            var tableScript = BuildTableCommand().CommandText;
            stringBuilder.Append(SqlBuilder.WrapScriptTextWithComments(tableScript, tableName));
            stringBuilder.AppendLine();
            return stringBuilder.ToString();
        }

        public List<string> GetColumnForTable(DbTransaction transaction, string tableName)
        {
            throw new NotImplementedException();
        }

        public bool NeedToCreateTable(DbTransaction transaction, DmTable tableDescription, DbBuilderOption builderOption)
        {
            (var connection, var trans) = GetTypedConnection(transaction);

            switch (builderOption)
            {
                case DbBuilderOption.Create:
                    return true;
                case DbBuilderOption.Skip:
                    return false;
                case DbBuilderOption.CreateOrUseExisting:
                    return !SqlManagementUtils.TableExists(connection, trans, this.originalTableName.QuotedString);
            }
            return false;
        }


    }
}
