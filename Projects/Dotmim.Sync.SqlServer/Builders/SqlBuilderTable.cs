using Dotmim.Sync.Core.Builders;
using System;
using System.Collections.Generic;
using System.Text;
using Dotmim.Sync.Data;
using System.Data.Common;
using Dotmim.Sync.Core.Common;
using System.Data.SqlClient;
using Dotmim.Sync.Core.Scope;
using System.Linq;
using System.Data;
using Dotmim.Sync.Core.Log;

namespace Dotmim.Sync.SqlServer.Builders
{
    public class SqlBuilderTable : IDbBuilderTableHelper
    {
        private ObjectNameParser tableName;
        private ObjectNameParser trackingName;
        private DmTable tableDescription;
        private SqlConnection connection;
        private SqlTransaction transaction;

        public DmTable TableDescription
        {
            get
            {
                return this.tableDescription;
            }
            set
            {
                this.tableDescription = value;
                (this.tableName, this.trackingName) = SqlBuilder.GetParsers(TableDescription);

            }
        }
        public SqlBuilderTable(DbConnection connection, DbTransaction transaction = null)
        {
            this.connection = connection as SqlConnection;
            this.transaction = transaction as SqlTransaction;
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
        public void CreateForeignKeyConstraints()
        {

            bool alreadyOpened = connection.State == ConnectionState.Open;

            try
            {
                if (!alreadyOpened)
                    connection.Open();

                foreach (DmRelation constraint in TableDescription.ChildRelations)
                {
               
                    using (var command = BuildForeignKeyConstraintsCommand(constraint))
                    {
                        command.Connection = connection;

                        if (transaction != null)
                            command.Transaction = transaction;

                        command.ExecuteNonQuery();
                    }
                }

            }
            catch (Exception ex)
            {
                Logger.Current.Error($"Error during CreateForeignKeyConstraints : {ex}");
                throw;

            }
            finally
            {
                if (!alreadyOpened && connection.State != ConnectionState.Closed)
                    connection.Close();

            }

        }
        public string CreateForeignKeyConstraintsScriptText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            foreach (DmRelation constraint in TableDescription.ChildRelations)
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

            stringBuilder.AppendLine($"ALTER TABLE {tableName.QuotedString} ADD CONSTRAINT [PK_{tableName.UnquotedStringWithUnderScore}] PRIMARY KEY(");
            for (int i = 0; i < TableDescription.PrimaryKey.Columns.Length; i++)
            {
                DmColumn pkColumn = TableDescription.PrimaryKey.Columns[i];
                var quotedColumnName = new ObjectNameParser(pkColumn.ColumnName, "[", "]").QuotedString;

                stringBuilder.Append(quotedColumnName);

                if (i < TableDescription.PrimaryKey.Columns.Length - 1)
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
                Logger.Current.Error($"Error during Create Pk Command : {ex}");
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
            SqlCommand command = new SqlCommand();

            StringBuilder stringBuilder = new StringBuilder($"CREATE TABLE {tableName.QuotedString} (");
            string empty = string.Empty;
            stringBuilder.AppendLine();
            foreach (var column in TableDescription.Columns)
            {
                var columnName = new ObjectNameParser(column.ColumnName);
                var columnType = $"{column.GetSqlDbTypeString()} {column.GetSqlTypePrecisionString()}";
                var identity = string.Empty;

                if (column.AutoIncrement)
                {
                    var s = column.GetAutoIncrementSeedAndStep();
                    identity = $"IDENTITY({s.Step},{s.Seed})";
                }
                var nullString = column.AllowDBNull ? "NULL" : "NOT NULL";

                stringBuilder.AppendLine($"\t{empty}{columnName.QuotedString} {columnType} {identity} {nullString}");
                empty = ",";
            }
            stringBuilder.Append(")");
            return new SqlCommand(stringBuilder.ToString());
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
                Logger.Current.Error($"Error during CreateTable : {ex}");
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
                Logger.Current.Error($"Error during EnsureForeignKeysTableExist : {ex}");
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
        public bool NeedToCreateTable(DbBuilderOption builderOptions)
        {
            if (builderOptions.HasFlag(DbBuilderOption.CreateOrUseExistingSchema))
                return !SqlManagementUtils.TableExists(connection, transaction, tableName.QuotedString);

            return false;
        }

    }
}
