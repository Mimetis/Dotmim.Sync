using Dotmim.Sync.Core.Builders;
using System;
using System.Collections.Generic;
using System.Text;
using Dotmim.Sync.Data;
using System.Data.Common;
using Dotmim.Sync.Core.Common;
using System.Data.SqlClient;
using System.Linq;

namespace Dotmim.Sync.SqlServer.Builders
{
    public class SqlBuilderTriggerHelper : IDbBuilderTriggerHelper
    {
        private enum TriggerType
        {
            Insert,
            Update,
            Delete
        }

        private DmTable table;
        private ObjectNameParser originalTableName;
        private ObjectNameParser trackingTableName;

        private string _insTriggerName;
        private string _updTriggerName;
        private string _delTriggerName;

        public SqlBuilderTriggerHelper(DmTable tableDescription)
        {
            this.table = tableDescription;
            string tableAndPrefixName = String.IsNullOrWhiteSpace(this.table.Prefix) ? this.table.TableName : $"{this.table.Prefix}.{this.table.TableName}";
            this.originalTableName = new ObjectNameParser(tableAndPrefixName, "[", "]");
            this.trackingTableName = new ObjectNameParser($"{tableAndPrefixName}_tracking", "[", "]");

            this.SetPrefixedTriggerNames();
        }

        private (SqlConnection, SqlTransaction) GetTypedConnection(DbTransaction transaction)
        {
            SqlTransaction sqlTransaction = transaction as SqlTransaction;

            if (sqlTransaction == null)
                throw new Exception("Transaction is not a SqlTransaction. Wrong provider");

            SqlConnection sqlConnection = sqlTransaction.Connection;

            return (sqlConnection, sqlTransaction);

        }


        public List<DmColumn> FilterColumns { get; set; }

        private string DeleteTriggerBodyText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("UPDATE [side] ");
            stringBuilder.AppendLine("SET \t[sync_row_is_tombstone] = 1");
            stringBuilder.AppendLine("\t,[update_scope_name] = NULL -- since the update if from local, it's a NULL");
            stringBuilder.AppendLine("\t,[update_timestamp] = @@DBTS+1");
            stringBuilder.AppendLine("\t,[last_change_datetime] = GetDate()");
            // Filter columns
            if (this.FilterColumns != null)
            {
                for (int i = 0; i < this.FilterColumns.Count; i++)
                {
                    var filterColumn = this.FilterColumns[i];

                    if (this.table.PrimaryKey.Columns.Any(c => c.ColumnName == filterColumn.ColumnName))
                        continue;

                    ObjectNameParser columnName = new ObjectNameParser(filterColumn.ColumnName);

                    stringBuilder.AppendLine($"\t,{columnName.QuotedString} = [d].{columnName.QuotedString}");

                }
                stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine($"FROM {this.trackingTableName.QuotedString} [side]");
            stringBuilder.Append($"JOIN DELETED AS [d] ON ");
            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.table.PrimaryKey.Columns, "[side]", "[d]"));
            return stringBuilder.ToString();
        }
        public void CreateDeleteTrigger(DbTransaction transaction, DbBuilderOption builderOption)
        {
            (var connection, var trans) = GetTypedConnection(transaction);

            if (!NeedToCreateTrigger(TriggerType.Delete, connection, trans, builderOption))
                return;

            StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER {this._delTriggerName} ON {this.originalTableName.QuotedString} FOR DELETE AS");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.DeleteTriggerBodyText());

            using (var sqlCommand = new SqlCommand(createTrigger.ToString(), connection, trans))
            {
                sqlCommand.ExecuteNonQuery();
            }
        }
        public string CreateDeleteTriggerScriptText(DbTransaction transaction, DbBuilderOption builderOption)
        {
            (var connection, var trans) = GetTypedConnection(transaction);

            if (!NeedToCreateTrigger(TriggerType.Delete, connection, trans, builderOption))
                return string.Empty;

            StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER {this._delTriggerName} ON {this.originalTableName.QuotedString} FOR DELETE AS");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.DeleteTriggerBodyText());

            string str = $"Delete Trigger for table {this.originalTableName.QuotedString}";
            return SqlBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);
        }
        public void AlterDeleteTrigger(DbTransaction transaction)
        {
            (var connection, var trans) = GetTypedConnection(transaction);

            StringBuilder createTrigger = new StringBuilder($"ALTER TRIGGER {this._delTriggerName} ON {this.originalTableName.QuotedString} FOR DELETE AS ");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.DeleteTriggerBodyText());

            using (var sqlCommand = new SqlCommand(createTrigger.ToString(), connection, trans))
            {
                sqlCommand.ExecuteNonQuery();
            }
        }
        public string AlterDeleteTriggerScriptText()
        {
            StringBuilder createTrigger = new StringBuilder($"ALTER TRIGGER {this._delTriggerName} ON {this.originalTableName.QuotedString} FOR DELETE AS");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.InsertTriggerBodyText());

            string str = $"ALTER Trigger Delete for table {this.originalTableName.QuotedString}";
            return SqlBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);
        }


        private string InsertTriggerBodyText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("-- If row was deleted before, it already exists, so just make an update");
            stringBuilder.AppendLine("UPDATE [side] ");
            stringBuilder.AppendLine("SET \t[sync_row_is_tombstone] = 0");
            stringBuilder.AppendLine("\t,[update_scope_name] = NULL -- since the update if from local, it's a NULL");
            stringBuilder.AppendLine("\t,[last_change_datetime] = GetDate()");
            // Filter columns
            if (this.FilterColumns != null)
            {
                for (int i = 0; i < this.FilterColumns.Count; i++)
                {
                    var filterColumn = this.FilterColumns[i];

                    if (this.table.PrimaryKey.Columns.Any(c => c.ColumnName == filterColumn.ColumnName))
                        continue;

                    ObjectNameParser columnName = new ObjectNameParser(filterColumn.ColumnName);

                    stringBuilder.AppendLine($"\t,{columnName.QuotedString} = [i].{columnName.QuotedString}");

                }
                stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine($"FROM {this.trackingTableName.QuotedString} [side]");
            stringBuilder.Append($"JOIN INSERTED AS [i] ON ");
            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.table.PrimaryKey.Columns, "[side]", "[i]"));
            stringBuilder.AppendLine();

            stringBuilder.AppendLine($"INSERT INTO {trackingTableName.QuotedString} (");

            StringBuilder stringBuilderArguments = new StringBuilder();
            StringBuilder stringPkAreNull = new StringBuilder();

            string argComma = string.Empty;
            string argAnd = string.Empty;
            foreach (var mutableColumn in this.table.PrimaryKey.Columns)
            {
                ObjectNameParser columnName = new ObjectNameParser(mutableColumn.ColumnName);
                stringBuilderArguments.AppendLine($"\t{argComma}[i].{columnName.QuotedString}");
                stringPkAreNull.Append($"{argAnd}[side].{columnName.QuotedString} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }

            stringBuilder.Append(stringBuilderArguments.ToString());
            stringBuilder.AppendLine("\t,[create_scope_name]");
            stringBuilder.AppendLine("\t,[create_timestamp]");
            stringBuilder.AppendLine("\t,[update_scope_name]");
            stringBuilder.AppendLine("\t,[update_timestamp]");
            stringBuilder.AppendLine("\t,[sync_row_is_tombstone]");
            stringBuilder.AppendLine("\t,[last_change_datetime]");

            StringBuilder filterColumnsString = new StringBuilder();
            // Filter columns
            if (this.FilterColumns != null)
            {
                for (int i = 0; i < this.FilterColumns.Count; i++)
                {
                    var filterColumn = this.FilterColumns[i];

                    if (this.table.PrimaryKey.Columns.Any(c => c.ColumnName == filterColumn.ColumnName))
                        continue;

                    ObjectNameParser columnName = new ObjectNameParser(filterColumn.ColumnName);

                    filterColumnsString.AppendLine($"\t,[i].{columnName.QuotedString}");

                }

                stringBuilder.AppendLine(filterColumnsString.ToString());
            }
            stringBuilder.AppendLine(") ");
            stringBuilder.AppendLine("SELECT");
            stringBuilder.Append(stringBuilderArguments.ToString());
            stringBuilder.AppendLine("\t,NULL");
            stringBuilder.AppendLine("\t,@@DBTS+1");
            stringBuilder.AppendLine("\t,NULL");
            stringBuilder.AppendLine("\t,@@DBTS+1");
            stringBuilder.AppendLine("\t,0");
            stringBuilder.AppendLine("\t,GetDate()");

            if (FilterColumns != null)
                stringBuilder.AppendLine(filterColumnsString.ToString());

            stringBuilder.AppendLine("FROM INSERTED [i]");
            stringBuilder.Append($"LEFT JOIN {this.trackingTableName.QuotedString} [side] ON ");
            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.table.PrimaryKey.Columns, "[i]", "[side]"));
            stringBuilder.Append("WHERE ");
            stringBuilder.AppendLine(stringPkAreNull.ToString());
            return stringBuilder.ToString();
        }
        public void CreateInsertTrigger(DbTransaction transaction, DbBuilderOption builderOption)
        {
            (var connection, var trans) = GetTypedConnection(transaction);

            if (!NeedToCreateTrigger(TriggerType.Insert, connection, trans, builderOption))
                return;

            StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER {this._insTriggerName} ON {this.originalTableName.QuotedString} FOR INSERT AS");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.InsertTriggerBodyText());

            using (var sqlCommand = new SqlCommand(createTrigger.ToString(), connection, trans))
            {
                sqlCommand.ExecuteNonQuery();
            }

        }
        public string CreateInsertTriggerScriptText(DbTransaction transaction, DbBuilderOption builderOption)
        {
            (var connection, var trans) = GetTypedConnection(transaction);

            if (!NeedToCreateTrigger(TriggerType.Insert, connection, trans, builderOption))
                return string.Empty;

            StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER {this._insTriggerName} ON {this.originalTableName.QuotedString} FOR INSERT AS");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.InsertTriggerBodyText());

            string str = $"Insert Trigger for table {this.originalTableName.QuotedString}";
            return SqlBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);

        }
        public void AlterInsertTrigger(DbTransaction transaction)
        {
            (var connection, var trans) = GetTypedConnection(transaction);

            StringBuilder createTrigger = new StringBuilder($"ALTER TRIGGER {this._insTriggerName} ON {this.originalTableName.QuotedString} FOR INSERT AS ");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.InsertTriggerBodyText());

            using (var sqlCommand = new SqlCommand(createTrigger.ToString(), connection, trans))
            {
                sqlCommand.ExecuteNonQuery();
            }
        }
        public string AlterInsertTriggerScriptText()
        {
            StringBuilder createTrigger = new StringBuilder($"ALTER TRIGGER {this._insTriggerName} ON {this.originalTableName.QuotedString} FOR INSERT AS");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.InsertTriggerBodyText());

            string str = $"ALTER Trigger Insert for table {this.originalTableName.QuotedString}";
            return SqlBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);
        }


        private string UpdateTriggerBodyText()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.AppendLine();
            stringBuilder.AppendLine("UPDATE [side] ");
            stringBuilder.AppendLine("SET \t[update_scope_name] = NULL -- since the update if from local, it's a NULL");
            stringBuilder.AppendLine("\t,[update_timestamp] = @@DBTS+1");
            stringBuilder.AppendLine("\t,[last_change_datetime] = GetDate()");
            // Filter columns
            if (this.FilterColumns != null)
            {
                for (int i = 0; i < this.FilterColumns.Count; i++)
                {
                    var filterColumn = this.FilterColumns[i];

                    if (this.table.PrimaryKey.Columns.Any(c => c.ColumnName == filterColumn.ColumnName))
                        continue;

                    ObjectNameParser columnName = new ObjectNameParser(filterColumn.ColumnName);

                    stringBuilder.AppendLine($"\t,{columnName.QuotedString} = [i].{columnName.QuotedString}");

                }
                stringBuilder.AppendLine();
            }

            stringBuilder.AppendLine($"FROM {this.trackingTableName.QuotedString} [side]");
            stringBuilder.Append($"JOIN INSERTED AS [i] ON ");
            stringBuilder.AppendLine(SqlManagementUtils.JoinTwoTablesOnClause(this.table.PrimaryKey.Columns, "[side]", "[i]"));
            return stringBuilder.ToString();
        }
        public void CreateUpdateTrigger(DbTransaction transaction, DbBuilderOption builderOption)
        {
            (var connection, var trans) = GetTypedConnection(transaction);

            if (!NeedToCreateTrigger(TriggerType.Update, connection, trans, builderOption))
                return;

            StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER {this._updTriggerName} ON {this.originalTableName.QuotedString} FOR UPDATE AS");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.UpdateTriggerBodyText());

            using (var sqlCommand = new SqlCommand(createTrigger.ToString(), connection, trans))
            {
                sqlCommand.ExecuteNonQuery();
            }
        }
        public string CreateUpdateTriggerScriptText(DbTransaction transaction, DbBuilderOption builderOption)
        {
            (var connection, var trans) = GetTypedConnection(transaction);

            if (!NeedToCreateTrigger(TriggerType.Update, connection, trans, builderOption))
                return string.Empty;

            StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER {this._updTriggerName} ON {this.originalTableName.QuotedString} FOR UPDATE AS");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.UpdateTriggerBodyText());

            string str = $"Update Trigger for table {this.originalTableName.QuotedString}";
            return SqlBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);
        }
        public void AlterUpdateTrigger(DbTransaction transaction)
        {
            (var connection, var trans) = GetTypedConnection(transaction);

            StringBuilder createTrigger = new StringBuilder($"ALTER TRIGGER {this._insTriggerName} ON {this.originalTableName.QuotedString} FOR UPDATE AS ");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.UpdateTriggerBodyText());

            using (var sqlCommand = new SqlCommand(createTrigger.ToString(), connection, trans))
            {
                sqlCommand.ExecuteNonQuery();
            }
        }
        public string AlterUpdateTriggerScriptText()
        {
            StringBuilder createTrigger = new StringBuilder($"ALTER TRIGGER {this._updTriggerName} ON {this.originalTableName.QuotedString} FOR UPDATE AS");
            createTrigger.AppendLine();
            createTrigger.AppendLine(this.InsertTriggerBodyText());

            string str = $"ALTER Trigger Update for table {this.originalTableName.QuotedString}";
            return SqlBuilder.WrapScriptTextWithComments(createTrigger.ToString(), str);
        }
        private bool NeedToCreateTrigger(TriggerType type, SqlConnection connection, SqlTransaction transaction, DbBuilderOption option)
        {
            switch (option)
            {
                case DbBuilderOption.Create:
                    {
                        return true;
                    }
                case DbBuilderOption.Skip:
                    {
                        return false;
                    }
                case DbBuilderOption.CreateOrUseExisting:
                    {
                        string triggerName = string.Empty;
                        switch (type)
                        {
                            case TriggerType.Insert:
                                {
                                    triggerName = this._insTriggerName;
                                    break;
                                }
                            case TriggerType.Update:
                                {
                                    triggerName = this._updTriggerName;
                                    break;
                                }
                            case TriggerType.Delete:
                                {
                                    triggerName = this._delTriggerName;
                                    break;
                                }
                        }
                        return !SqlManagementUtils.TriggerExists(connection, transaction, triggerName);
                    }
            }
            return false;
        }


        internal void SetPrefixedTriggerNames(string prefix = null)
        {
            string str = "[";
            if (!string.IsNullOrEmpty(trackingTableName.SchemaName))
                str = string.Concat(trackingTableName.QuotedSchemaName, ".[");

            this._insTriggerName = SqlManagementUtils.GetQuotedPrefixedName(prefix, string.Concat(str, originalTableName.ObjectName, "_insert_trigger]"), string.Empty);
            this._updTriggerName = SqlManagementUtils.GetQuotedPrefixedName(prefix, string.Concat(str, originalTableName.ObjectName, "_update_trigger]"), string.Empty);
            this._delTriggerName = SqlManagementUtils.GetQuotedPrefixedName(prefix, string.Concat(str, originalTableName.ObjectName, "_delete_trigger]"), string.Empty);
        }

    }
}
