using Dotmim.Sync.Builders;
using System.Text;

using System.Data.Common;
using System.Threading.Tasks;
using System.Collections.Generic;
using Dotmim.Sync.Manager;
using System;
using Microsoft.Data.Sqlite;
using System.Linq;

namespace Dotmim.Sync.Sqlite
{

    /// <summary>
    /// The SqlBuilder class is the Sql implementation of DbBuilder class.
    /// In charge of creating tracking table, stored proc, triggers and adapters.
    /// </summary>
    public class SqliteTableBuilder : DbTableBuilder
    {

        private SqliteObjectNames sqliteObjectNames;
        private SqliteDbMetadata sqliteDbMetadata;

        public SqliteTableBuilder(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup) : base(tableDescription, tableName, trackingTableName, setup)
        {
            this.sqliteObjectNames = new SqliteObjectNames(tableDescription, this.TableName, this.TrackingTableName, setup);
            this.sqliteDbMetadata = new SqliteDbMetadata();
        }

        private string BuildTableCommandText()
        {
            var stringBuilder = new StringBuilder($"CREATE TABLE IF NOT EXISTS {this.TableName.Quoted().ToString()} (");
            string empty = string.Empty;
            stringBuilder.AppendLine();
            foreach (var column in this.TableDescription.Columns)
            {
                var columnName = ParserName.Parse(column).Quoted().ToString();

                var columnTypeString = this.sqliteDbMetadata.TryGetOwnerDbTypeString(column.OriginalDbType, column.GetDbType(), false, false, column.MaxLength, this.TableDescription.OriginalProvider, SqliteSyncProvider.ProviderType);
                var columnPrecisionString = this.sqliteDbMetadata.TryGetOwnerDbTypePrecision(column.OriginalDbType, column.GetDbType(), false, false, column.MaxLength, column.Precision, column.Scale, this.TableDescription.OriginalProvider, SqliteSyncProvider.ProviderType);
                var columnType = $"{columnTypeString} {columnPrecisionString}";

                // check case
                string casesensitive = "";
                if (this.sqliteDbMetadata.IsTextType(column.GetDbType()))
                {
                    casesensitive = SyncGlobalization.IsCaseSensitive() ? "" : "COLLATE NOCASE";

                    //check if it's a primary key, then, even if it's case sensitive, we turn on case insensitive
                    if (SyncGlobalization.IsCaseSensitive())
                    {
                        if (this.TableDescription.PrimaryKeys.Contains(column.ColumnName))
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
            for (int i = 0; i < this.TableDescription.PrimaryKeys.Count; i++)
            {
                var pkColumn = this.TableDescription.PrimaryKeys[i];
                var quotedColumnName = ParserName.Parse(pkColumn).Quoted().ToString();

                stringBuilder.Append(quotedColumnName);

                if (i < this.TableDescription.PrimaryKeys.Count - 1)
                    stringBuilder.Append(", ");
            }
            stringBuilder.Append(")");

            // Constraints
            foreach (var constraint in this.TableDescription.GetRelations())
            {
                // Don't want foreign key on same table since it could be a problem on first 
                // sync. We are not sure that parent row will be inserted in first position
                if (constraint.GetParentTable().EqualsByName(constraint.GetTable()))
                    continue;

                var parentTable = constraint.GetParentTable();
                var parentTableName = ParserName.Parse(parentTable.TableName).Quoted().ToString();

                stringBuilder.AppendLine();
                stringBuilder.Append($"\tFOREIGN KEY (");
                empty = string.Empty;
                foreach (var column in constraint.Keys)
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
            return stringBuilder.ToString();
        }

        private string BuildTrackingTableCommandText()
        {
            var stringBuilder = new StringBuilder();
            stringBuilder.AppendLine($"CREATE TABLE IF NOT EXISTS {this.TrackingTableName.Quoted().ToString()} (");

            // Adding the primary key
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var quotedColumnName = ParserName.Parse(pkColumn).Quoted().ToString();

                var columnTypeString = this.sqliteDbMetadata.TryGetOwnerDbTypeString(pkColumn.OriginalDbType, pkColumn.GetDbType(), false, false, pkColumn.MaxLength, this.TableDescription.OriginalProvider, SqliteSyncProvider.ProviderType);
                var columnPrecisionString = this.sqliteDbMetadata.TryGetOwnerDbTypePrecision(pkColumn.OriginalDbType, pkColumn.GetDbType(), false, false, pkColumn.MaxLength, pkColumn.Precision, pkColumn.Scale, this.TableDescription.OriginalProvider, SqliteSyncProvider.ProviderType);
                var quotedColumnType = ParserName.Parse(columnTypeString).Quoted().ToString();
                quotedColumnType += columnPrecisionString;

                stringBuilder.AppendLine($"{quotedColumnName} {quotedColumnType} NOT NULL COLLATE NOCASE, ");
            }

            // adding the tracking columns
            stringBuilder.AppendLine($"[update_scope_id] [text] NULL COLLATE NOCASE, ");
            stringBuilder.AppendLine($"[timestamp] [integer] NULL, ");
            stringBuilder.AppendLine($"[sync_row_is_tombstone] [integer] NOT NULL default(0), ");
            stringBuilder.AppendLine($"[last_change_datetime] [datetime] NULL, ");

            stringBuilder.Append(" PRIMARY KEY (");
            for (int i = 0; i < this.TableDescription.PrimaryKeys.Count; i++)
            {
                var pkColumn = this.TableDescription.PrimaryKeys[i];
                var quotedColumnName = ParserName.Parse(pkColumn).Quoted().ToString();

                stringBuilder.Append(quotedColumnName);

                if (i < this.TableDescription.PrimaryKeys.Count - 1)
                    stringBuilder.Append(", ");
            }
            stringBuilder.Append(")");


            stringBuilder.Append(");");

            stringBuilder.AppendLine($"CREATE INDEX IF NOT EXISTS [{this.TrackingTableName.Schema().Unquoted().Normalized().ToString()}_timestamp_index] ON {this.TrackingTableName.Schema().Quoted().ToString()} (");
            stringBuilder.AppendLine($"\t [timestamp] ASC");
            stringBuilder.AppendLine($"\t,[update_scope_id] ASC");
            stringBuilder.AppendLine($"\t,[sync_row_is_tombstone] ASC");
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnName = ParserName.Parse(pkColumn).Quoted().ToString();
                stringBuilder.AppendLine($"\t,{columnName} ASC");
            }
            stringBuilder.Append(");");
            return stringBuilder.ToString();
        }


        public override Task<DbCommand> GetCreateSchemaCommandAsync(DbConnection connection, DbTransaction transaction) => Task.FromResult<DbCommand>(null);

        public override Task<DbCommand> GetCreateTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var commandText = BuildTableCommandText();

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;

            command.CommandText = commandText;

            return Task.FromResult(command);
        }
        public override Task<DbCommand> GetExistsTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var tbl = this.TableName.Unquoted().ToString();

            var command = connection.CreateCommand();

            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = "select count(*) from sqlite_master where name = @tableName AND type='table'";

            var parameter = command.CreateParameter();
            parameter.ParameterName = "@tableName";
            parameter.Value = tbl;
            command.Parameters.Add(parameter);

            return Task.FromResult(command);
        }

        public override Task<DbCommand> GetExistsSchemaCommandAsync(DbConnection connection, DbTransaction transaction) => Task.FromResult<DbCommand>(null);
        public override Task<DbCommand> GetDropTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();

            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = $"drop table if exist {this.TableName.Quoted().ToString()}";

            return Task.FromResult(command);
        }
        public override Task<DbCommand> GetExistsStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction) => Task.FromResult<DbCommand>(null);
        public override Task<DbCommand> GetCreateStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction) => Task.FromResult<DbCommand>(null);
        public override Task<DbCommand> GetDropStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction) => Task.FromResult<DbCommand>(null);

        public override Task<DbCommand> GetCreateTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var commandText = BuildTrackingTableCommandText();

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;

            command.CommandText = commandText;

            return Task.FromResult(command);
        }
        public override Task<DbCommand> GetDropTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();

            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = $"drop table if exist {this.TrackingTableName.Quoted().ToString()}";

            return Task.FromResult(command);
        }

        public override Task<DbCommand> GetRenameTrackingTableCommandAsync(ParserName oldTableName, DbConnection connection, DbTransaction transaction)
        {
            var tableNameString = this.TrackingTableName.Quoted().ToString();
            var oldTableNameString = oldTableName.Quoted().ToString();

            var command = connection.CreateCommand();

            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = $"ALTER TABLE {oldTableNameString} RENAME TO {tableNameString};";

            return Task.FromResult(command);

        }

        public override Task<DbCommand> GetExistsTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var tbl = this.TrackingTableName.Unquoted().ToString();

            var command = connection.CreateCommand();

            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = "select count(*) from sqlite_master where name = @tableName AND type='table'";

            var parameter = command.CreateParameter();
            parameter.ParameterName = "@tableName";
            parameter.Value = tbl;
            command.Parameters.Add(parameter);

            return Task.FromResult(command);
        }

        public override Task<DbCommand> GetExistsTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
        {
            var commandTriggerName = string.Format(this.sqliteObjectNames.GetTriggerCommandName(triggerType), TableName.Unquoted().ToString());
            var triggerName = ParserName.Parse(commandTriggerName).ToString();

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = "select count(*) from sqlite_master where name = @triggerName AND type='trigger'";

            var parameter = command.CreateParameter();
            parameter.ParameterName = "@triggerName";
            parameter.Value = triggerName;
            command.Parameters.Add(parameter);

            return Task.FromResult(command);
        }


        private DbCommand CreateInsertTriggerCommand(DbConnection connection, DbTransaction transaction)
        {
            var insTriggerName = string.Format(this.sqliteObjectNames.GetTriggerCommandName(DbTriggerType.Insert), TableName.Unquoted().ToString());
            
            StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER IF NOT EXISTS {insTriggerName} AFTER INSERT ON {TableName.Quoted().ToString()} ");
            createTrigger.AppendLine();

            var stringBuilderArguments = new StringBuilder();
            var stringBuilderArguments2 = new StringBuilder();
            var stringPkAreNull = new StringBuilder();
            string argComma = string.Empty;
            string argAnd = string.Empty;

            createTrigger.AppendLine();
            createTrigger.AppendLine("BEGIN");
            createTrigger.AppendLine("-- If row was deleted before, it already exists, so just make an update");

            createTrigger.AppendLine($"\tINSERT OR REPLACE INTO {this.TrackingTableName.Quoted().ToString()} (");
            foreach (var mutableColumn in this.TableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();

                stringBuilderArguments.AppendLine($"\t\t{argComma}{columnName}");
                stringBuilderArguments2.AppendLine($"\t\t{argComma}new.{columnName}");
                stringPkAreNull.Append($"{argAnd}{TrackingTableName.Quoted().ToString()}.{columnName} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }

            createTrigger.Append(stringBuilderArguments.ToString());
            createTrigger.AppendLine("\t\t,[update_scope_id]");
            createTrigger.AppendLine("\t\t,[timestamp]");
            createTrigger.AppendLine("\t\t,[sync_row_is_tombstone]");
            createTrigger.AppendLine("\t\t,[last_change_datetime]");

            createTrigger.AppendLine("\t) ");
            createTrigger.AppendLine("\tVALUES (");
            createTrigger.Append(stringBuilderArguments2.ToString());
            createTrigger.AppendLine("\t\t,NULL");
            createTrigger.AppendLine($"\t\t,{SqliteObjectNames.TimestampValue}");
            createTrigger.AppendLine("\t\t,0");
            createTrigger.AppendLine("\t\t,datetime('now')");
            createTrigger.AppendLine("\t);");
            createTrigger.AppendLine("END;");

            return new SqliteCommand(createTrigger.ToString(), (SqliteConnection)connection, (SqliteTransaction)transaction);
        }

        private DbCommand CreateDeleteTriggerCommand(DbConnection connection, DbTransaction transaction)
        {
            var delTriggerName = string.Format(this.sqliteObjectNames.GetTriggerCommandName(DbTriggerType.Delete), TableName.Unquoted().ToString());

            StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER IF NOT EXISTS {delTriggerName} AFTER DELETE ON {TableName.Quoted().ToString()} ");
            createTrigger.AppendLine();

            var stringBuilderArguments = new StringBuilder();
            var stringBuilderArguments2 = new StringBuilder();
            var stringPkAreNull = new StringBuilder();
            string argComma = string.Empty;
            string argAnd = string.Empty;

            createTrigger.AppendLine();
            createTrigger.AppendLine("BEGIN");

            createTrigger.AppendLine($"\tINSERT OR REPLACE INTO {TrackingTableName.Quoted().ToString()} (");
            foreach (var mutableColumn in this.TableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();

                stringBuilderArguments.AppendLine($"\t\t{argComma}{columnName}");
                stringBuilderArguments2.AppendLine($"\t\t{argComma}old.{columnName}");
                stringPkAreNull.Append($"{argAnd}{TrackingTableName.Quoted().ToString()}.{columnName} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }

            createTrigger.Append(stringBuilderArguments.ToString());
            createTrigger.AppendLine("\t\t,[update_scope_id]");
            createTrigger.AppendLine("\t\t,[timestamp]");
            createTrigger.AppendLine("\t\t,[sync_row_is_tombstone]");
            createTrigger.AppendLine("\t\t,[last_change_datetime]");

            createTrigger.AppendLine("\t) ");
            createTrigger.AppendLine("\tVALUES (");
            createTrigger.Append(stringBuilderArguments2.ToString());
            createTrigger.AppendLine("\t\t,NULL");
            createTrigger.AppendLine($"\t\t,{SqliteObjectNames.TimestampValue}");
            createTrigger.AppendLine("\t\t,1");
            createTrigger.AppendLine("\t\t,datetime('now')");
            createTrigger.AppendLine("\t);");
            createTrigger.AppendLine("END;");

            return new SqliteCommand(createTrigger.ToString(), (SqliteConnection)connection, (SqliteTransaction)transaction);
        }


        private DbCommand CreateUpdateTriggerCommand(DbConnection connection, DbTransaction transaction)
        {
            var updTriggerName = string.Format(this.sqliteObjectNames.GetTriggerCommandName(DbTriggerType.Update), TableName.Unquoted().ToString());

            StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER IF NOT EXISTS {updTriggerName} AFTER UPDATE ON {TableName.Quoted().ToString()} ");
            createTrigger.AppendLine();


            createTrigger.AppendLine();
            createTrigger.AppendLine($"Begin ");
            createTrigger.AppendLine($"\tUPDATE {TrackingTableName.Quoted().ToString()} ");
            createTrigger.AppendLine("\tSET [update_scope_id] = NULL -- scope id is always NULL when update is made locally");
            createTrigger.AppendLine($"\t\t,[timestamp] = {SqliteObjectNames.TimestampValue}");
            createTrigger.AppendLine("\t\t,[last_change_datetime] = datetime('now')");

            createTrigger.Append($"\tWhere ");
            createTrigger.Append(SqliteManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, TrackingTableName.Quoted().ToString(), "new"));


            if (this.TableDescription.GetMutableColumns().Count() > 0)
            {
                createTrigger.AppendLine();
                createTrigger.AppendLine("\t AND (");
                string or = "    ";
                foreach (var column in this.TableDescription.GetMutableColumns())
                {
                    var quotedColumn = ParserName.Parse(column).Quoted().ToString();

                    createTrigger.Append("\t");
                    createTrigger.Append(or);
                    createTrigger.Append("IFNULL(");
                    createTrigger.Append("NULLIF(");
                    createTrigger.Append("[old].");
                    createTrigger.Append(quotedColumn);
                    createTrigger.Append(", ");
                    createTrigger.Append("[new].");
                    createTrigger.Append(quotedColumn);
                    createTrigger.Append(")");
                    createTrigger.Append(", ");
                    createTrigger.Append("NULLIF(");
                    createTrigger.Append("[new].");
                    createTrigger.Append(quotedColumn);
                    createTrigger.Append(", ");
                    createTrigger.Append("[old].");
                    createTrigger.Append(quotedColumn);
                    createTrigger.Append(")");
                    createTrigger.AppendLine(") IS NOT NULL");

                    or = " OR ";
                }
                createTrigger.AppendLine("\t ) ");
            }

            createTrigger.AppendLine($"; ");


            var stringBuilderArguments = new StringBuilder();
            var stringBuilderArguments2 = new StringBuilder();
            var stringPkAreNull = new StringBuilder();
            string argComma = string.Empty;
            string argAnd = string.Empty;

            createTrigger.AppendLine($"\tINSERT OR IGNORE INTO {TrackingTableName.Quoted().ToString()} (");
            foreach (var mutableColumn in this.TableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
            {
                var columnName = ParserName.Parse(mutableColumn).Quoted().ToString();

                stringBuilderArguments.AppendLine($"\t\t{argComma}{columnName}");
                stringBuilderArguments2.AppendLine($"\t\t{argComma}new.{columnName}");
                stringPkAreNull.Append($"{argAnd}{TrackingTableName.Quoted().ToString()}.{columnName} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }

            createTrigger.Append(stringBuilderArguments.ToString());
            createTrigger.AppendLine("\t\t,[update_scope_id]");
            createTrigger.AppendLine("\t\t,[timestamp]");
            createTrigger.AppendLine("\t\t,[sync_row_is_tombstone]");
            createTrigger.AppendLine("\t\t,[last_change_datetime]");

            createTrigger.AppendLine("\t) ");
            createTrigger.AppendLine("\tVALUES (");
            createTrigger.Append(stringBuilderArguments2.ToString());
            createTrigger.AppendLine("\t\t,NULL");
            createTrigger.AppendLine($"\t\t,{SqliteObjectNames.TimestampValue}");
            createTrigger.AppendLine("\t\t,0");
            createTrigger.AppendLine("\t\t,datetime('now')");
            createTrigger.AppendLine("\t);");

            createTrigger.AppendLine($"End; ");

            return new SqliteCommand(createTrigger.ToString(), (SqliteConnection)connection, (SqliteTransaction)transaction);
        }

        public override Task<DbCommand> GetCreateTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
        {
            return triggerType switch
            {
                DbTriggerType.Insert => Task.FromResult(CreateInsertTriggerCommand(connection, transaction)),
                DbTriggerType.Update => Task.FromResult(CreateUpdateTriggerCommand(connection, transaction)),
                DbTriggerType.Delete => Task.FromResult(CreateDeleteTriggerCommand(connection, transaction)),
                _ => throw new NotImplementedException("This trigger type is not supported when creating the sqlite trigger")
            };
        }

        public override Task<DbCommand> GetDropTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
        {
            var triggerNameString = string.Format(this.sqliteObjectNames.GetTriggerCommandName(triggerType), this.TableDescription.GetFilter());

            var triggerName = ParserName.Parse(triggerNameString).ToString();

            DbCommand dbCommand = connection.CreateCommand();
            dbCommand.CommandText = $"drop trigger if exist {triggerName}";

            return Task.FromResult(dbCommand);

        }

        public override async Task<IEnumerable<SyncColumn>> GetColumnsAsync(DbConnection connection, DbTransaction transaction)
        {
            var columns = new List<SyncColumn>();
            // Get the columns definition
            var columnsList = await SqliteManagementUtils.GetColumnsForTableAsync(connection as SqliteConnection, transaction as SqliteTransaction, 
                                                                                  this.TableName.Unquoted().ToString());
            var sqlDbMetadata = new SqliteDbMetadata();

            foreach (var c in columnsList.Rows.OrderBy(r => Convert.ToInt32(r["cid"])))
            {
                var typeName = c["type"].ToString();
                var name = c["name"].ToString();

                // Gets the datastore owner dbType 
                var datastoreDbType = (SqliteType)sqlDbMetadata.ValidateOwnerDbType(typeName, false, false, 0);

                // once we have the datastore type, we can have the managed type
                var columnType = sqlDbMetadata.ValidateType(datastoreDbType);

                var sColumn = new SyncColumn(name, columnType);
                sColumn.OriginalDbType = datastoreDbType.ToString();
                sColumn.Ordinal = Convert.ToInt32(c["cid"]);
                sColumn.OriginalTypeName = c["type"].ToString();
                sColumn.AllowDBNull = !Convert.ToBoolean(c["notnull"]);
                sColumn.DefaultValue = c["dflt_value"].ToString();

                // No unsigned type in SQL Server
                sColumn.IsUnsigned = false;

                columns.Add(sColumn);
            }

            return columns;
        }

        public override async Task<IEnumerable<DbRelationDefinition>> GetRelationsAsync(DbConnection connection, DbTransaction transaction)
        {

            var relations = new List<DbRelationDefinition>();
            var relationsTable = await SqliteManagementUtils.GetRelationsForTableAsync(connection as SqliteConnection, transaction as SqliteTransaction, 
                                                                                       this.TableName.Unquoted().ToString());

            if (relationsTable != null && relationsTable.Rows.Count > 0)
            {

                foreach (var fk in relationsTable.Rows.GroupBy(row =>
                new
                {
                    Name = row["id"].ToString(),
                    TableName = this.TableName.Quoted().ToString(),
                    ReferenceTableName = (string)row["table"],
                }))
                {
                    var relationDefinition = new DbRelationDefinition()
                    {
                        ForeignKey = fk.Key.Name,
                        TableName = fk.Key.TableName,
                        ReferenceTableName = fk.Key.ReferenceTableName,
                    };

                    relationDefinition.Columns.AddRange(fk.Select(dmRow =>
                       new DbRelationColumnDefinition
                       {
                           KeyColumnName = dmRow["from"].ToString(),
                           ReferenceColumnName = dmRow["to"].ToString(),
                           Order = Convert.ToInt32(dmRow["seq"])
                       }));

                    relations.Add(relationDefinition);
                }
            }

            return relations.OrderBy(t => t.ForeignKey).ToArray();
        }

        public override async Task<IEnumerable<SyncColumn>> GetPrimaryKeysAsync(DbConnection connection, DbTransaction transaction)
        {
            var keys = await SqliteManagementUtils.GetPrimaryKeysForTableAsync(connection as SqliteConnection, transaction as SqliteTransaction, 
                this.TableName.Unquoted().ToString());

            var lstKeys = new List<SyncColumn>();

            foreach (var key in keys.Rows)
            {
                var keyColumn = SyncColumn.Create<string>((string)key["name"]);
                keyColumn.Ordinal = Convert.ToInt32(key["cid"]);
                lstKeys.Add(keyColumn);
            }

            return lstKeys;

        }
    }
}
