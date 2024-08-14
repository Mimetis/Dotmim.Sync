using Dotmim.Sync.Builders;
using Dotmim.Sync.DatabaseStringParsers;
using Dotmim.Sync.Manager;
using Microsoft.Data.Sqlite;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dotmim.Sync.Sqlite
{

    /// <summary>
    /// The SqlBuilder class is the Sql implementation of DbBuilder class.
    /// In charge of creating tracking table, stored proc, triggers and adapters.
    /// </summary>
    public class SqliteTableBuilder : DbTableBuilder
    {

        private DbTableNames tableNames;
        private DbTableNames trackingTableNames;

        /// <summary>
        /// Gets the SqliteObjectNames.
        /// </summary>
        public SqliteObjectNames SqliteObjectNames { get; }

        /// <summary>
        /// Gets the SqliteDbMetadata.
        /// </summary>
        public SqliteDbMetadata SqliteDbMetadata { get; }

        /// <inheritdoc cref="SqliteTableBuilder" />
        public SqliteTableBuilder(SyncTable tableDescription, ScopeInfo scopeInfo, bool disableSqlFiltersGeneration)
            : base(tableDescription, scopeInfo)
        {
            this.SqliteObjectNames = new SqliteObjectNames(tableDescription, scopeInfo, disableSqlFiltersGeneration);

            this.tableNames = new DbTableNames(
                SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote,
                this.SqliteObjectNames.TableName,
                this.SqliteObjectNames.TableNormalizedFullName,
                this.SqliteObjectNames.TableNormalizedShortName,
                this.SqliteObjectNames.TableQuotedFullName,
                this.SqliteObjectNames.TableQuotedShortName,
                this.SqliteObjectNames.TableSchemaName);

            this.trackingTableNames = new DbTableNames(
                SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote,
                this.SqliteObjectNames.TrackingTableName,
                this.SqliteObjectNames.TrackingTableNormalizedFullName,
                this.SqliteObjectNames.TrackingTableNormalizedShortName,
                this.SqliteObjectNames.TrackingTableQuotedFullName,
                this.SqliteObjectNames.TrackingTableQuotedShortName,
                this.SqliteObjectNames.TrackingTableSchemaName);

            this.SqliteDbMetadata = new SqliteDbMetadata();
        }

        /// <inheritdoc />
        public override DbColumnNames GetParsedColumnNames(SyncColumn column)
        {
            var columnParser = new ObjectParser(column.ColumnName, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote);
            return new(columnParser.QuotedShortName, columnParser.NormalizedShortName);
        }

        /// <inheritdoc />
        public override DbTableNames GetParsedTableNames() => this.tableNames;

        /// <inheritdoc />
        public override DbTableNames GetParsedTrackingTableNames() => this.trackingTableNames;

        private string BuildTableCommandText()
        {
            var stringBuilder = new StringBuilder($"CREATE TABLE IF NOT EXISTS {this.tableNames.QuotedName} (");
            string empty = string.Empty;
            stringBuilder.AppendLine();
            foreach (var column in this.TableDescription.Columns)
            {
                var columnParser = new ObjectParser(column.ColumnName, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote);

                var columnType = this.SqliteDbMetadata.GetCompatibleColumnTypeDeclarationString(column, this.TableDescription.OriginalProvider);

                // check case
                string casesensitive = string.Empty;
                if (this.SqliteDbMetadata.IsTextType(column))
                {
                    casesensitive = SyncGlobalization.IsCaseSensitive() ? string.Empty : "COLLATE NOCASE";

                    // check if it's a primary key, then, even if it's case sensitive, we turn on case insensitive
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

                    // identity = $"AUTOINCREMENT";
                    // Actually no need to set AutoIncrement, if we insert a null value
                    identity = string.Empty;
                }

                var nullString = column.AllowDBNull ? "NULL" : "NOT NULL";

                // if auto inc, don't specify NOT NULL option, since we need to insert a null value to make it auto inc.
                if (column.IsAutoIncrement)
                    nullString = string.Empty;

                // if it's a readonly column, it could be a computed column, so we need to allow null
                else if (column.IsReadOnly)
                    nullString = "NULL";

                stringBuilder.AppendLine($"\t{empty}{columnParser.QuotedShortName} {columnType} {identity} {nullString} {casesensitive}");
                empty = ",";
            }

            stringBuilder.Append("\t,PRIMARY KEY (");
            for (int i = 0; i < this.TableDescription.PrimaryKeys.Count; i++)
            {
                var pkColumn = this.TableDescription.PrimaryKeys[i];
                var columnParser = new ObjectParser(pkColumn, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote);

                stringBuilder.Append(columnParser.QuotedShortName);

                if (i < this.TableDescription.PrimaryKeys.Count - 1)
                    stringBuilder.Append(", ");
            }

            stringBuilder.Append(")");

            // Constraints
            foreach (var constraint in this.TableDescription.GetRelations())
            {
                // Don't want foreign key on same table since it could be a problem on first
                // sync. We are not sure that parent row will be inserted in first position
                // if (constraint.GetParentTable().EqualsByName(constraint.GetTable()))
                //    continue;
                var parentTable = constraint.GetParentTable();
                var parentParser = new TableParser(parentTable.TableName, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote);

                stringBuilder.AppendLine();
                stringBuilder.Append($"\tFOREIGN KEY (");
                empty = string.Empty;
                foreach (var column in constraint.Keys)
                {
                    var columnParser = new ObjectParser(column.ColumnName, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote);
                    stringBuilder.Append($"{empty} {columnParser.QuotedShortName}");
                    empty = ", ";
                }

                stringBuilder.Append($") ");
                stringBuilder.Append($"REFERENCES {parentParser.QuotedShortName}(");
                empty = string.Empty;
                foreach (var column in constraint.ParentKeys)
                {
                    var columnParser = new ObjectParser(column.ColumnName, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote);
                    stringBuilder.Append($"{empty} {columnParser.QuotedShortName}");
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
            stringBuilder.AppendLine($"CREATE TABLE IF NOT EXISTS {this.trackingTableNames.QuotedName} (");

            // Adding the primary key
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(pkColumn.ColumnName, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote);
                var columnType = this.SqliteDbMetadata.GetCompatibleColumnTypeDeclarationString(pkColumn, this.TableDescription.OriginalProvider);
                stringBuilder.AppendLine($"{columnParser.QuotedShortName} {columnType} NOT NULL COLLATE NOCASE, ");
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
                var columnParser = new ObjectParser(pkColumn, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote);

                stringBuilder.Append(columnParser.QuotedShortName);

                if (i < this.TableDescription.PrimaryKeys.Count - 1)
                    stringBuilder.Append(", ");
            }

            stringBuilder.Append(")");

            stringBuilder.Append(");");

            stringBuilder.AppendLine($"CREATE INDEX IF NOT EXISTS [{this.trackingTableNames.NormalizedName}_timestamp_index] ON {this.trackingTableNames.QuotedName} (");
            stringBuilder.AppendLine($"\t [timestamp] ASC");
            stringBuilder.AppendLine($"\t,[update_scope_id] ASC");
            stringBuilder.AppendLine($"\t,[sync_row_is_tombstone] ASC");
            foreach (var pkColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(pkColumn.ColumnName, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote);
                stringBuilder.AppendLine($"\t,{columnParser.QuotedShortName} ASC");
            }

            stringBuilder.Append(");");
            return stringBuilder.ToString();
        }

        /// <inheritdoc />
        public override Task<DbCommand> GetCreateSchemaCommandAsync(DbConnection connection, DbTransaction transaction) => Task.FromResult<DbCommand>(null);

        /// <inheritdoc />
        public override Task<DbCommand> GetCreateTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var commandText = this.BuildTableCommandText();

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;

            command.CommandText = commandText;

            return Task.FromResult(command);
        }

        /// <inheritdoc />
        public override Task<DbCommand> GetExistsTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();

            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = "select count(*) from sqlite_master where name = @tableName AND type='table'";

            var parameter = command.CreateParameter();
            parameter.ParameterName = "@tableName";
            parameter.Value = this.tableNames.Name;
            command.Parameters.Add(parameter);

            return Task.FromResult(command);
        }

        /// <inheritdoc />
        public override Task<DbCommand> GetExistsSchemaCommandAsync(DbConnection connection, DbTransaction transaction) => Task.FromResult<DbCommand>(null);

        /// <inheritdoc />
        public override Task<DbCommand> GetDropTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();

            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = $"drop table if exists {this.tableNames.QuotedName}";

            return Task.FromResult(command);
        }

        /// <inheritdoc />
        public override Task<DbCommand> GetExistsStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
            => Task.FromResult<DbCommand>(null);

        /// <inheritdoc />
        public override Task<DbCommand> GetCreateStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
            => Task.FromResult<DbCommand>(null);

        /// <inheritdoc />
        public override Task<DbCommand> GetDropStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
            => Task.FromResult<DbCommand>(null);

        /// <inheritdoc />
        public override Task<DbCommand> GetCreateTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var commandText = this.BuildTrackingTableCommandText();

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;

            command.CommandText = commandText;

            return Task.FromResult(command);
        }

        /// <inheritdoc />
        public override Task<DbCommand> GetDropTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();

            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = $"drop table if exists {this.trackingTableNames.QuotedName}";

            return Task.FromResult(command);
        }

        /// <inheritdoc />
        public override Task<DbCommand> GetExistsTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
        {
            var command = connection.CreateCommand();

            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = "select count(*) from sqlite_master where name = @tableName AND type='table'";

            var parameter = command.CreateParameter();
            parameter.ParameterName = "@tableName";
            parameter.Value = this.trackingTableNames.Name;
            command.Parameters.Add(parameter);

            return Task.FromResult(command);
        }

        /// <inheritdoc />
        public override Task<DbCommand> GetExistsTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
        {
            var triggerName = this.SqliteObjectNames.GetTriggerCommandName(triggerType);
            var triggerParser = new ObjectParser(triggerName, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote);

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = "select count(*) from sqlite_master where name = @triggerName AND type='trigger'";

            var parameter = command.CreateParameter();
            parameter.ParameterName = "@triggerName";
            parameter.Value = triggerParser.ObjectName;
            command.Parameters.Add(parameter);

            return Task.FromResult(command);
        }

        private SqliteCommand CreateInsertTriggerCommand(DbConnection connection, DbTransaction transaction)
        {
            var insTriggerName = this.SqliteObjectNames.GetTriggerCommandName(DbTriggerType.Insert);

            StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER IF NOT EXISTS {insTriggerName} AFTER INSERT ON {this.tableNames.QuotedName} ");
            createTrigger.AppendLine();

            var stringBuilderArguments = new StringBuilder();
            var stringBuilderArguments2 = new StringBuilder();
            var stringPkAreNull = new StringBuilder();
            string argComma = string.Empty;
            string argAnd = string.Empty;

            createTrigger.AppendLine();
            createTrigger.AppendLine("BEGIN");
            createTrigger.AppendLine("-- If row was deleted before, it already exists, so just make an update");

            createTrigger.AppendLine($"\tINSERT OR REPLACE INTO {this.trackingTableNames.QuotedName} (");
            foreach (var mutableColumn in this.TableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
            {
                var columnParser = new ObjectParser(mutableColumn.ColumnName, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote);

                stringBuilderArguments.AppendLine($"\t\t{argComma}{columnParser.QuotedShortName}");
                stringBuilderArguments2.AppendLine($"\t\t{argComma}new.{columnParser.QuotedShortName}");
                stringPkAreNull.Append($"{argAnd}{this.trackingTableNames.QuotedName}.{columnParser.QuotedShortName} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }

            createTrigger.Append(stringBuilderArguments);
            createTrigger.AppendLine("\t\t,[update_scope_id]");
            createTrigger.AppendLine("\t\t,[timestamp]");
            createTrigger.AppendLine("\t\t,[sync_row_is_tombstone]");
            createTrigger.AppendLine("\t\t,[last_change_datetime]");

            createTrigger.AppendLine("\t) ");
            createTrigger.AppendLine("\tVALUES (");
            createTrigger.Append(stringBuilderArguments2);
            createTrigger.AppendLine("\t\t,NULL");
            createTrigger.AppendLine($"\t\t,{SqliteObjectNames.TimestampValue}");
            createTrigger.AppendLine("\t\t,0");
            createTrigger.AppendLine("\t\t,datetime('now')");
            createTrigger.AppendLine("\t);");
            createTrigger.AppendLine("END;");

            return new SqliteCommand(createTrigger.ToString(), (SqliteConnection)connection, (SqliteTransaction)transaction);
        }

        private SqliteCommand CreateDeleteTriggerCommand(DbConnection connection, DbTransaction transaction)
        {
            var delTriggerName = this.SqliteObjectNames.GetTriggerCommandName(DbTriggerType.Delete);

            StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER IF NOT EXISTS {delTriggerName} AFTER DELETE ON {this.tableNames.QuotedName} ");
            createTrigger.AppendLine();

            var stringBuilderArguments = new StringBuilder();
            var stringBuilderArguments2 = new StringBuilder();
            var stringPkAreNull = new StringBuilder();
            string argComma = string.Empty;
            string argAnd = string.Empty;

            createTrigger.AppendLine();
            createTrigger.AppendLine("BEGIN");

            createTrigger.AppendLine($"\tINSERT OR REPLACE INTO {this.trackingTableNames.QuotedName} (");
            foreach (var mutableColumn in this.TableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
            {
                var columnParser = new ObjectParser(mutableColumn.ColumnName, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote);

                stringBuilderArguments.AppendLine($"\t\t{argComma}{columnParser.QuotedShortName}");
                stringBuilderArguments2.AppendLine($"\t\t{argComma}old.{columnParser.QuotedShortName}");
                stringPkAreNull.Append($"{argAnd}{this.trackingTableNames.QuotedName}.{columnParser.QuotedShortName} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }

            createTrigger.Append(stringBuilderArguments);
            createTrigger.AppendLine("\t\t,[update_scope_id]");
            createTrigger.AppendLine("\t\t,[timestamp]");
            createTrigger.AppendLine("\t\t,[sync_row_is_tombstone]");
            createTrigger.AppendLine("\t\t,[last_change_datetime]");

            createTrigger.AppendLine("\t) ");
            createTrigger.AppendLine("\tVALUES (");
            createTrigger.Append(stringBuilderArguments2);
            createTrigger.AppendLine("\t\t,NULL");
            createTrigger.AppendLine($"\t\t,{SqliteObjectNames.TimestampValue}");
            createTrigger.AppendLine("\t\t,1");
            createTrigger.AppendLine("\t\t,datetime('now')");
            createTrigger.AppendLine("\t);");
            createTrigger.AppendLine("END;");

            return new SqliteCommand(createTrigger.ToString(), (SqliteConnection)connection, (SqliteTransaction)transaction);
        }

        private SqliteCommand CreateUpdateTriggerCommand(DbConnection connection, DbTransaction transaction)
        {
            var updTriggerName = this.SqliteObjectNames.GetTriggerCommandName(DbTriggerType.Update);

            StringBuilder createTrigger = new StringBuilder($"CREATE TRIGGER IF NOT EXISTS {updTriggerName} AFTER UPDATE ON {this.tableNames.QuotedName} ");
            createTrigger.AppendLine();

            createTrigger.AppendLine();
            createTrigger.AppendLine($"Begin ");

            createTrigger.AppendLine($"\tUPDATE {this.trackingTableNames.QuotedName} ");
            createTrigger.AppendLine("\tSET [update_scope_id] = NULL -- scope id is always NULL when update is made locally");
            createTrigger.AppendLine($"\t\t,[timestamp] = {SqliteObjectNames.TimestampValue}");
            createTrigger.AppendLine("\t\t,[last_change_datetime] = datetime('now')");

            createTrigger.Append($"\tWhere ");
            createTrigger.Append(SqliteManagementUtils.JoinTwoTablesOnClause(this.TableDescription.PrimaryKeys, this.trackingTableNames.QuotedName.ToString(), "new"));
            createTrigger.AppendLine($"; ");

            var stringBuilderArguments = new StringBuilder();
            var stringBuilderArguments2 = new StringBuilder();
            var stringPkAreNull = new StringBuilder();
            string argComma = string.Empty;
            string argAnd = string.Empty;

            createTrigger.AppendLine($"\tINSERT OR IGNORE INTO {this.trackingTableNames.QuotedName} (");
            foreach (var mutableColumn in this.TableDescription.GetPrimaryKeysColumns().Where(c => !c.IsReadOnly))
            {
                var columnParser = new ObjectParser(mutableColumn.ColumnName, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote);

                stringBuilderArguments.AppendLine($"\t\t{argComma}{columnParser.QuotedShortName}");
                stringBuilderArguments2.AppendLine($"\t\t{argComma}new.{columnParser.QuotedShortName}");
                stringPkAreNull.Append($"{argAnd}{this.trackingTableNames.QuotedName}.{columnParser.QuotedShortName} IS NULL");
                argComma = ",";
                argAnd = " AND ";
            }

            createTrigger.Append(stringBuilderArguments);
            createTrigger.AppendLine("\t\t,[update_scope_id]");
            createTrigger.AppendLine("\t\t,[timestamp]");
            createTrigger.AppendLine("\t\t,[sync_row_is_tombstone]");
            createTrigger.AppendLine("\t\t,[last_change_datetime]");

            createTrigger.AppendLine("\t) ");
            createTrigger.AppendLine("\tSELECT ");
            createTrigger.Append(stringBuilderArguments2);
            createTrigger.AppendLine("\t\t,NULL");
            createTrigger.AppendLine($"\t\t,{SqliteObjectNames.TimestampValue}");
            createTrigger.AppendLine("\t\t,0");
            createTrigger.AppendLine("\t\t,datetime('now')");

            createTrigger.Append($"\tWHERE (SELECT COUNT(*) FROM {this.trackingTableNames.QuotedName} WHERE ");
            var str1 = string.Empty;
            foreach (var pkeyColumn in this.TableDescription.GetPrimaryKeysColumns())
            {
                var columnParser = new ObjectParser(pkeyColumn.ColumnName, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote);
                createTrigger.Append($"{str1}{columnParser.QuotedShortName}=new.{columnParser.QuotedShortName}");
                str1 = " AND ";
            }

            createTrigger.AppendLine(")=0");
            createTrigger.AppendLine($"; ");
            createTrigger.AppendLine($"End; ");

            return new SqliteCommand(createTrigger.ToString(), (SqliteConnection)connection, (SqliteTransaction)transaction);
        }

#pragma warning disable CA2000 // Dispose objects before losing scope
        /// <inheritdoc />
        public override Task<DbCommand> GetCreateTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction) =>

            triggerType switch
            {
                DbTriggerType.Insert => Task.FromResult((DbCommand)this.CreateInsertTriggerCommand(connection, transaction)),
                DbTriggerType.Update => Task.FromResult((DbCommand)this.CreateUpdateTriggerCommand(connection, transaction)),
                DbTriggerType.Delete => Task.FromResult((DbCommand)this.CreateDeleteTriggerCommand(connection, transaction)),
                _ => throw new NotImplementedException("This trigger type is not supported when creating the sqlite trigger"),
            };
#pragma warning restore CA2000 // Dispose objects before losing scope

        /// <inheritdoc />
        public override Task<DbCommand> GetDropTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
        {
            var triggerName = this.SqliteObjectNames.GetTriggerCommandName(triggerType);

            DbCommand dbCommand = connection.CreateCommand();
            dbCommand.CommandText = $"drop trigger if exists {triggerName}";

            return Task.FromResult(dbCommand);
        }

        /// <inheritdoc />
        public override async Task<IEnumerable<SyncColumn>> GetColumnsAsync(DbConnection connection, DbTransaction transaction)
        {
            var columns = new List<SyncColumn>();

            // Get the columns definition
            var columnsList = await SqliteManagementUtils.GetColumnsForTableAsync(
                this.tableNames.Name,
                connection as SqliteConnection, transaction as SqliteTransaction).ConfigureAwait(false);
            var sqlDbMetadata = new SqliteDbMetadata();

            foreach (var c in columnsList.Rows.OrderBy(r => Convert.ToInt32(r["cid"])))
            {
                var typeName = c["type"].ToString();
                var name = c["name"].ToString();

                var sColumn = new SyncColumn(name)
                {
                    OriginalDbType = typeName,
                    Ordinal = Convert.ToInt32(c["cid"]),
                    OriginalTypeName = c["type"].ToString(),
                    AllowDBNull = !Convert.ToBoolean(c["notnull"]),
                    DefaultValue = c["dflt_value"].ToString(),

                    // No unsigned type in SQLite
                    IsUnsigned = false,
                };

                columns.Add(sColumn);
            }

            return columns;
        }

        /// <inheritdoc />
        public override async Task<IEnumerable<DbRelationDefinition>> GetRelationsAsync(DbConnection connection, DbTransaction transaction)
        {

            var relations = new List<DbRelationDefinition>();
            var relationsTable = await SqliteManagementUtils.GetRelationsForTableAsync(connection as SqliteConnection, transaction as SqliteTransaction,
                                                                                       this.tableNames.Name).ConfigureAwait(false);

            if (relationsTable != null && relationsTable.Rows.Count > 0)
            {

                foreach (var fk in relationsTable.Rows.GroupBy(row =>
                new
                {
                    Name = row["id"].ToString(),
                    TableName = this.tableNames.QuotedName,
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
                           Order = Convert.ToInt32(dmRow["seq"]),
                       }));

                    relations.Add(relationDefinition);
                }
            }

            return [.. relations.OrderBy(t => t.ForeignKey)];
        }

        /// <inheritdoc />
        public override async Task<IEnumerable<SyncColumn>> GetPrimaryKeysAsync(DbConnection connection, DbTransaction transaction)
        {
            var keys = await SqliteManagementUtils.GetPrimaryKeysForTableAsync(connection as SqliteConnection, transaction as SqliteTransaction,
                this.tableNames.Name).ConfigureAwait(false);

            var lstKeys = new List<SyncColumn>();

            foreach (var key in keys.Rows)
            {
                var keyColumn = SyncColumn.Create<string>((string)key["name"]);
                keyColumn.Ordinal = Convert.ToInt32(key["cid"]);
                lstKeys.Add(keyColumn);
            }

            return lstKeys;
        }

        /// <inheritdoc />
        public override Task<DbCommand> GetExistsColumnCommandAsync(string columnName, DbConnection connection, DbTransaction transaction)
        {
            var columnParser = new ObjectParser(columnName, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote);

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;
            command.CommandText = $"SELECT count(*) FROM pragma_table_info('{this.tableNames.Name}') WHERE name=@columnName;";

            var parameter = command.CreateParameter();
            parameter.ParameterName = "@columnName";
            parameter.Value = columnParser.ObjectName;
            command.Parameters.Add(parameter);

            return Task.FromResult(command);
        }

        /// <inheritdoc />
        public override Task<DbCommand> GetAddColumnCommandAsync(string columnName, DbConnection connection, DbTransaction transaction)
        {
            var stringBuilder = new StringBuilder($"ALTER TABLE {this.tableNames.QuotedName} ADD COLUMN");

            var command = connection.CreateCommand();
            command.Connection = connection;
            command.Transaction = transaction;

            var column = this.TableDescription.Columns[columnName];
            var columnParser = new ObjectParser(column.ColumnName, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote);

            var columnType = this.SqliteDbMetadata.GetCompatibleColumnTypeDeclarationString(column, this.TableDescription.OriginalProvider);

            // check case
            string casesensitive = string.Empty;
            if (this.SqliteDbMetadata.IsTextType(column))
            {
                casesensitive = SyncGlobalization.IsCaseSensitive() ? string.Empty : "COLLATE NOCASE";

                // check if it's a primary key, then, even if it's case sensitive, we turn on case insensitive
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

                // identity = $"AUTOINCREMENT";
                // Actually no need to set AutoIncrement, if we insert a null value
                identity = string.Empty;
            }

            var nullString = column.AllowDBNull ? "NULL" : "NOT NULL";

            // if auto inc, don't specify NOT NULL option, since we need to insert a null value to make it auto inc.
            if (column.IsAutoIncrement)
                nullString = string.Empty;

            // if it's a readonly column, it could be a computed column, so we need to allow null
            else if (column.IsReadOnly)
                nullString = "NULL";

            stringBuilder.AppendLine($" {columnParser.QuotedShortName} {columnType} {identity} {nullString} {casesensitive};");

            command.CommandText = stringBuilder.ToString();

            return Task.FromResult(command);
        }

        /// <inheritdoc />
        public override Task<DbCommand> GetDropColumnCommandAsync(string columnName, DbConnection connection, DbTransaction transaction)
            => throw new NotImplementedException();
    }
}