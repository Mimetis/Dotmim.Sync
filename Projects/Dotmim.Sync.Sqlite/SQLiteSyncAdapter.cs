using Dotmim.Sync.Builders;
using Dotmim.Sync.DatabaseStringParsers;
using Microsoft.Data.Sqlite;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync.Sqlite
{
    /// <summary>
    /// Sqlite sync adapter.
    /// </summary>
    public class SqliteSyncAdapter : DbSyncAdapter
    {
        private bool disableSqlFiltersGeneration;

        /// <inheritdoc />
        public override bool SupportsOutputParameters => false;

        /// <summary>
        /// Gets or sets the SqliteObjectNames.
        /// </summary>
        public SqliteObjectNames SqliteObjectNames { get; set; }

        /// <inheritdoc cref="SqliteSyncAdapter"/>
        public SqliteSyncAdapter(SyncTable tableDescription, ScopeInfo scopeInfo, bool disableSqlFiltersGeneration)
            : base(tableDescription, scopeInfo)
        {

            this.SqliteObjectNames = new SqliteObjectNames(this.TableDescription, scopeInfo, disableSqlFiltersGeneration);
            this.disableSqlFiltersGeneration = disableSqlFiltersGeneration;
        }

        /// <inheritdoc />
        public override DbColumnNames GetParsedColumnNames(string name)
        {
            var columnParser = new ObjectParser(name, SqliteObjectNames.LeftQuote, SqliteObjectNames.RightQuote);
            return new DbColumnNames(columnParser.QuotedShortName, columnParser.NormalizedShortName);
        }

        /// <inheritdoc />
        public override DbTableBuilder GetTableBuilder() => new SqliteTableBuilder(this.TableDescription, this.ScopeInfo, this.disableSqlFiltersGeneration);

        /// <inheritdoc />
        public override (DbCommand, bool) GetCommand(SyncContext context, DbCommandType commandType, SyncFilter filter = null)
        {
            var command = new SqliteCommand();
            string text;
            text = this.SqliteObjectNames.GetCommandName(commandType, filter);

            // on Sqlite, everything is text :)
            command.CommandType = CommandType.Text;
            command.CommandText = text;

            return (command, false);
        }

        /// <inheritdoc />
        public override void AddCommandParameterValue(SyncContext context, DbParameter parameter, object value, DbCommand command, DbCommandType commandType)
            => parameter.Value = value == null || value == DBNull.Value ? DBNull.Value : SyncTypeConverter.TryConvertFromDbType(value, parameter.DbType);

        /// <inheritdoc />
        public override DbCommand EnsureCommandParametersValues(SyncContext context, DbCommand command, DbCommandType commandType, DbConnection connection, DbTransaction transaction)
            => command;

        /// <inheritdoc />
        public override Task ExecuteBatchCommandAsync(SyncContext context, DbCommand cmd, Guid senderScopeId, IEnumerable<SyncRow> arrayItems, SyncTable schemaChangesTable, SyncTable failedRows, long? lastTimestamp, DbConnection connection, DbTransaction transaction = null)
            => throw new NotImplementedException();
    }
}