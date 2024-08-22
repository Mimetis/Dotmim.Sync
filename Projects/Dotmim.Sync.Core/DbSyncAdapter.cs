using Dotmim.Sync.Builders;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync
{
    /// <summary>
    /// The SyncAdapter is the datasource manager for ONE table
    /// Should be implemented by every database provider and provide every SQL action.
    /// </summary>
    public abstract class DbSyncAdapter
    {
        /// <summary>
        /// Gets the table description.
        /// </summary>
        public SyncTable TableDescription { get; private set; }

        /// <summary>
        /// Gets the Scope Info.
        /// </summary>
        public ScopeInfo ScopeInfo { get; }

        /// <summary>
        /// Gets or sets a value indicating whether for provider supporting it, set if we are using bulk operations or not.
        /// </summary>
        public bool UseBulkOperations { get; set; }

        /// <summary>
        /// Gets get or Sets the prefix to use for Parameters.
        /// </summary>
        public virtual string ParameterPrefix { get; } = "@";

        /// <summary>
        /// Gets a value indicating whether gets or Sets a value that indicates if provider supports output parameters.
        /// </summary>
        public virtual bool SupportsOutputParameters { get; } = true;

        /// <summary>
        /// Initializes a new instance of the <see cref="DbSyncAdapter"/> class.
        /// Create a Sync Adapter.
        /// </summary>
        protected DbSyncAdapter(SyncTable tableDescription, ScopeInfo scopeInfo, bool useBulkOperation = false)
        {
            this.TableDescription = tableDescription;
            this.ScopeInfo = scopeInfo;
            this.UseBulkOperations = useBulkOperation;
        }

        /// <summary>
        /// Gets the quoted name and normalized name for a string.
        /// </summary>
        public abstract DbColumnNames GetParsedColumnNames(string name);

        /// <summary>
        /// Gets the table builder for the current adapter.
        /// </summary>
        public abstract DbTableBuilder GetTableBuilder();

        /// <summary>
        /// Gets a command from the current table in the adapter.
        /// </summary>
        public abstract (DbCommand Command, bool IsBatchCommand) GetCommand(SyncContext context, DbCommandType commandType, SyncFilter filter = null);

        /// <summary>
        /// Execute a batch command.
        /// </summary>
        public abstract Task ExecuteBatchCommandAsync(SyncContext context, DbCommand cmd, Guid senderScopeId, IEnumerable<SyncRow> arrayItems, SyncTable schemaChangesTable,
                                                      SyncTable failedRows, long? lastTimestamp, DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Adding a parameter value to a command.
        /// </summary>
        public virtual void AddCommandParameterValue(SyncContext context, DbParameter parameter, object value, DbCommand command, DbCommandType commandType)
        {
            if (value == null || value == DBNull.Value)
            {
                parameter.Value = DBNull.Value;
            }
            else
            {
                var convValue = SyncTypeConverter.TryConvertFromDbType(value, parameter.DbType);
                parameter.Value = convValue;
            }
        }

        /// <summary>
        /// Parameters have been added to command.
        /// Ensure all parameters are correct from the provider perspective.
        /// </summary>
        public virtual DbCommand EnsureCommandParameters(SyncContext context, DbCommand command, DbCommandType commandType, DbConnection connection, DbTransaction transaction, SyncFilter filter = null)
            => command;

        /// <summary>
        /// Parameters values have been added to command
        /// Ensure all values are correct from the provider perspective.
        /// </summary>
        public virtual DbCommand EnsureCommandParametersValues(SyncContext context, DbCommand command, DbCommandType commandType, DbConnection connection, DbTransaction transaction)
            => command;

        /// <summary>
        /// Get a parameter even if it's a @param or :param or in_param.
        /// </summary>
        public virtual DbParameter GetParameter(SyncContext context, DbCommand command, string parameterName) => InternalGetParameter(command, parameterName);

        /// <summary>
        /// Get a parameter even if it's a @param or :param or param.
        /// </summary>
        internal static DbParameter InternalGetParameter(DbCommand command, string parameterName)
            => command == null || command.Parameters == null || command.Parameters.Count <= 0
                ? null
                : command.Parameters.Contains($"@{parameterName}")
                ? command.Parameters[$"@{parameterName}"]
                : command.Parameters.Contains($":{parameterName}")
                ? command.Parameters[$":{parameterName}"]
                : command.Parameters.Contains($"in_{parameterName}")
                ? command.Parameters[$"in_{parameterName}"]
                : !command.Parameters.Contains(parameterName) ? null : command.Parameters[parameterName];
    }
}