using Dotmim.Sync.Enumerations;
using Dotmim.Sync.Manager;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using Dotmim.Sync.Builders;
using System.Diagnostics;
using System.ComponentModel;
using System.Threading.Tasks;

using System.Reflection;
using System.Collections.Concurrent;
using System.Text;
using System.Threading;

namespace Dotmim.Sync
{
    /// <summary>
    /// The SyncAdapter is the datasource manager for ONE table
    /// Should be implemented by every database provider and provide every SQL action
    /// </summary>
    public abstract class DbSyncAdapter
    {

        /// <summary>
        /// Gets the table description
        /// </summary>
        public SyncTable TableDescription { get; private set; }

        /// <summary>
        /// Gets the setup used 
        /// </summary>
        public SyncSetup Setup { get; }

        /// <summary>
        /// Gets the scope name
        /// </summary>
        public string ScopeName { get; }

        /// <summary>
        /// For provider supporting it, set if we are using bulk operations or not.
        /// </summary>
        public bool UseBulkOperations { get; set; }


        /// <summary>
        /// Get or Sets the prefix to use for Parameters
        /// </summary>
        public virtual string ParameterPrefix { get; } = "@";

        /// <summary>
        /// Gets or Sets the escape character to use for quoted identifiers on left side
        /// </summary>
        public virtual string QuotePrefix { get; } = "[";

        /// <summary>
        /// Gets or Sets the escape character to use for quoted identifiers on right side
        /// </summary>
        public virtual string QuoteSuffix { get; } = "]";

        /// <summary>
        /// Gets or Sets a value that indicates if provider supports output parameters
        /// </summary>
        public virtual bool SupportsOutputParameters { get; } = true;

        /// <summary>
        /// Create a Sync Adapter
        /// </summary>
        public DbSyncAdapter(SyncTable tableDescription, SyncSetup setup, string scopeName, bool useBulkOperation = false)
        {
            this.TableDescription = tableDescription;
            this.Setup = setup;
            this.ScopeName = scopeName;
            this.UseBulkOperations = useBulkOperation;
        }

        //public abstract object GetProviderDbType(SyncColumn column, DbCommand command, DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Gets a command from the current table in the adapter
        /// </summary>
        public abstract (DbCommand Command, bool IsBatchCommand) GetCommand(DbCommandType commandType, SyncFilter filter = null);

        /// <summary>
        /// Adding a parameter value to a command
        /// </summary>
        public virtual void AddCommandParameterValue(DbParameter parameter, object value, DbCommand command, DbCommandType commandType)
        {
            if (value == null || value == DBNull.Value)
                parameter.Value = DBNull.Value;
            else
            {
                var convValue = SyncTypeConverter.TryConvertFromDbType(value, parameter.DbType);
                parameter.Value = convValue;
            }
        }

        /// <summary>
        /// Parameters have been added to command.
        /// Ensure all parameters are correct from the provider perspective
        /// </summary>
        public virtual DbCommand EnsureCommandParameters(DbCommand command, DbCommandType commandType, DbConnection connection, DbTransaction transaction, SyncFilter filter = null)
            => command;

        /// <summary>
        /// Parameters values have been added to command
        /// Ensure all values are correct from the provider perspective
        /// </summary>
        public virtual DbCommand EnsureCommandParametersValues(DbCommand command, DbCommandType commandType, DbConnection connection, DbTransaction transaction)
            => command;

        /// <summary>
        /// Get a parameter even if it's a @param or :param or in_param
        /// </summary>
        public virtual DbParameter GetParameter(DbCommand command, string parameterName) => InternalGetParameter(command, parameterName);

        /// <summary>
        /// Get a parameter even if it's a @param or :param or param
        /// </summary>
        internal static DbParameter InternalGetParameter(DbCommand command, string parameterName)
        {
            if (command == null || command.Parameters == null || command.Parameters.Count <= 0)
                return null;

            if (command.Parameters.Contains($"@{parameterName}"))
                return command.Parameters[$"@{parameterName}"];

            if (command.Parameters.Contains($":{parameterName}"))
                return command.Parameters[$":{parameterName}"];

            if (command.Parameters.Contains($"in_{parameterName}"))
                return command.Parameters[$"in_{parameterName}"];

            if (!command.Parameters.Contains(parameterName))
                return null;

            return command.Parameters[parameterName];
        }


        /// <summary>
        /// Execute a batch command
        /// </summary>
        public abstract Task ExecuteBatchCommandAsync(DbCommand cmd, Guid senderScopeId, IEnumerable<SyncRow> arrayItems, SyncTable schemaChangesTable,
                                                      SyncTable failedRows, long? lastTimestamp, DbConnection connection, DbTransaction transaction);



    }
}
