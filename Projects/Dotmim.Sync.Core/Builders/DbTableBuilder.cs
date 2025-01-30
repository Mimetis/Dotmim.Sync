﻿using Dotmim.Sync.Manager;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading.Tasks;

namespace Dotmim.Sync.Builders
{
    /// <summary>
    /// Table builder for a database provider.
    /// </summary>
    public abstract partial class DbTableBuilder
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DbTableBuilder"/> class.
        /// Construct a DbBuilder.
        /// </summary>
        protected DbTableBuilder(SyncTable tableDescription, ScopeInfo scopeInfo)
        {
            this.TableDescription = tableDescription;
            this.ScopeInfo = scopeInfo;
        }

        /// <summary>
        /// Gets or sets the table description for the current DbBuilder.
        /// </summary>
        public SyncTable TableDescription { get; set; }

        /// <summary>
        /// Gets the scope information.
        /// </summary>
        public ScopeInfo ScopeInfo { get; }

        /// <summary>
        /// Gets the parsed name of the table.
        /// </summary>
        public abstract DbTableNames GetParsedTableNames();

        /// <summary>
        /// Gets the parsed name of the table.
        /// </summary>
        public abstract DbTableNames GetParsedTrackingTableNames();

        /// <summary>
        /// Gets the parsed columns names.
        /// </summary>
        public abstract DbColumnNames GetParsedColumnNames(SyncColumn column);

        /// <summary>
        /// Returns a command to create a schema.
        /// </summary>
        public abstract Task<DbCommand> GetCreateSchemaCommandAsync(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to create a table.
        /// </summary>
        public abstract Task<DbCommand> GetCreateTableCommandAsync(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to check if a table exists.
        /// </summary>
        public abstract Task<DbCommand> GetExistsTableCommandAsync(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to check if a schema exists.
        /// </summary>
        public abstract Task<DbCommand> GetExistsSchemaCommandAsync(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to drop a table.
        /// </summary>
        public abstract Task<DbCommand> GetDropTableCommandAsync(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to drop a schema.
        /// </summary>
        public abstract Task<DbCommand> GetExistsColumnCommandAsync(string columnName, DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to add a new column to a table.
        /// </summary>
        public abstract Task<DbCommand> GetAddColumnCommandAsync(string columnName, DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to drop a column.
        /// </summary>
        public abstract Task<DbCommand> GetDropColumnCommandAsync(string columnName, DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to check if a stored procedure exists.
        /// </summary>
        public abstract Task<DbCommand> GetExistsStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to create a stored procedure.
        /// </summary>
        public abstract Task<DbCommand> GetCreateStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to drop a stored procedure.
        /// </summary>
        public abstract Task<DbCommand> GetDropStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to create a tracking table.
        /// </summary>
        public abstract Task<DbCommand> GetCreateTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to drop a tracking table.
        /// </summary>
        public abstract Task<DbCommand> GetDropTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to check if a tracking table exists.
        /// </summary>
        public abstract Task<DbCommand> GetExistsTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to check if a trigger exists.
        /// </summary>
        public abstract Task<DbCommand> GetExistsTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to create a trigger.
        /// </summary>
        public abstract Task<DbCommand> GetCreateTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Returns a command to drop a trigger.
        /// </summary>
        public abstract Task<DbCommand> GetDropTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Gets a columns list from the datastore.
        /// </summary>
        public abstract Task<IEnumerable<SyncColumn>> GetColumnsAsync(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Gets all relations from a current table. If composite, must be ordered.
        /// </summary>
        public abstract Task<IEnumerable<DbRelationDefinition>> GetRelationsAsync(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Get all primary keys. If composite, must be ordered.
        /// </summary>
        public abstract Task<IEnumerable<SyncColumn>> GetPrimaryKeysAsync(DbConnection connection, DbTransaction transaction);
    }
}