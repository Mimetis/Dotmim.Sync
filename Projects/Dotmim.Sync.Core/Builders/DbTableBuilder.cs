
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using System.Linq;
using System.Data;
using System.Diagnostics;
using Dotmim.Sync.Enumerations;
using System.Threading.Tasks;
using Dotmim.Sync.Manager;

namespace Dotmim.Sync.Builders
{
    public abstract partial class DbTableBuilder
    {
        public abstract IEnumerable<DbTriggerType> SupportedTriggers { get; }

        /// <summary>
        /// Gets the table description for the current DbBuilder
        /// </summary>
        public SyncTable TableDescription { get; set; }

        /// <summary>
        /// Gets or Sets Setup, containing naming prefix and suffix if needed
        /// </summary>
        public SyncSetup Setup { get; }

        /// <summary>
        /// Gets the table parsed name
        /// </summary>
        public ParserName TableName { get; }

        /// <summary>
        /// Gets the tracking table parsed name
        /// </summary>
        public ParserName TrackingTableName { get; }

        /// <summary>
        /// Construct a DbBuilder
        /// </summary>
        public DbTableBuilder(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup)
        {
            this.TableDescription = tableDescription;
            this.Setup = setup;
            this.TableName = tableName;
            this.TrackingTableName = trackingTableName;
        }

        public abstract Task<DbCommand> GetCreateSchemaCommandAsync(DbConnection connection, DbTransaction transaction);
        public abstract Task<DbCommand> GetCreateTableCommandAsync(DbConnection connection, DbTransaction transaction);
        public abstract Task<DbCommand> GetExistsTableCommandAsync(DbConnection connection, DbTransaction transaction);
        public abstract Task<DbCommand> GetExistsSchemaCommandAsync(DbConnection connection, DbTransaction transaction);
        public abstract Task<DbCommand> GetDropTableCommandAsync(DbConnection connection, DbTransaction transaction);
        public abstract Task<DbCommand> GetExistsColumnCommandAsync(string columnName, DbConnection connection, DbTransaction transaction);
        public abstract Task<DbCommand> GetAddColumnCommandAsync(string columnName, DbConnection connection, DbTransaction transaction);
        public abstract Task<DbCommand> GetDropColumnCommandAsync(string columnName, DbConnection connection, DbTransaction transaction);

        public abstract Task<DbCommand> GetExistsStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction);
        public abstract Task<DbCommand> GetCreateStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction);
        public abstract Task<DbCommand> GetDropStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction);

        public abstract Task<DbCommand> GetCreateTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction);
        public abstract Task<DbCommand> GetDropTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction);
        public abstract Task<DbCommand> GetRenameTrackingTableCommandAsync(ParserName oldTableName, DbConnection connection, DbTransaction transaction);
        public abstract Task<DbCommand> GetExistsTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction);


        public abstract Task<DbCommand> GetExistsTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction);
        public abstract Task<DbCommand> GetCreateTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction);
        public abstract Task<DbCommand> GetDropTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Gets a columns list from the datastore
        /// </summary>
        public abstract Task<IEnumerable<SyncColumn>> GetColumnsAsync(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Gets all relations from a current table. If composite, must be ordered
        /// </summary>
        public abstract Task<IEnumerable<DbRelationDefinition>> GetRelationsAsync(DbConnection connection, DbTransaction transaction);

        /// <summary>
        /// Get all primary keys. If composite, must be ordered
        /// </summary>
        public abstract Task<IEnumerable<SyncColumn>> GetPrimaryKeysAsync(DbConnection connection, DbTransaction transaction);
    }
}
