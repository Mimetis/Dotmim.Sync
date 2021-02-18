﻿using Dotmim.Sync.Builders;
using System.Text;

using System.Data.Common;
using System.Threading.Tasks;
using System.Collections.Generic;
using Dotmim.Sync.Manager;

namespace Dotmim.Sync.MySql
{

    /// <summary>
    /// The MySqlBuilder class is the MySql implementation of DbBuilder class.
    /// In charge of creating tracking table, stored proc, triggers and adapters.
    /// </summary>
    public class MyTableSqlBuilder : DbTableBuilder
    {

        MySqlObjectNames sqlObjectNames;
        private MySqlBuilderProcedure sqlBuilderProcedure;
        private MySqlBuilderTable sqlBuilderTable;
        private MySqlBuilderTrackingTable sqlBuilderTrackingTable;
        private MySqlBuilderTrigger sqlBuilderTrigger;

        public MyTableSqlBuilder(SyncTable tableDescription, ParserName tableName, ParserName trackingTableName, SyncSetup setup) : base(tableDescription, tableName, trackingTableName, setup)
        {

            this.sqlObjectNames = new MySqlObjectNames(tableDescription, this.TableName, this.TrackingTableName, setup);
            this.sqlBuilderProcedure = new MySqlBuilderProcedure(tableDescription, TableName, TrackingTableName, setup);
            this.sqlBuilderTable = new MySqlBuilderTable(tableDescription, TableName, TrackingTableName, setup);
            this.sqlBuilderTrackingTable = new MySqlBuilderTrackingTable(tableDescription, tableName, trackingTableName, Setup);
            this.sqlBuilderTrigger = new MySqlBuilderTrigger(tableDescription, tableName, trackingTableName, Setup);

        }

        public override IEnumerable<DbTriggerType> SupportedTriggers =>
            new[] {DbTriggerType.Insert, DbTriggerType.Update, DbTriggerType.Delete};

        public override Task<DbCommand> GetCreateSchemaCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTable.GetCreateSchemaCommandAsync(connection, transaction);
        public override Task<DbCommand> GetCreateTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTable.GetCreateTableCommandAsync(connection, transaction);
        public override Task<DbCommand> GetExistsTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTable.GetExistsTableCommandAsync(connection, transaction);
        public override Task<DbCommand> GetExistsSchemaCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTable.GetExistsSchemaCommandAsync(connection, transaction);
        public override Task<DbCommand> GetDropTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTable.GetDropTableCommandAsync(connection, transaction);

        public override Task<IEnumerable<SyncColumn>> GetColumnsAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTable.GetColumnsAsync(connection, transaction);
        public override Task<IEnumerable<DbRelationDefinition>> GetRelationsAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTable.GetRelationsAsync(connection, transaction);
        public override Task<IEnumerable<SyncColumn>> GetPrimaryKeysAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTable.GetPrimaryKeysAsync(connection, transaction);


        public override Task<DbCommand> GetExistsStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderProcedure.GetExistsStoredProcedureCommandAsync(storedProcedureType, filter, connection, transaction);
        public override Task<DbCommand> GetCreateStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderProcedure.GetCreateStoredProcedureCommandAsync(storedProcedureType, filter, connection, transaction);
        public override Task<DbCommand> GetDropStoredProcedureCommandAsync(DbStoredProcedureType storedProcedureType, SyncFilter filter, DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderProcedure.GetDropStoredProcedureCommandAsync(storedProcedureType, filter, connection, transaction);

        public override Task<DbCommand> GetCreateTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTrackingTable.GetCreateTrackingTableCommandAsync(connection, transaction);
        public override Task<DbCommand> GetDropTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTrackingTable.GetDropTrackingTableCommandAsync(connection, transaction);
        public override Task<DbCommand> GetRenameTrackingTableCommandAsync(ParserName oldTableName, DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTrackingTable.GetRenameTrackingTableCommandAsync(oldTableName, connection, transaction);
        public override Task<DbCommand> GetExistsTrackingTableCommandAsync(DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTrackingTable.GetExistsTrackingTableCommandAsync(connection, transaction);

        public override Task<DbCommand> GetExistsTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTrigger.GetExistsTriggerCommandAsync(triggerType, connection, transaction);
        public override Task<DbCommand> GetCreateTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTrigger.GetCreateTriggerCommandAsync(triggerType, connection, transaction);
        public override Task<DbCommand> GetDropTriggerCommandAsync(DbTriggerType triggerType, DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTrigger.GetDropTriggerCommandAsync(triggerType, connection, transaction);
        public override Task<DbCommand> GetExistsColumnCommandAsync(string columnName, DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTable.GetExistsColumnCommandAsync(columnName, connection, transaction);
        public override Task<DbCommand> GetAddColumnCommandAsync(string columnName, DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTable.GetAddColumnCommandAsync(columnName, connection, transaction);
        public override Task<DbCommand> GetDropColumnCommandAsync(string columnName, DbConnection connection, DbTransaction transaction)
            => this.sqlBuilderTable.GetDropColumnCommandAsync(columnName, connection, transaction);
    }
}
